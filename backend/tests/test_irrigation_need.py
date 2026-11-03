import math
from array import array

import pytest

from app.planning import irrigation as irr
from app.planning.climatology import DAYS, Climatology

NAN = float("nan")
WHEAT_KC = {"initial": 0.3, "development": 0.7, "mid-season": 1.15, "late-season": 0.4}
WHEAT_LEN = {"initial": (15, 45), "development": (45, 105), "mid-season": (105, 180), "late-season": (180, 230)}
PE = irr.load_pe_method()


def year(et0=5.0, rain=0.0, years=range(2010, 2020)):
    return {y: (array("f", [et0] * DAYS), array("f", [rain] * DAYS)) for y in years}


def rows_of(kc=WHEAT_KC, lengths=WHEAT_LEN):
    return [{"species": "wheat", "stage": s, "kc": kc[s], "d1": lengths[s][0], "d2": lengths[s][1], "source": "FAO-56"}
            for s in irr.STAGES]


def test_kc_curve_wheat_follows_fao56_eq66():
    kc = irr.kc_curve(WHEAT_KC, WHEAT_LEN)
    assert len(kc) == 230
    assert kc[0] == pytest.approx(0.3) and kc[44] == pytest.approx(0.3)  # day 45: end of initial
    assert kc[75 - 1] == pytest.approx(0.3 + (75 - 45) / 60 * (1.15 - 0.3))  # linear through development
    assert kc[104] == pytest.approx(1.15) and kc[179] == pytest.approx(1.15)  # mid-season constant
    assert kc[229] == pytest.approx(0.4)  # linear to Kc_end at harvest
    assert kc[204 - 1] == pytest.approx(1.15 + (204 - 180) / 50 * (0.4 - 1.15))
    assert all(0.3 - 1e-9 <= k <= 1.15 + 1e-9 for k in kc)


def test_kc_before_initial_d1_is_kc_ini():
    kc = irr.kc_curve(WHEAT_KC, WHEAT_LEN)
    assert kc[0] == kc[14] == pytest.approx(0.3)


def test_dry_season_need_is_sum_of_kc_et0():
    kc = irr.kc_curve(WHEAT_KC, WHEAT_LEN)
    res = irr.irrigation_need(kc, 100, Climatology(year(5.0, 0.0)), PE)
    assert res.p50 == pytest.approx(sum(k * 5.0 for k in kc), rel=1e-4)
    assert res.p10 == pytest.approx(res.p50, rel=1e-4) == pytest.approx(res.p90, rel=1e-4)
    assert res.n_years == 10 and (res.first_year, res.last_year) == (2010, 2019)


def test_rain_covering_everything_gives_zero():
    kc = irr.kc_curve(WHEAT_KC, WHEAT_LEN)
    res = irr.irrigation_need(kc, 100, Climatology(year(5.0, 30.0)), PE)
    assert res.p10 == res.p50 == res.p90 == 0.0


def test_percentiles_are_ordered_between_years():
    kc = irr.kc_curve(WHEAT_KC, WHEAT_LEN)
    series = {y: year(3.0 + i * 0.5, 0.0, [y])[y] for i, y in enumerate(range(2010, 2020))}
    res = irr.irrigation_need(kc, 100, Climatology(series), PE)
    assert res.p10 < res.p50 < res.p90


def test_season_crossing_new_year_uses_both_years_and_harvest_year():
    kc = irr.kc_curve(WHEAT_KC, WHEAT_LEN)
    # Sown on day 290: harvest in the next calendar year; the first climatology year has no sowing year.
    res = irr.irrigation_need(kc, 290, Climatology(year(5.0, 0.0, range(2010, 2020))), PE)
    assert res.n_years == 9 and res.first_year == 2011
    assert res.p50 == pytest.approx(sum(k * 5.0 for k in kc), rel=1e-4)


def test_season_year_with_a_hole_in_the_sowing_year_is_dropped():
    kc = irr.kc_curve(WHEAT_KC, WHEAT_LEN)
    series = year(5.0, 0.0, range(2010, 2020))
    for i in range(300, 330):  # > 5 % of the season missing in 2012 (sowing year of harvest year 2013)
        series[2012][0][i] = NAN
    res = irr.irrigation_need(kc, 290, Climatology(series), PE)
    assert res.n_years == 8  # harvest years 2011-2019 minus 2013


def test_gap_filled_year_equals_the_complete_year_when_missing_days_resemble_the_rest():
    kc = irr.kc_curve(WHEAT_KC, WHEAT_LEN)
    complete = irr.irrigation_need(kc, 100, Climatology(year(5.0, 0.0)), PE).p50
    series = year(5.0, 0.0)
    for i in range(100, 105):  # 5 of 230 days, inside the constant-Kc part
        series[2015][0][i] = NAN
        series[2015][1][i] = NAN
    res = irr.irrigation_need(kc, 100, Climatology(series), PE)
    assert res.n_years == 10
    assert res.p10 == pytest.approx(complete, rel=0.01)  # skipped, scaled to the block's expected days: no bias


def test_gap_in_the_short_last_block_is_scaled_to_its_expected_days():
    kc = irr.kc_curve(WHEAT_KC, WHEAT_LEN)  # 230 days: 23 full blocks, no short one; use a 235-day curve
    kc = kc + [0.4] * 5
    full = irr.irrigation_need(kc, 100, Climatology(year(5.0, 0.0)), PE).p50
    series = year(5.0, 0.0)
    series[2015][0][100 + 233] = NAN
    series[2015][1][100 + 233] = NAN
    res = irr.irrigation_need(kc, 100, Climatology(series), PE)
    assert res.p10 == pytest.approx(full, rel=0.01)


def test_result_is_memoized_on_the_climatology():
    kc = irr.kc_curve(WHEAT_KC, WHEAT_LEN)
    c = Climatology(year(5.0, 0.0))
    first = irr.irrigation_need(kc, 100, c, PE)
    assert irr.irrigation_need(kc, 100, c, PE) is first


def test_non_positive_kc_is_no_kc():
    bad = rows_of(kc={**WHEAT_KC, "late-season": 0.0})
    assert irr.stage_set(bad) == (None, "no_kc")
    assert irr.stage_set(rows_of(kc={**WHEAT_KC, "initial": -0.3})) == (None, "no_kc")


def test_fewer_than_five_valid_years_is_none():
    kc = irr.kc_curve(WHEAT_KC, WHEAT_LEN)
    assert irr.irrigation_need(kc, 100, Climatology(year(5.0, 0.0, range(2015, 2019))), PE) is None
    assert irr.irrigation_need(kc, 100, Climatology(year(5.0, 0.0, range(2015, 2020))), PE).n_years == 5


def test_effective_rain_scs_decadal():
    assert PE.pe(0) == 0
    assert PE.pe(10) == pytest.approx(10 * (125 - 6) / 125)
    b = PE.break_mm
    assert PE.pe(b) == pytest.approx(50.0)
    assert PE.pe(b + 1e-9) == pytest.approx(50.0, abs=1e-6)  # continuous at the break
    assert PE.pe(100) == pytest.approx(125 / 3 + 10)
    assert "USDA" in PE.citation and "CROPWAT" in PE.citation


def test_stage_set_valid_and_generic_only():
    stages, why = irr.stage_set(rows_of())
    assert why is None and stages.kc == WHEAT_KC and stages.kc_source == "FAO-56"


def test_stage_set_second_parameter_makes_stage_ambiguous():
    rows = rows_of() + [{"species": "wheat", "stage": "initial", "kc": 0.5, "d1": 1.3, "d2": 5.5, "source": "x"}]
    assert irr.stage_set(rows) == (None, "no_kc")


def test_stage_set_missing_kc_and_bad_lengths():
    assert irr.stage_set([r for r in rows_of() if r["stage"] != "late-season"]) == (None, "no_kc")
    bad = rows_of()
    bad[1]["d1"] = 50  # gap after initial
    assert irr.stage_set(bad) == (None, "no_stage_lengths")
    bad = rows_of()
    bad[2]["d2"] = None
    assert irr.stage_set(bad) == (None, "no_stage_lengths")


def test_allotment_reading(caplog):
    ok = {"type": "Property", "value": 300, "unitCode": "MMT"}
    assert irr.read_allotment(ok) == 300.0
    irr.read_allotment({"value": 1, "unitCode": "MTQ"}, parcel_id="p9", tenant_id="tenant-a")
    assert "tenant=tenant-a parcel=p9" in caplog.text
    assert irr.read_allotment(None) is None
    for bad in ({"value": 300, "unitCode": "MTQ"}, {"value": 300}, {"value": -1, "unitCode": "MMT"},
                {"value": "x", "unitCode": "MMT"}, {"value": True, "unitCode": "MMT"}, "300"):
        assert irr.read_allotment(bad, parcel_id="p") is None
    assert "ignored" in caplog.text
    assert irr.read_allotment({"value": 0, "unitCode": "MMT"}) == 0.0
    assert not math.isnan(irr.read_allotment(ok))

