import math
from array import array

import pytest

from app.planning import irrigation as irr
from app.planning.climatology import DAYS, Climatology

NAN = float("nan")
PE = irr.load_pe_method()
# FAO-56 Table 11 "Winter Wheat" (November, Mediterranean) 30/140/40/30; Table 12 "Winter Wheat - with
# non-frozen soils" 0.7 / 1.15 / 0.25: read from the cited YAML, never from the graph.
WHEAT = irr.load_stage_table()["wheat"]


def wheat_kc():
    return irr.kc_curve(WHEAT)


def year(et0=5.0, rain=0.0, years=range(2010, 2020)):
    return {y: (array("f", [et0] * DAYS), array("f", [rain] * DAYS)) for y in years}


def test_wheat_stages_come_from_the_fao56_yaml():
    assert WHEAT.lengths == (30, 140, 40, 30)
    assert (WHEAT.kc_ini, WHEAT.kc_mid, WHEAT.kc_end) == (0.7, 1.15, 0.25)
    assert WHEAT.bounds() == {"initial": (0, 30), "development": (30, 170), "mid-season": (170, 210),
                              "late-season": (210, 240)}
    assert WHEAT.kc_source == ("FAO-56 Table 11 'Winter Wheat' (November, Mediterranean); "
                               "Table 12 'Winter Wheat - with non-frozen soils'")


def test_kc_curve_wheat_follows_fao56_eq66():
    kc = wheat_kc()
    assert len(kc) == 240
    assert kc[0] == pytest.approx(0.7) and kc[29] == pytest.approx(0.7)  # initial starts on sowing day, ends day 30
    assert kc[100 - 1] == pytest.approx(0.7 + (100 - 30) / 140 * (1.15 - 0.7))  # linear through development
    assert kc[170 - 1] == pytest.approx(1.15) and kc[209] == pytest.approx(1.15)  # mid-season constant
    assert kc[225 - 1] == pytest.approx(1.15 + (225 - 210) / 30 * (0.25 - 1.15))
    assert kc[239] == pytest.approx(0.25)  # linear to Kc_end at harvest
    assert all(0.25 - 1e-9 <= k <= 1.15 + 1e-9 for k in kc)


def test_dry_season_need_is_sum_of_kc_et0():
    kc = wheat_kc()
    res = irr.irrigation_need(kc, 100, Climatology(year(5.0, 0.0)), PE)
    assert res.p50 == pytest.approx(sum(k * 5.0 for k in kc), rel=1e-4)
    assert res.p10 == pytest.approx(res.p50, rel=1e-4) == pytest.approx(res.p90, rel=1e-4)
    assert res.n_years == 10 and (res.first_year, res.last_year) == (2010, 2019)


def test_rain_covering_everything_gives_zero():
    kc = wheat_kc()
    res = irr.irrigation_need(kc, 100, Climatology(year(5.0, 30.0)), PE)
    assert res.p10 == res.p50 == res.p90 == 0.0


def test_percentiles_are_ordered_between_years():
    kc = wheat_kc()
    series = {y: year(3.0 + i * 0.5, 0.0, [y])[y] for i, y in enumerate(range(2010, 2020))}
    res = irr.irrigation_need(kc, 100, Climatology(series), PE)
    assert res.p10 < res.p50 < res.p90


def test_season_crossing_new_year_uses_both_years_and_harvest_year():
    kc = wheat_kc()
    # Sown on day 290: harvest in the next calendar year; the first climatology year has no sowing year.
    res = irr.irrigation_need(kc, 290, Climatology(year(5.0, 0.0, range(2010, 2020))), PE)
    assert res.n_years == 9 and res.first_year == 2011
    assert res.p50 == pytest.approx(sum(k * 5.0 for k in kc), rel=1e-4)


def test_season_year_with_a_hole_in_the_sowing_year_is_dropped():
    kc = wheat_kc()
    series = year(5.0, 0.0, range(2010, 2020))
    for i in range(300, 330):  # > 5 % of the season missing in 2012 (sowing year of harvest year 2013)
        series[2012][0][i] = NAN
    res = irr.irrigation_need(kc, 290, Climatology(series), PE)
    assert res.n_years == 8  # harvest years 2011-2019 minus 2013


def test_gap_filled_year_equals_the_complete_year_when_missing_days_resemble_the_rest():
    kc = wheat_kc()
    complete = irr.irrigation_need(kc, 100, Climatology(year(5.0, 0.0)), PE).p50
    series = year(5.0, 0.0)
    for i in range(100, 105):  # 5 of 240 days, inside the constant-Kc part
        series[2015][0][i] = NAN
        series[2015][1][i] = NAN
    res = irr.irrigation_need(kc, 100, Climatology(series), PE)
    assert res.n_years == 10
    assert res.p10 == pytest.approx(complete, rel=0.01)  # skipped, scaled to the block's expected days: no bias


def test_gap_in_the_short_last_block_is_scaled_to_its_expected_days():
    kc = wheat_kc()  # 240 days: 24 full blocks, no short one; use a 245-day curve
    kc = kc + [0.25] * 5
    full = irr.irrigation_need(kc, 100, Climatology(year(5.0, 0.0)), PE).p50
    series = year(5.0, 0.0)
    series[2015][0][100 + 243] = NAN
    series[2015][1][100 + 243] = NAN
    res = irr.irrigation_need(kc, 100, Climatology(series), PE)
    assert res.p10 == pytest.approx(full, rel=0.01)


def test_result_is_memoized_on_the_climatology():
    kc = wheat_kc()
    c = Climatology(year(5.0, 0.0))
    first = irr.irrigation_need(kc, 100, c, PE)
    assert irr.irrigation_need(kc, 100, c, PE) is first




def test_fewer_than_five_valid_years_is_none():
    kc = wheat_kc()
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



# ── FAO-56 stage table (data/fao56_stage_lengths.yaml) ───────────────────────

FAO56_URL = "https://www.fao.org/4/x0490e/x0490e0b.htm"
# Values read from FAO-56 Tables 11 and 12 (see the YAML for rows and notes): lengths, (Kc ini, mid, end).
FAO56_EXPECTED = {
    "TRZAX": ((30, 140, 40, 30), (0.7, 1.15, 0.25)),
    "TRZDU": ((30, 140, 40, 30), (0.7, 1.15, 0.25)),
    "HORVX": ((40, 60, 60, 40), (0.3, 1.15, 0.25)),
    "ZEAMX": ((30, 40, 50, 30), (0.3, 1.20, 0.35)),
    "HELAN": ((25, 35, 45, 25), (0.35, 1.15, 0.35)),
    "PIBSX": ((20, 30, 35, 15), (0.4, 1.15, 0.30)),
    "VICFX": ((20, 30, 35, 15), (0.5, 1.15, 0.30)),
    "SOLTU": ((30, 35, 50, 30), (0.5, 1.15, 0.75)),
    "BEAVX": ((25, 35, 50, 50), (0.35, 1.20, 0.70)),
    "LYPES": ((30, 40, 45, 30), (0.6, 1.15, 0.70)),
}


def _raw_table():
    import os
    from pathlib import Path

    import yaml
    return yaml.safe_load((Path(os.getenv("DATA_DIR", "./data")) / "fao56_stage_lengths.yaml").read_text(encoding="utf-8"))


def test_fao56_yaml_schema_every_crop_cited():
    from pathlib import Path

    import yaml

    from app.species_registry import resolve_species
    crops = _raw_table()["crops"]
    eurostat = set(yaml.safe_load(Path("data/eurostat/crop_codes.yaml").read_text(encoding="utf-8"))["crops"].values())
    assert set(crops) <= eurostat | {"MEDSA", "LYPES"}  # only the main irrigated crops (spec 13.3)
    for eppo, c in crops.items():
        assert resolve_species(eppo), eppo
        lengths = [c["lengths_days"][k] for k in irr.YAML_STAGES]
        assert len(lengths) == 4 and all(isinstance(v, int) and v > 0 for v in lengths), eppo
        assert sum(lengths) <= irr.MAX_SEASON_DAYS, eppo
        assert all(isinstance(c["kc"][k], (int, float)) and c["kc"][k] > 0 for k in ("ini", "mid", "end")), eppo
        t11, t12 = c["source"]["stage_lengths"], c["source"]["kc"]
        assert t11["table"] == 11 and t12["table"] == 12, eppo
        for part in (t11, t12):
            assert part["row"].strip() and part["url"] == FAO56_URL, eppo
        assert isinstance(t11["region"], str) and t11["plant_date"].strip(), eppo
        assert t11["rule"] in irr.ROW_RULES, eppo
        if t11["rule"] == "mediterranean_or_spain":
            assert any(w in t11["region"] for w in ("Mediterranean", "Medit.", "Spain")), eppo
        assert t12["ini_row"].strip(), eppo


def test_fao56_yaml_values_and_coverage():
    crops = _raw_table()["crops"]
    assert set(crops) == set(FAO56_EXPECTED)  # BRSNN and MEDSA: no usable FAO-56 Table 11 season
    for eppo, (lengths, kc) in FAO56_EXPECTED.items():
        assert tuple(crops[eppo]["lengths_days"][k] for k in irr.YAML_STAGES) == lengths, eppo
        assert tuple(crops[eppo]["kc"][k] for k in ("ini", "mid", "end")) == kc, eppo
    table = irr.load_stage_table()
    assert len(table) == len(FAO56_EXPECTED) and "maize" in table  # ZEAMX resolves through the registry alias
    assert table["barley"].kc_source.startswith("FAO-56 Table 11 'Barley/Oats/Wheat' (Nov, no region printed)")


def test_fao56_yaml_keeps_the_copyright_and_licence_note():
    from pathlib import Path
    head = Path("data/fao56_stage_lengths.yaml").read_text(encoding="utf-8")[:1500]
    assert "(c) FAO 1998" in head and "licence request" in head


def test_invalid_table_entry_fails_the_load(tmp_path, monkeypatch):
    import yaml
    raw = _raw_table()
    raw["crops"]["TRZAX"]["lengths_days"]["mid_season"] = 0
    (tmp_path / "fao56_stage_lengths.yaml").write_text(yaml.safe_dump(raw), encoding="utf-8")
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    irr.load_stage_table.cache_clear()
    try:
        with pytest.raises(ValueError, match="TRZAX"):
            irr.load_stage_table()
        del raw["crops"]["TRZAX"]["source"]["kc"]["url"]
        raw["crops"]["TRZAX"]["lengths_days"]["mid_season"] = 40
        (tmp_path / "fao56_stage_lengths.yaml").write_text(yaml.safe_dump(raw), encoding="utf-8")
        irr.load_stage_table.cache_clear()
        with pytest.raises(ValueError, match="TRZAX"):
            irr.load_stage_table()
    finally:
        irr.load_stage_table.cache_clear()


def test_crop_absent_from_the_table_is_no_stage_lengths():
    from app.planning.service import IrrigationContext, species_irrigation_need
    ctx = IrrigationContext("irrigated", None, None, irr.load_stage_table())
    for slug, eppo in (("rapeseed", "BRSNN"), ("alfalfa", "MEDSA"), ("onion", "ALLCE")):
        assert species_irrigation_need(ctx, slug, eppo, {}) == (None, "no_stage_lengths")
    # In the table, the next missing input is reported instead (here: no parcel location).
    assert species_irrigation_need(ctx, "wheat", "TRZAX", {}) == (None, "weather_unavailable")
    assert species_irrigation_need(IrrigationContext("irrigated", None, None, None), "wheat", "TRZAX", {}) == (
        None, "kc_unavailable")


def test_irrigation_never_reads_graph_phenology_params():
    """Guard: Kc and stage lengths come from the cited FAO-56 YAML only, never PhenologyParams d1/d2/kc."""
    import inspect
    import re

    from app.planning import service
    sources = [inspect.getsource(irr), inspect.getsource(service.species_irrigation_need),
               inspect.getsource(service.IrrigationContext), inspect.getsource(service.parcel_fit)]
    for src in sources:
        assert "PhenologyParams" not in src and "HAS_STAGE" not in src and "PhenologyStage" not in src
    code = inspect.getsource(irr)
    assert not re.search(r"\bd[12]\b", code)
    assert not hasattr(irr, "load_stage_rows") and not hasattr(service, "load_stage_rows")
