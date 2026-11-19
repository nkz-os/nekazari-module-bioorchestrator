from datetime import date

import pytest

from app.planning.aquacrop_provider import aquacrop_gate, map_result
from app.planning.estimates import disagreement
from app.planning.models import Estimate, Unavailable

TODAY = date(2026, 10, 12)


@pytest.mark.parametrize("crop, regime, country, sowing, reason", [
    ("TRZAX", "rainfed", "ES", date(2026, 11, 15), None),
    ("TRZAX", "rainfed", "ES", TODAY, None),
    ("TRZAX", "rainfed", "ES", date(2027, 10, 13), None),  # today + 366
    ("HORVX", "rainfed", "ES", date(2026, 11, 15), "not_validated"),
    ("TRZAX", "irrigated", "ES", date(2026, 11, 15), "not_validated"),
    ("TRZAX", "rainfed", "PT", date(2026, 11, 15), "not_validated"),
    ("TRZAX", "rainfed", None, date(2026, 11, 15), "not_validated"),
    ("TRZAX", "rainfed", "ES", None, "needs_sowing_date"),
    ("TRZAX", "rainfed", "ES", date(2026, 10, 11), "sowing_date_out_of_range"),
    ("TRZAX", "rainfed", "ES", date(2027, 10, 14), "sowing_date_out_of_range"),
    # not validated wins over a missing date: an unvalidated crop never asks for more input
    ("HORVX", "rainfed", "ES", None, "not_validated"),
])
def test_gate_truth_table(crop, regime, country, sowing, reason):
    assert aquacrop_gate(crop, regime, country, sowing, TODAY) == reason


def _result(**over):
    r = {"status": "preseason",
         "yield_basis": {"moisture": "commercial", "grain_moisture_pct": 13.0, "source": "x"},
         "yield_t_ha": {"p10": 2.1, "p50": 3.456, "p90": 4.9},
         "ensemble": {"n_years": 14, "method": "climatological_ensemble", "years": []},
         "expected_yield": {"status": "ok", "basis": "gap_corrected", "range_t_ha": {"low": 2.4, "high": 5.123},
                            "coverage": 0.78, "gap_factor": 0.7,
                            "calibration": {"version": "c1", "source": "mapa",
                                            "validation": {"rmse_kg_ha": 1680, "anomaly_r_median": 0.5}}}}
    r.update(over)
    return r


def test_map_result_ok_gives_expected_range_then_ceiling():
    ests, unav = map_result(_result(), "aq-1")
    assert unav == []
    exp, ceil = ests
    for e in ests:
        Estimate.model_validate(e)
    assert exp["basis"] == "gap_corrected" and exp["percentiles"] is None
    assert exp["range"] == {"low": 2.4, "high": 5.12} and exp["coverage"] == 0.78
    assert exp["n"] == 14 and exp["n_kind"] == "years" and exp["provider_version"] == "aq-1"
    assert exp["validation"]["status"] == "validated"
    assert exp["validation"]["metric"] == "RMSE 1.68 t/ha, anomaly r 0.50"
    assert "MAPA" in exp["validation"]["scope"]
    assert exp["moisture"] == {"basis": "commercial", "pct": 13.0}
    assert ceil["basis"] == "water_limited" and ceil["percentiles"] == {"p10": 2.1, "p50": 3.46, "p90": 4.9}
    assert ceil["range"] is None and ceil["validation"]["status"] == "not_applicable"
    assert ceil["moisture"] == {"basis": "commercial", "pct": 13.0}


def test_static_metric_when_the_result_carries_no_validation():
    r = _result()
    r["expected_yield"]["calibration"] = {}
    ests, _ = map_result(r, "v")
    assert ests[0]["validation"]["metric"] == "RMSE 1.65 t/ha, anomaly r 0.48"


def test_expected_unavailable_keeps_the_ceiling_and_says_why():
    ests, unav = map_result(_result(expected_yield={"status": "unavailable", "reason": "outside_calibrated_regions"}), "v")
    assert [e["basis"] for e in ests] == ["water_limited"]
    assert unav == [{"provider_id": "aquacrop", "reason": "expected_outside_calibrated_regions"}]
    Unavailable.model_validate(unav[0])


def test_dry_matter_moisture():
    ests, _ = map_result(_result(yield_basis={"moisture": "dry_matter", "grain_moisture_pct": None}), "v")
    assert all(e["moisture"] == {"basis": "dry_matter", "pct": None} for e in ests)


def test_missing_pieces_never_read_as_good():
    ests, unav = map_result({"status": "preseason"}, "v")
    assert ests == []
    assert {u["reason"] for u in unav} == {"expected_missing", "water_limited_missing"}
    ests, unav = map_result(_result(yield_t_ha=3.2), "v")  # a scalar is not the ensemble shape
    assert [e["basis"] for e in ests] == ["gap_corrected"] and unav[0]["reason"] == "water_limited_missing"


def _e(basis, p50, moisture):
    return {"basis": basis, "percentiles": {"p10": p50, "p50": p50, "p90": p50}, "moisture": moisture}


C13 = {"basis": "commercial", "pct": 13.0}
DM = {"basis": "dry_matter", "pct": None}


def test_disagreement_needs_same_basis_and_same_moisture():
    assert disagreement([_e("water_limited", 8.0, C13), _e("water_limited", 3.0, DM)]) is False
    assert disagreement([_e("water_limited", 8.0, C13), _e("water_limited", 3.0, {"basis": "commercial", "pct": 14.0})]) is False
    assert disagreement([_e("water_limited", 8.0, C13), _e("water_limited", 3.0, C13)]) is True


def test_range_only_estimates_do_not_take_part_in_disagreement():
    rng = {"basis": "gap_corrected", "percentiles": None, "moisture": C13}
    assert disagreement([rng, rng, _e("gap_corrected", 5.0, C13)]) is False


from app.planning.aquacrop_provider import (
    IN_SEASON_METRIC,
    IN_SEASON_SCOPE,
    map_in_season,
)

IN_SEASON = {"status": "in_season", "yield_basis": {"moisture": "commercial", "grain_moisture_pct": 13.0},
             "yield_t_ha": {"p10": 3.0, "p50": 4.0, "p90": 5.0}, "ensemble": {"n_years": 14},
             "expected_yield": {"status": "ok", "range_t_ha": {"low": 2.4, "high": 3.9}, "coverage": 0.8}}


def test_in_season_expected_yield_is_experimental():
    est, unav = map_in_season(IN_SEASON, "v")
    gap = next(e for e in est if e["basis"] == "gap_corrected")
    assert gap["validation"] == {"status": "experimental", "metric": IN_SEASON_METRIC, "scope": IN_SEASON_SCOPE}
    assert "0.14 (1 Apr)" in IN_SEASON_METRIC and "before May" in IN_SEASON_SCOPE
    assert gap["n"] == 14 and unav == []
    ceiling = next(e for e in est if e["basis"] == "water_limited")
    assert ceiling["validation"] == {"status": "not_applicable"}


def test_a_complete_season_is_validated_with_its_single_ceiling():
    done = {**IN_SEASON, "status": "complete", "yield_t_ha": 4.2, "ensemble": None}
    est, unav = map_in_season(done, "v")
    gap = next(e for e in est if e["basis"] == "gap_corrected")
    assert gap["validation"]["status"] == "validated" and gap["n"] == 1
    ceiling = next(e for e in est if e["basis"] == "water_limited")
    assert ceiling["percentiles"] == {"p10": 4.2, "p50": 4.2, "p90": 4.2} and unav == []


def test_in_season_without_expected_yield_says_why():
    est, unav = map_in_season({**IN_SEASON, "expected_yield": {"status": "unavailable",
                                                               "reason": "region_not_calibrated"}}, "v")
    assert [e["basis"] for e in est] == ["water_limited"]
    assert unav == [{"provider_id": "aquacrop", "reason": "expected_region_not_calibrated"}]


VALIDATED_RUNNING = (
    [{"provider_id": "aquacrop", "provider_version": "v-test", "unit": "t/ha", "n": 14, "n_kind": "years",
      "moisture": {"basis": "commercial", "pct": 13.0}, "warnings": [], "attributions": [], "graph_id": None,
      "basis": "gap_corrected", "percentiles": None, "range": {"low": 2.4, "high": 3.9}, "coverage": 0.8,
      "validation": {"status": "experimental",
                     "metric": ("year-to-year r by issue date: 0.14 (1 Apr), 0.34 (1 May), 0.45 (1 Jun), "
                                "0.49 (complete season); RMSE 1.6 t/ha at every date"),
                     "scope": ("in-season forecast; before May it reflects the normal of the area, not this "
                               "season (provincial statistics, rainfed soft wheat, ES 2008-2022)")}},
     {"provider_id": "aquacrop", "provider_version": "v-test", "unit": "t/ha", "n": 14, "n_kind": "years",
      "moisture": {"basis": "commercial", "pct": 13.0}, "warnings": [], "attributions": [], "graph_id": None,
      "basis": "water_limited", "range": None, "coverage": None,
      "percentiles": {"p10": 3.0, "p50": 4.0, "p90": 5.0}, "validation": {"status": "not_applicable"}}], [])


def test_validated_running_season_output_is_unchanged():
    assert map_in_season(IN_SEASON, "v-test") == VALIDATED_RUNNING


def test_validated_complete_season_output_is_unchanged():
    done = {**IN_SEASON, "status": "complete", "yield_t_ha": 4.2, "ensemble": None}
    gap, ceiling = (dict(e) for e in VALIDATED_RUNNING[0])
    gap.update(n=1, validation={"status": "validated", "metric": "RMSE 1.65 t/ha, anomaly r 0.48",
                                "scope": "ES, 44 provinces, MAPA 2008–2022"})
    ceiling.update(n=1, percentiles={"p10": 4.2, "p50": 4.2, "p90": 4.2})
    assert map_in_season(done, "v-test") == ([gap, ceiling], [])


from app.planning.aquacrop_provider import (
    SCOPE_BARLEY,
    SCOPE_DURUM,
    SCOPE_DURUM_IRRIGATED,
    SCOPE_IRRIGATED,
    UNVALIDATED_IN_SEASON,
    VALIDATED,
)
from app.planning.estimates import estimate_providers

OPENED = [("HORVX", "rainfed", "ES"), ("TRZDU", "rainfed", "ES"), ("TRZAX", "irrigated", "ES"),
          ("TRZDU", "irrigated", "ES"), ("HORVX", "irrigated", "ES")]
WHEAT_FILE_EXPECTED = {"status": "ok", "range_t_ha": {"low": 2.4, "high": 3.9}, "coverage": 0.8, "gap_factor": 0.7,
                       "calibration": {"validation": {"rmse_kg_ha": 1650, "anomaly_r_median": 0.48}}}


def test_allow_list_is_exactly_the_approved_set_and_disjoint_from_validated():
    assert set(UNVALIDATED_IN_SEASON) == set(OPENED)
    assert not set(UNVALIDATED_IN_SEASON) & set(VALIDATED)
    assert {k[2] for k in UNVALIDATED_IN_SEASON} == {"ES"}


def test_scope_texts():
    s = UNVALIDATED_IN_SEASON
    assert s[("TRZDU", "rainfed", "ES")] == SCOPE_DURUM and "soft-wheat parameters" in SCOPE_DURUM
    assert "not an expected harvest" in SCOPE_DURUM and "not an expected harvest" in SCOPE_BARLEY
    assert s[("HORVX", "rainfed", "ES")] == SCOPE_BARLEY and "not validated" in SCOPE_BARLEY
    assert s[("TRZAX", "irrigated", "ES")] == s[("HORVX", "irrigated", "ES")] == SCOPE_IRRIGATED
    assert "full irrigation" in SCOPE_IRRIGATED and "no regional calibration" in SCOPE_IRRIGATED
    assert s[("TRZDU", "irrigated", "ES")] == SCOPE_DURUM_IRRIGATED and "soft-wheat" in SCOPE_DURUM_IRRIGATED


@pytest.mark.parametrize("key", OPENED)
@pytest.mark.parametrize("expected", [WHEAT_FILE_EXPECTED,
                                      {"status": "unavailable", "reason": "irrigated"},
                                      {"status": "unavailable", "reason": "crop_not_calibrated"}])
def test_opened_combination_returns_only_an_unvalidated_ceiling(key, expected):
    scope = UNVALIDATED_IN_SEASON[key]
    est, unav = map_in_season({**IN_SEASON, "expected_yield": expected}, "v", unvalidated_scope=scope)
    assert [e["basis"] for e in est] == ["water_limited"]
    e = est[0]
    Estimate.model_validate(e)
    assert e["validation"] == {"status": "unvalidated", "metric": None, "scope": scope}
    assert e["range"] is None and e["coverage"] is None
    assert e["percentiles"] == {"p10": 3.0, "p50": 4.0, "p90": 5.0} and e["n"] == 14
    assert e["attributions"] == []
    assert len(unav) == 1 and unav[0]["provider_id"] == "aquacrop"
    assert unav[0]["reason"] in {"expected_crop_not_calibrated", "expected_irrigated"}


def test_durum_never_carries_soft_wheats_metric_factor_or_range():
    # the engine answers ok for durum (shared wheat file) with soft wheat's range, gap factor and metric
    est, unav = map_in_season({**IN_SEASON, "expected_yield": WHEAT_FILE_EXPECTED}, "v",
                              unvalidated_scope=UNVALIDATED_IN_SEASON[("TRZDU", "rainfed", "ES")])
    dumped = repr((est, unav))
    for leaked in ("RMSE", "1.65", "MAPA", "gap_factor", "'low'", "'high'", "gap_corrected"):
        assert leaked not in dumped
    assert unav == [{"provider_id": "aquacrop", "reason": "expected_crop_not_calibrated"}]


def test_engine_reason_is_kept_and_not_duplicated():
    _, unav = map_in_season({**IN_SEASON, "expected_yield": {"status": "unavailable", "reason": "irrigated"}},
                            "v", unvalidated_scope=SCOPE_IRRIGATED)
    assert unav == [{"provider_id": "aquacrop", "reason": "expected_irrigated"}]


def test_opened_complete_season_stays_unvalidated_with_its_single_ceiling():
    done = {**IN_SEASON, "status": "complete", "yield_t_ha": 4.2, "ensemble": None}
    est, _ = map_in_season(done, "v", unvalidated_scope=SCOPE_BARLEY)
    assert len(est) == 1 and est[0]["n"] == 1
    assert est[0]["percentiles"] == {"p10": 4.2, "p50": 4.2, "p90": 4.2}
    assert est[0]["validation"]["status"] == "unvalidated"


def test_opened_combination_without_a_numeric_yield_returns_no_estimate():
    est, unav = map_in_season({**IN_SEASON, "yield_t_ha": None}, "v", unvalidated_scope=SCOPE_DURUM)
    assert est == []
    assert unav == [{"provider_id": "aquacrop", "reason": "water_limited_missing"},
                    {"provider_id": "aquacrop", "reason": "expected_crop_not_calibrated"}]


@pytest.mark.parametrize("crop, regime", [("HORVX", "rainfed"), ("TRZDU", "rainfed"), ("TRZDU", "irrigated"),
                                          ("HORVX", "irrigated"), ("TRZAX", "irrigated")])
def test_planning_gate_and_providers_do_not_open(crop, regime):
    assert aquacrop_gate(crop, regime, "ES", date(2026, 11, 15), TODAY) == "not_validated"
    assert "aquacrop" not in estimate_providers(crop, has_trials=False, regime=regime, country="ES")
