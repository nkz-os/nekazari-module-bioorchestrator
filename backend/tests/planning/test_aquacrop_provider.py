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
    assert ests[0]["validation"]["metric"] == "RMSE 1.68 t/ha, anomaly r 0.50"


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
