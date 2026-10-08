"""Resolvers of real simulation inputs: sowing date and soil layers."""
from __future__ import annotations

from datetime import date

import pytest

from app.services.sim_inputs import (
    SimInputError,
    resolve_sowing_date,
    soil_layers_from_summary,
)

TODAY = date(2026, 10, 8)


def op(op_type="sowing", status="completed", **attrs):
    d = {"id": "urn:ngsi-ld:AgriParcelOperation:x", "type": "AgriParcelOperation",
         "operationType": {"type": "Property", "value": op_type},
         "status": {"type": "Property", "value": status}}
    for k, v in attrs.items():
        d[k] = v
    return d


def dt(v):
    return {"type": "Property", "value": {"@type": "DateTime", "@value": v}}


def prop(v):
    return {"type": "Property", "value": v}


def test_sowing_suggested_excluded():
    ops = [op(status="suggested", plannedStartAt=prop("2026-05-01T00:00:00Z"))]
    with pytest.raises(SimInputError, match="no sowing operation"):
        resolve_sowing_date(ops, TODAY)


def test_sowing_planned_date_wrapped_form():
    ops = [op(status="planned", plannedDate=dt("2026-04-03T08:00:00Z"))]
    assert resolve_sowing_date(ops, TODAY) == date(2026, 4, 3)


def test_sowing_started_at_plain_and_case_insensitive():
    ops = [op(op_type="Sowing", startedAt=prop("2026-03-02T10:00:00Z"))]
    assert resolve_sowing_date(ops, TODAY) == date(2026, 3, 2)


def test_sowing_planned_start_at_accepted_when_not_suggested():
    ops = [op(status="planned", plannedStartAt=prop("2026-02-01T00:00:00Z"))]
    assert resolve_sowing_date(ops, TODAY) == date(2026, 2, 1)


def test_sowing_future_ignored():
    ops = [op(plannedDate=dt("2026-12-01T00:00:00Z")), op(startedAt=prop("2026-03-01"))]
    assert resolve_sowing_date(ops, TODAY) == date(2026, 3, 1)


def test_sowing_most_recent_wins():
    ops = [op(startedAt=prop("2025-10-01")), op(startedAt=prop("2026-04-10")),
           op(startedAt=prop("2026-01-01"))]
    assert resolve_sowing_date(ops, TODAY) == date(2026, 4, 10)


def test_sowing_ignores_other_operation_types():
    with pytest.raises(SimInputError):
        resolve_sowing_date([op(op_type="tillage", startedAt=prop("2026-03-01"))], TODAY)


def test_sowing_none():
    with pytest.raises(SimInputError, match="no sowing operation"):
        resolve_sowing_date([], TODAY)


def test_sowing_started_at_wins_over_planned_in_same_op():
    ops = [op(startedAt=prop("2026-03-05"), plannedDate=dt("2026-03-01T00:00:00Z"))]
    assert resolve_sowing_date(ops, TODAY) == date(2026, 3, 5)


# Fixture shaped after nkz-module-soil GET /v1/soil/parcel/{id}/summary
# (reading.py parcel_summary + ingest.py _horizon_to_dict): top-level `horizons`
# plain list, camelCase keys, ksatSaturated in mm/h, fieldCapacity/wiltingPoint cm3/cm3.
def horizon(frm, to, fc=0.30, wp=0.15, ksat=10.0, **extra):
    h = {"depthFrom": frm, "depthTo": to, "sand": 40.0, "silt": 30.0, "clay": 30.0,
         "organicCarbon": 1.2, "ksatSaturated": ksat, "availableWaterCapacity": fc - wp,
         "fieldCapacity": fc, "wiltingPoint": wp, "usdaTextureClass": "clay loam"}
    h.update(extra)
    return h


def test_soil_mapping_ordering_and_ksat():
    summary = {"horizons": [horizon(30, 60, fc=0.28, wp=0.14, ksat=5.0), horizon(0, 30)],
               "dataSource": {"type": "Property", "value": "soilgrids"}}
    layers = soil_layers_from_summary(summary)
    assert [l["top_cm"] for l in layers] == [0, 30]
    first = layers[0]
    assert first["bottom_cm"] == 30
    assert first["thickness_m"] == pytest.approx(0.30)
    assert first["fc"] == 0.30 and first["wp"] == 0.15
    assert first["ksat_mm_day"] == pytest.approx(240.0)
    assert first["sat"] is None
    assert first["source"] == "soilgrids"
    assert layers[1]["ksat_mm_day"] == pytest.approx(120.0)


def test_soil_value_wrapped_horizons_and_saturation():
    summary = {"horizons": {"type": "Property", "value": [horizon(0, 20, saturation=0.46)]}}
    layers = soil_layers_from_summary(summary)
    assert layers[0]["sat"] == 0.46


def test_soil_missing_fc_names_horizon():
    bad = horizon(30, 60)
    del bad["fieldCapacity"]
    with pytest.raises(SimInputError, match="30-60"):
        soil_layers_from_summary({"horizons": [horizon(0, 30), bad]})


def test_soil_missing_ksat_is_error():
    bad = horizon(0, 30)
    del bad["ksatSaturated"]
    with pytest.raises(SimInputError, match="0-30"):
        soil_layers_from_summary({"horizons": [bad]})


@pytest.mark.parametrize("fc,wp", [(0.15, 0.30), (0.2, 0.2), (0.3, 0.0), (1.2, 0.1)])
def test_soil_invalid_ranges(fc, wp):
    with pytest.raises(SimInputError):
        soil_layers_from_summary({"horizons": [horizon(0, 30, fc=fc, wp=wp)]})


def test_soil_no_horizons():
    with pytest.raises(SimInputError, match="no horizons"):
        soil_layers_from_summary({"horizons": []})
    with pytest.raises(SimInputError):
        soil_layers_from_summary({})


def test_soil_bad_depths():
    with pytest.raises(SimInputError):
        soil_layers_from_summary({"horizons": [horizon(30, 30)]})
