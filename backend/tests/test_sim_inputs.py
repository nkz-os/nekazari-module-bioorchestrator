"""Resolvers of real simulation inputs: sowing date and soil layers."""
from __future__ import annotations

from datetime import date

import pytest

from app.services.sim_inputs import (
    SOWING_METHODS,
    SimInputError,
    resolve_sowing_date,
    soil_layers_from_summary,
    sowing_method_from_operations,
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
    with pytest.raises(SimInputError, match="no completed sowing"):
        resolve_sowing_date(ops, TODAY)


def test_sowing_planned_operation_is_not_a_sowing_date():
    # what happened counts, never a plan (the platform's crop-cycle rule)
    ops = [op(status="planned", plannedDate=dt("2026-04-03T08:00:00Z"))]
    with pytest.raises(SimInputError, match="no completed sowing"):
        resolve_sowing_date(ops, TODAY)


def test_sowing_completed_wrapped_form():
    ops = [op(endedAt=dt("2026-04-03T08:00:00Z"))]
    assert resolve_sowing_date(ops, TODAY) == date(2026, 4, 3)


def test_sowing_started_at_plain_and_case_insensitive():
    ops = [op(op_type="Sowing", startedAt=prop("2026-03-02T10:00:00Z"))]
    assert resolve_sowing_date(ops, TODAY) == date(2026, 3, 2)


def test_sowing_started_but_not_completed_is_not_a_sowing_date():
    ops = [op(status="incomplete", startedAt=prop("2026-02-01T00:00:00Z"))]
    with pytest.raises(SimInputError, match="no completed sowing"):
        resolve_sowing_date(ops, TODAY)


def test_sowing_future_ignored():
    ops = [op(endedAt=dt("2026-12-01T00:00:00Z")), op(startedAt=prop("2026-03-01"))]
    assert resolve_sowing_date(ops, TODAY) == date(2026, 3, 1)


def test_sowing_most_recent_wins():
    ops = [op(startedAt=prop("2025-10-01")), op(startedAt=prop("2026-04-10")),
           op(startedAt=prop("2026-01-01"))]
    assert resolve_sowing_date(ops, TODAY) == date(2026, 4, 10)


def test_sowing_ignores_other_operation_types():
    with pytest.raises(SimInputError):
        resolve_sowing_date([op(op_type="tillage", startedAt=prop("2026-03-01"))], TODAY)


def test_sowing_none():
    with pytest.raises(SimInputError, match="no completed sowing"):
        resolve_sowing_date([], TODAY)


def test_sowing_ended_at_wins_over_started_at_and_plans():
    ops = [op(startedAt=prop("2026-03-01"), plannedStartAt=prop("2026-02-10T00:00:00Z"),
              endedAt=prop("2026-03-05T10:00:00Z"))]
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


# ------------------------------------------------------------- sowing method
def _p(v):
    return {"type": "Property", "value": v}


def _sow(day, method=None, status="completed", key="endedAt"):
    op = {"operationType": _p("sowing"), "status": _p(status),
          key: _p({"@type": "DateTime", "@value": f"{day}T08:00:00Z"})}
    if method is not None:
        op["sowingMethod"] = _p(method)
    return op


D = date(2025, 10, 20)


def test_vocabulary_is_esyrce_d_n():
    assert SOWING_METHODS == ("direct", "conventional")


def test_method_of_the_completed_sowing_on_the_day():
    assert sowing_method_from_operations([_sow("2025-10-20", "direct")], D) == ("direct", [])


def test_started_at_dates_a_sowing_without_end():
    assert sowing_method_from_operations([_sow("2025-10-20", "direct", key="startedAt")], D) == ("direct", [])


def test_other_day_planned_or_other_type_do_not_count():
    ops = [_sow("2025-10-19", "direct"), _sow("2025-10-20", "direct", status="planned"),
           {"operationType": _p("tillage"), "status": _p("completed"),
            "endedAt": _p("2025-10-20T08:00:00Z"), "sowingMethod": _p("direct")}]
    assert sowing_method_from_operations(ops, D) == (None, [])


def test_sowing_without_method_is_no_information():
    assert sowing_method_from_operations([_sow("2025-10-20")], D) == (None, [])


def test_unknown_method_is_ignored_with_warning():
    method, warns = sowing_method_from_operations([_sow("2025-10-20", "no-till")], D)
    assert method is None
    assert warns == ["sowing method: unknown value 'no-till' on the sowing of 2025-10-20 ignored"]


def test_agreeing_sowings_give_the_method():
    ops = [_sow("2025-10-20", "direct"), _sow("2025-10-20", "direct")]
    assert sowing_method_from_operations(ops, D) == ("direct", [])


def test_disagreeing_sowings_fall_back_with_warning():
    ops = [_sow("2025-10-20", "direct"), _sow("2025-10-20", "conventional")]
    method, warns = sowing_method_from_operations(ops, D)
    assert method is None
    assert warns == [("sowing method: completed sowings on 2025-10-20 disagree "
                      "(conventional, direct); bare soil (conventional) assumed")]


def test_plain_string_values_are_accepted():
    op = {"operationType": "sowing", "status": "completed", "endedAt": "2025-10-20",
          "sowingMethod": "conventional"}
    assert sowing_method_from_operations([op], D) == ("conventional", [])
