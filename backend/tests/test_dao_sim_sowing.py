"""run_crop_simulation: the sowing date comes from the platform's crop cycle, never from a plan."""
from __future__ import annotations

import asyncio
from datetime import date
from unittest.mock import AsyncMock, patch

import pytest

from app.graph.dao import GraphDAO
from app.services.sim_errors import SimulationError

PARCEL = "urn:ngsi-ld:AgriParcel:p-1"
TENANT = "test-tenant"
TODAY = date(2026, 10, 11)
POLYGON = {"type": "Polygon", "coordinates": [[[-1.6, 42.5], [-1.5, 42.5], [-1.5, 42.6],
                                              [-1.6, 42.6], [-1.6, 42.5]]]}


def _prop(v):
    return {"type": "Property", "value": v}


class _FakeOrion:
    def __init__(self, tenant_id, ops=None, parcel_extra=None):
        self._ops = ops or []
        self._parcel_extra = parcel_extra or {}

    async def get_entity(self, entity_id):
        return {
            "id": PARCEL,
            "hasAgriCrop": {"type": "Relationship", "object": "urn:ngsi-ld:AgriCrop:TRZAX"},
            "location": {"type": "GeoProperty", "value": POLYGON},
            **self._parcel_extra,
        }

    async def query_entities(self, **kwargs):
        return self._ops

    async def close(self):
        pass


def _run(mock_driver, cycles, ops=None, parcel_extra=None, sowing_date=None, sowing_method=None,
         orion_cls=None):
    dao = GraphDAO(mock_driver)
    captured = {}

    async def _sim(**kwargs):
        captured.update(kwargs)
        return {"ok": True}

    orion = orion_cls or (lambda tid: _FakeOrion(tid, ops, parcel_extra))
    with patch("app.graph.dao.OrionClient", orion), \
         patch("app.graph.dao.fetch_crop_cycles", AsyncMock(return_value=cycles)), \
         patch("app.services.crop_simulation.run_crop_simulation", _sim):
        asyncio.run(dao.run_crop_simulation(PARCEL, TENANT, sowing_date_str=sowing_date, today=TODAY,
                                            sowing_method=sowing_method))
    return captured


def _missing(mock_driver, *args, **kwargs):
    with pytest.raises(SimulationError) as exc:
        _run(mock_driver, *args, **kwargs)
    assert exc.value.code == "sowing_date_missing"


def test_uses_cycle_start_and_reports_provenance(mock_driver):
    cycles = {"current": {"start": {"date": "2026-03-12", "provenance": "actual"}}}
    ops = [{"operationType": _prop("sowing"), "status": _prop("completed"),
            "endedAt": _prop("2026-03-01T00:00:00Z")}]

    captured = _run(mock_driver, cycles, ops)

    assert captured["sowing_date"] == date(2026, 3, 12)
    assert (captured["sowing_source"], captured["sowing_provenance"]) == ("crop_cycle", "actual")


def test_fallback_reads_completed_sowing_end_not_planned_start(mock_driver):
    ops = [
        {"operationType": _prop("sowing"), "status": _prop("planned"),
         "plannedStartAt": _prop("2026-02-01T00:00:00Z")},
        {"operationType": _prop("sowing"), "status": _prop("completed"),
         "plannedStartAt": _prop("2026-02-10T00:00:00Z"), "endedAt": _prop("2026-03-05T10:00:00Z")},
    ]

    captured = _run(mock_driver, None, ops)

    assert captured["sowing_date"] == date(2026, 3, 5)
    assert captured["sowing_source"] == "field_operations"


def test_planned_only_operation_is_not_a_sowing_date(mock_driver):
    ops = [{"operationType": _prop("sowing"), "status": _prop("planned"),
            "plannedStartAt": _prop("2026-02-01T00:00:00Z")}]

    _missing(mock_driver, None, ops)


def test_without_cycle_or_operation_uses_legacy_parcel_season(mock_driver):
    captured = _run(mock_driver, None, [], parcel_extra={"cropSeasonStart": _prop("2025-10-01")})

    assert captured["sowing_date"] == date(2025, 10, 1)
    assert captured["sowing_source"] == "legacy_parcel_season"


# The platform answered: its "no current cycle" is an answer, not an outage. The parcel's legacy
# season copy (no longer written, stale or planned) must not stand in for a sowing date then.
_LEGACY = {"cropSeasonStart": _prop("2025-10-01")}
_NO_CURRENT = {"current": None, "previous": None, "next": None}
_PLANNED_NEXT = {"current": None, "previous": None,
                 "next": {"start": {"date": "2999-04-10", "provenance": "planned"}}}


def test_platform_answered_without_current_ignores_legacy_parcel_season(mock_driver):
    _missing(mock_driver, _NO_CURRENT, [], parcel_extra=_LEGACY)


def test_planned_next_cycle_is_not_a_sowing_date(mock_driver):
    _missing(mock_driver, _PLANNED_NEXT, [], parcel_extra=_LEGACY)


def test_platform_answered_without_current_still_reads_completed_sowing(mock_driver):
    ops = [{"operationType": _prop("sowing"), "status": _prop("completed"),
            "endedAt": _prop("2026-03-05T10:00:00Z")}]

    captured = _run(mock_driver, _PLANNED_NEXT, ops, parcel_extra=_LEGACY)

    assert captured["sowing_date"] == date(2026, 3, 5)
    assert captured["sowing_provenance"] == "actual"


def test_explicit_sowing_date_wins_over_missing_current(mock_driver):
    captured = _run(mock_driver, _NO_CURRENT, [], sowing_date="2026-02-20")

    assert captured["sowing_date"] == date(2026, 2, 20)
    assert captured["sowing_source"] == "request"


# ------------------------------------------------------------- sowing method
def _sow(day, method=None):
    op = {"operationType": _prop("sowing"), "status": _prop("completed"),
          "endedAt": _prop(f"{day}T00:00:00Z")}
    if method:
        op["sowingMethod"] = _prop(method)
    return op


def _method(captured):
    return captured["sowing_method"], captured["sowing_method_source"], captured["input_warnings"]


def test_request_method_wins_over_the_recorded_sowing(mock_driver):
    captured = _run(mock_driver, None, [_sow("2026-03-05", "conventional")], sowing_method="direct")
    assert _method(captured) == ("direct", "request", [])


def test_method_of_the_sowing_that_dated_the_season(mock_driver):
    captured = _run(mock_driver, None, [_sow("2026-03-05", "direct")])
    assert captured["sowing_date"] == date(2026, 3, 5)
    assert _method(captured) == ("direct", "field_operations", [])


def test_cycle_date_matches_the_sowing_of_that_day(mock_driver):
    cycles = {"current": {"start": {"date": "2026-03-12", "provenance": "actual"}}}
    captured = _run(mock_driver, cycles, [_sow("2026-03-01", "conventional"), _sow("2026-03-12", "direct")])
    assert _method(captured) == ("direct", "field_operations", [])


def test_no_matching_sowing_is_default(mock_driver):
    cycles = {"current": {"start": {"date": "2026-03-12", "provenance": "actual"}}}
    captured = _run(mock_driver, cycles, [_sow("2026-03-01", "direct")])
    assert _method(captured) == ("conventional", "default", [])


def test_preseason_without_method_is_default_bare_soil(mock_driver):
    captured = _run(mock_driver, None, [_sow("2026-03-05", "direct")], sowing_date="2026-11-01")
    assert captured["sowing_date"] == date(2026, 11, 1)
    assert _method(captured) == ("conventional", "default", [])


class _OpsDown(_FakeOrion):
    async def query_entities(self, **kwargs):
        raise RuntimeError("orion down")


def test_operations_down_only_for_method_is_a_warning(mock_driver):
    captured = _run(mock_driver, None, sowing_date="2026-03-05", orion_cls=lambda tid: _OpsDown(tid))
    assert _method(captured) == (
        "conventional", "default",
        ["sowing method: field operations could not be read; bare soil (conventional) assumed"])


def test_operations_down_for_the_date_is_still_503(mock_driver):
    with pytest.raises(SimulationError) as ei:
        _run(mock_driver, None, orion_cls=lambda tid: _OpsDown(tid))
    assert ei.value.code == "operations_unavailable" and ei.value.status_code == 503


class _NoRead(_FakeOrion):
    def __init__(self, tenant_id):
        raise AssertionError("no Orion read expected")


def test_invalid_request_method_is_422_before_any_read(mock_driver):
    with pytest.raises(SimulationError) as ei:
        _run(mock_driver, None, [], sowing_method="no-till", orion_cls=_NoRead)
    assert ei.value.code == "invalid_sowing_method"
