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


def _run(mock_driver, cycles, ops=None, parcel_extra=None, sowing_date=None):
    dao = GraphDAO(mock_driver)
    captured = {}

    async def _sim(**kwargs):
        captured.update(kwargs)
        return {"ok": True}

    with patch("app.graph.dao.OrionClient", lambda tid: _FakeOrion(tid, ops, parcel_extra)), \
         patch("app.graph.dao.fetch_crop_cycles", AsyncMock(return_value=cycles)), \
         patch("app.services.crop_simulation.run_crop_simulation", _sim):
        asyncio.run(dao.run_crop_simulation(PARCEL, TENANT, sowing_date_str=sowing_date, today=TODAY))
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
