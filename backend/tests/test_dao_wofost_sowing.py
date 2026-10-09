"""run_wofost_simulation: the sowing date comes from the platform's crop cycle, never from a plan."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

from app.graph.dao import GraphDAO

PARCEL = "urn:ngsi-ld:AgriParcel:p-1"
TENANT = "test-tenant"


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
            **self._parcel_extra,
        }

    async def query_entities(self, **kwargs):
        return self._ops

    async def close(self):
        pass


def _run(mock_driver, cycles, ops=None, parcel_extra=None, sowing_date=None):
    dao = GraphDAO(mock_driver)
    captured = {}

    def _sim(**kwargs):
        captured.update(kwargs)
        return {"yield": 1}

    with patch("app.graph.dao.OrionClient", lambda tid: _FakeOrion(tid, ops, parcel_extra)), \
         patch("app.graph.dao.fetch_crop_cycles", AsyncMock(return_value=cycles)), \
         patch("app.services.wofost_service.run_wofost_simulation", _sim), \
         patch("app.services.soil_client.get_parcel_soil_properties",
               AsyncMock(return_value={"data_available": False})), \
         patch.object(GraphDAO, "get_phenology_params", AsyncMock(return_value=None)), \
         patch("httpx.AsyncClient.get", AsyncMock(side_effect=RuntimeError("no network"))):
        result = asyncio.run(dao.run_wofost_simulation(PARCEL, TENANT, sowing_date_str=sowing_date))
    return result, captured


def test_wofost_uses_cycle_start_and_reports_provenance(mock_driver):
    cycles = {"current": {"start": {"date": "2026-03-12", "provenance": "actual"}}}
    ops = [{"operationType": _prop("sowing"), "status": _prop("completed"),
            "endedAt": _prop("2026-03-01T00:00:00Z")}]

    result, captured = _run(mock_driver, cycles, ops)

    assert result["sowing_date"] == "2026-03-12"
    assert result["sowing_provenance"] == "actual"
    assert captured["sowing_date"].isoformat() == "2026-03-12"


def test_wofost_fallback_reads_completed_sowing_end_not_planned_start(mock_driver):
    ops = [
        {"operationType": _prop("sowing"), "status": _prop("planned"),
         "plannedStartAt": _prop("2026-02-01T00:00:00Z")},
        {"operationType": _prop("sowing"), "status": _prop("completed"),
         "plannedStartAt": _prop("2026-02-10T00:00:00Z"), "endedAt": _prop("2026-03-05T10:00:00Z")},
    ]

    result, _ = _run(mock_driver, None, ops)

    assert result["sowing_date"] == "2026-03-05"


def test_wofost_planned_only_operation_is_not_a_sowing_date(mock_driver):
    ops = [{"operationType": _prop("sowing"), "status": _prop("planned"),
            "plannedStartAt": _prop("2026-02-01T00:00:00Z")}]

    result, _ = _run(mock_driver, None, ops)

    assert "error" in result and "sowing date" in result["error"].lower()


def test_wofost_without_cycle_or_operation_uses_legacy_parcel_season(mock_driver):
    result, _ = _run(mock_driver, None, [], parcel_extra={"cropSeasonStart": _prop("2025-10-01")})

    assert result["sowing_date"] == "2025-10-01"


# The platform answered: its "no current cycle" is an answer, not an outage. The parcel's legacy
# season copy (no longer written, stale or planned) must not stand in for a sowing date then.
_LEGACY = {"cropSeasonStart": _prop("2025-10-01")}
_NO_CURRENT = {"current": None, "previous": None, "next": None}
_PLANNED_NEXT = {"current": None, "previous": None,
                 "next": {"start": {"date": "2999-04-10", "provenance": "planned"}}}


def test_wofost_platform_answered_without_current_ignores_legacy_parcel_season(mock_driver):
    result, _ = _run(mock_driver, _NO_CURRENT, [], parcel_extra=_LEGACY)

    assert "error" in result and "sowing date" in result["error"].lower()


def test_wofost_planned_next_cycle_is_not_a_sowing_date(mock_driver):
    result, _ = _run(mock_driver, _PLANNED_NEXT, [], parcel_extra=_LEGACY)

    assert "error" in result and "sowing date" in result["error"].lower()


def test_wofost_platform_answered_without_current_still_reads_completed_sowing(mock_driver):
    ops = [{"operationType": _prop("sowing"), "status": _prop("completed"),
            "endedAt": _prop("2026-03-05T10:00:00Z")}]

    result, _ = _run(mock_driver, _PLANNED_NEXT, ops, parcel_extra=_LEGACY)

    assert result["sowing_date"] == "2026-03-05"
    assert result["sowing_provenance"] == "actual"


def test_wofost_explicit_sowing_date_wins_over_missing_current(mock_driver):
    result, captured = _run(mock_driver, _NO_CURRENT, [], sowing_date="2026-02-20")

    assert result["sowing_date"] == "2026-02-20"
    assert captured["sowing_date"].isoformat() == "2026-02-20"
