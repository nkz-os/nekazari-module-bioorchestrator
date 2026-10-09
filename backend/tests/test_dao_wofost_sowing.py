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


def _run(mock_driver, cycles, ops=None, parcel_extra=None):
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
        result = asyncio.run(dao.run_wofost_simulation(PARCEL, TENANT))
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
