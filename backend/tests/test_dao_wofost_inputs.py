"""run_wofost_simulation resolves real inputs; no synthetic weather, no default soil."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from app.graph.dao import GraphDAO

PARCEL_ID = "urn:ngsi-ld:AgriParcel:p1"
TENANT = "t1"

PARCEL = {"id": PARCEL_ID, "type": "AgriParcel",
          "hasAgriCrop": {"type": "Relationship", "object": "urn:ngsi-ld:AgriCrop:TRZAW"}}
SOWING = {"id": "urn:ngsi-ld:AgriParcelOperation:s", "type": "AgriParcelOperation",
          "operationType": {"type": "Property", "value": "sowing"},
          "status": {"type": "Property", "value": "completed"},
          "plannedDate": {"type": "Property", "value": {"@type": "DateTime", "@value": "2026-03-01T00:00:00Z"}}}
SUMMARY = {"horizons": [
    {"depthFrom": 0, "depthTo": 30, "fieldCapacity": 0.30, "wiltingPoint": 0.15, "ksatSaturated": 10.0},
    {"depthFrom": 30, "depthTo": 60, "fieldCapacity": 0.27, "wiltingPoint": 0.14, "ksatSaturated": 8.0}]}


class _Orion:
    queries: list = []  # noqa: RUF012

    def __init__(self, tenant_id):
        pass

    async def get_entity(self, entity_id):
        return PARCEL

    async def query_entities(self, **kw):
        _Orion.queries.append(kw)
        return [SOWING]

    async def close(self):
        pass


def _http(status=200, payload=None, raises=None):
    resp = MagicMock(status_code=status)
    resp.json.return_value = payload if payload is not None else []
    client = MagicMock()
    client.get = AsyncMock(side_effect=raises) if raises else AsyncMock(return_value=resp)
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return MagicMock(return_value=cm)


def _run(dao, http, summary=SUMMARY, sim=None):
    sim = sim or MagicMock(return_value={"model": "x"})
    with patch("app.graph.dao.OrionClient", _Orion), \
         patch("httpx.AsyncClient", http), \
         patch.object(GraphDAO, "get_phenology_params", AsyncMock(return_value=None)), \
         patch("app.services.soil_client.get_parcel_soil_summary", AsyncMock(return_value=summary)), \
         patch("app.services.wofost_service.run_wofost_simulation", sim):
        return asyncio.run(dao.run_wofost_simulation(PARCEL_ID, tenant_id=TENANT)), sim


def test_weather_failure_is_error_and_no_simulation(mock_driver):
    res, sim = _run(GraphDAO(mock_driver), _http(raises=RuntimeError("down")))
    assert "error" in res and "weather" in res["error"].lower()
    sim.assert_not_called()


def test_weather_empty_is_error_and_no_simulation(mock_driver):
    res, sim = _run(GraphDAO(mock_driver), _http(payload=[]))
    assert "error" in res
    sim.assert_not_called()


def test_weather_non_200_is_error(mock_driver):
    res, sim = _run(GraphDAO(mock_driver), _http(status=500))
    assert "error" in res
    sim.assert_not_called()


def test_soil_summary_used_and_real_weather_passed(mock_driver):
    wx = [{"date": "2026-03-01", "tmin": 1, "tmax": 9, "precip": 0.0}]
    res, sim = _run(GraphDAO(mock_driver), _http(payload=wx))
    assert "error" not in res
    kw = sim.call_args.kwargs
    assert kw["weather_data"] == wx
    props = kw["soil_hydraulic_props"]
    assert props["theta_sat"] is None
    assert abs(props["theta_fc"] - (0.30 + 0.27) / 2) < 1e-9
    assert kw["sowing_date"].isoformat() == "2026-03-01"
    assert _Orion.queries[-1]["limit"] == 200
    assert "sand_pct" not in res.get("soil_inputs", {})


def test_soil_incomplete_is_error(mock_driver):
    bad = {"horizons": [{"depthFrom": 0, "depthTo": 30, "wiltingPoint": 0.1, "ksatSaturated": 5.0}]}
    res, sim = _run(GraphDAO(mock_driver), _http(payload=[{"date": "2026-03-01"}]), summary=bad)
    assert "fieldCapacity" in res["error"]
    sim.assert_not_called()
