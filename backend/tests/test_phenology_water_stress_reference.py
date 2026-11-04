"""phenology-params serves the water stress reference next to the existing fields."""

from __future__ import annotations

import json
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1 import graph as graph_mod
from app.graph.dao import GraphDAO
from neo4j import AsyncDriver

BLOCK = {
    "baselines": [
        {"id": "a", "evidence": "primary_read", "source_id": "src-a", "ref": "T1",
         "upper": {"form": "constant", "value_c": 3.0}, "a0_c": 1.0, "a1_c_per_kpa": -1.0,
         "valid": {"clear_sky": True}},
        {"id": "b", "evidence": "secondary", "source_id": "src-unserved", "ref": "T2",
         "upper": {"form": "constant", "value_c": 4.0}, "a0_c": 1.0, "a1_c_per_kpa": -1.0,
         "valid": {"clear_sky": True}},
    ],
    "stages": {"flowering": {"water_sensitive": True, "rdi_tolerant": False,
                             "source_ids": ["src-s"], "ref": "T3"}},
}
SERVED = {"waterStress": "served"}
PARAMS = {"species": "wheat", "stage": "mid-season", "kc": 1.15, "d1": None, "d2": None,
          "ky": 0.9, "mds_ref": None, "provenance": {"short": "FAO-56"}, "match_level": "generic"}
EXISTING_KEYS = {
    "species", "stage", "kc", "d1", "d2", "ky", "mds_ref", "provenance", "match_level",
    "dataAvailable", "agronomic",
}


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(graph_mod.router, prefix="/api/graph")
    app.dependency_overrides[graph_mod.get_neo4j_driver] = lambda: MagicMock()
    return TestClient(app)


def _get(ref, params=None, url="/api/graph/phenology-params?species=TRZAX"):
    ref_mock = AsyncMock(side_effect=ref) if isinstance(ref, Exception) else AsyncMock(return_value=ref)
    with patch("app.graph.dao.GraphDAO.get_phenology_params",
               AsyncMock(return_value=dict(params or PARAMS))), \
         patch("app.graph.dao.GraphDAO.get_water_stress_reference", ref_mock) as m:
        r = _client().get(url)
    return r, m


def test_reference_attached_when_present():
    served = {"baselines": [], "stages": {}, "sourceIds": ["src-a"]}
    with patch.object(graph_mod, "attach_attributions",
                      side_effect=lambda p, ids: {**p, "attributions": [{"ids": sorted(ids)}]}) as att:
        r, m = _get(served)
    assert r.status_code == 200
    body = r.json()
    assert body["water_stress_reference"] == served
    assert body["attributions"] == [{"ids": ["src-a"]}]
    assert m.await_args.args[0] == "wheat"  # resolved slug, not the EPPO query string
    assert att.call_args.args[1] == ["src-a"]


def test_reference_null_when_absent():
    r, _ = _get(None)
    assert r.status_code == 200
    assert r.json()["water_stress_reference"] is None
    assert r.json()["attributions"] == []


def test_reference_error_does_not_fail_request(caplog):
    with caplog.at_level(logging.WARNING):
        r, _ = _get(RuntimeError("boom"))
    assert r.status_code == 200
    assert r.json()["water_stress_reference"] is None
    assert any("wheat" in rec.getMessage() for rec in caplog.records if rec.levelno == logging.WARNING)
    assert not any("acme" in rec.getMessage() for rec in caplog.records)


def test_unresolved_slug_skips_lookup():
    r, m = _get(None, url="/api/graph/phenology-params?species=zzz")
    assert r.status_code == 200
    assert r.json()["water_stress_reference"] is None
    m.assert_not_awaited()


def test_existing_fields_unchanged():
    r, _ = _get(None)
    body = r.json()
    assert set(body) == EXISTING_KEYS | {"water_stress_reference", "attributions"}
    for key in ("kc", "d1", "d2", "ky", "mds_ref", "provenance", "match_level", "stage", "species"):
        assert body[key] == PARAMS[key]
    assert body["dataAvailable"] == {"kc": True, "d1": False, "d2": False, "mds": False}
    assert set(body["agronomic"]) == {"kc", "d1", "d2", "mds_ref", "ky"}


# --- DAO ---------------------------------------------------------------------------------

class _Res:
    def __init__(self, rows):
        self._rows = rows

    async def data(self):
        return self._rows


def _dao(rows):
    driver = MagicMock(spec=AsyncDriver)
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    session.run = AsyncMock(return_value=_Res(rows))
    driver.session = MagicMock(return_value=session)
    return GraphDAO(driver), session


@pytest.mark.asyncio
async def test_dao_serves_only_primary_rows():
    dao, session = _dao([{"payload": json.dumps(BLOCK)}])
    out = await dao.get_water_stress_reference("wheat")
    assert [b["id"] for b in out["baselines"]] == ["a"]
    assert out["sourceIds"] == ["src-a", "src-s"]  # the unserved secondary row's source is absent
    assert session.run.await_args.kwargs["species"] == "wheat"


@pytest.mark.asyncio
async def test_dao_none_when_node_missing():
    dao, _ = _dao([])
    assert await dao.get_water_stress_reference("wheat") is None


@pytest.mark.asyncio
async def test_dao_malformed_payload_raises():
    dao, _ = _dao([{"payload": "{not json"}])
    with pytest.raises(ValueError):
        await dao.get_water_stress_reference("wheat")
