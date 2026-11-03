"""Phenology routes resolve EPPO codes / aliases to the species slug before the DAO."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1 import graph as graph_mod


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(graph_mod.router, prefix="/api/graph")
    app.dependency_overrides[graph_mod.get_neo4j_driver] = lambda: MagicMock()
    return TestClient(app)


def test_phenology_params_accepts_eppo():
    with patch("app.graph.dao.GraphDAO.get_phenology_params",
               AsyncMock(return_value={"kc": 1.0})) as dao:
        r = _client().get("/api/graph/phenology-params?species=TRZAX")
    assert r.status_code == 200
    assert dao.await_args.kwargs["species"] == "wheat"


def test_phenology_stages_accepts_eppo():
    with patch("app.graph.dao.GraphDAO.get_phenology_stages",
               AsyncMock(return_value=[])) as dao:
        r = _client().get("/api/graph/phenology-stages?species=TRZAX")
    assert r.status_code == 200
    assert dao.await_args.args[0] == "wheat"


def test_unknown_identifier_passes_through():
    with patch("app.graph.dao.GraphDAO.get_phenology_params",
               AsyncMock(return_value={"kc": 1.0})) as p, \
         patch("app.graph.dao.GraphDAO.get_phenology_stages",
               AsyncMock(return_value=[])) as s:
        _client().get("/api/graph/phenology-params?species=zzz")
        _client().get("/api/graph/phenology-stages?species=zzz")
    assert p.await_args.kwargs["species"] == "zzz"
    assert s.await_args.args[0] == "zzz"
