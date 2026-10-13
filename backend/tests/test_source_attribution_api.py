"""Mandatory source attribution: the ``attributions`` of the API responses.

Each endpoint lists the attributions of the sources actually present in its response; the
public listing exposes every attribution. Registry fields and helper: ``test_source_attribution``.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.api.v1.attribution import (
    attach_attributions,
    recommendation_source_ids,
    rows_source_ids,
)
from app.auth import SKIP_AUTH_PREFIXES
from app.auth_policy import requires_identity
from app.core.dependencies import get_neo4j_driver
from app.graph.dao import GraphDAO
from app.main import app

GENVCE_TEXT = (
    "Fuente: Datos Abiertos GENVCE. Url: https://genvce.org/mapa-de-resultados/ "
    "(Descarga: 01/06/2026.)"
)
def test_attach_attributions_returns_a_new_dict_and_always_sets_the_key():
    payload = {"x": 1}
    out = attach_attributions(payload, ["CREA"])
    assert "attributions" not in payload and out["x"] == 1
    assert [a["source_id"] for a in out["attributions"]] == ["CREA"]
    assert attach_attributions(payload, [])["attributions"] == []


def test_source_id_collectors():
    result = {"recommendations": [
        {"evidence": {"sources": ["GENVCE", "CREA"]}},
        {"evidence": {"sources": ["GENVCE"]}},
        {"evidence": {}},
        {},
    ]}
    assert recommendation_source_ids(result) == {"GENVCE", "CREA"}
    assert recommendation_source_ids({}) == set()
    assert rows_source_ids([{"source_id": "CREA"}, {"source_id": None}, {}]) == {"CREA"}
    assert rows_source_ids(None) == set()


# ── endpoints ───────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def client():
    mp = pytest.MonkeyPatch()
    mp.setenv("AUTH_DISABLED", "true")
    app.dependency_overrides[get_neo4j_driver] = lambda: MagicMock()
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.pop(get_neo4j_driver, None)
        mp.undo()


def _ids(body):
    return [a["source_id"] for a in body["attributions"]]


def test_recommend_evidence_lists_attributions_of_the_page_items(client):
    page = {"items": [{"source_id": "CREA"}, {"source_id": "CREA"}], "total": 2, "page": 1,
            "page_size": 20, "purpose": "main", "tier": "field"}
    with patch.object(GraphDAO, "get_similar_sites", AsyncMock(return_value=[{"name": "S"}])), \
         patch.object(GraphDAO, "list_trial_evidence", AsyncMock(return_value=page)):
        body = client.get("/api/graph/agriculture/recommend/evidence",
                          params={"climate_class": "Cfb", "crop": "ZEAMX"}).json()
    assert _ids(body) == ["CREA"]
    assert body["items"] == page["items"] and body["total"] == 2


def test_variety_trials_lists_attributions_of_the_trials(client):
    trials = [{"variety": "A", "source_id": "GENVCE"}, {"variety": "B", "source_id": "CREA"},
              {"variety": "C", "source_id": "GENVCE"}]
    with patch.object(GraphDAO, "get_variety_trials", AsyncMock(return_value=trials)):
        body = client.get("/api/graph/agriculture/variety-trials", params={"crop": "ZEAMX"}).json()
    assert _ids(body) == ["CREA", "GENVCE"]
    assert body["total"] == 3 and body["trials"] == trials


def test_variety_trials_without_trials_has_an_empty_list(client):
    with patch.object(GraphDAO, "get_variety_trials", AsyncMock(return_value=[])):
        body = client.get("/api/graph/agriculture/variety-trials", params={"crop": "ZEAMX"}).json()
    assert body["attributions"] == []


# ── round 2: the other surfaces that show source data ──────────────────────

def test_trial_sites_lists_attributions(client):
    sites = [{"name": "Lleida", "source_ids": ["GENVCE"]}, {"name": "Orphan", "source_ids": []}]
    with patch.object(GraphDAO, "get_trial_sites_summary", AsyncMock(return_value=sites)):
        body = client.get("/api/graph/agriculture/trial-sites").json()
    assert _ids(body) == ["GENVCE"] and body["total"] == 2


def test_crops_lists_attributions(client):
    crops = [{"eppo_code": "ZEAMX", "source_ids": ["CREA", "GENVCE"]}, {"eppo_code": "TRZAX", "source_ids": ["GENVCE"]}]
    with patch.object(GraphDAO, "get_available_crops", AsyncMock(return_value=crops)):
        body = client.get("/api/graph/agriculture/crops").json()
    assert _ids(body) == ["CREA", "GENVCE"] and body["total"] == 2


def test_graph_stats_lists_attributions_of_the_sources_in_the_graph(client):
    stats = {"trials": 3, "source_ids": ["GENVCE", "UNREGISTERED"]}
    with patch.object(GraphDAO, "graph_quality_stats", AsyncMock(return_value=stats)):
        body = client.get("/api/graph/agriculture/graph-stats").json()
    assert body["trials"] == 3 and _ids(body) == ["GENVCE"]  # a source without a credit is left out


# ── public listing ──────────────────────────────────────────────────────────

_LISTING = "/api/graph/agriculture/sources/attributions"


def test_attributions_listing_is_public_and_get_only(client):
    assert any(_LISTING.startswith(p) for p in SKIP_AUTH_PREFIXES)
    assert requires_identity(_LISTING, "GET", {}) is False
    r = client.get(_LISTING)
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == len(body["attributions"]) >= 2
    by_id = {a["source_id"]: a for a in body["attributions"]}
    assert by_id["GENVCE"]["text"] == GENVCE_TEXT
    assert by_id["GENVCE"]["download_date"] == "2026-06-01"
    assert "CC BY 3.0" in by_id["CREA"]["text"]
    for method in ("post", "put", "patch", "delete"):
        assert getattr(client, method)(_LISTING).status_code == 405


def test_attributions_listing_needs_no_identity_with_auth_enabled(client, monkeypatch):
    monkeypatch.setenv("AUTH_DISABLED", "false")
    assert client.get(_LISTING).status_code == 200


# ── DAO ─────────────────────────────────────────────────────────────────────
