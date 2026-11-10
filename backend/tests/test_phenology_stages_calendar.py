"""GET /phenology-stages: the engine's calendar for wheat, durum wheat and barley; others untouched."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1 import graph as graph_mod
from app.services import crop_calendar as cc

NAVARRA = {"lat": 42.7, "lon": -1.6}  # synthetic point inside a calibrated province
STAGES = [{"stage": "V6", "gddMin": 100, "gddMax": 200, "baseTemp": 10}]


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(graph_mod.router, prefix="/api/graph")
    app.dependency_overrides[graph_mod.get_neo4j_driver] = lambda: MagicMock()
    return TestClient(app)


def test_other_species_response_is_unchanged_byte_for_byte():
    with patch("app.graph.dao.GraphDAO.get_phenology_stages", AsyncMock(return_value=STAGES)):
        r = _client().get("/api/graph/phenology-stages", params={"species": "ZEAMX", **NAVARRA, "irrigation": "rainfed"})
    assert r.content == json.dumps({"species": "maize", "stages": STAGES}, separators=(",", ":")).encode()


def test_wheat_builtin_without_point():
    dao = AsyncMock(return_value=STAGES)
    with patch("app.graph.dao.GraphDAO.get_phenology_stages", dao):
        r = _client().get("/api/graph/phenology-stages", params={"species": "TRZAX"})
    body = r.json()
    cal = cc.effective_calendar("wheat")
    assert r.status_code == 200 and not dao.await_count  # the sourceless table is not served
    assert body["species"] == "wheat"
    assert [s["stage"] for s in body["stages"]] == ["emergence", "vegetative", "flowering", "grain_filling", "maturity"]
    assert body["gddMethod"] == "aquacrop_m3" and body["origin"] == "sowing" and body["upperCutoff"] == cal.tupp
    assert body["model"]["id"] == "aquacrop_calendar" and body["model"]["cultivar"] == "WheatGDD"
    assert body["calibration"]["status"] == "builtin"
    assert body["validation"] == {"status": "not_validated", "reason": "calendar_not_measured"}
    assert [(x["bbchFrom"], x["bbchTo"]) for x in body["stages"]] == [(0, 9), (10, 49), (51, 69), (71, 87), (89, 89)]
    assert "heading" in next(x for x in body["sources"] if x["short"] == "Engine assumption")["detail"]
    assert body["sources"] and all({"short", "detail"} <= set(s) for s in body["sources"])


def test_wheat_calibrated_with_point_changes_the_thresholds():
    c = _client()
    plain = c.get("/api/graph/phenology-stages", params={"species": "wheat"}).json()
    cal = c.get("/api/graph/phenology-stages", params={"species": "wheat", **NAVARRA}).json()
    assert cal["calibration"]["status"] == "calibrated" and cal["calibration"]["region"]["code"]
    assert cal["stages"][-1]["gddMin"] != plain["stages"][-1]["gddMin"]
    irrigated = c.get("/api/graph/phenology-stages",
                      params={"species": "wheat", **NAVARRA, "irrigation": "irrigated"}).json()
    assert irrigated["calibration"]["status"] == "builtin" and irrigated["stages"] == plain["stages"]
    nm = {"status": "not_validated", "reason": "calendar_not_measured"}
    assert plain["validation"] == nm and irrigated["validation"] == nm
    v = cal["validation"]
    assert v["status"] == "measured" and v["measured_on"] == "calibrated"
    assert v["groups"]["winter_type"]["heading"]["n"] > 0


def test_durum_validation_follows_the_calendar_used():
    c = _client()
    p = {"species": "durum_wheat"}
    nm = {"status": "not_validated", "reason": "calendar_not_measured"}
    assert c.get("/api/graph/phenology-stages", params=p).json()["validation"] == nm
    assert c.get("/api/graph/phenology-stages", params={**p, **NAVARRA, "irrigation": "irrigated"}).json()["validation"] == nm
    v = c.get("/api/graph/phenology-stages", params={**p, **NAVARRA}).json()["validation"]
    assert v["status"] == "measured" and v["measured_on"] == "calibrated"


def test_barley_validation_follows_the_calendar_used():
    c = _client()
    p = {"species": "HORVX"}
    nm = {"status": "not_validated", "reason": "calendar_not_measured"}
    assert c.get("/api/graph/phenology-stages", params=p).json()["validation"] == nm
    v = c.get("/api/graph/phenology-stages", params={**p, **NAVARRA}).json()["validation"]
    assert v["status"] == "measured" and v["measured_on"] == "calibrated" and v["note"]


def test_barley_stage_table_at_a_calibrated_point():
    body = _client().get("/api/graph/phenology-stages", params={"species": "HORVX", **NAVARRA}).json()
    rows = {s["stage"]: (s["gddMin"], s["gddMax"]) for s in body["stages"]}
    assert rows["emergence"] == (0, 117) and rows["vegetative"] == (117, 1003)
    assert rows["flowering"] == (1003, 1183) and rows["grain_filling"][0] == 1183
    assert body["stages"][-1]["gddMin"] == 1746
    assert body["calibration"]["status"] == "calibrated" and body["calibration"]["region"]["code"] == "31"
    assert body["calibration"]["version"] == "2026-11-10"


@pytest.mark.parametrize("species,slug", [("durum_wheat", "durum_wheat"), ("HORVX", "barley")])
def test_durum_and_barley_use_the_engine_calendar(species, slug):
    body = _client().get("/api/graph/phenology-stages", params={"species": species}).json()
    assert body["species"] == slug
    assert body["stages"][-1]["gddMin"] == cc.effective_calendar(slug).maturity


@pytest.mark.parametrize("params", [{"lat": 42.7}, {"lon": -1.6}, {"irrigation": "flood"}, {"lat": 95, "lon": 0}])
def test_invalid_point_or_regime_is_422(params):
    r = _client().get("/api/graph/phenology-stages", params={"species": "wheat", **params})
    assert r.status_code == 422


@pytest.mark.parametrize("params", [{"irrigation": "flood"}, {"lat": 95, "lon": 0}, {"lat": 42.7}, {"lon": "x"}])
def test_other_species_ignore_the_cereal_params(params):
    c = _client()
    with patch("app.graph.dao.GraphDAO.get_phenology_stages", AsyncMock(return_value=STAGES)):
        plain = c.get("/api/graph/phenology-stages", params={"species": "ZEAMX"})
        odd = c.get("/api/graph/phenology-stages", params={"species": "ZEAMX", **params})
    assert odd.status_code == 200 and odd.content == plain.content
