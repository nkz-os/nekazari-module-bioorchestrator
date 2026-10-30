"""recommend_fertilizer is demand-only and deprecated; fertilizer rules are inactive."""
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
import yaml
from fastapi.testclient import TestClient

from app.core.dependencies import get_neo4j_driver
from app.graph.dao import GraphDAO
from app.main import app

URL = "/api/graph/recommendations/fertilizer"
ROWS = [
    {"element": "nitrogen", "uptake": 0.56, "total": 50.0,
     "source_short": "Src-A", "source_doi": "10.1000/a"},
    {"element": "potassium", "uptake": 1.2, "total": None,
     "source_short": None, "source_doi": None},
]


def _driver(rows):
    result = MagicMock()
    result.data = AsyncMock(return_value=rows)
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    session.run = AsyncMock(return_value=result)
    driver = MagicMock()
    driver.session = MagicMock(return_value=session)
    return driver, session


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("AUTH_DISABLED", "true")
    app.dependency_overrides[get_neo4j_driver] = lambda: MagicMock()
    yield TestClient(app)
    app.dependency_overrides.pop(get_neo4j_driver, None)


@pytest.mark.asyncio
async def test_soil_params_ignored_status_demand_only():
    driver, session = _driver(ROWS)
    low = await GraphDAO(driver).recommend_fertilizer("wheat", "tillering", 20, 0, 0)
    high = await GraphDAO(driver).recommend_fertilizer("wheat", "tillering", 9999, 9999, 9999)
    assert low == high
    for rec in low["recommendations"]:
        assert rec["status"] == "demand_only"
        assert not {"soil_level", "action"} & rec.keys()
    n = low["recommendations"][0]
    assert n["uptake_kg_ha_day"] == 0.56 and n["total_kg_ha"] == 50.0
    assert n["source_short"] == "Src-A" and n["source_doi"] == "10.1000/a"
    assert "totalKgHa" in session.run.call_args.args[0]


@pytest.mark.asyncio
async def test_no_rows_is_none():
    driver, _ = _driver([])
    assert await GraphDAO(driver).recommend_fertilizer("x", "y") is None


def test_route_sends_deprecation_header_and_logs_caller(client, monkeypatch, caplog):
    payload = {"species": "wheat", "stage": "tillering", "recommendations": []}
    monkeypatch.setattr(GraphDAO, "recommend_fertilizer", AsyncMock(return_value=payload))
    with caplog.at_level("WARNING"):
        r = client.get(URL, params={"species": "wheat", "soil_n": 20},
                       headers={"X-User-ID": "caller-a"})
    assert r.status_code == 200 and r.json() == payload
    assert r.headers["Deprecation"] == "true"
    assert any("deprecated" in m and "caller-a" in m for m in caplog.messages)


def test_route_404_still_deprecated(client, monkeypatch):
    monkeypatch.setattr(GraphDAO, "recommend_fertilizer", AsyncMock(return_value=None))
    r = client.get(URL, params={"species": "wheat"})
    assert r.status_code == 404


def test_route_marked_deprecated_in_openapi(client):
    op = app.openapi()["paths"][URL]["get"]
    assert op.get("deprecated") is True


def test_fertilizer_rules_inactive():
    data = yaml.safe_load(Path("data/action_rules.yaml").read_text())
    by_id = {r["id"]: r for r in data["rules"]}
    for rid in ("cobertera_nitrogenada", "fosforo_presiembra"):
        assert by_id[rid]["active"] is False
