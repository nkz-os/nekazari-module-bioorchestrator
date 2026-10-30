"""Cover-crop reference: pure selection, data loader and the planning endpoint."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.planning.cover_reference import (
    COVERAGE,
    SOURCE_ID,
    n_accumulated,
    reference_for,
    select_row,
)
from neo4j import AsyncDriver

M = lambda mean, se=0.1, n=3: {"mean": mean, "se": se, "n": n}
AUT = {"dm_t_ha": M(5.0), "n_pct": M(4.0), "c_n": M(10.0)}
SPR = {"dm_t_ha": M(3.0), "n_pct": M(3.0), "c_n": M(12.0)}
UNS = {"dm_t_ha": M(2.0), "n_pct": None, "c_n": None}


def test_select_exact_season_first():
    assert select_row({"autumn": AUT, "spring": SPR, "unspecified": UNS}, "spring") == ("spring", SPR)


def test_select_falls_back_to_unspecified_only():
    assert select_row({"autumn": AUT, "unspecified": UNS}, "spring") == ("unspecified", UNS)


def test_select_never_uses_the_other_season():
    assert select_row({"autumn": AUT}, "spring") == (None, None)
    assert select_row({}, "autumn") == (None, None)


def test_n_accumulated_is_dm_times_n_pct():
    # 4.99 t/ha * 1000 * 3.95 / 100 = 197.105 -> 197 kg N/ha
    assert n_accumulated({"dm_t_ha": M(4.99), "n_pct": M(3.95)}) == 197


def test_n_accumulated_null_when_an_input_is_missing():
    assert n_accumulated({"dm_t_ha": M(4.99), "n_pct": None}) is None
    assert n_accumulated({"dm_t_ha": None, "n_pct": M(3.95)}) is None


def test_reference_ok_payload_shape():
    out = reference_for("hairy_vetch", "autumn", {"hairy_vetch": {"autumn": AUT}})
    assert out["species"] == "hairy_vetch" and out["status"] == "ok"
    assert out["season_requested"] == "autumn" and out["season_used"] == "autumn"
    assert out["aerial"] == {**AUT, "n_accumulated_kg_ha": 200}
    assert out["coverage"] == COVERAGE == "europe_mostly_north_central"


def test_reference_no_data_has_no_aerial_and_no_other_species():
    out = reference_for("oat", "spring", {"hairy_vetch": {"spring": SPR}})
    assert out["status"] == "no_data" and out["aerial"] is None and out["season_used"] is None


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr("app.core.dependencies.get_driver", lambda: MagicMock(spec=AsyncDriver))
    monkeypatch.setattr("app.core.knowledge.get_driver", lambda: MagicMock(spec=AsyncDriver))
    return TestClient(app)


URL = "/api/graph/planning/cover-crops/{}/reference"


def test_endpoint_vicia_villosa_autumn_from_the_committed_data(client):
    r = client.get(URL.format("hairy_vetch"), params={"season": "autumn"})
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok" and body["season_used"] == "autumn"
    assert body["aerial"]["dm_t_ha"] == {"mean": 4.99, "se": 0.44, "n": 15}
    assert body["aerial"]["n_accumulated_kg_ha"] == 197
    assert [a["source_id"] for a in body["attributions"]] == [SOURCE_ID]


def test_endpoint_accepts_the_eppo_code(client):
    r = client.get(URL.format("VICVI"), params={"season": "spring"})
    assert r.status_code == 200 and r.json()["species"] == "hairy_vetch"


def test_endpoint_unknown_species_is_404(client):
    assert client.get(URL.format("no_such_species"), params={"season": "autumn"}).status_code == 404


def test_endpoint_bad_or_missing_season_is_422(client):
    assert client.get(URL.format("hairy_vetch"), params={"season": "winter"}).status_code == 422
    assert client.get(URL.format("hairy_vetch")).status_code == 422


def test_endpoint_no_data_still_carries_attributions(client, monkeypatch):
    monkeypatch.setattr("app.api.v1.planning.load_reference_data", dict)
    body = client.get(URL.format("hairy_vetch"), params={"season": "autumn"}).json()
    assert body["status"] == "no_data" and body["aerial"] is None
    assert [a["source_id"] for a in body["attributions"]] == [SOURCE_ID]


def test_endpoint_requires_a_verified_tenant(client, monkeypatch):
    monkeypatch.setenv("AUTH_DISABLED", "false")
    assert client.get(URL.format("hairy_vetch"), params={"season": "autumn"}).status_code == 401
