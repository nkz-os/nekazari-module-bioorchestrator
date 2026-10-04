"""Contribution identity and review state against a real Neo4j.

A contributed parameter map is caller-controlled. These tests prove that it can
never set the review status or the contributor identity of the stored node,
both through the HTTP endpoint (rejected) and at the DAO (set after the map).
"""
from __future__ import annotations

import asyncio
import shutil

import httpx
import pytest
from nkz_platform_sdk.crypto import generate_hmac_signature
from testcontainers.neo4j import Neo4jContainer

from app.graph.dao import GraphDAO
from neo4j import AsyncGraphDatabase
from tests.gateway_token import gateway_token

pytestmark = pytest.mark.skipif(
    shutil.which("docker") is None,
    reason="docker unavailable for testcontainers",
)

_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
_PASSWORD = "testpassword"
CROP = "urn:ngsi-ld:AgriCrop:maize"
RESERVED = {
    "status": "approved",
    "contributedBy": "spoofed-user",
    "contributorTenant": "spoofed-tenant",
    "contributedAt": "1999-01-01",
    "sourceDoi": "spoofed-doi",
}


def _run(coro):
    return _loop.run_until_complete(coro)


@pytest.fixture(scope="module")
def driver():
    with Neo4jContainer("neo4j:5.26-community", password=_PASSWORD) as n:
        d = AsyncGraphDatabase.driver(n.get_connection_url(), auth=(n.username, n.password))
        yield d
        _run(d.close())


@pytest.fixture(autouse=True)
def _crop(driver):
    async def _reset():
        async with driver.session() as s:
            await s.run("MATCH (n) DETACH DELETE n")
            await s.run("CREATE (:AgriCrop {uri: $uri})", uri=CROP)

    _run(_reset())


def _stored(driver) -> list[dict]:
    async def _read():
        async with driver.session() as s:
            result = await s.run(
                "MATCH (:AgriCrop {uri: $uri})-[:HAS_PARAMETER]->(p:PhenologyParams) "
                "RETURN properties(p) AS p",
                uri=CROP,
            )
            return [r["p"] async for r in result]

    return _run(_read())


def test_dao_reserved_keys_cannot_override_identity_or_status(driver):
    created = _run(GraphDAO(driver).contribute_crop_parameters(
        CROP,
        {"kc": 0.5, **RESERVED},
        contributed_by="verified-user",
        contributor_tenant="verified-tenant",
        provenance={"doi": "10.1/real"},
    ))
    assert created is True
    (node,) = _stored(driver)
    assert node["status"] == "pending_review"
    assert node["contributedBy"] == "verified-user"
    assert node["contributorTenant"] == "verified-tenant"
    assert node["sourceDoi"] == "10.1/real"
    assert not isinstance(node["contributedAt"], str)  # server datetime, not the spoofed text
    assert node["kc"] == 0.5


def test_dao_unknown_crop_writes_nothing(driver):
    created = _run(GraphDAO(driver).contribute_crop_parameters(
        "urn:ngsi-ld:AgriCrop:nope", {"kc": 0.5},
        contributed_by="u", contributor_tenant="t", provenance={},
    ))
    assert created is False
    assert _stored(driver) == []


@pytest.fixture
def post(driver, monkeypatch):
    monkeypatch.setenv("AUTH_DISABLED", "false")
    monkeypatch.setenv("AUTH_STRICT", "true")
    monkeypatch.setenv("HMAC_SECRET", "hmac-test")
    monkeypatch.setattr("app.core.dependencies.get_driver", lambda: driver)
    from app.main import app

    def _post(body: dict, roles=("TechnicalConsultant",)):
        token = gateway_token(sub="verified-user", tenant="verified-tenant", roles=roles)
        headers = {
            "Authorization": f"Bearer {token}",
            "X-Tenant-ID": "verified-tenant",
            "X-Auth-Signature": generate_hmac_signature("hmac-test", token, "verified-tenant"),
        }

        async def _call():
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
                return await c.post("/api/crop/catalog/contribute", json=body, headers=headers)

        return _run(_call())

    return _post


def test_endpoint_rejects_reserved_keys_and_stores_nothing(driver, post):
    resp = post({"crop_id": CROP, "params": {"kc": 0.5, **RESERVED}, "provenance": {}})
    assert resp.status_code == 400
    assert _stored(driver) == []


def test_endpoint_stores_verified_identity_and_pending_review(driver, post):
    resp = post({
        "crop_id": CROP,
        "params": {"kc": 0.5, "d1": 3},
        "provenance": {"doi": "10.1/real", "author": "A. Author"},
    })
    assert resp.status_code == 200
    assert resp.json()["applied_to_catalog"] is False
    (node,) = _stored(driver)
    assert node["status"] == "pending_review"
    assert node["contributedBy"] == "verified-user"
    assert node["contributorTenant"] == "verified-tenant"
    assert node["kc"] == 0.5 and node["d1"] == 3


def test_endpoint_unknown_crop_is_404_and_stores_nothing(driver, post):
    resp = post({"crop_id": "urn:ngsi-ld:AgriCrop:nope", "params": {"kc": 0.5}, "provenance": {}})
    assert resp.status_code == 404
    assert _stored(driver) == []
