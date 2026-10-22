"""Equivalence of a graph rebuilt with the CLI from the real bundles against a deployed instance (plan task 11).

Opt-in (marker ``prod_readonly``; run with ``-m prod_readonly``): needs Docker, ``NKZ_DATA_SOURCES_DIR`` (the raw-data
repository) and ``NKZ_KG_PROD_API_BASE`` (the deployed API's ``.../api/graph`` URL, GET only, no credentials; no
default, so no deployment is named here). The deployed side is read through its public API; the local side runs this
repository's own DAO on the rebuilt graph. Every difference of a recommendation must have a cause the rebuild is
meant to produce (``classify_recommendations``); the field-tier evidence of a located site must be identical.
"""
from __future__ import annotations

import asyncio
import os
import shutil
from pathlib import Path

import pytest
from testcontainers.neo4j import Neo4jContainer

from app.kg import cli
from neo4j import AsyncGraphDatabase
from tests.kg import equivalence_harness as h

RAW = os.environ.get("NKZ_DATA_SOURCES_DIR", "")
API = os.environ.get("NKZ_KG_PROD_API_BASE", "")
pytestmark = [
    pytest.mark.prod_readonly,
    pytest.mark.skipif(shutil.which("docker") is None, reason="docker unavailable"),
    pytest.mark.skipif(not (RAW and API), reason="set NKZ_DATA_SOURCES_DIR and NKZ_KG_PROD_API_BASE"),
]
PASSWORD = "testpassword"


class _Cell:
    async def __call__(self, lat, lon):
        return {"koppen": "Cfa", "annual_temp_c": 12.5, "annual_rainfall_mm": 830.0, "annual_et0_mm": 806.0,
                "coldest_month_min_c": -1.5, "source": "test cell"}


@pytest.fixture(scope="module")
def results(tmp_path_factory):
    loop = asyncio.new_event_loop()
    with Neo4jContainer("neo4j:5.26-community", password=PASSWORD) as n:
        url = n.get_connection_url()
        out = tmp_path_factory.mktemp("equivalence")
        cfg = cli.BuildConfig(
            sources=("GENVCE", "CREA"), raw_dir=Path(RAW), out_dir=out / "out", target=url,
            target_label="test-equivalence", execute=True, chelsa_online=True, climate_cache=out / "cache.json",
            allow_dirty=True)
        built = cli.run_build(cfg, env={"NEO4J_PASSWORD": PASSWORD, "NKZ_KG_ALLOWED_TARGET_HOSTS": "localhost"},
                              climate_reader=_Cell())
        assert built.status == "ok", built.summary
        driver = AsyncGraphDatabase.driver(url, auth=("neo4j", PASSWORD))
        new = loop.run_until_complete(h.collect(driver))
        loop.run_until_complete(driver.close())
    loop.close()
    return h.fetch_http(API), new


def _rows(response):
    return sorted((i["variety"], i["yield_kg_ha"]) for i in response.get("items", []))


def test_every_difference_of_a_recommendation_has_an_intended_cause(results):
    prod, new = results
    causes, unexplained = h.classify_recommendations(prod, new)
    assert not unexplained, unexplained[:5]
    assert causes["identical"] + causes["both empty"] > 0


def test_field_evidence_of_the_located_site_is_the_same_trials(results):
    prod, new = results
    for key in sorted(k for k in prod if k.startswith("evidence|") and k.endswith("|field")):
        a, b = prod[key], new[key]
        assert a.get("total") == b.get("total"), key
        assert _rows(a) == _rows(b), key


def test_the_countrys_zone_evidence_contains_what_the_city_sites_served(results):
    prod, new = results
    for crop in h.CROPS:
        before = prod[f"evidence|ES-Csa|{crop}|regional"].get("total") or 0
        after = new[f"evidence-country|ES-Csa|{crop}|regional"].get("total") or 0
        assert after >= before, (crop, before, after)
