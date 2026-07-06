"""Behavioral idempotency + TRIAL_AT linking against a real Neo4j.

Proves the fix for the audit's finding #1 end-to-end:
  - re-running merge() does NOT duplicate nodes (was 810 -> 1620),
  - trials sharing the short mergeKey but distinct content do NOT collapse,
  - TRIAL_AT relationships are created (were 0 -> ~16k orphans).
"""
from __future__ import annotations

import asyncio
import shutil

import pytest
from neo4j import AsyncGraphDatabase
from testcontainers.neo4j import Neo4jContainer

from app.ingestion.base_ingester import BaseIngester

pytestmark = pytest.mark.skipif(
    shutil.which("docker") is None,
    reason="docker unavailable for testcontainers",
)

_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()


def _run(coro):
    return _loop.run_until_complete(coro)


_NEO4J_PASSWORD = "testpassword"


@pytest.fixture(scope="module")
def neo4j_container():
    with Neo4jContainer("neo4j:5.26-community", password=_NEO4J_PASSWORD) as n:
        yield n


@pytest.fixture(scope="module")
def driver(neo4j_container):
    uri = neo4j_container.get_connection_url()
    d = AsyncGraphDatabase.driver(
        uri, auth=(neo4j_container.username, neo4j_container.password)
    )
    yield d
    _run(d.close())


class _BslLike(BaseIngester):
    SOURCE_ID = "BSL"

    async def _parse_nodes(self, data):  # unused; we feed merge() directly
        return {"trial_sites": [], "article_sources": [],
                "variety_trials": [], "management_trials": []}


def _nodes():
    site = {"name": "Lleida", "source_id": "BSL", "mergeKey": "bsl|lleida"}
    short = "bsl|zeamx|p1921|lleida|2021"
    # Same short mergeKey, different yield -> two legitimately distinct trials.
    vt1 = {"mergeKey": short, "source_id": "BSL", "cropEppo": "ZEAMX",
           "variety": "p1921", "year": 2021, "yieldKgHa": 12000,
           "trialLocation": "Lleida"}
    vt2 = dict(vt1, yieldKgHa=13500)
    return {"trial_sites": [site], "article_sources": [],
            "variety_trials": [vt1, vt2], "management_trials": []}


def test_merge_is_idempotent_and_links_without_collapsing(driver):
    ing = _BslLike(driver=driver)  # injected driver -> merge() won't close it

    async def _scenario():
        async with driver.session() as s:
            await s.run("MATCH (n) DETACH DELETE n")
        await ing.merge(_nodes())
        await ing.merge(_nodes())  # re-run must not duplicate
        async with driver.session() as s:
            vt = (await (await s.run(
                "MATCH (v:VarietyTrial) RETURN count(v) AS c")).single())["c"]
            rels = (await (await s.run(
                "MATCH (:VarietyTrial)-[:TRIAL_AT]->(:TrialSite) "
                "RETURN count(*) AS c")).single())["c"]
        return vt, rels

    vt, rels = _run(_scenario())
    assert vt == 2   # two distinct trials: not 4 (idempotent), not 1 (no collapse)
    assert rels == 2  # both linked to the Lleida TrialSite (no orphans)


def test_two_sources_share_site_and_both_link(driver):
    """Source B ingests after source A — its trial links to A's existing site.

    Pre-source-agnostic fix: each source had its own TrialSite (source-scoped
    MERGE by mergeKey, source-scoped link by source_id). Now MERGE finds the
    shared site by siteKey, and _merge_relationships links trials to ANY
    matching TrialSite regardless of which source created it.
    """
    from app.ingestion.base_ingester import normalize_site_key

    class _SrcA(BaseIngester):
        SOURCE_ID = "GENVCE"
        async def _parse_nodes(self, data):
            return {"trial_sites": [], "article_sources": [],
                    "variety_trials": [], "management_trials": []}

    class _SrcB(BaseIngester):
        SOURCE_ID = "ITACYL"
        async def _parse_nodes(self, data):
            return {"trial_sites": [], "article_sources": [],
                    "variety_trials": [], "management_trials": []}

    async def _scenario():
        async with driver.session() as s:
            await s.run("MATCH (n) DETACH DELETE n")

        # Source A creates the site + one trial
        ing_a = _SrcA(driver=driver)
        site_a = {"name": "Lleida", "source_id": "GENVCE",
                  "siteKey": normalize_site_key("Lleida"),
                  "municipalityKey": normalize_site_key("Lleida")}
        vt_a = {"mergeKey": "g1", "source_id": "GENVCE", "cropEppo": "TRZAX",
                "variety": "V1", "year": 2021, "yieldKgHa": 8000,
                "trialLocation": "Lleida",
                "trialLocationKey": normalize_site_key("Lleida")}
        await ing_a._merge_trial_sites(driver, [site_a])
        await ing_a._merge_variety_trials(driver, [vt_a])
        await ing_a._merge_relationships(driver, [vt_a], [])

        # Source B ingests later — its trial must link to the same Lleida site
        ing_b = _SrcB(driver=driver)
        vt_b = {"mergeKey": "i1", "source_id": "ITACYL", "cropEppo": "HORVX",
                "variety": "V2", "year": 2021, "yieldKgHa": 6000,
                "trialLocation": "Lleida",
                "trialLocationKey": normalize_site_key("Lleida")}
        # No site from source B — the existing Lleida must be found
        await ing_b._merge_variety_trials(driver, [vt_b])
        await ing_b._merge_relationships(driver, [vt_b], [])

        async with driver.session() as s:
            sites = (await (await s.run(
                "MATCH (t:TrialSite) RETURN count(t) AS c")).single())["c"]
            linked_b = (await (await s.run(
                "MATCH (:VarietyTrial {source_id:'ITACYL'})-[:TRIAL_AT]->(:TrialSite {siteKey:'lleida'}) "
                "RETURN count(*) AS c")).single())["c"]
        return sites, linked_b

    sites, linked_b = _run(_scenario())
    assert sites == 1  # one site for both sources
    assert linked_b == 1  # source B's trial linked to the shared Lleida site
