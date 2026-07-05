"""Idempotency + TRIAL_AT linking contract for BaseIngester.

Root cause (audit 2026-07-01/02): `_merge_variety_trials` MERGEd by the stable
long key `merge_key|content_hash` but then SET `vt.mergeKey = $merge_key` (short),
so re-runs no longer matched existing nodes (duplicates, 810->1620) and
`_merge_relationships` matched by the long key that no longer existed (0 TRIAL_AT,
~16k orphans invisible to extrapolate).

Decision (evidence-based, option a): keep the content-hash in the node identity
(short key alone collapses legitimately distinct trials — e.g. 12 real maize
yields at genvce|ZEAMX|p1921|lleida|2021), stop overwriting it, and link
deterministically by (source_id, trialLocation == TrialSite.name) as the proven
EuTrialsIngester override does.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.ingestion.base_ingester import BaseIngester


class _T(BaseIngester):
    SOURCE_ID = "BSL"

    async def _parse_nodes(self, data):
        return {
            "trial_sites": [],
            "article_sources": [],
            "variety_trials": [],
            "management_trials": [],
        }


def _vt(**over):
    node = {
        "mergeKey": "bsl|zeamx|p1921|lleida|2021",
        "source_id": "BSL",
        "trial_id": "urn:x:1",
        "cropEppo": "ZEAMX",
        "variety": "p1921",
        "year": 2021,
        "yieldKgHa": 12000,
        "trialLocation": "Lleida",
    }
    node.update(over)
    return node


def _mock_driver():
    session = AsyncMock()
    session.run.return_value.single = AsyncMock(return_value={"c": 15884})
    driver = MagicMock()
    driver.session.return_value.__aenter__ = AsyncMock(return_value=session)
    driver.session.return_value.__aexit__ = AsyncMock(return_value=False)
    return driver, session


# ── Stable node identity (content-hash suffixed) ──────────────────────────────

def test_variety_unique_key_is_deterministic():
    assert BaseIngester._variety_unique_key(_vt()) == BaseIngester._variety_unique_key(_vt())


def test_variety_unique_key_distinguishes_yield():
    # Two trials that share the short mergeKey but differ in yield MUST NOT collapse
    # (option b would destroy the 12 distinct genvce|ZEAMX|p1921|lleida|2021 trials).
    a = BaseIngester._variety_unique_key(_vt(yieldKgHa=12000))
    b = BaseIngester._variety_unique_key(_vt(yieldKgHa=13500))
    assert a != b


def test_variety_unique_key_ignores_volatile_fields():
    # @id / source_id / short mergeKey are not content -> re-scrape with a new @id
    # must map to the SAME identity (idempotent, no duplicate).
    a = BaseIngester._variety_unique_key(_vt(trial_id="urn:x:1"))
    b = BaseIngester._variety_unique_key(_vt(trial_id="urn:x:999"))
    assert a == b


def test_variety_unique_key_none_without_merge_key():
    assert BaseIngester._variety_unique_key({"variety": "x"}) is None


# ── Source-agnostic site identity (precomputed keys) ────────────────────────

@pytest.mark.asyncio
async def test_normalize_nodes_sets_site_and_trial_keys():
    """normalize_nodes precomputes siteKey, municipalityKey, and trialLocationKey."""
    nodes = {
        "trial_sites": [{"name": "C\u00f3rdoba (Alameda del Obispo)", "municipality": "C\u00f3rdoba"}],
        "variety_trials": [{"trialLocation": "C\u00f3rdoba (Alameda del Obispo)",
                            "variety": "X", "cropEppo": "TRZAX", "year": 2023}],
        "management_trials": [],
        "article_sources": [],
    }
    out = await _T().normalize_nodes(nodes)
    assert out["trial_sites"][0]["siteKey"] == "cordoba"
    assert out["trial_sites"][0]["municipalityKey"] == "cordoba"
    assert out["trial_sites"][0]["source_id"] == "BSL"
    assert out["variety_trials"][0]["trialLocationKey"] == "cordoba"


def test_normalize_site_key_imported_by_base_ingester():
    """base_ingester imports normalize_site_key — keys agree byte-for-byte with migration."""
    from app.ingestion.base_ingester import normalize_site_key  # noqa: F401 — proves import
    assert normalize_site_key("C\u00f3rdoba (Alameda del Obispo)") == "cordoba"


# ── MERGE must not overwrite the stable key with the short one ────────────────

@pytest.mark.asyncio
async def test_merge_variety_trials_does_not_overwrite_stable_key():
    driver, session = _mock_driver()
    await _T()._merge_variety_trials(driver, [_vt()])
    cypher = session.run.call_args[0][0]
    assert "MERGE (vt:VarietyTrial {mergeKey: $unique_key})" in cypher
    # The root-cause line. Overwriting identity with the short key breaks re-run
    # idempotency and orphans every TRIAL_AT.
    assert "vt.mergeKey = $merge_key" not in cypher


# ── Deterministic, source-scoped linking (EU-proven pattern) ─────────────────

@pytest.mark.asyncio
async def test_merge_relationships_links_by_source_and_location():
    driver, session = _mock_driver()
    n = await _T()._merge_relationships(driver, [_vt()], [])
    assert n == 15884
    cypher = session.run.call_args[0][0]
    assert "TRIAL_AT" in cypher
    assert "source_id" in cypher
    assert "trialLocation" in cypher and "t.name" in cypher
    # Must NOT depend on the fragile recomputed content-hash unique_key.
    assert "unique_key" not in cypher
    assert session.run.call_args.kwargs.get("sid") == "BSL"
