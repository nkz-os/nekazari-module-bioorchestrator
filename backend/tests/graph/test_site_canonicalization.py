"""Tests for TrialSite canonicalization planning (0.4 spec).

Pure planning logic (no Neo4j): group same-name TrialSites, flag groups whose
members disagree on municipality (needsHumanReview), else merge into the richest
survivor with backfill. See internal-docs 2026-07-03-task0.4-canonicalization.
"""

from __future__ import annotations

import pytest
from app.graph.site_canonicalization import plan_site_canonicalization, normalize_site_key, haversine_km


def _site(sid, name, municipality=None, climateClass=None, latitude=None,
          longitude=None, soilTexture=None, annualRainfallMm=None):
    return {
        "id": sid,
        "name": name,
        "municipality": municipality,
        "climateClass": climateClass,
        "latitude": latitude,
        "longitude": longitude,
        "soilTexture": soilTexture,
        "annualRainfallMm": annualRainfallMm,
    }


def test_all_unique_names_produce_no_plan():
    sites = [
        _site("a", "Lleida"),
        _site("b", "Valladolid"),
        _site("c", "Córdoba"),
    ]
    assert plan_site_canonicalization(sites) == []


def test_ifapa_variants_same_name_merge_not_flag():
    # Regression: municipality disagreement must NOT flag anymore.
    sites = [
        _site("a", "Córdoba (Alameda del Obispo)", municipality="Córdoba"),
        _site("b", "Córdoba (Alameda del Obispo)", municipality="Alameda"),
    ]
    plans = plan_site_canonicalization(sites)
    assert len(plans) == 1 and plans[0]["action"] == "merge"
    assert plans[0]["site_key"] == "cordoba"


def test_same_name_geo_close_merges():
    sites = [
        _site("a", "Sartaguda", latitude=42.38, longitude=-2.05, climateClass="Csa"),
        _site("b", "Sartaguda", latitude=42.39, longitude=-2.06),
    ]
    plans = plan_site_canonicalization(sites)
    assert plans[0]["action"] == "merge" and plans[0]["survivor_id"] == "a"


def test_same_name_geo_far_splits_and_flags():
    sites = [
        _site("a", "Springfield", latitude=39.80, longitude=-89.64),
        _site("b", "Springfield", latitude=42.10, longitude=-72.59),  # ~1600 km
    ]
    plans = plan_site_canonicalization(sites)
    assert plans[0]["action"] == "split"
    keys = {c["site_key"] for c in plans[0]["clusters"]}
    assert len(keys) == 2 and all(k.startswith("springfield#") for k in keys)


def test_same_name_no_geo_merges():
    sites = [_site("a", "Lleida"), _site("b", "Lleida")]
    assert plan_site_canonicalization(sites)[0]["action"] == "merge"


def test_merge_group_picks_richest_survivor():
    rich = _site("rich", "Sartaguda", municipality="Sartaguda",
                 climateClass="BSk", latitude=42.3, soilTexture="loam",
                 annualRainfallMm=400)
    sparse = _site("sparse", "Sartaguda", municipality="Sartaguda")
    plans = plan_site_canonicalization([sparse, rich])
    assert len(plans) == 1
    p = plans[0]
    assert p["action"] == "merge"
    assert p["survivor_id"] == "rich"
    assert p["merge_ids"] == ["sparse"]


def test_backfill_fills_survivor_nulls_from_siblings():
    # survivor is richest overall but lacks soilTexture; sibling supplies it.
    survivor = _site("s", "Cárcar", municipality="Cárcar",
                     climateClass="BSk", latitude=42.4, annualRainfallMm=380)
    sibling = _site("o", "Cárcar", municipality="Cárcar", soilTexture="clay")
    plans = plan_site_canonicalization([survivor, sibling])
    p = plans[0]
    assert p["survivor_id"] == "s"
    assert p["backfill"] == {"soilTexture": "clay"}


def test_already_canonical_input_is_idempotent_noop():
    # Post-merge state: every name unique -> no further work.
    sites = [_site("a", "Sartaguda"), _site("b", "Cárcar"), _site("c", "Lleida")]
    assert plan_site_canonicalization(sites) == []


@pytest.mark.parametrize("raw,expected", [
    ("Córdoba (Alameda del Obispo)", "cordoba"),
    ("Córdoba", "cordoba"),
    ("  La  Mojonera ", "la mojonera"),
    ("ALMERÍA (La Mojonera)", "almeria"),
    (None, ""),
    ("", ""),
])
def test_normalize_site_key(raw, expected):
    assert normalize_site_key(raw) == expected


def test_haversine_km_known_distance():
    # Madrid ~ Zaragoza ≈ 273 km
    d = haversine_km(40.4168, -3.7038, 41.6488, -0.8891)
    assert 260 < d < 285


def test_haversine_km_zero():
    assert haversine_km(40.0, -3.0, 40.0, -3.0) == pytest.approx(0.0, abs=1e-6)
