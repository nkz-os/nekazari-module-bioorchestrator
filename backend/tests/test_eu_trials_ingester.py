import pytest

from app.ingestion.eu_trials_ingester import EuTrialsIngester


def _ing():
    return EuTrialsIngester()


TRIAL = {
    "@id": "urn:nkz:trial:EU-TRIAL-REPORTS:stava-sweden-2021",
    "@type": "VarietyTrial",
    "crop_eppo": "TRZAX",
    "crop_scientific": "Triticum aestivum",
    "variety": "Stava",
    "year": 2021,
    "yield_kg_ha": 5041,
    "trial_location": "Sweden (mean of 3 trials)",
    "climate_class": "Dfb",
    "country": "Sweden",
    "production_system": "organic",
    "confidence": "high",
}


def test_convert_trial_maps_snake_to_camel():
    t = _ing()._convert_trial(TRIAL)
    assert t["cropEppo"] == "TRZAX"
    assert t["cropScientific"] == "Triticum aestivum"
    assert t["variety"] == "Stava"
    assert t["year"] == 2021
    assert t["yieldKgHa"] == 5041
    assert t["trialLocation"] == "Sweden (mean of 3 trials)"
    assert t["climateClass"] == "Dfb"
    assert t["locationCountry"] == "Sweden"
    assert t["productionSystem"] == "organic"
    assert t["confidence"] == "high"


def test_convert_trial_strips_eppo_prefix():
    assert _ing()._convert_trial({"crop_eppo": "eppo:ZEAMX"})["cropEppo"] == "ZEAMX"


def test_convert_trial_mergekey_includes_eppo_and_production_system():
    # organic vs conventional at same variety/location/year must NOT collapse
    mk = _ing()._convert_trial(TRIAL)["mergeKey"]
    assert "trzax" in mk.lower() and "organic" in mk and "2021" in mk
    conv = dict(TRIAL, production_system="conventional")
    assert _ing()._convert_trial(conv)["mergeKey"] != mk


def test_convert_site_preserves_name_for_trial_at_match():
    s = _ing()._convert_site({"name": "Sweden (mean of 3 trials)", "climateClass": "Dfb"})
    assert s["name"] == "Sweden (mean of 3 trials)"
    assert s["climateClass"] == "Dfb"
    assert s["mergeKey"]


def test_convert_article_maps_title_and_year():
    a = _ing()._convert_article({"article_title": "EU Variety Trial Reports", "year": 2025})
    assert a["articleTitle"] == "EU Variety Trial Reports"
    assert a["year"] == 2025
    assert a["source_id"] == "EU-TRIAL-REPORTS"


@pytest.mark.asyncio
async def test_eu_ingester_uses_base_merge_relationships():
    """After removing the override, EuTrialsIngester inherits the source-agnostic
    base implementation (no source_id filter on TrialSite)."""
    from app.ingestion.base_ingester import BaseIngester
    assert EuTrialsIngester._merge_relationships is BaseIngester._merge_relationships


@pytest.mark.asyncio
async def test_parse_nodes_routes_by_type():
    data = {"@graph": [
        {"@type": "ArticleSource", "article_title": "x", "year": 2025},
        {"@type": "TrialSite", "name": "S", "climateClass": "Dfb"},
        TRIAL,
    ]}
    nodes = await _ing()._parse_nodes(data)
    assert len(nodes["variety_trials"]) == 1
    assert len(nodes["trial_sites"]) == 1
    assert len(nodes["article_sources"]) == 1
    assert nodes["management_trials"] == []
