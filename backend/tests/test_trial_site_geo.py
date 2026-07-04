"""Tests for TrialSite geo registry resolution."""

from app.ingestion.trial_site_geo import (
    geo_updates_for_neo4j,
    is_aggregate_site_name,
    resolve_trial_site_geo,
)


def test_aggregate_detection():
    assert is_aggregate_site_name("Múltiples localidades")
    assert is_aggregate_site_name("Hungary (average)")
    assert not is_aggregate_site_name("Babilafuente")


def test_resolve_lfl_site_from_registry():
    entry = resolve_trial_site_geo("Freising")
    assert entry is not None
    assert entry["latitude"] == 48.4
    assert entry["geoConfidence"] == "chelsa_enriched"


def test_resolve_spanish_municipality():
    entry = resolve_trial_site_geo("Babilafuente")
    assert entry is not None
    assert entry["latitude"]
    assert entry["longitude"]


def test_geo_updates_for_neo4j_includes_climate():
    entry = {
        "latitude": 42.0,
        "longitude": -2.0,
        "climateClass": "BSk",
        "annualRainfallMm": 400,
        "geoConfidence": "chelsa_enriched",
        "geoSource": "test",
    }
    updates = geo_updates_for_neo4j(entry)
    assert updates["latitude"] == 42.0
    assert updates["climateClass"] == "BSk"
    assert updates["annualRainfallMm"] == 400


def test_research_station_gut_ving_osm_farm():
    entry = resolve_trial_site_geo("Gut Ving (Nörvenich)")
    assert entry is not None
    assert entry["latitude"] == 50.8336
    assert entry["geoConfidence"] == "research_station_osm_farm"


def test_research_station_haus_duesse_ostinghausen():
    entry = resolve_trial_site_geo("Haus Düsse (Ostinghausen)")
    assert entry is not None
    assert entry["latitude"] == 51.636
    assert entry["longitude"] == 8.198


def test_research_station_kerpen_buir():
    entry = resolve_trial_site_geo("Kerpen-Buir (Vettweiß)")
    assert entry is not None
    assert entry["latitude"] == 50.861
    assert entry["geoConfidence"] == "research_station_lfl_historical"


def test_bembeke_malawi_not_belgium():
    entry = resolve_trial_site_geo("Bembeke")
    assert entry is not None
    assert entry["latitude"] < 0
    assert entry["climateClass"] == "Cwb"
