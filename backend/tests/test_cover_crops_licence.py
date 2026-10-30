"""Licence and provenance tests for cover_crops and removed ikerketa stubs."""

from __future__ import annotations

from pathlib import Path

from app.services import cover_crops

FORBIDDEN_TERMS = ("intia", "ifapa", "itacyl", "navarra agraria")


def test_protein_crops_carry_identity_only():
    # No cited source publishes per-climate cover or legume parameters: the catalogue only says what the crop is.
    for eppo, entry in cover_crops.PROTEIN_CROPS.items():
        assert set(entry) == {"scientific", "common_name", "type"}, eppo
        assert entry["type"] == "legume", eppo


def test_no_per_climate_cover_parameters():
    for name in ("COVER_CROPS", "lookup", "select_cover_crops", "estimate_n_fixation", "estimate_dates",
                 "SOWING_WINDOWS", "TERMINATION_MONTH"):
        assert not hasattr(cover_crops, name), name


def test_module_names_no_restricted_source():
    text = Path(cover_crops.__file__).read_text(encoding="utf-8").lower()
    for term in FORBIDDEN_TERMS:
        assert term not in text, term


def test_scraped_enrichment_functions_are_removed():
    assert not hasattr(cover_crops, "_apply_scraped_enrichment")
    assert not hasattr(cover_crops, "get_trial_observations")
    assert not hasattr(cover_crops, "get_scraped_stats")


def test_zombie_ikerketa_routes_are_removed():
    from app.api.v1.graph import router

    paths = {route.path for route in router.routes}
    assert "/protected-area-check" not in paths
    assert "/pollinators" not in paths
    assert "/terrain" not in paths
    assert "/climate-reference" not in paths
