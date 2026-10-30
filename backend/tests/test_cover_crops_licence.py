"""Licence and provenance tests for cover_crops and removed ikerketa stubs."""

from __future__ import annotations

from app.kg.registries import DEFAULT_REGISTRIES_PATH, load_registries
from app.services import cover_crops

FORBIDDEN_TERMS = ("intia", "ifapa", "itacyl", "navarra agraria")


def test_every_cover_and_protein_parameter_uses_a_loadable_source():
    reg = load_registries(DEFAULT_REGISTRIES_PATH)
    loadable_ids = {s.source_id for s in reg.loadable_sources()}

    for catalog_name, catalog in (("COVER_CROPS", cover_crops.COVER_CROPS),
                                  ("PROTEIN_CROPS", cover_crops.PROTEIN_CROPS)):
        for eppo, entry in catalog.items():
            climates = entry.get("climates", {})
            assert climates, f"{catalog_name}[{eppo}] has no climates"
            for climate, params in climates.items():
                for param_name, spec in params.items():
                    assert isinstance(spec, dict), f"{catalog_name}[{eppo}][{climate}][{param_name}] must be a dict"
                    sid = spec.get("source_id")
                    assert sid in loadable_ids, (
                        f"{catalog_name}[{eppo}][{climate}][{param_name}] uses non-loadable source_id={sid!r}"
                    )
                    src_label = (spec.get("source") or "").lower()
                    for term in FORBIDDEN_TERMS:
                        assert term not in src_label, (
                            f"{catalog_name}[{eppo}][{climate}][{param_name}] references forbidden source {term!r}"
                        )


def test_scraped_enrichment_functions_are_removed():
    assert not hasattr(cover_crops, "_apply_scraped_enrichment")
    assert not hasattr(cover_crops, "get_trial_observations")
    assert not hasattr(cover_crops, "get_scraped_stats")


def test_select_cover_crops_returns_ranked_candidates():
    csa = cover_crops.select_cover_crops("Csa", management="any")
    bsk = cover_crops.select_cover_crops("BSk", management="organic")
    assert len(csa) >= 4
    assert len(bsk) >= 4
    suitable_csa = [c for c in csa if c["suitable"]]
    assert len(suitable_csa) >= 2
    assert suitable_csa[0]["target_biomass_t_ha"] >= suitable_csa[-1]["target_biomass_t_ha"]


def test_zombie_ikerketa_routes_are_removed():
    from app.api.v1.graph import router

    paths = {route.path for route in router.routes}
    assert "/protected-area-check" not in paths
    assert "/pollinators" not in paths
    assert "/terrain" not in paths
    assert "/climate-reference" not in paths
