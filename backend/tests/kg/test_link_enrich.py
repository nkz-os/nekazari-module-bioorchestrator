"""Link and enrich (plan task 8).

Pure tests pin the enrichment rules (field sites only, cache, offline, country check) and the
unlabelled-table site. Container tests (testcontainers Neo4j) pin the link verification. Nothing here
reaches CHELSA or a production graph: the reader is injected.
"""
# ruff: noqa: F811
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from app.kg import enrich, identity, link, loader
from app.kg.adapters import crea, genvce
from app.kg.contracts import ContractError, run_contract
from app.kg.model import SiteRow

from .test_loader import (  # noqa: F401  (fixtures and helpers of the loader tests, same event loop)
    CREA_CONTRACT,
    GENVCE_CONTRACT,
    REGISTRIES,
    _bundle,
    _q,
    _run,
    container,
    crea_bundle,
    db,
    driver,
    genvce_bundle,
    genvce_rows,
    needs_docker,
)

DATA = Path(__file__).resolve().parents[2] / "data" / "sources"
UNLABELLED = "ES-GENVCE-UNLABELLED"


# ═════════════════════════════════════════════════════════════════════════════
# the site of a table without a label
# ═════════════════════════════════════════════════════════════════════════════

def test_an_unlabelled_unit_is_placed_on_the_aggregate_and_keeps_its_key(genvce_bundle, genvce_rows):
    unlabelled = [u for u in genvce_bundle.units if u.raw_site is None]
    assert unlabelled and all(u.site_key == UNLABELLED for u in unlabelled)
    site = next(s for s in genvce_bundle.sites if s.site_id == UNLABELLED)
    assert (site.site_kind, site.country, site.latitude, site.longitude, site.climate_class) == (
        "aggregate", "ES", None, None, None)
    assert "without zone label" in site.name
    plain = GENVCE_CONTRACT.model_copy(update={"sites": GENVCE_CONTRACT.sites.model_copy(update={"unlabelled_site": None})})
    before = _bundle(plain, genvce_rows)
    assert sorted(identity.unit_key(u) for u in before.units) == sorted(identity.unit_key(u) for u in genvce_bundle.units)
    assert all(u.site_key is None for u in before.units if u.raw_site is None)


def test_the_unlabelled_site_of_a_contract_must_be_a_registered_aggregate(genvce_rows):
    for bad in ("ES-VALLADOLID", "ES-NOWHERE"):
        contract = GENVCE_CONTRACT.model_copy(update={
            "sites": GENVCE_CONTRACT.sites.model_copy(update={"unlabelled_site": bad})})
        with pytest.raises(ContractError, match="unlabelled_site"):
            _bundle(contract, genvce_rows)


def test_a_labelled_unit_never_gets_the_unlabelled_site(genvce_bundle):
    assert all(u.site_key != UNLABELLED for u in genvce_bundle.units if u.raw_site is not None)


# ═════════════════════════════════════════════════════════════════════════════
# enrichment, no network
# ═════════════════════════════════════════════════════════════════════════════

def _climate(koppen="Csa"):
    return {"koppen": koppen, "annual_temp_c": 15.0, "annual_rainfall_mm": 500.0, "annual_et0_mm": 1200.0,
            "coldest_month_min_c": 1.0, "source": "test"}


class _Reader:
    def __init__(self, result=None):
        self.calls: list[tuple[float, float]] = []
        self.result = _climate() if result is None else (result or None)

    async def __call__(self, lat, lon):
        self.calls.append((lat, lon))
        return self.result


def _field(site_id="ES-X", lat=41.65, lon=-4.72, country="ES"):
    return SiteRow(site_id=site_id, name=site_id, site_kind="field", country=country, latitude=lat, longitude=lon,
                   coordinate_source="test")


def _aggregate(site_id="ES-GENVCE-X"):
    return SiteRow(site_id=site_id, name=site_id, site_kind="aggregate", country="ES")


def test_aggregates_are_skipped_explicitly_even_when_one_carries_coordinates():
    reader = _Reader()
    sneaky = SiteRow.model_construct(site_id="ES-GENVCE-S", name="s", site_kind="aggregate", country="ES",
                                     latitude=41.65, longitude=-4.72, coordinate_source="x", climate_class=None,
                                     source_ids=(), gaps=())
    report = _run(enrich.enrich_sites([_aggregate(), sneaky, _field()], reader=reader))
    assert report.counts["skipped_aggregate"] == 2 and report.counts["enriched"] == 1
    assert list(report.enriched) == ["ES-X"] and len(reader.calls) == 1


def test_the_real_bundles_enrich_nothing_but_their_field_sites(genvce_bundle, crea_bundle):
    reader = _Reader()
    genvce_report = _run(enrich.enrich_sites(genvce_bundle.sites, reader=reader))
    assert genvce_report.counts["skipped_aggregate"] == len(genvce_bundle.sites) and not genvce_report.enriched
    crea_report = _run(enrich.enrich_sites(crea_bundle.sites, reader=reader))
    kinds = {s.site_kind for s in crea_bundle.sites}
    assert crea_report.counts["skipped_aggregate"] == sum(1 for s in crea_bundle.sites if s.site_kind != "field")
    assert "field" in kinds
    assert all(crea_bundle_site.site_kind == "field"
               for crea_bundle_site in crea_bundle.sites if crea_bundle_site.site_id in crea_report.enriched)


def test_a_field_site_without_coordinates_is_left_alone():
    site = SiteRow(site_id="ES-NOCOORD", name="n", site_kind="field", country="ES")
    reader = _Reader()
    report = _run(enrich.enrich_sites([site], reader=reader))
    assert report.counts["no_coordinates"] == 1 and not reader.calls and not report.enriched and report.ok


def test_the_cache_makes_a_second_run_offline_and_identical(tmp_path):
    cache = tmp_path / "cache" / "climate.json"
    first = _run(enrich.enrich_sites([_field()], cache_path=cache, reader=_Reader()))
    assert cache.exists() and first.counts["fetched"] == 1
    reader = _Reader()
    second = _run(enrich.enrich_sites([_field()], cache_path=cache, reader=reader))
    assert not reader.calls and second.counts["from_cache"] == 1 and second.enriched == first.enriched
    third = _run(enrich.enrich_sites([_field()], cache_path=cache, reader=reader, offline=True))
    assert third.enriched == first.enriched
    assert cache.read_text() == json.dumps(json.loads(cache.read_text()), sort_keys=True, indent=1) + "\n"


def test_offline_never_reads_and_a_miss_is_reported_not_guessed(tmp_path):
    reader = _Reader()
    report = _run(enrich.enrich_sites([_field()], cache_path=tmp_path / "c.json", reader=reader, offline=True))
    assert not reader.calls and not report.enriched and report.failed == ("ES-X",) and not report.ok


def test_a_failed_or_incomplete_read_is_not_cached(tmp_path):
    cache = tmp_path / "c.json"
    for bad in (None, {"koppen": None, "annual_rainfall_mm": None}):
        report = _run(enrich.enrich_sites([_field()], cache_path=cache, reader=_Reader(result=bad or {})))
        assert report.failed == ("ES-X",) and not cache.exists()


def test_a_raising_reader_fails_that_cell_only():
    class _Boom:
        async def __call__(self, lat, lon):
            if lat > 40:
                raise RuntimeError("down")
            return _climate()

    report = _run(enrich.enrich_sites([_field("ES-A", 41.65, -4.72), _field("ES-B", 37.88, -4.77)], reader=_Boom()))
    assert list(report.enriched) == ["ES-B"] and report.failed == ("ES-A",)


def test_a_country_that_disagrees_with_the_coordinates_is_reported_not_overwritten():
    report = _run(enrich.enrich_sites([_field(country="FR")], reader=_Reader()))
    assert "ES-X" in report.country_mismatch and "coordinates ES" in report.country_mismatch["ES-X"]


# ═════════════════════════════════════════════════════════════════════════════
# link and enrichment against a graph
# ═════════════════════════════════════════════════════════════════════════════

@needs_docker
def test_every_variety_trial_has_exactly_one_trial_at_after_a_load(db, genvce_bundle):
    _run(loader.load(genvce_bundle, db, registries=REGISTRIES))
    bad = _run(_q(db, "MATCH (v:VarietyTrial) WHERE size([(v)-[:TRIAL_AT]->() | 1]) <> 1 RETURN count(v) AS c"))
    assert bad[0]["c"] == 0
    report = _run(link.link(genvce_bundle, db, registries=REGISTRIES))
    assert report.ok and report.units == len(genvce_bundle.units)
    assert report.units_without_site == 0 and report.relinked["trial_at"] == 0


@needs_docker
def test_link_restores_deleted_relationships_and_creates_nothing_twice(db, genvce_bundle):
    _run(loader.load(genvce_bundle, db, registries=REGISTRIES))
    _run(_q(db, "MATCH (:ObservationUnit)-[r:TRIAL_AT|OF_VARIETY]->() WITH r LIMIT 5 DELETE r"))
    first = _run(link.link(genvce_bundle, db, registries=REGISTRIES))
    assert first.ok and first.relinked["trial_at"] + first.relinked["unit_variety"] == 5
    again = _run(link.link(genvce_bundle, db, registries=REGISTRIES))
    assert again.ok and not any(again.relinked.values())


@needs_docker
def test_link_reports_a_unit_with_a_second_site_and_deletes_nothing(db, genvce_bundle):
    _run(loader.load(genvce_bundle, db, registries=REGISTRIES))
    _run(_q(db, "MATCH (v:VarietyTrial) WITH v LIMIT 1 MERGE (s:TrialSite {siteKey: 'ES-STALE'}) MERGE (v)-[:TRIAL_AT]->(s)"))
    report = _run(link.link(genvce_bundle, db, registries=REGISTRIES))
    assert not report.ok and report.wrong_cardinality["site"] == 1 and "TRIAL_AT" in report.problems[0]
    assert _run(_q(db, "MATCH ()-[r:TRIAL_AT]->(:TrialSite {siteKey: 'ES-STALE'}) RETURN count(r) AS c"))[0]["c"] == 1


@needs_docker
def test_link_falls_back_to_the_registry_alias_for_an_observed_name(db, genvce_bundle):
    labelled = next(u for u in genvce_bundle.units if u.raw_site is not None)
    site = next(s for s in genvce_bundle.sites if s.site_id == labelled.site_key)
    stripped = genvce_bundle.model_copy(update={"units": tuple(
        u.model_copy(update={"site_key": None}) if u is labelled else u for u in genvce_bundle.units)})
    _run(loader.load(genvce_bundle, db, registries=REGISTRIES))
    _run(_q(db, "MATCH (v:ObservationUnit {unitKey: $k})-[r:TRIAL_AT]->() DELETE r", k=identity.unit_key(labelled)))
    assert site.name  # the alias lookup resolves the printed name
    report = _run(link.link(stripped, db, registries=REGISTRIES))
    assert report.ok and report.alias_resolved >= 1


@needs_docker
def test_link_reports_a_name_no_registry_resolves(db, genvce_bundle):
    labelled = next(u for u in genvce_bundle.units if u.raw_site is not None)
    broken = genvce_bundle.model_copy(update={"units": tuple(
        u.model_copy(update={"site_key": None}) if u is labelled else u for u in genvce_bundle.units)})

    class _NoSites:  # a registry that knows no site name
        def site(self, name):
            return None

    _run(loader.load(genvce_bundle, db, registries=REGISTRIES))
    report = _run(link.link(broken, db, registries=_NoSites()))
    assert report.unresolved_sites == {labelled.raw_site: 1} and not report.ok


@needs_docker
def test_an_enrichment_reaches_field_sites_only_and_survives_a_reload(db, crea_bundle):
    _run(loader.load(crea_bundle, db, registries=REGISTRIES))
    field_site = next(s for s in crea_bundle.sites if s.site_kind == "field")
    sited = field_site.model_copy(update={"latitude": 45.0, "longitude": 7.5, "coordinate_source": "test"})
    report = _run(enrich.enrich_sites([sited], reader=_Reader()))
    _run(_q(db, "MATCH (n:TrialSite {siteKey: $k}) SET n.latitude = 45.0, n.longitude = 7.5", k=field_site.site_id))
    assert _run(enrich.apply_enrichment(report, db)) == 1
    row = _run(_q(db, "MATCH (n:TrialSite {siteKey: $k}) RETURN n.climateClass AS c, n.climateClassChelsa AS cc, "
                      "n.climateChelsaCellKey AS ck", k=field_site.site_id))[0]
    assert row["c"] == row["cc"] == "Csa" and row["ck"]
    _run(loader.load(crea_bundle, db, registries=REGISTRIES))
    assert _run(_q(db, "MATCH (n:TrialSite {siteKey: $k}) RETURN n.climateClass AS c", k=field_site.site_id))[0]["c"] == "Csa"


@needs_docker
def test_the_write_never_touches_an_aggregate(db, genvce_bundle):
    _run(loader.load(genvce_bundle, db, registries=REGISTRIES))
    aggregate = next(s for s in genvce_bundle.sites if s.site_kind == "aggregate")
    forged = enrich.EnrichReport(
        enriched={aggregate.site_id: enrich.SiteEnrichment(aggregate.site_id, "k", "Csa", 1.0, 1.0, 1.0, 1.0, "t")},
        counts={}, country_mismatch={})
    with pytest.raises(RuntimeError, match="matched 0 of 1"):
        _run(enrich.apply_enrichment(forged, db))
    assert _run(_q(db, "MATCH (n:TrialSite) WHERE n.climateClass IS NOT NULL RETURN count(n) AS c"))[0]["c"] == 0


RAW_REPO = os.environ.get("NKZ_DATA_SOURCES_DIR", "")


@needs_docker
@pytest.mark.skipif(not RAW_REPO, reason="set NKZ_DATA_SOURCES_DIR to the raw-data repository to run")
def test_the_real_bundles_link_completely_and_enrich_only_field_sites(db):
    bundles = []
    for adapter, contract in ((genvce, GENVCE_CONTRACT), (crea, CREA_CONTRACT)):
        bundle = run_contract(contract, REGISTRIES, adapter.load(Path(RAW_REPO) / contract.source_id.lower()).rows)
        _run(loader.load(bundle, db, registries=REGISTRIES))
        bundles.append(bundle)
    assert _run(_q(db, "MATCH (v:VarietyTrial) WHERE size([(v)-[:TRIAL_AT]->() | 1]) <> 1 RETURN count(v) AS c"))[0]["c"] == 0
    for bundle in bundles:
        report = _run(link.link(bundle, db, registries=REGISTRIES))
        assert report.ok and report.units_without_site == 0 and not any(report.relinked.values())
    reader = _Reader()
    sites = [s for b in bundles for s in b.sites]
    report = _run(enrich.enrich_sites(sites, reader=reader))
    assert report.counts["skipped_aggregate"] == sum(1 for s in sites if s.site_kind != "field")
    assert all(s.site_kind == "field" for s in sites if s.site_id in report.enriched)
