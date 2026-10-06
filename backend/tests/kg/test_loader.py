"""Loader (plan task 7): bundles into Neo4j.

Pure tests (no database) pin the closure checks and the legacy copy of a unit. Container tests
(testcontainers Neo4j 5.26, skipped without Docker) pin idempotence, the cardinalities, batch-size
independence and the memory bound. Nothing here touches a production graph.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import shutil
from pathlib import Path
from unittest import mock

import pytest
from testcontainers.neo4j import Neo4jContainer

from app.ingestion.genvce_ingester import GenvceIngester
from app.kg import identity, loader
from app.kg.adapters import crea, genvce
from app.kg.contracts import BuildReport, Bundle, load_contract, run_contract
from app.kg.existing_graph import Neo4jExistingGraph
from app.kg.gate import run_gate, unit_content_key
from app.kg.migrations import apply_migrations
from app.kg.model import UnitRow
from app.kg.registries import load_registries
from neo4j import AsyncGraphDatabase, GraphDatabase

DATA = Path(__file__).resolve().parents[2] / "data" / "sources"
FIXTURES = Path(__file__).parent / "fixtures"
REGISTRIES = load_registries()
GENVCE_CONTRACT = load_contract(DATA / "GENVCE.yaml")
CREA_CONTRACT = load_contract(DATA / "CREA.yaml")

_loop = asyncio.new_event_loop()


def _run(coro):
    return _loop.run_until_complete(coro)


def _bundle(contract, rows) -> Bundle:
    """Build a bundle from raw rows; the contract's expected counts belong to the whole raw data."""
    with mock.patch("app.kg.contracts._check_expected"):
        return run_contract(contract, REGISTRIES, rows)


@pytest.fixture(scope="module")
def genvce_rows():
    return genvce.load(FIXTURES / "genvce").rows


@pytest.fixture(scope="module")
def genvce_bundle(genvce_rows) -> Bundle:
    return _bundle(GENVCE_CONTRACT, genvce_rows)


@pytest.fixture(scope="module")
def crea_bundle() -> Bundle:
    return _bundle(CREA_CONTRACT, crea.load(FIXTURES / "crea").rows)


# ═════════════════════════════════════════════════════════════════════════════
# closure of the bundle, no database
# ═════════════════════════════════════════════════════════════════════════════

def _without(bundle: Bundle, **changes) -> Bundle:
    return bundle.model_copy(update=changes)


def test_the_plan_of_a_real_bundle_is_closed_and_sorted(genvce_bundle):
    plan = loader._plan(genvce_bundle, REGISTRIES)
    assert sum(len(v) for v in plan.units_by_label.values()) == len(genvce_bundle.units)
    assert len(plan.observations) == len(genvce_bundle.observations)
    assert [o["obsKey"] for o in plan.observations] == sorted(o["obsKey"] for o in plan.observations)
    assert set(plan.units_by_label) == {"VarietyTrial"}


def test_a_bundle_built_with_other_registries_is_refused(genvce_bundle):
    stale = _without(genvce_bundle, report=genvce_bundle.report.model_copy(update={"registries_hash": "0" * 64}))
    with pytest.raises(loader.LoadError, match="other registries"):
        loader._plan(stale, REGISTRIES)


def test_a_unit_naming_a_missing_document_is_refused(genvce_bundle):
    victim = identity.document_key(genvce_bundle.documents[0])
    kept = tuple(d for d in genvce_bundle.documents if identity.document_key(d) != victim)
    with pytest.raises(loader.LoadError, match="document"):
        loader._plan(_without(genvce_bundle, documents=kept), REGISTRIES)


def test_a_unit_naming_a_missing_site_variety_or_study_is_refused(genvce_bundle):
    with pytest.raises(loader.LoadError, match="site"):
        loader._plan(_without(genvce_bundle, sites=()), REGISTRIES)
    with pytest.raises(loader.LoadError, match="variety"):
        loader._plan(_without(genvce_bundle, varieties=()), REGISTRIES)
    with pytest.raises(loader.LoadError, match="no study"):
        loader._plan(_without(genvce_bundle, studies=()), REGISTRIES)


def test_an_observation_on_a_missing_unit_is_refused(genvce_bundle):
    orphan = genvce_bundle.observations[0].model_copy(update={"unit_key": "f" * 64})
    with pytest.raises(loader.LoadError, match="lacks"):
        loader._plan(_without(genvce_bundle, observations=(*genvce_bundle.observations, orphan)), REGISTRIES)


def test_a_duplicate_row_is_refused(genvce_bundle):
    with pytest.raises(loader.LoadError, match="duplicate unit"):
        loader._plan(_without(genvce_bundle, units=(*genvce_bundle.units, genvce_bundle.units[0])), REGISTRIES)


def test_a_row_of_another_source_is_refused(genvce_bundle):
    alien = genvce_bundle.units[0].model_copy(update={"source_id": "CREA"})
    with pytest.raises(loader.LoadError, match="source 'CREA'"):
        loader._plan(_without(genvce_bundle, units=(alien, *genvce_bundle.units[1:])), REGISTRIES)


def test_a_yield_that_disagrees_with_its_observation_is_refused(genvce_bundle):
    unit = next(u for u in genvce_bundle.units if u.yield_kg_ha is not None)
    obs = [o for o in genvce_bundle.observations if o.unit_key == identity.unit_key(unit)]
    bad = unit.model_copy(update={"yield_kg_ha": unit.yield_kg_ha + 1})
    with pytest.raises(loader.LoadError, match="disagree"):
        loader.unit_properties(bad, obs, REGISTRIES)


def test_a_flagged_variable_without_a_unit_property_is_refused(genvce_bundle):
    patched = {k: v for k, v in loader.DENORMALIZED_COPIES.items() if k != "relative_yield_pct"}
    with mock.patch.object(loader, "DENORMALIZED_COPIES", patched), pytest.raises(loader.LoadError, match="flagged denormalize"):
        loader._plan(genvce_bundle, REGISTRIES)


def test_two_observations_of_a_copied_variable_are_not_copyable(genvce_bundle):
    unit = next(u for u in genvce_bundle.units if u.yield_kg_ha is not None)
    key = identity.unit_key(unit)
    obs = [o for o in genvce_bundle.observations if o.unit_key == key]
    rel = next(o for o in obs if o.variable_id == "relative_yield_pct")
    twin = rel.model_copy(update={"qualifier": "other", "value": 1.0, "value_original": 1.0})
    with pytest.raises(loader.LoadError, match="exactly one"):
        loader.unit_properties(unit, [*obs, twin], REGISTRIES)


def test_the_batch_size_must_be_positive(genvce_bundle):
    with pytest.raises(ValueError, match="batch_size"):
        _run(loader.load(genvce_bundle, object(), batch_size=0))


# ═════════════════════════════════════════════════════════════════════════════
# the denormalized copy and the legacy properties equal the old ingester's output, no database
# ═════════════════════════════════════════════════════════════════════════════

IGNORED_BY_CONTRACT = {entry.field.rsplit(".", 1)[-1] for entry in GENVCE_CONTRACT.ignore}


def _old_properties(rows) -> list[dict]:
    """What the old GENVCE ingester wrote for each adapter row (convert, then normalise)."""
    ingester = GenvceIngester()
    nodes = [{
        "@type": "VarietyTrial", "crop_eppo": REGISTRIES.crop(row["crop"]).eppo, "variety": row["variety"],
        "year": int(row["season"]) if row["season"].isdigit() else None, "yield_kg_ha": row["yield_kg_ha"],
        "yield_relative_pct": row["yield_relative_pct"], "irrigation_regime": row["irrigation"],
        "production_system": row["production_system"], "trial_location": None,
        "quality_params": row.get("quality_params"), "disease_scores": row.get("disease_scores"),
    } for row in rows]
    converted = [ingester._convert_trial(node) for node in nodes]
    normalised = _run(ingester.normalize_nodes({"variety_trials": converted}))
    return normalised["variety_trials"]


def _unified(text: str | None) -> dict:
    """The unified map without the entries of a value the source never gave, or whose key the contract ignores."""
    return {k: v for k, v in (json.loads(text) if text else {}).items()
            if v["rawValue"] is not None and v["sourceKey"] not in IGNORED_BY_CONTRACT}


def _unit_for(bundle: Bundle, row) -> UnitRow:
    eppo = REGISTRIES.crop(row["crop"]).eppo
    hits = [u for u in bundle.units if (u.crop_eppo, u.raw_variety, u.row_discriminator, u.raw_site)
            == (eppo, row["variety"], row["table"]["number"], row["zone"])]
    assert len(hits) == 1, (row["variety"], row["table"], row["zone"])
    return hits[0]


def test_the_legacy_properties_equal_the_old_ingesters_output(genvce_bundle, genvce_rows):
    by_unit = {}
    for obs in genvce_bundle.observations:
        by_unit.setdefault(obs.unit_key, []).append(obs)
    old_rows = _old_properties(genvce_rows)
    compared = {"yield": 0, "relative": 0, "quality": 0, "disease": 0, "unified": 0, "irrigation": 0, "derived": 0, "year": 0}
    for row, old in zip(genvce_rows, old_rows, strict=True):
        unit = _unit_for(genvce_bundle, row)
        new = loader.unit_properties(unit, by_unit.get(identity.unit_key(unit), ()), REGISTRIES)
        assert (new["cropEppo"], new["cropScientific"], new["variety"], new["varietyNormalized"]) == (
            old["cropEppo"], old["cropScientific"], old["variety"], old["varietyNormalized"])
        assert new["yieldKgHa"] == old.get("yieldKgHa")
        assert new["yieldRelativePct"] == old.get("yieldRelativePct")
        compared["yield"] += new["yieldKgHa"] is not None
        compared["relative"] += new["yieldRelativePct"] is not None
        if new["year"] is not None:
            assert new["year"] == old["year"]
            compared["year"] += 1
        # the old value is a literal, the new one its vocabulary's stored form
        if unit.irrigation_derivation is None:
            assert new["irrigationRegime"] == REGISTRIES.vocab("irrigation", old.get("irrigationRegime"))
            compared["irrigation"] += new["irrigationRegime"] is not None
        else:  # deliberate difference: the yield cutoff fills a regime where the source is silent
            assert old.get("irrigationRegime") is None
            compared["derived"] += 1
        assert new["productionSystem"] == REGISTRIES.vocab("production_system", old.get("productionSystem"))
        for legacy, old_name in (("qualityParams", "qualityParams"), ("diseaseScores", "diseaseScores")):
            old_map = json.loads(old[old_name]) if old.get(old_name) else {}
            new_map = json.loads(new[legacy]) if new[legacy] else {}
            # a null the old text carried is a value the source did not give: absent now, by design
            assert {k: v for k, v in old_map.items() if k not in IGNORED_BY_CONTRACT and v is not None} == new_map, legacy
            compared["quality" if legacy == "qualityParams" else "disease"] += bool(new_map)
        assert _unified(new["diseaseScoresUnified"]) == _unified(old.get("diseaseScoresUnified"))
        compared["unified"] += new["diseaseScoresUnified"] is not None
    # the comparison was not vacuous
    assert all(count > 0 for count in compared.values()), compared


def test_the_crea_legacy_properties_equal_the_old_ingesters_output(crea_bundle):
    from app.ingestion.crea_ingester import CreaIngester

    rows = crea.load(FIXTURES / "crea").rows
    ingester = CreaIngester()
    by_unit = {}
    for obs in crea_bundle.observations:
        by_unit.setdefault(obs.unit_key, []).append(obs)
    for row in rows:
        old = ingester._convert_trial({
            "crop_eppo": row["crop"], "variety": row["variety"], "year": int(row["season"]),
            "yield_kg_ha": row["yield_kg_ha"], "irrigation_regime": row["irrigation"],
            "trial_location": row["site"]})
        unit = next(u for u in crea_bundle.units if (u.raw_variety, u.raw_site, u.raw_season)
                    == (row["variety"], row["site"], row["season"]))
        new = loader.unit_properties(unit, by_unit.get(identity.unit_key(unit), ()), REGISTRIES)
        assert (new["cropEppo"], new["variety"], new["year"], new["yieldKgHa"], new["trialLocation"]) == (
            old["cropEppo"], old["variety"], old["year"], old.get("yieldKgHa"), old["trialLocation"])
        assert new["source_id"] == old["source_id"] == "CREA"


def test_the_copy_of_a_unit_comes_from_its_observations_and_clears_what_is_absent(genvce_bundle):
    template = next(u for u in genvce_bundle.units if u.yield_kg_ha is not None)
    plain = template.model_copy(update=dict.fromkeys((
        "yield_kg_ha", "yield_metric", "yield_basis", "yield_moisture_pct", "yield_value_original",
        "yield_unit_original", "derivation_method", "irrigation_derivation", "irrigation_yield_low_kg_ha",
        "irrigation_yield_high_kg_ha")))
    props = loader.unit_properties(plain, [], REGISTRIES)
    for name in ("yieldKgHa", "yieldMetric", "yieldBasis", "yieldMoisturePct", "yieldRelativePct",
                 "qualityParams", "diseaseScores", "agronomicTraits"):
        assert props[name] is None, name
    yielding = next(u for u in genvce_bundle.units if u.yield_kg_ha is not None)
    obs = [o for o in genvce_bundle.observations if o.unit_key == identity.unit_key(yielding)]
    props = loader.unit_properties(yielding, obs, REGISTRIES)
    assert props["yieldKgHa"] == yielding.yield_kg_ha
    assert (props["yieldMetric"], props["yieldBasis"], props["yieldMoisturePct"]) == (
        yielding.yield_metric, yielding.yield_basis, yielding.yield_moisture_pct)
    assert props["mergeKey"] == props["unitKey"] == identity.unit_key(yielding)
    assert props["dataSource"] == props["source_id"] == "GENVCE"


def test_the_derived_irrigation_regime_and_its_thresholds_travel_with_the_unit(genvce_bundle):
    derived = [u for u in genvce_bundle.units if u.irrigation_derivation is not None]
    assert derived, "fixture has no derived regime: the test would be vacuous"
    unit = derived[0]
    props = loader.unit_properties(unit, [o for o in genvce_bundle.observations if o.unit_key == identity.unit_key(unit)],
                                   REGISTRIES)
    assert props["irrigationDerivation"] == unit.irrigation_derivation
    assert (props["irrigationYieldLowKgHa"], props["irrigationYieldHighKgHa"]) == (
        unit.irrigation_yield_low_kg_ha, unit.irrigation_yield_high_kg_ha)
    assert props["rawIrrigation"] is None


# ═════════════════════════════════════════════════════════════════════════════
# container tests
# ═════════════════════════════════════════════════════════════════════════════

needs_docker = pytest.mark.skipif(shutil.which("docker") is None, reason="docker unavailable")


@pytest.fixture(scope="module")
def container():
    if shutil.which("docker") is None:
        pytest.skip("docker unavailable")
    with Neo4jContainer("neo4j:5.26-community", password="testpassword") as n:
        yield n


@pytest.fixture(scope="module")
def driver(container):
    d = AsyncGraphDatabase.driver(container.get_connection_url(), auth=(container.username, container.password))
    _run(apply_migrations(d))
    yield d
    _run(d.close())


@pytest.fixture(scope="module")
def sync_driver(container):
    """A synchronous driver on the same server, for the read-only ExistingGraph reader."""
    d = GraphDatabase.driver(container.get_connection_url(), auth=(container.username, container.password))
    yield d
    d.close()


async def _q(d, cypher: str, **params) -> list[dict]:
    async with d.session() as s:
        res = await s.run(cypher, **params)
        return [dict(r) async for r in res]


@pytest.fixture
def db(driver):
    _run(_q(driver, "MATCH (n) WHERE NOT n:SchemaVersion DETACH DELETE n"))
    return driver


def _counts(d) -> dict:
    nodes = _run(_q(d, "MATCH (n) WHERE NOT n:SchemaVersion UNWIND labels(n) AS l RETURN l, count(*) AS c ORDER BY l"))
    rels = _run(_q(d, "MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c ORDER BY t"))
    return {"nodes": {r["l"]: r["c"] for r in nodes}, "rels": {r["t"]: r["c"] for r in rels}}


def _fingerprint(d) -> str:
    """Hash of every node's labels and properties and every relationship, order-independent."""
    nodes = _run(_q(d, "MATCH (n) WHERE NOT n:SchemaVersion RETURN labels(n) AS l, properties(n) AS p"))
    rels = _run(_q(d, "MATCH (a)-[r]->(b) RETURN labels(a) AS la, coalesce(a.unitKey, a.obsKey, a.documentKey, a.studyKey, "
                      "a.varietyKey, a.siteKey, a.sourceId, a.eppo) AS a, type(r) AS t, "
                      "coalesce(b.unitKey, b.obsKey, b.documentKey, b.studyKey, b.varietyKey, b.siteKey, b.sourceId, "
                      "b.eppo, b.variableId) AS b"))
    lines = sorted(json.dumps([sorted(r["l"]), r["p"]], sort_keys=True, default=str) for r in nodes)
    lines += sorted(json.dumps([r["a"], r["t"], r["b"]], default=str) for r in rels)
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()


@needs_docker
def test_a_load_writes_every_row_and_a_second_load_creates_nothing(db, genvce_bundle):
    first = _run(loader.load(genvce_bundle, db, registries=REGISTRIES))
    counts = _counts(db)
    assert counts["nodes"]["ObservationUnit"] == counts["nodes"]["VarietyTrial"] == len(genvce_bundle.units)
    assert counts["nodes"]["Observation"] == len(genvce_bundle.observations)
    assert counts["nodes"]["TrialSite"] == len(genvce_bundle.sites)
    assert counts["nodes"]["ArticleSource"] == len(genvce_bundle.documents)
    assert counts["nodes"]["Study"] == len(genvce_bundle.studies)
    assert counts["nodes"]["Variety"] == len(genvce_bundle.varieties)
    assert counts["nodes"]["Source"] == 1
    assert first.nodes_created == sum(counts["nodes"].values()) - counts["nodes"]["VarietyTrial"]  # a label, not a node
    before = _fingerprint(db)

    second = _run(loader.load(genvce_bundle, db, registries=REGISTRIES))
    assert second.nodes_created == second.relationships_created == 0
    assert _counts(db) == counts
    assert _fingerprint(db) == before


@needs_docker
def test_the_relationship_cardinalities_are_as_expected(db, genvce_bundle):
    _run(loader.load(genvce_bundle, db, registries=REGISTRIES))
    units = len(genvce_bundle.units)
    with_site = sum(1 for u in genvce_bundle.units if u.site_key)
    rels = _counts(db)["rels"]
    assert rels["IN_STUDY"] == rels["OF_VARIETY"] == rels["SOURCED_FROM"] == units
    assert rels["TRIAL_AT"] == with_site
    assert rels["ON_UNIT"] == rels["OF_VARIABLE"] == len(genvce_bundle.observations)
    assert rels["PART_OF"] == len(genvce_bundle.documents) + len(genvce_bundle.studies)
    # every unit has exactly one of each mandatory link, at most one site
    for rel, bound in (("IN_STUDY", 1), ("OF_VARIETY", 1), ("SOURCED_FROM", 1), ("OF_CROP", 1)):
        rows = _run(_q(db, f"MATCH (u:ObservationUnit) OPTIONAL MATCH (u)-[r:{rel}]->() "
                           "WITH u, count(r) AS n WHERE n <> $bound RETURN count(u) AS bad", bound=bound))
        assert rows[0]["bad"] == 0, rel
    bad = _run(_q(db, "MATCH (u:ObservationUnit) OPTIONAL MATCH (u)-[r:TRIAL_AT]->() "
                      "WITH u, count(r) AS n WHERE n > 1 RETURN count(u) AS bad"))
    assert bad[0]["bad"] == 0
    # no observation without its unit and variable, nothing dangling the other way
    assert _run(_q(db, "MATCH (o:Observation) WHERE NOT (o)-[:ON_UNIT]->() OR NOT (o)-[:OF_VARIABLE]->() "
                       "RETURN count(o) AS bad"))[0]["bad"] == 0


@needs_docker
def test_the_unit_nodes_carry_the_legacy_properties_dao_reads(db, genvce_bundle):
    _run(loader.load(genvce_bundle, db, registries=REGISTRIES))
    unit = next(u for u in genvce_bundle.units if u.yield_kg_ha is not None and u.site_key)
    row = _run(_q(db, "MATCH (vt:VarietyTrial {mergeKey: $k})-[:TRIAL_AT]->(ts:TrialSite) "
                      "RETURN vt, ts.name AS site, ts.siteKind AS kind", k=identity.unit_key(unit)))[0]
    vt = row["vt"]
    assert (vt["cropEppo"], vt["variety"], vt["yieldKgHa"], vt["source_id"], vt["dataSource"]) == (
        unit.crop_eppo, unit.raw_variety, unit.yield_kg_ha, "GENVCE", "GENVCE")
    assert vt["yieldMetric"] == unit.yield_metric and vt["yieldBasis"] == unit.yield_basis
    assert row["kind"] == "aggregate"
    # the existing evidence-policy readers run unchanged on it
    from app.graph import evidence_policy as ep

    rows = _run(_q(db, f"MATCH (vt:VarietyTrial)-[:TRIAL_AT]->(ts:TrialSite) WHERE {ep.cypher_numeric_candidate('vt')} "
                       "RETURN count(vt) AS c"))
    assert rows[0]["c"] > 0


@needs_docker
@pytest.mark.parametrize("batch_size", [1, 7])
def test_the_batch_size_never_changes_the_graph(db, genvce_bundle, batch_size):
    _run(loader.load(genvce_bundle, db, registries=REGISTRIES))
    reference = _fingerprint(db)
    _run(_q(db, "MATCH (n) WHERE NOT n:SchemaVersion DETACH DELETE n"))
    report = _run(loader.load(genvce_bundle, db, batch_size=batch_size, registries=REGISTRIES))
    assert _fingerprint(db) == reference
    assert report.step("observation").batches == -(-len(genvce_bundle.observations) // batch_size)


@needs_docker
def test_a_refused_bundle_leaves_the_graph_untouched(db, genvce_bundle):
    with pytest.raises(loader.LoadError):
        _run(loader.load(_without(genvce_bundle, sites=()), db, registries=REGISTRIES))
    assert _counts(db) == {"nodes": {}, "rels": {}}


@needs_docker
def test_a_site_keeps_a_climate_class_an_enrichment_wrote_and_accumulates_sources(db, genvce_bundle):
    _run(loader.load(genvce_bundle, db, registries=REGISTRIES))
    site = genvce_bundle.sites[0]
    _run(_q(db, "MATCH (t:TrialSite {siteKey: $k}) SET t.climateClass = 'Csa', t.sourceIds = ['OTHER']",
            k=identity.site_key(site)))
    _run(loader.load(genvce_bundle, db, registries=REGISTRIES))
    row = _run(_q(db, "MATCH (t:TrialSite {siteKey: $k}) RETURN t.climateClass AS c, t.sourceIds AS s, t.source_id AS sid",
                  k=identity.site_key(site)))[0]
    assert row["c"] == "Csa" and row["s"] == ["OTHER", "GENVCE"] and row["sid"] == "OTHER"


@needs_docker
def test_two_sources_load_side_by_side(db, genvce_bundle, crea_bundle):
    _run(loader.load(genvce_bundle, db, registries=REGISTRIES))
    _run(loader.load(crea_bundle, db, registries=REGISTRIES))
    counts = _counts(db)
    assert counts["nodes"]["Source"] == 2
    assert counts["nodes"]["ObservationUnit"] == len(genvce_bundle.units) + len(crea_bundle.units)
    assert counts["nodes"]["Crop"] >= 2
    # the shared crop is one node
    assert _run(_q(db, "MATCH (c:Crop {eppo: 'ZEAMX'}) RETURN count(c) AS c"))[0]["c"] == 1


@needs_docker
def test_phenology_stages_get_their_species_name_once(db):
    _run(_q(db, "CREATE (:Species {name: 'Zea mays'})-[:HAS_STAGE]->(:PhenologyStage {name: 'V6'}), "
                "(:Species {name: 'Triticum aestivum'})-[:HAS_STAGE]->(:PhenologyStage {name: 'V6'}), "
                "(:Species {name: 'A'})-[:HAS_STAGE]->(sh:PhenologyStage {name: 'shared'}), "
                "(:Species {name: 'B'})-[:HAS_STAGE]->(sh)"))
    assert _run(loader.stamp_phenology_species_name(db)) == 2
    rows = _run(_q(db, "MATCH (s:Species)-[:HAS_STAGE]->(st:PhenologyStage) "
                       "RETURN s.name AS sp, st.name AS n, st.speciesName AS stamped ORDER BY sp"))
    assert {(r["sp"], r["stamped"]) for r in rows if r["n"] == "V6"} == {("Zea mays", "Zea mays"),
                                                                       ("Triticum aestivum", "Triticum aestivum")}
    assert all(r["stamped"] is None for r in rows if r["n"] == "shared")  # two species: not guessed
    assert _run(loader.stamp_phenology_species_name(db)) == 0


@needs_docker
@pytest.mark.parametrize("page_size", [1000, 5])
def test_the_graph_reader_returns_the_gates_own_identities_in_pages(db, sync_driver, genvce_bundle, page_size):
    _run(loader.load(genvce_bundle, db, registries=REGISTRIES))
    expected = {(identity.unit_key(u), unit_content_key(u)) for u in genvce_bundle.units}
    reader = Neo4jExistingGraph(sync_driver, page_size=page_size)
    got = list(reader.unit_identities("GENVCE"))
    assert len(got) == len(genvce_bundle.units) and set(got) == expected
    assert [k for k, _ in got] == sorted(k for k, _ in got)  # keyset order
    assert list(reader.unit_identities("CREA")) == []


@needs_docker
def test_the_gate_flags_a_content_duplicate_against_the_real_graph_and_the_reader_writes_nothing(
        db, sync_driver, genvce_bundle):
    _run(loader.load(genvce_bundle, db, registries=REGISTRIES))
    before = _fingerprint(db)
    reader = Neo4jExistingGraph(sync_driver)
    same = run_gate(genvce_bundle, REGISTRIES, "local-test", existing=reader)
    assert same.content_duplicate_check == "run"
    assert not any(w.rule == "content_duplicate_in_graph" for w in same.warnings)
    # the same content printed in another table of the report: another key, the same content key
    unit = next(u for u in genvce_bundle.units if u.yield_kg_ha is not None)
    twin = unit.model_copy(update={"row_discriminator": "another-table"})
    twin_obs = tuple(o.model_copy(update={"unit_key": identity.unit_key(twin)}) for o in genvce_bundle.observations
                     if o.unit_key == identity.unit_key(unit))
    probe = genvce_bundle.model_copy(update={"units": (twin,), "observations": twin_obs})
    report = run_gate(probe, REGISTRIES, "local-test", existing=reader)
    assert [w.count for w in report.warnings if w.rule == "content_duplicate_in_graph"] == [1]
    assert _fingerprint(db) == before


def test_the_reader_statement_has_no_write_clause():
    # a READ session is only a routing hint on a single server, so the statement itself must be read-only
    from app.kg import existing_graph

    assert not re.search(r"\b(CREATE|MERGE|SET|DELETE|REMOVE|DROP|CALL|LOAD)\b", existing_graph._PAGE, re.IGNORECASE)


# ═════════════════════════════════════════════════════════════════════════════
# the real bundles (opt-in: the private raw-data repository)
# ═════════════════════════════════════════════════════════════════════════════

RAW_REPO = os.environ.get("NKZ_DATA_SOURCES_DIR", "")


@needs_docker
@pytest.mark.skipif(not RAW_REPO, reason="set NKZ_DATA_SOURCES_DIR to the raw-data repository to run")
def test_the_real_bundles_load_complete_and_a_reload_changes_nothing(db, sync_driver):
    bundles = {}
    for source_id, adapter, contract in (("GENVCE", genvce, GENVCE_CONTRACT), ("CREA", crea, CREA_CONTRACT)):
        bundles[source_id] = run_contract(contract, REGISTRIES, adapter.load(Path(RAW_REPO) / source_id.lower()).rows)
        _run(loader.load(bundles[source_id], db, registries=REGISTRIES))
    units = sum(len(b.units) for b in bundles.values())
    observations = sum(len(b.observations) for b in bundles.values())
    counts = _counts(db)
    assert counts["nodes"]["VarietyTrial"] == units == 4182
    assert counts["nodes"]["Observation"] == observations == 22410
    assert counts["rels"]["ON_UNIT"] == observations
    # every yield on a unit is the yield of its crop_yield Observation, and none is lost
    rows = _run(_q(db, "MATCH (o:Observation {variableId: 'crop_yield'})-[:ON_UNIT]->(u:ObservationUnit) "
                       "WHERE u.yieldKgHa IS NULL OR u.yieldKgHa <> o.value RETURN count(u) AS bad"))
    assert rows[0]["bad"] == 0
    with_yield = sum(1 for b in bundles.values() for u in b.units if u.yield_kg_ha is not None)
    assert _run(_q(db, "MATCH (u:VarietyTrial) WHERE u.yieldKgHa IS NOT NULL RETURN count(u) AS c"))[0]["c"] == with_yield
    before = _fingerprint(db)
    for bundle in bundles.values():
        again = _run(loader.load(bundle, db, registries=REGISTRIES))
        assert again.nodes_created == again.relationships_created == 0
    assert _fingerprint(db) == before
    reader = Neo4jExistingGraph(sync_driver)
    assert sum(1 for _ in reader.unit_identities("GENVCE")) == len(bundles["GENVCE"].units)


# ═════════════════════════════════════════════════════════════════════════════
# memory: 20k units under a 256 MiB transaction limit
# ═════════════════════════════════════════════════════════════════════════════

SYNTHETIC_UNITS = 20_000


def _synthetic_bundle(n: int) -> Bundle:
    """``n`` CREA-like units, two observations each (yield and relative yield), one study, one document."""
    base = _bundle(CREA_CONTRACT, crea.load(FIXTURES / "crea").rows)
    doc = base.documents[0]
    study = next(s for s in base.studies if s.study_type == "variety" and s.crop_eppo == "ZEAMX")
    site = next(s for s in base.sites if s.site_kind == "field")
    variety = base.varieties[0]
    template = next(u for u in base.units if u.yield_kg_ha is not None and u.site_key == site.site_id)
    t_yield = next(o for o in base.observations if o.unit_key == identity.unit_key(template) and o.variable_id == "crop_yield")
    t_rel = next((o for o in base.observations if o.unit_key == identity.unit_key(template)
                  and o.variable_id == "relative_yield_pct"), None)
    units, observations = [], []
    for i in range(n):
        unit = template.model_copy(update={
            "document_key": identity.document_key(doc), "study_key": identity.study_key(study),
            "variety_key": identity.variety_key(variety), "raw_variety": variety.name,
            "row_discriminator": f"synthetic-{i}"})
        key = identity.unit_key(unit)
        units.append(unit)
        observations.append(t_yield.model_copy(update={"unit_key": key}))
        if t_rel is not None:
            observations.append(t_rel.model_copy(update={"unit_key": key}))
    return Bundle(
        source_id=base.source_id, documents=(doc,), studies=(study,), sites=(site,), varieties=(variety,),
        units=tuple(units), observations=tuple(observations),
        report=BuildReport(source_id=base.source_id, contract_hash="0" * 64,
                           registries_hash=REGISTRIES.registries_hash, raw_rows=n))


@needs_docker
def test_a_20k_unit_load_succeeds_under_a_256_mib_transaction_limit():
    bundle = _synthetic_bundle(SYNTHETIC_UNITS)
    assert len(bundle.units) == SYNTHETIC_UNITS
    container = (Neo4jContainer("neo4j:5.26-community", password="testpassword")
                 .with_env("NEO4J_db_memory_transaction_max", "256m")
                 .with_env("NEO4J_dbms_memory_transaction_total_max", "256m"))
    with container as n:
        d = AsyncGraphDatabase.driver(n.get_connection_url(), auth=(n.username, n.password))
        try:
            _run(apply_migrations(d))
            limit = _run(_q(d, "CALL dbms.listConfig('db.memory.transaction.max') YIELD value RETURN value"))
            assert limit and limit[0]["value"].lower().startswith("256"), limit
            report = _run(loader.load(bundle, d, registries=REGISTRIES))
            assert _run(_q(d, "MATCH (u:ObservationUnit) RETURN count(u) AS c"))[0]["c"] == SYNTHETIC_UNITS
            assert _run(_q(d, "MATCH (o:Observation) RETURN count(o) AS c"))[0]["c"] == len(bundle.observations)
            assert report.step("unit:VarietyTrial").batches == -(-SYNTHETIC_UNITS // loader.DEFAULT_BATCH_SIZE)
            again = _run(loader.load(bundle, d, registries=REGISTRIES))
            assert again.nodes_created == again.relationships_created == 0
        finally:
            _run(d.close())
