"""Strict Cypher migration runner: parsing (no DB) and behaviour on a real empty Neo4j 5.26 Community."""
from __future__ import annotations

import asyncio
import hashlib
import logging
import shutil
from pathlib import Path

import pytest
from testcontainers.neo4j import Neo4jContainer

from app.kg.migrations import (
    MIGRATIONS_DIR,
    MigrationError,
    MigrationParseError,
    apply_migrations,
    discover_migrations,
    is_schema_statement,
    split_statements,
)
from neo4j import AsyncGraphDatabase

# --------------------------------------------------------------------------- parsing (pure)


def test_comment_before_statement_is_stripped_not_dropped():
    """P4: the old runner discarded any chunk that *started* with a comment, i.e. the first
    statement after every comment block."""
    text = "// header\n// more\nCREATE INDEX a IF NOT EXISTS FOR (n:A) ON (n.x);\n"
    assert split_statements(text) == ["CREATE INDEX a IF NOT EXISTS FOR (n:A) ON (n.x)"]


def test_statements_split_in_order_across_comment_blocks():
    text = (
        "// first\nCREATE INDEX a IF NOT EXISTS FOR (n:A) ON (n.x);\n\n"
        "// second block\n// still comment\nCREATE INDEX b IF NOT EXISTS FOR (n:B) ON (n.y);\n"
        "CREATE INDEX c IF NOT EXISTS FOR (n:C) ON (n.z);"
    )
    assert [s.split()[2] for s in split_statements(text)] == ["a", "b", "c"]


def test_block_comments_and_trailing_line_comments():
    text = "/* a; b */ MATCH (n) /* mid; */ RETURN n // tail; with semicolon\n;"
    assert split_statements(text) == ["MATCH (n)   RETURN n"]


def test_semicolon_and_comment_markers_inside_strings_are_kept():
    text = (
        "MERGE (:S {a: 'x;y', b: \"http://z/*not a comment*/\"});\n"
        "MERGE (:S {c: 'it\\'s; fine'});\n"
        "MATCH (n:`odd;label`) RETURN n"
    )
    assert split_statements(text) == [
        "MERGE (:S {a: 'x;y', b: \"http://z/*not a comment*/\"})",
        "MERGE (:S {c: 'it\\'s; fine'})",
        "MATCH (n:`odd;label`) RETURN n",
    ]


def test_blank_and_comment_only_chunks_are_dropped():
    assert split_statements("// nothing\n;;\n  ;\n/* x */ ;") == []


def test_last_statement_without_semicolon_is_kept():
    assert split_statements("RETURN 1; RETURN 2") == ["RETURN 1", "RETURN 2"]


@pytest.mark.parametrize("text", ["RETURN 'open", "RETURN 1 /* open", "MATCH (n:`open) RETURN n"])
def test_unterminated_construct_raises_instead_of_swallowing(text):
    with pytest.raises(MigrationParseError):
        split_statements(text)


@pytest.mark.parametrize(
    "stmt,expected",
    [
        ("CREATE CONSTRAINT a IF NOT EXISTS FOR (n:A) REQUIRE n.x IS UNIQUE", True),
        ("create index a if not exists for (n:A) on (n.x)", True),
        ("CREATE RANGE INDEX a IF NOT EXISTS FOR (n:A) ON (n.x)", True),
        ("CREATE TEXT INDEX a IF NOT EXISTS FOR (n:A) ON (n.x)", True),
        ("DROP CONSTRAINT a IF EXISTS", True),
        ("DROP INDEX a IF EXISTS", True),
        ("MERGE (:Entitlement {name: 'open'})", False),
        ("MATCH (n) SET n.a = 1 RETURN count(n)", False),
        ("CREATE (n:Constraint {name: 'x'})", False),
    ],
)
def test_is_schema_statement(stmt, expected):
    assert is_schema_statement(stmt) is expected


# --------------------------------------------------------------------------- real Neo4j 5.26 Community

needs_docker = pytest.mark.skipif(shutil.which("docker") is None, reason="docker unavailable")

_loop = asyncio.new_event_loop()


def _run(coro):
    return _loop.run_until_complete(coro)


@pytest.fixture(scope="module")
def driver():
    if shutil.which("docker") is None:
        pytest.skip("docker unavailable")
    with Neo4jContainer("neo4j:5.26-community", password="testpassword") as n:
        d = AsyncGraphDatabase.driver(n.get_connection_url(), auth=(n.username, n.password))
        yield d
        _run(d.close())


async def _wipe(d) -> None:
    async with d.session() as s:
        await (await s.run("MATCH (n) DETACH DELETE n")).consume()
        for kind, shown in (("CONSTRAINT", "CONSTRAINTS"), ("INDEX", "INDEXES")):
            res = await s.run(f"SHOW {shown} YIELD name, type WHERE type <> 'LOOKUP' RETURN name")
            for rec in [r async for r in res]:
                await (await s.run(f"DROP {kind} `{rec['name']}` IF EXISTS")).consume()


@pytest.fixture
def db(driver):
    _run(_wipe(driver))
    return driver


async def _q(d, cypher: str, **params) -> list[dict]:
    async with d.session() as s:
        res = await s.run(cypher, **params)
        return [dict(r) async for r in res]


def constraints(d) -> set[tuple]:
    rows = _run(_q(d, "SHOW CONSTRAINTS YIELD name, type, labelsOrTypes, properties"))
    return {(r["name"], r["type"], tuple(r["labelsOrTypes"]), tuple(r["properties"])) for r in rows}


def indexes(d) -> set[tuple]:
    rows = _run(_q(d, "SHOW INDEXES YIELD name, type, labelsOrTypes, properties WHERE type <> 'LOOKUP'"))
    return {(r["name"], r["type"], tuple(r["labelsOrTypes"]), tuple(r["properties"])) for r in rows}


def schema_versions(d) -> dict[str, dict]:
    rows = _run(_q(d, "MATCH (v:SchemaVersion) RETURN v.file AS file, v.sha256 AS sha256, "
                      "v.mode AS mode, toString(v.appliedAt) AS appliedAt"))
    return {r["file"]: r for r in rows}


def write(dirpath: Path, name: str, text: str) -> Path:
    path = dirpath / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


@pytest.fixture
def mdir(tmp_path: Path) -> Path:
    d = tmp_path / "cypher_migrations"
    d.mkdir()
    return d


COMMENTED = """\
// header comment
// second header line
CREATE CONSTRAINT p4_a IF NOT EXISTS FOR (n:P4A) REQUIRE n.k IS UNIQUE;

/* block
   comment; with a semicolon */
CREATE CONSTRAINT p4_b IF NOT EXISTS FOR (n:P4B) REQUIRE n.k IS UNIQUE;

// trailing-comment chunk
CREATE INDEX p4_c IF NOT EXISTS FOR (n:P4C) ON (n.k); // after statement
CREATE INDEX p4_d IF NOT EXISTS FOR (n:P4D) ON (n.k)
"""


@needs_docker
def test_comment_before_statement_applies_every_statement(db, mdir):
    """P4 regression: the old startup runner dropped the first statement after each comment."""
    write(mdir, "001_commented.cypher", COMMENTED)
    old = [s.strip() for s in COMMENTED.split(";") if s.strip() and not s.strip().startswith("//")]
    assert len(old) < 4  # the previous splitter really lost statements on this input

    _run(apply_migrations(db, mdir))

    assert {c[0] for c in constraints(db)} >= {"p4_a", "p4_b"}
    assert {i[0] for i in indexes(db)} >= {"p4_c", "p4_d"}


@needs_docker
def test_rerun_is_a_noop(db, mdir):
    write(mdir, "001_commented.cypher", COMMENTED)
    first = _run(apply_migrations(db, mdir))
    schema_1, versions_1 = (constraints(db), indexes(db)), schema_versions(db)

    second = _run(apply_migrations(db, mdir))

    assert (constraints(db), indexes(db)) == schema_1
    assert schema_versions(db) == versions_1  # same row, same appliedAt: nothing rewritten
    assert first.applied[0].recorded is True and second.applied[0].recorded is False
    assert second.applied[0].already_present == 0  # IF NOT EXISTS: the server accepted them silently
    assert len(schema_versions(db)) == 1


@needs_docker
def test_schema_version_records_file_and_sha256(db, mdir):
    path = write(mdir, "001_commented.cypher", COMMENTED)
    write(mdir, "002_more.cypher", "CREATE INDEX more IF NOT EXISTS FOR (n:More) ON (n.k);")

    report = _run(apply_migrations(db, mdir))

    versions = schema_versions(db)
    assert set(versions) == {"001_commented.cypher", "002_more.cypher"}
    assert versions["001_commented.cypher"]["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert versions["001_commented.cypher"]["mode"] == "full"
    assert versions["001_commented.cypher"]["appliedAt"]
    assert report.files == ("001_commented.cypher", "002_more.cypher")


@needs_docker
def test_changed_file_updates_record_and_warns(db, mdir, log_records):
    path = write(mdir, "001_a.cypher", "CREATE INDEX a IF NOT EXISTS FOR (n:A) ON (n.k);")
    _run(apply_migrations(db, mdir))
    old_sha = schema_versions(db)["001_a.cypher"]["sha256"]

    path.write_text("CREATE INDEX a IF NOT EXISTS FOR (n:A) ON (n.k);\nCREATE INDEX a2 IF NOT EXISTS FOR (n:A) ON (n.j);")
    records = log_records("app.kg.migrations", logging.WARNING)
    _run(apply_migrations(db, mdir))

    assert schema_versions(db)["001_a.cypher"]["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest() != old_sha
    assert any("changed since last applied" in r.getMessage() for r in records)
    assert {i[0] for i in indexes(db)} >= {"a", "a2"}


@needs_docker
def test_broken_statement_raises_names_file_and_stops(db, mdir):
    write(mdir, "001_ok.cypher", "CREATE INDEX ok1 IF NOT EXISTS FOR (n:Ok) ON (n.k);")
    write(
        mdir, "002_broken.cypher",
        "CREATE INDEX before_break IF NOT EXISTS FOR (n:B) ON (n.k);\n"
        "THIS IS NOT CYPHER;\n"
        "CREATE INDEX after_break IF NOT EXISTS FOR (n:B) ON (n.j);",
    )
    write(mdir, "003_never.cypher", "CREATE INDEX never IF NOT EXISTS FOR (n:Never) ON (n.k);")

    with pytest.raises(MigrationError) as exc:
        _run(apply_migrations(db, mdir))

    assert exc.value.file == "002_broken.cypher"
    assert exc.value.statement_no == 2
    assert exc.value.code == "Neo.ClientError.Statement.SyntaxError"
    names = {i[0] for i in indexes(db)}
    assert "ok1" in names and "before_break" in names  # DDL is not transactional: prior ones stand
    assert "after_break" not in names and "never" not in names  # nothing runs after the failure
    assert set(schema_versions(db)) == {"001_ok.cypher"}  # the failed file is not recorded


@needs_docker
def test_enterprise_only_statement_in_main_path_raises(db, mdir):
    write(mdir, "001_nk.cypher", "CREATE CONSTRAINT nk FOR (n:NK) REQUIRE (n.a, n.b) IS NODE KEY;")
    with pytest.raises(MigrationError) as exc:
        _run(apply_migrations(db, mdir))
    assert exc.value.file == "001_nk.cypher"
    assert "Enterprise" in str(exc.value)


@needs_docker
def test_equivalent_rule_already_exists_is_success(db, mdir):
    _run(_q(db, "CREATE CONSTRAINT pre FOR (n:Pre) REQUIRE n.k IS UNIQUE"))
    write(
        mdir, "001_dupes.cypher",
        "CREATE CONSTRAINT other_name FOR (n:Pre) REQUIRE n.k IS UNIQUE;\n"  # same schema, no IF NOT EXISTS
        "CREATE INDEX on_constraint FOR (n:Pre) ON (n.k);\n"                # index backed by the constraint
        "CREATE CONSTRAINT pre FOR (n:Pre) REQUIRE n.k IS UNIQUE;\n"        # same name and schema
        "CREATE INDEX fresh IF NOT EXISTS FOR (n:Pre) ON (n.z);",
    )

    report = _run(apply_migrations(db, mdir))

    assert report.applied[0].executed == 4 and report.applied[0].already_present == 3
    assert {c[0] for c in constraints(db)} >= {"pre"}
    assert "other_name" not in {c[0] for c in constraints(db)}


@needs_docker
def test_unique_constraint_over_existing_plain_index_raises(db, mdir):
    """Neo4j answers IndexAlreadyExists here, and the constraint is NOT created: not a success."""
    _run(_q(db, "CREATE INDEX pre_idx FOR (n:Species) ON (n.name)"))
    write(mdir, "001_u.cypher", "CREATE CONSTRAINT species_name IF NOT EXISTS FOR (n:Species) REQUIRE n.name IS UNIQUE;")
    with pytest.raises(MigrationError) as exc:
        _run(apply_migrations(db, mdir))
    assert exc.value.code == "Neo.ClientError.Schema.IndexAlreadyExists"
    assert "species_name" not in {c[0] for c in constraints(db)}


@needs_docker
def test_same_name_different_schema_raises(db, mdir):
    _run(_q(db, "CREATE CONSTRAINT clash FOR (n:One) REQUIRE n.k IS UNIQUE"))
    write(mdir, "001_clash.cypher", "CREATE CONSTRAINT clash FOR (n:Two) REQUIRE n.k IS UNIQUE;")
    with pytest.raises(MigrationError) as exc:
        _run(apply_migrations(db, mdir))
    assert exc.value.file == "001_clash.cypher"


@needs_docker
def test_uniqueness_violation_in_existing_data_raises(db, mdir):
    _run(_q(db, "CREATE (:Dup {k: 1}), (:Dup {k: 1})"))
    write(mdir, "001_dup.cypher", "CREATE CONSTRAINT dup_k IF NOT EXISTS FOR (n:Dup) REQUIRE n.k IS UNIQUE;")
    with pytest.raises(MigrationError) as exc:
        _run(apply_migrations(db, mdir))
    assert exc.value.statement_no == 1
    assert "dup_k" not in {c[0] for c in constraints(db)}
    assert schema_versions(db) == {}


@needs_docker
def test_enterprise_directory_is_skipped(db, mdir):
    write(mdir, "001_ok.cypher", "CREATE INDEX ok IF NOT EXISTS FOR (n:Ok) ON (n.k);")
    write(mdir, "enterprise/001_nk.cypher", "CREATE CONSTRAINT nk FOR (n:NK) REQUIRE (n.a, n.b) IS NODE KEY;")

    report = _run(apply_migrations(db, mdir))

    assert report.files == ("001_ok.cypher",)
    assert "nk" not in {c[0] for c in constraints(db)}
    assert set(schema_versions(db)) == {"001_ok.cypher"}


@needs_docker
def test_data_statements_skipped_when_schema_only_then_applied(db, mdir):
    write(
        mdir, "001_mixed.cypher",
        "CREATE CONSTRAINT mx IF NOT EXISTS FOR (e:Ent) REQUIRE e.name IS UNIQUE;\n"
        "MERGE (:Ent {name: 'open'});",
    )

    report = _run(apply_migrations(db, mdir, include_data=False))
    assert report.applied[0].skipped_data == 1 and report.applied[0].mode == "schema-only"
    assert _run(_q(db, "MATCH (e:Ent) RETURN count(e) AS c"))[0]["c"] == 0
    assert schema_versions(db)["001_mixed.cypher"]["mode"] == "schema-only"

    report = _run(apply_migrations(db, mdir, include_data=True))
    assert report.applied[0].skipped_data == 0 and report.applied[0].recorded is True
    assert _run(_q(db, "MATCH (e:Ent) RETURN count(e) AS c"))[0]["c"] == 1
    assert schema_versions(db)["001_mixed.cypher"]["mode"] == "full"

    _run(apply_migrations(db, mdir, include_data=True))  # MERGE: still exactly one
    assert _run(_q(db, "MATCH (e:Ent) RETURN count(e) AS c"))[0]["c"] == 1


@needs_docker
def test_double_slash_inside_string_is_not_a_comment(db, mdir):
    """The old ops script cut every line at the first ``//``, mangling URLs in literals."""
    write(mdir, "001_url.cypher", "MERGE (:Link {url: 'https://example.org/a//b'}); // note")
    _run(apply_migrations(db, mdir))
    assert _run(_q(db, "MATCH (l:Link) RETURN l.url AS u"))[0]["u"] == "https://example.org/a//b"


@needs_docker
def test_unparseable_file_aborts_before_touching_the_database(db, mdir):
    write(mdir, "001_ok.cypher", "CREATE INDEX ok IF NOT EXISTS FOR (n:Ok) ON (n.k);")
    write(mdir, "002_bad.cypher", "MERGE (:X {a: 'unterminated});")
    with pytest.raises(MigrationParseError) as exc:
        _run(apply_migrations(db, mdir))
    assert exc.value.file == "002_bad.cypher"
    assert indexes(db) == set() and schema_versions(db) == {}


def test_bad_file_names_and_duplicate_numbers_are_errors(mdir):
    write(mdir, "001_a.cypher", "RETURN 1;")
    write(mdir, "11_typo.cypher", "RETURN 1;")
    with pytest.raises(MigrationError, match="unexpected migration file name"):
        _run(apply_migrations(None, mdir))
    (mdir / "11_typo.cypher").unlink()
    write(mdir, "001_b.cypher", "RETURN 1;")
    with pytest.raises(MigrationError, match="duplicate migration number 001"):
        _run(apply_migrations(None, mdir))


def test_missing_directory_is_an_error(tmp_path):
    with pytest.raises(MigrationError, match="not found"):
        _run(apply_migrations(None, tmp_path / "nope"))


def test_shipped_migrations_are_found_in_order_and_enterprise_is_not_scanned():
    names = [m.name for m in discover_migrations()]
    assert names and names == sorted(names)
    assert all("/" not in n for n in names)


# --------------------------------------------------------------------------- the shipped migrations

# Every constraint is UNIQUENESS: Community has no NODE KEY / existence constraints.
EXPECTED_CONSTRAINTS = {
    # 001
    "resource_uri": ("Resource", ("uri",)),
    "species_name": ("Species", ("name",)),
    "species_eppo": ("Species", ("eppoCode",)),
    "agricrop_uri": ("AgriCrop", ("uri",)),
    "variety_uri": ("AgriCropVariety", ("uri",)),
    "stage_species_name": ("PhenologyStage", ("speciesName", "name")),
    "heat_tolerance_species": ("CropHeatTolerance", ("species",)),
    "frost_tolerance_species": ("CropFrostTolerance", ("species",)),
    "cropcoeff_crop": ("CropCoefficient", ("cropCommonName",)),
    "nutrient_profile_species_stage": ("CropNutrientProfile", ("species", "stage", "element")),
    "soil_suitability_species": ("CropSoilSuitability", ("species",)),
    "management_trial_key": ("ManagementTrial", ("mergeKey",)),
    "harvest_data_key": ("HarvestData", ("mergeKey",)),
    "article_source_key": ("ArticleSource", ("mergeKey",)),
    "pest_eppo": ("Pest", ("eppoCode",)),
    "gdd_model_pest_stage": ("GDDModel", ("pestEppo", "stageName")),
    "natural_enemy_eppo": ("NaturalEnemy", ("eppoCode",)),
    "companion_relation_pair": ("CompanionRelation", ("cropA", "cropB")),
    "host_association_pair": ("HostAssociation", ("pestEppo", "hostEppo")),
    "active_substance_code": ("ActiveSubstance", ("substanceCode",)),
    "mrl_substance_crop": ("MRLEntry", ("substanceCode", "cropEppo")),
    "rotation_constraint_pair": ("RotationConstraint", ("cropA", "cropB")),
    "module_id": ("Module", ("id",)),  # 006's module_id_unique is the same rule: a no-op
    # 002
    "trial_site_sitekey": ("TrialSite", ("siteKey",)),
    # 006
    "capability_key_unique": ("Capability", ("entityType", "attributeName")),
    "entitlement_name_unique": ("Entitlement", ("name",)),
    # 008
    "climate_cell_key": ("ClimateCell", ("key",)),
    # 010
    "observation_unit_unitkey": ("ObservationUnit", ("unitKey",)),
    "observation_obskey": ("Observation", ("obsKey",)),
    "variety_varietykey": ("Variety", ("varietyKey",)),
    "crop_eppo": ("Crop", ("eppo",)),
    "variable_variableid": ("Variable", ("variableId",)),
    "source_sourceid": ("Source", ("sourceId",)),
    "study_studykey": ("Study", ("studyKey",)),
    "article_source_documentkey": ("ArticleSource", ("documentKey",)),
    # runner bootstrap
    "schema_version_file": ("SchemaVersion", ("file",)),
}

# Indexes that are not the backing index of a constraint.
EXPECTED_PLAIN_INDEXES = {
    # 001 (species_name_lookup is absent on purpose: equivalent to the species_name constraint)
    "species_scientific_name": ("Species", ("scientificName",)),
    "trial_site_climate": ("TrialSite", ("climateClass",)),
    "trial_site_soil": ("TrialSite", ("soilType",)),
    "trial_site_rainfall": ("TrialSite", ("annualRainfallMm",)),
    "variety_trial_crop_year": ("VarietyTrial", ("cropEppo", "year")),
    "variety_trial_yield": ("VarietyTrial", ("yieldKgHa",)),
    "mgmt_trial_exp_type": ("ManagementTrial", ("experimentType",)),
    "pest_name": ("Pest", ("prefName",)),
    "active_substance_name": ("ActiveSubstance", ("commonName",)),
    "article_source_year": ("ArticleSource", ("year",)),
    "phenology_stage_species": ("PhenologyStage", ("speciesName",)),
    "harvest_data_crop": ("HarvestData", ("cropEppo",)),
    # 002, 006, 009
    "trial_site_municipality_key": ("TrialSite", ("municipalityKey",)),
    "capability_entity_type_ix": ("Capability", ("entityType",)),
    "capability_entitlement_ix": ("Capability", ("entitlement",)),
    "trial_site_name": ("TrialSite", ("name",)),
    # 010
    "variety_trial_merge_key": ("VarietyTrial", ("mergeKey",)),
    "variety_trial_crop_eppo": ("VarietyTrial", ("cropEppo",)),
    "observation_variable_id": ("Observation", ("variableId",)),
    "variety_trial_source_crop": ("VarietyTrial", ("source_id", "cropEppo")),
}


def _expected_constraint_set() -> set[tuple]:
    return {(n, "UNIQUENESS", (lbl,), props) for n, (lbl, props) in EXPECTED_CONSTRAINTS.items()}


def _expected_index_set() -> set[tuple]:
    backing = {(n, "RANGE", (lbl,), props) for n, (lbl, props) in EXPECTED_CONSTRAINTS.items()}
    plain = {(n, "RANGE", (lbl,), props) for n, (lbl, props) in EXPECTED_PLAIN_INDEXES.items()}
    return backing | plain


@needs_docker
def test_all_shipped_migrations_apply_on_empty_community_and_match_expected_schema(db):
    report = _run(apply_migrations(db))

    assert report.files == tuple(m.name for m in discover_migrations())
    assert report.files[0] == "001_schema_constraints.cypher" and report.files[-1] == "010_kg_identity.cypher"
    assert constraints(db) == _expected_constraint_set()
    assert indexes(db) == _expected_index_set()
    versions = schema_versions(db)
    assert set(versions) == set(report.files)
    for mf in discover_migrations():
        assert versions[mf.name]["sha256"] == mf.sha256 and versions[mf.name]["mode"] == "full"


@needs_docker
def test_shipped_migrations_rerun_is_a_noop(db):
    _run(apply_migrations(db))
    before = (constraints(db), indexes(db), schema_versions(db))
    entitlements = _run(_q(db, "MATCH (e:Entitlement) RETURN count(e) AS c"))[0]["c"]

    report = _run(apply_migrations(db))

    assert (constraints(db), indexes(db), schema_versions(db)) == before
    assert not any(a.recorded for a in report.applied)
    assert _run(_q(db, "MATCH (e:Entitlement) RETURN count(e) AS c"))[0]["c"] == entitlements == 3


@needs_docker
def test_startup_mode_builds_the_same_schema_but_runs_no_data_statements(db):
    """App startup runs schema only: 006's Entitlement MERGEs and 007's bulk SET never ran there."""
    report = _run(apply_migrations(db, include_data=False))

    assert constraints(db) == _expected_constraint_set()
    assert indexes(db) == _expected_index_set()
    assert _run(_q(db, "MATCH (e:Entitlement) RETURN count(e) AS c"))[0]["c"] == 0
    modes = {a.file: a.mode for a in report.applied}
    assert modes["006_capability_registry.cypher"] == "schema-only"
    assert modes["007_management_regime.cypher"] == "schema-only"
    assert modes["001_schema_constraints.cypher"] == "full"
    assert sum(a.skipped_data for a in report.applied) == report.skipped_data > 0


def _enterprise_files() -> list[Path]:
    return sorted((MIGRATIONS_DIR / "enterprise").glob("*.cypher"))


@needs_docker
def test_enterprise_statements_really_are_enterprise_only(db):
    """Nothing Community can run was parked in enterprise/: every statement there fails on 5.26 Community."""
    files = _enterprise_files()
    assert files
    statements = [s for f in files for s in split_statements(f.read_text(encoding="utf-8"))]
    assert len(statements) == 14
    for stmt in statements:
        with pytest.raises(Exception, match="Enterprise"):
            _run(_q(db, stmt))
    assert constraints(db) == set()


def test_enterprise_directory_holds_no_runnable_migration_names():
    """The runner reads only top-level NNN_*.cypher; enterprise/ is a sibling directory it never opens."""
    assert [m.name for m in discover_migrations()] == [
        "001_schema_constraints.cypher",
        "002_trial_site_sitekey.cypher",
        "006_capability_registry.cypher",
        "007_management_regime.cypher",
        "008_climate_cell.cypher",
        "009_trial_site_name_index.cypher",
        "010_kg_identity.cypher",
    ]


@needs_docker
def test_applies_cleanly_over_a_schema_shaped_like_the_current_deployment(db):
    """Constraints/indexes already present (as listed in the rebuild inventory, same names and
    schemas) are no-ops, including ``variety_trial_source_crop``, which only a backfill script made."""
    for stmt in (
        "CREATE CONSTRAINT species_eppo FOR (s:Species) REQUIRE s.eppoCode IS UNIQUE",
        "CREATE CONSTRAINT trial_site_sitekey FOR (ts:TrialSite) REQUIRE ts.siteKey IS UNIQUE",
        "CREATE CONSTRAINT climate_cell_key FOR (c:ClimateCell) REQUIRE c.key IS UNIQUE",
        "CREATE CONSTRAINT capability_key_unique FOR (c:Capability) REQUIRE (c.entityType, c.attributeName) IS UNIQUE",
        "CREATE CONSTRAINT entitlement_name_unique FOR (e:Entitlement) REQUIRE e.name IS UNIQUE",
        "CREATE INDEX capability_entity_type_ix FOR (c:Capability) ON (c.entityType)",
        "CREATE INDEX capability_entitlement_ix FOR (c:Capability) ON (c.entitlement)",
        "CREATE INDEX species_scientific_name FOR (s:Species) ON (s.scientificName)",
        "CREATE INDEX trial_site_name FOR (ts:TrialSite) ON (ts.name)",
        "CREATE INDEX trial_site_municipality_key FOR (ts:TrialSite) ON (ts.municipalityKey)",
        "CREATE INDEX variety_trial_yield FOR (vt:VarietyTrial) ON (vt.yieldKgHa)",
        "CREATE INDEX variety_trial_source_crop FOR (vt:VarietyTrial) ON (vt.source_id, vt.cropEppo)",
    ):
        _run(_q(db, stmt))

    _run(apply_migrations(db, include_data=False))

    assert constraints(db) == _expected_constraint_set()
    assert indexes(db) == _expected_index_set()
