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
def test_changed_file_updates_record_and_warns(db, mdir, caplog):
    path = write(mdir, "001_a.cypher", "CREATE INDEX a IF NOT EXISTS FOR (n:A) ON (n.k);")
    _run(apply_migrations(db, mdir))
    old_sha = schema_versions(db)["001_a.cypher"]["sha256"]

    path.write_text("CREATE INDEX a IF NOT EXISTS FOR (n:A) ON (n.k);\nCREATE INDEX a2 IF NOT EXISTS FOR (n:A) ON (n.j);")
    with caplog.at_level(logging.WARNING, logger="app.kg.migrations"):
        _run(apply_migrations(db, mdir))

    assert schema_versions(db)["001_a.cypher"]["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest() != old_sha
    assert any("changed since last applied" in r.getMessage() for r in caplog.records)
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
def test_enterprise_directory_is_skipped_by_default(db, mdir):
    write(mdir, "001_ok.cypher", "CREATE INDEX ok IF NOT EXISTS FOR (n:Ok) ON (n.k);")
    write(mdir, "enterprise/001_nk.cypher", "CREATE CONSTRAINT nk FOR (n:NK) REQUIRE (n.a, n.b) IS NODE KEY;")

    report = _run(apply_migrations(db, mdir))

    assert report.files == ("001_ok.cypher",)
    assert "nk" not in {c[0] for c in constraints(db)}
    assert set(schema_versions(db)) == {"001_ok.cypher"}


@needs_docker
def test_enterprise_directory_runs_only_when_asked(db, mdir):
    write(mdir, "001_ok.cypher", "CREATE INDEX ok IF NOT EXISTS FOR (n:Ok) ON (n.k);")
    write(mdir, "enterprise/001_nk.cypher", "CREATE CONSTRAINT nk FOR (n:NK) REQUIRE (n.a, n.b) IS NODE KEY;")
    with pytest.raises(MigrationError) as exc:  # Community cannot: proves the file is attempted
        _run(apply_migrations(db, mdir, include_enterprise=True))
    assert exc.value.file == "enterprise/001_nk.cypher"


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


def test_shipped_enterprise_dir_is_not_scanned_as_a_migration():
    names = [m.name for m in discover_migrations(include_enterprise=False)]
    assert all("/" not in n for n in names) and names == sorted(names)
