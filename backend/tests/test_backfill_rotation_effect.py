"""Backfill derives effect from the YAML and applies it idempotently (behavioural)."""
import importlib.util
import os
import sys
from pathlib import Path

import pytest
from neo4j import GraphDatabase
from testcontainers.neo4j import Neo4jContainer

SCRIPT = Path(__file__).parent.parent / "scripts" / "backfill_rotation_effect.py"


def _load_module():
    """Import the script with NEO4J_* set so its module-level guard passes."""
    os.environ.setdefault("NEO4J_URI", "bolt://unused")
    os.environ.setdefault("NEO4J_USER", "neo4j")
    os.environ.setdefault("NEO4J_PASSWORD", "unused")
    spec = importlib.util.spec_from_file_location("bre", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["bre"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def driver():
    with Neo4jContainer("neo4j:5.26-community", password="testpassword") as n:
        d = GraphDatabase.driver(n.get_connection_url(), auth=(n.username, n.password))
        yield d
        d.close()


def _wipe(driver):
    with driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n")


def test_effect_map_derives_from_yaml():
    """The map is read from the YAML, not embedded; 34 restrictions + 14 benefits."""
    mod = _load_module()
    m = mod.effect_map()
    assert len(m) == 48
    assert list(m.values()).count("benefit") == 14
    assert list(m.values()).count("restriction") == 34
    assert m[("wheat", "legume")] == "benefit"
    assert m[("wheat", "maize")] == "restriction"


def test_backfill_populates_and_is_idempotent(driver):
    mod = _load_module()
    _wipe(driver)
    with driver.session() as s:
        # Two nodes with no effect, matching real YAML pairs.
        s.run("CREATE (:RotationConstraint {cropA:'pea', cropB:'wheat', intervalYears:1})")
        s.run("CREATE (:RotationConstraint {cropA:'wheat', cropB:'maize', intervalYears:1})")

    first = mod.apply(driver, execute=True)
    assert first["written"] == 2
    with driver.session() as s:
        got = {
            (r["a"], r["b"]): r["effect"]
            for r in s.run(
                "MATCH (r:RotationConstraint) RETURN r.cropA AS a, r.cropB AS b, r.effect AS effect"
            )
        }
    assert got[("pea", "wheat")] == "benefit"
    assert got[("wheat", "maize")] == "restriction"

    second = mod.apply(driver, execute=True)
    assert second["written"] == 0, "a second pass must write nothing"


def test_unknown_pairs_are_reported_not_touched(driver):
    mod = _load_module()
    _wipe(driver)
    with driver.session() as s:
        s.run("CREATE (:RotationConstraint {cropA:'zzz', cropB:'qqq', intervalYears:1})")

    result = mod.apply(driver, execute=True)
    assert ("zzz", "qqq") in result["unknown"]
    with driver.session() as s:
        effect = s.run(
            "MATCH (r:RotationConstraint {cropA:'zzz', cropB:'qqq'}) RETURN r.effect AS e"
        ).single()["e"]
    assert effect is None, "an unclassified pair must be left untouched"


def test_dry_run_writes_nothing(driver):
    mod = _load_module()
    _wipe(driver)
    with driver.session() as s:
        s.run("CREATE (:RotationConstraint {cropA:'pea', cropB:'wheat', intervalYears:1})")

    result = mod.apply(driver, execute=False)
    assert result["pending"] == 1
    with driver.session() as s:
        effect = s.run(
            "MATCH (r:RotationConstraint {cropA:'pea', cropB:'wheat'}) RETURN r.effect AS e"
        ).single()["e"]
    assert effect is None, "dry-run must not write"


def test_no_hardcoded_credentials():
    """Security lint: a public repo must not carry a Neo4j password default."""
    src = SCRIPT.read_text()
    assert '"bioorchestrator"' not in src
    assert 'os.getenv("NEO4J_PASSWORD", "")' in src
