"""Seeding must persist `effect` on RotationConstraint nodes (behavioural)."""
import sys
from pathlib import Path

import pytest
from neo4j import GraphDatabase
from testcontainers.neo4j import Neo4jContainer

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
import seed_phenology  # noqa: E402


@pytest.fixture(scope="module")
def driver():
    with Neo4jContainer("neo4j:5.26-community", password="testpassword") as n:
        d = GraphDatabase.driver(n.get_connection_url(), auth=(n.username, n.password))
        yield d
        d.close()


def _wipe(driver):
    with driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n")


def test_seed_persists_effect(driver):
    _wipe(driver)
    data = {
        "species": [],
        "rotation_constraints": [
            {"crop_a": "pea", "crop_b": "wheat", "interval_years": 1,
             "reason": "N fixation", "source_short": "FAO", "effect": "benefit"},
            {"crop_a": "wheat", "crop_b": "sunflower", "interval_years": 3,
             "reason": "Sclerotinia", "source_short": "FAO", "effect": "restriction"},
        ],
    }
    seed_phenology.seed(driver, data)
    with driver.session() as s:
        rows = {
            (r["a"], r["b"]): r["effect"]
            for r in s.run(
                "MATCH (r:RotationConstraint) "
                "RETURN r.cropA AS a, r.cropB AS b, r.effect AS effect"
            )
        }
    assert rows[("pea", "wheat")] == "benefit"
    assert rows[("wheat", "sunflower")] == "restriction"


def test_seed_missing_effect_fails_safe_to_restriction(driver):
    _wipe(driver)
    data = {
        "species": [],
        "rotation_constraints": [
            {"crop_a": "x", "crop_b": "y", "interval_years": 1,
             "reason": "legacy", "source_short": "FAO"},  # no effect key
        ],
    }
    seed_phenology.seed(driver, data)
    with driver.session() as s:
        effect = s.run(
            "MATCH (r:RotationConstraint {cropA:'x', cropB:'y'}) RETURN r.effect AS e"
        ).single()["e"]
    assert effect == "restriction", "a missing effect must fail safe to restriction"
