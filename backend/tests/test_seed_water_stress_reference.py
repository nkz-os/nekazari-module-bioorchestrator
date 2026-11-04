"""Seed script for WaterStressReference: row building (pure) and Neo4j behaviour (docker)."""
import copy
import json
import sys
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
import seed_water_stress_reference as seed

DATA = Path(__file__).parent.parent / "data"


def _doc():
    return yaml.safe_load((DATA / "water_stress_reference.yaml").read_text())


def test_build_rows_one_per_species_with_raw_block():
    doc = _doc()
    rows = seed.build_rows(doc)
    assert {r["species"] for r in rows} == set(doc["species"])
    for r in rows:
        assert json.loads(r["payload"]) == doc["species"][r["species"]]
        assert r["sourceIds"] == sorted(set(r["sourceIds"]))
        assert r["datasetVersion"] == str(doc["dataset_version"])


def test_build_rows_keeps_unserved_rows():
    doc = _doc()
    slug = next(s for s, b in doc["species"].items()
                if any(x.get("upper") is None for x in b.get("baselines") or []))
    row = next(r for r in seed.build_rows(doc) if r["species"] == slug)
    assert any(b.get("upper") is None for b in json.loads(row["payload"])["baselines"])


def _cli(monkeypatch, tmp_path, doc, extra=()):
    (tmp_path / "water_stress_reference.yaml").write_text(yaml.safe_dump(doc))
    (tmp_path / "phenology_sources.yaml").write_text((DATA / "phenology_sources.yaml").read_text())
    monkeypatch.setattr(sys, "argv", ["seed", "--data-dir", str(tmp_path), *extra])
    seed.main()


def test_invalid_file_exits_1(monkeypatch, tmp_path):
    doc = _doc()
    slug = next(iter(doc["species"]))
    doc["species"][slug]["baselines"][0]["source_id"] = "no-such-source"
    with pytest.raises(SystemExit) as e:
        _cli(monkeypatch, tmp_path, doc)
    assert e.value.code == 1


def test_dry_run_writes_nothing(monkeypatch, tmp_path, capsys):
    def boom(*a, **k):
        raise AssertionError("driver must not be created on dry run")
    monkeypatch.setattr(seed.GraphDatabase, "driver", boom)
    _cli(monkeypatch, tmp_path, _doc())
    assert "dry run" in capsys.readouterr().out


@pytest.fixture(scope="module")
def driver():
    from docker.errors import DockerException
    from testcontainers.neo4j import Neo4jContainer

    from neo4j import GraphDatabase
    try:
        c = Neo4jContainer("neo4j:5.26-community", password="testpassword")
        c.start()
    except DockerException as exc:
        pytest.skip(f"docker unavailable: {exc}")
    d = GraphDatabase.driver(c.get_connection_url(), auth=(c.username, c.password))
    yield d
    d.close()
    c.stop()


def _species(driver):
    with driver.session() as s:
        return {r["sp"]: r for r in s.run(
            "MATCH (n:WaterStressReference) RETURN n.species AS sp, n.payload AS p, "
            "n.sourceIds AS ids")}


def test_execute_merges_and_prunes(driver):
    doc = _doc()
    assert {"wheat", "olive"} <= set(doc["species"])
    seed.write(driver, seed.build_rows(doc))
    got = _species(driver)
    assert set(got) == set(doc["species"])
    assert json.loads(got["wheat"]["p"]) == doc["species"]["wheat"]

    seed.write(driver, seed.build_rows(doc))  # idempotent
    assert len(_species(driver)) == len(doc["species"])

    smaller = copy.deepcopy(doc)
    del smaller["species"]["olive"]
    removed = seed.write(driver, seed.build_rows(smaller))
    got = _species(driver)
    assert removed == 1 and "olive" not in got and "wheat" in got
