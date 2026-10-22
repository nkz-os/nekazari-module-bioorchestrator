"""``get_phenology_params`` with no stage is deterministic: the FAO-56 reference stage (mid-season),
else the first stage of the canonical order (initial, development, late-season, then others by name),
whatever the physical order of the tied rows. Real Neo4j."""
from __future__ import annotations

import asyncio
import itertools
import shutil

import pytest
from testcontainers.neo4j import Neo4jContainer

from app.graph.dao import PHENOLOGY_DEFAULT_STAGE, PHENOLOGY_STAGE_ORDER, GraphDAO
from neo4j import AsyncGraphDatabase

pytestmark = [
    pytest.mark.real_batch,
    pytest.mark.skipif(shutil.which("docker") is None, reason="docker unavailable for testcontainers"),
]

_loop = asyncio.new_event_loop()
_KC = {"initial": 0.3, "development": 0.7, "mid-season": 1.15, "late-season": 0.4, "stem_elongation": 0.9}
_STAGES = list(_KC)


def _run(coro):
    return _loop.run_until_complete(coro)


async def _seed_species(driver, name: str, stage_order: list[str], extra_param: bool = False):
    async with driver.session() as s:
        await s.run("MERGE (:Species {name: $n, scientificName: $n})", n=name)
        for st in stage_order:  # stage nodes and their default row are created in this order
            await s.run(
                """MATCH (sp:Species {name: $n})
                CREATE (sp)-[:HAS_STAGE]->(stage:PhenologyStage {name: $st})
                CREATE (stage)-[:HAS_PARAMETER]->(:PhenologyParams
                    {cultivar: '__generic__', management: '__standard__', climateZone: '__any__',
                     isDefault: true, kc: $kc})""",
                n=name, st=st, kc=_KC[st])
        if extra_param:  # a second default row in the 'mid-season' stage (tied score)
            await s.run(
                """MATCH (:Species {name: $n})-[:HAS_STAGE]->(stage:PhenologyStage {name: 'mid-season'})
                CREATE (stage)-[:HAS_PARAMETER]->(:PhenologyParams
                    {cultivar: 'Aaa', management: '__standard__', climateZone: '__any__',
                     isDefault: true, kc: 1.16})""", n=name)


@pytest.fixture(scope="module")
def dao():
    with Neo4jContainer("neo4j:5.26-community", password="testpassword") as n:
        driver = AsyncGraphDatabase.driver(n.get_connection_url(), auth=(n.username, n.password))
        d = GraphDAO(driver)

        async def seed():
            # Every rotation of the stage order: the physical order of the tied rows differs per species.
            for i, perm in enumerate(itertools.islice(itertools.permutations(_STAGES), 0, 120, 7)):
                await _seed_species(driver, f"crop{i:02d}", list(perm))
            await _seed_species(driver, "tied", list(reversed(_STAGES)), extra_param=True)
            await _seed_species(driver, "nomid", ["stem_elongation", "late-season", "development", "initial"])
        _run(seed())
        d.n_species = len(list(itertools.islice(itertools.permutations(_STAGES), 0, 120, 7)))
        yield d
        _run(driver.close())


def test_default_stage_is_fao56_mid_season():
    assert PHENOLOGY_DEFAULT_STAGE == "mid-season" and PHENOLOGY_STAGE_ORDER[0] == "initial"


def test_no_stage_returns_mid_season_whatever_the_row_order(dao):
    assert dao.n_species >= 10
    for i in range(dao.n_species):
        got = _run(dao.get_phenology_params(species=f"crop{i:02d}"))
        assert got["stage"] == "mid-season" and got["kc"] == 1.15, i


def test_no_mid_season_falls_back_to_canonical_order(dao):
    got = _run(dao.get_phenology_params(species="nomid"))
    assert got["stage"] == "initial" and got["kc"] == 0.3


def test_repeated_calls_agree(dao):
    first = _run(dao.get_phenology_params(species="crop03"))
    assert all(_run(dao.get_phenology_params(species="crop03")) == first for _ in range(3))


def test_tied_rows_of_one_stage_resolve_by_name_not_by_storage_order(dao):
    got = _run(dao.get_phenology_params(species="tied"))
    assert got["stage"] == "mid-season" and got["cultivar"] == "Aaa"  # '' < ... : cultivar name ascending


def test_ambiguous_stage_name_resolves_by_canonical_order(dao):
    # 'season' matches 'mid-season' and 'late-season': the default stage wins
    for i in range(dao.n_species):
        assert _run(dao.get_phenology_params(species=f"crop{i:02d}", stage="season"))["stage"] == "mid-season"


def test_explicit_stage_is_unchanged(dao):
    got = _run(dao.get_phenology_params(species="crop00", stage="late-season"))
    assert got["stage"] == "late-season" and got["kc"] == 0.4


def test_species_stage_outside_the_order_comes_last(dao):
    got = _run(dao.get_phenology_params(species="crop00", stage="stem_elongation"))
    assert got["stage"] == "stem_elongation"
