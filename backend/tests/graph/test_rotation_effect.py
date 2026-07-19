"""recommend_next_crop must exclude restrictions only, never benefits."""
import pytest
from neo4j import AsyncGraphDatabase
from testcontainers.neo4j import Neo4jContainer

from app.graph.dao import GraphDAO

_PASSWORD = "testpassword"


@pytest.fixture(scope="module")
def neo4j_url():
    with Neo4jContainer("neo4j:5.26-community", password=_PASSWORD) as n:
        yield n.get_connection_url()


@pytest.fixture
async def dao(neo4j_url):
    driver = AsyncGraphDatabase.driver(neo4j_url, auth=("neo4j", _PASSWORD))
    async with driver.session() as s:
        await s.run("MATCH (n) DETACH DELETE n")
        # Species pool the recommender picks from
        for name in ("chickpea", "lentil", "pea", "sunflower", "maize", "barley", "wheat"):
            await s.run("CREATE (:Species {name: $n, scientificName: $n})", n=name)
        # Benefits: must NOT be excluded
        for b in ("chickpea", "lentil", "pea"):
            await s.run(
                "CREATE (:RotationConstraint {cropA:'wheat', cropB:$b, "
                "intervalYears:1, reason:'N fixation', sourceShort:'FAO', effect:'benefit'})",
                b=b,
            )
        # Restrictions: must be excluded
        await s.run(
            "CREATE (:RotationConstraint {cropA:'wheat', cropB:'sunflower', "
            "intervalYears:3, reason:'Sclerotinia', sourceShort:'FAO', effect:'restriction'})"
        )
        # Legacy node with no effect at all: must be treated as a restriction
        await s.run(
            "CREATE (:RotationConstraint {cropA:'wheat', cropB:'barley', "
            "intervalYears:1, reason:'take-all', sourceShort:'JRC MARS'})"
        )
    yield GraphDAO(driver)
    await driver.close()


@pytest.mark.asyncio
async def test_benefits_are_not_excluded(dao):
    names = {c["name"] for c in await dao.recommend_next_crop("wheat")}
    assert {"chickpea", "lentil", "pea"} <= names, \
        f"N-fixing successors must survive, got {names}"


@pytest.mark.asyncio
async def test_restrictions_are_excluded(dao):
    names = {c["name"] for c in await dao.recommend_next_crop("wheat")}
    assert "sunflower" not in names, "a restriction must be excluded"


@pytest.mark.asyncio
async def test_missing_effect_is_treated_as_restriction(dao):
    names = {c["name"] for c in await dao.recommend_next_crop("wheat")}
    assert "barley" not in names, \
        "a node without effect must fail safe to restriction"


@pytest.mark.asyncio
async def test_get_rotation_constraints_exposes_effect(dao):
    rows = await dao.get_rotation_constraints("wheat")
    by_crop = {r["crop_b"]: r["effect"] for r in rows}
    assert by_crop["chickpea"] == "benefit"
    assert by_crop["sunflower"] == "restriction"


@pytest.mark.asyncio
async def test_scenario_allows_benefit(dao):
    """A benefit must not raise a rotation issue, whatever its interval."""
    out = await dao.simulate_scenario("wheat", "chickpea")
    assert out["rotation_ok"] is True, out["rotation_issue"]


@pytest.mark.asyncio
async def test_scenario_flags_restriction(dao):
    out = await dao.simulate_scenario("wheat", "sunflower")
    assert out["rotation_ok"] is False
    assert "Sclerotinia" in out["rotation_issue"]
