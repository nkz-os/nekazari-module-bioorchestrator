"""recommend_next_crop must exclude restrictions only, never benefits."""
import pytest
from testcontainers.neo4j import Neo4jContainer

from app.graph.dao import GraphDAO
from neo4j import AsyncGraphDatabase

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
async def test_get_rotation_constraints_exposes_effect(dao):
    rows = await dao.get_rotation_constraints("wheat")
    by_crop = {r["crop_b"]: r["effect"] for r in rows}
    assert by_crop["chickpea"] == "benefit"
    assert by_crop["sunflower"] == "restriction"

