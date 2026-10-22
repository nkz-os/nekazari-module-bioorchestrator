"""Quick test: query phenology from Neo4j directly."""
import os
import sys

from neo4j import GraphDatabase

uri = sys.argv[1] if len(sys.argv) > 1 else "bolt://localhost:7687"
user = os.getenv("NEO4J_USER", "neo4j")
password = os.getenv("NEO4J_PASSWORD", "")
if not password:
    raise SystemExit("Falta NEO4J_PASSWORD. Expórtela antes de ejecutar.")
driver = GraphDatabase.driver(uri, auth=(user, password))

# Test 1: Direct Species query
with driver.session() as session:
    r = session.run("MATCH (s:Species {name: $n}) RETURN s.name AS name", n="olive")
    for rec in r:
        print(f"Direct query: {rec['name']}")

# Test 2: The DAO's match pattern
with driver.session() as session:
    r = session.run(
        "MATCH (s:Species) WHERE s.name CONTAINS $species "
        "WITH s ORDER BY CASE WHEN s.name = $species THEN 0 WHEN s.name CONTAINS $species THEN 1 ELSE 2 END "
        "LIMIT 1 "
        "OPTIONAL MATCH (s)-[:HAS_STAGE]->(st:PhenologyStage)-[:HAS_PARAMETER]->(p:PhenologyParams) "
        "RETURN s.name AS species, count(p) AS params_count",
        species="olive"
    )
    for rec in r:
        print(f"DAO query: {rec['species']}, params={rec['params_count']}")

# Test 3: List all Species names
with driver.session() as session:
    r = session.run("MATCH (s:Species) RETURN s.name AS name ORDER BY name")
    names = [rec["name"] for rec in r]
    matching = [n for n in names if "olive" in n.lower()]
    print(f"Species with 'olive': {matching}")
    print(f"Total species: {len(names)}, first 5: {names[:5]}")

driver.close()
print("Done")
