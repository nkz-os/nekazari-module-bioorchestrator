"""Run orphan PhenologyParams migration on Neo4j."""
import os
import sys

from neo4j import GraphDatabase

CYPHER = """
MATCH (p:PhenologyParams)
WHERE NOT EXISTS { MATCH (s:Species)-[:HAS_STAGE]->(:PhenologyStage)-[:HAS_PARAMETER]->(p) }
  AND NOT EXISTS { MATCH (c:AgriCrop)-[:HAS_PARAMETER]->(p) }
  AND p.sourceShort IS NOT NULL
WITH p,
     CASE
       WHEN toLower(p.sourceShort) CONTAINS 'olive' THEN 'olive'
       WHEN toLower(p.sourceShort) CONTAINS 'almond' THEN 'almond'
       WHEN toLower(p.sourceShort) CONTAINS 'wheat' THEN 'wheat'
       WHEN toLower(p.sourceShort) CONTAINS 'grape' OR toLower(p.sourceShort) CONTAINS 'wine' THEN 'wine grapes'
       WHEN toLower(p.sourceShort) CONTAINS 'maize' OR toLower(p.sourceShort) CONTAINS 'corn' THEN 'maize'
       WHEN toLower(p.sourceShort) CONTAINS 'rice' THEN 'rice'
       WHEN toLower(p.sourceShort) CONTAINS 'soybean' THEN 'soybean'
       WHEN toLower(p.sourceShort) CONTAINS 'sunflower' THEN 'sunflower'
       WHEN toLower(p.sourceShort) CONTAINS 'cotton' THEN 'cotton'
       WHEN toLower(p.sourceShort) CONTAINS 'alfalfa' THEN 'alfalfa'
       WHEN toLower(p.sourceShort) CONTAINS 'tomato' THEN 'tomato'
       WHEN toLower(p.sourceShort) CONTAINS 'potato' THEN 'potato'
       WHEN toLower(p.sourceShort) CONTAINS 'onion' THEN 'onion'
       WHEN toLower(p.sourceShort) CONTAINS 'barley' THEN 'barley'
       WHEN toLower(p.sourceShort) CONTAINS 'oat' THEN 'oat'
       WHEN toLower(p.sourceShort) CONTAINS 'rye' THEN 'rye'
       WHEN toLower(p.sourceShort) CONTAINS 'sorghum' THEN 'sorghum'
       WHEN toLower(p.sourceShort) CONTAINS 'millet' THEN 'millet'
       WHEN toLower(p.sourceShort) CONTAINS 'chickpea' THEN 'chickpea'
       WHEN toLower(p.sourceShort) CONTAINS 'lentil' THEN 'lentil'
       WHEN toLower(p.sourceShort) CONTAINS 'pea' OR toLower(p.sourceShort) CONTAINS 'pisum' THEN 'peas'
       WHEN toLower(p.sourceShort) CONTAINS 'faba' OR toLower(p.sourceShort) CONTAINS 'vicia' THEN 'beans'
       WHEN toLower(p.sourceShort) CONTAINS 'rapeseed' OR toLower(p.sourceShort) CONTAINS 'canola' THEN 'rapeseed'
       WHEN toLower(p.sourceShort) CONTAINS 'sugarcane' THEN 'sugarcane'
       ELSE NULL
     END AS species_name
WHERE species_name IS NOT NULL
MERGE (s:Species {name: species_name})
MERGE (s)-[:HAS_STAGE]->(st:PhenologyStage {name: COALESCE(p.cultivar, 'generic')})
SET st.description = 'Inferred from orphan PhenologyParams: ' + COALESCE(p.sourceShort, '')
MERGE (st)-[:HAS_PARAMETER]->(p)
SET p.migratedFromOrphan = true
RETURN count(p) AS linked_count, collect(DISTINCT species_name) AS species_linked
"""

uri = sys.argv[1] if len(sys.argv) > 1 else "bolt://localhost:7687"
user = os.getenv("NEO4J_USER", "neo4j")
password = os.getenv("NEO4J_PASSWORD", "")
if not password:
    raise SystemExit("Falta NEO4J_PASSWORD. Expórtela antes de ejecutar.")
driver = GraphDatabase.driver(uri, auth=(user, password))
with driver.session() as session:
    r = session.run(CYPHER)
    result = r.single()
    print(f"Linked: {result['linked_count']}")
    print(f"Species: {result['species_linked']}")
driver.close()
