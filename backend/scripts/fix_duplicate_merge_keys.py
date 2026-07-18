"""B1-modo2: fusionar VarietyTrial que comparten mergeKey y crear el constraint.

Causa raíz: no existe constraint de unicidad sobre VarietyTrial.mergeKey, así que
MERGE no garantiza unicidad ante ejecuciones solapadas. Ver
internal-docs-local/2026-07-18-diagnostico-duplicidad-grafo.md

Criterio de superviviente, en orden:
  1. tener yieldKgHa (el dato que importa)
  2. mayor número de propiedades no nulas
  3. updatedAt más reciente

Uso:
    python3 fix_mode2.py                # dry-run: solo informa
    python3 fix_mode2.py --execute      # aplica
"""
import argparse
import os
import sys

from neo4j import GraphDatabase

URI = os.getenv("NEO4J_URI", "bolt://bioorchestrator-neo4j:7687")
AUTH = (os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "bioorchestrator"))
CONSTRAINT = "variety_trial_mergekey_unique"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    driver = GraphDatabase.driver(URI, auth=AUTH)
    with driver.session() as s:
        before_nodes = s.run("MATCH (n:VarietyTrial) RETURN count(n) AS c").single()["c"]
        before_rels = s.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]

        groups = s.run("""
            MATCH (vt:VarietyTrial) WHERE vt.mergeKey IS NOT NULL
            WITH vt.mergeKey AS k, count(*) AS c
            WHERE c > 1
            RETURN count(k) AS groups, sum(c - 1) AS extra
        """).single()

        print(f"VarietyTrial ahora:        {before_nodes:,}")
        print(f"relaciones ahora:          {before_rels:,}")
        print(f"mergeKey duplicados:       {groups['groups']:,} grupos")
        print(f"nodos a eliminar:          {groups['extra']:,}")

        # Clasificación de seguridad. Un grupo NO es fusionable si dos de sus nodos
        # llevan rendimientos distintos: entonces no son duplicados sino ensayos
        # distintos colapsados por un mergeKey mal construido (la localidad no entró
        # en la clave). Fusionarlos destruiría observaciones reales.
        classes = s.run("""
            MATCH (vt:VarietyTrial) WHERE vt.mergeKey IS NOT NULL
            WITH vt.mergeKey AS k, collect(vt) AS nodes
            WHERE size(nodes) > 1
            WITH k, size(nodes) AS n,
                 [x IN nodes WHERE x.yieldKgHa IS NOT NULL | x.yieldKgHa] AS ys
            RETURN size(apoc.coll.toSet(ys)) > 1 AS unsafe,
                   count(*) AS groups, sum(n - 1) AS extra
        """).data()
        safe = next((c for c in classes if not c["unsafe"]), {"groups": 0, "extra": 0})
        unsafe = next((c for c in classes if c["unsafe"]), {"groups": 0, "extra": 0})
        print(f"  fusionables:             {safe['groups']:,} grupos "
              f"({safe['extra']:,} nodos a eliminar)")
        print(f"  NO fusionables:          {unsafe['groups']:,} grupos "
              f"({unsafe['extra']:,} nodos) — rendimientos distintos, "
              "requieren reconstruir el mergeKey con su localidad")

        if not args.execute:
            print("\nDRY-RUN — no se ha modificado nada. Repetir con --execute.")
            driver.close()
            return 0

        print("\nfusionando SOLO los grupos seguros...")
        merged = s.run("""
            MATCH (vt:VarietyTrial) WHERE vt.mergeKey IS NOT NULL
            WITH vt.mergeKey AS k, collect(vt) AS nodes
            WHERE size(nodes) > 1
            WITH k, nodes,
                 [x IN nodes WHERE x.yieldKgHa IS NOT NULL | x.yieldKgHa] AS ys
            WHERE size(apoc.coll.toSet(ys)) <= 1   // nunca fusionar rendimientos distintos
            // superviviente primero: con rendimiento > más propiedades > más reciente
            WITH k, apoc.coll.sortMulti(
                [n IN nodes | {
                    node: n,
                    hasYield: CASE WHEN n.yieldKgHa IS NOT NULL THEN 1 ELSE 0 END,
                    props: size(keys(n)),
                    updated: coalesce(toString(n.updatedAt), '')
                }],
                ['hasYield', 'props', 'updated']
            ) AS ranked
            WITH k, [r IN ranked | r.node] AS ordered
            // Rellenar en el superviviente las propiedades que él no tiene y sí
            // tienen los descartados, ANTES de fusionar. Sin esto, 'discard' se
            // lleva por delante campos complementarios (sanidad, rasgos
            // agronómicos) que solo estaban en la copia eliminada.
            WITH k, ordered, head(ordered) AS survivor, tail(ordered) AS losers
            WITH k, ordered, survivor,
                 apoc.map.mergeList([n IN losers | properties(n)]) AS lost
            SET survivor += apoc.map.merge(lost, properties(survivor))
            WITH k, ordered
            CALL apoc.refactor.mergeNodes(ordered, {
                properties: 'discard',
                mergeRels: true
            }) YIELD node
            RETURN count(node) AS merged
        """).single()["merged"]
        print(f"grupos fusionados: {merged:,}")

        after_nodes = s.run("MATCH (n:VarietyTrial) RETURN count(n) AS c").single()["c"]
        remaining = s.run("""
            MATCH (vt:VarietyTrial) WHERE vt.mergeKey IS NOT NULL
            WITH vt.mergeKey AS k, count(*) AS c WHERE c > 1
            RETURN count(k) AS c
        """).single()["c"]
        print(f"VarietyTrial tras fusión:  {after_nodes:,} (-{before_nodes - after_nodes:,})")
        print(f"mergeKey duplicados restantes: {remaining}")

        if remaining:
            print(f"\nConstraint NO creado: quedan {remaining} mergeKey duplicados.")
            print("Son los grupos con rendimientos distintos: no son duplicados, hay que")
            print("reconstruir su mergeKey incluyendo la localidad antes de poder exigir")
            print("unicidad. Esto es esperado en esta fase.")
            after_rels = s.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]
            print(f"relaciones tras fusión:    {after_rels:,} ({after_rels - before_rels:+,})")
            driver.close()
            return 0

        print("\ncreando constraint de unicidad...")
        s.run(f"""
            CREATE CONSTRAINT {CONSTRAINT} IF NOT EXISTS
            FOR (vt:VarietyTrial) REQUIRE vt.mergeKey IS UNIQUE
        """)
        exists = s.run(
            "SHOW CONSTRAINTS YIELD name WHERE name = $n RETURN count(*) AS c",
            n=CONSTRAINT,
        ).single()["c"]
        print(f"constraint creado: {'sí' if exists else 'NO'}")

        after_rels = s.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]
        print(f"relaciones tras fusión:    {after_rels:,} "
              f"({after_rels - before_rels:+,})")

    driver.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
