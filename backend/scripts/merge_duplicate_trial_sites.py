"""Fusiona TrialSite que son el mismo lugar físico bajo nombres distintos.

CAUSA RAÍZ. La identidad del sitio es su nombre normalizado
(`site_canonicalization.normalize_site_key`: minúsculas, plegado de diacríticos,
eliminación del paréntesis final). El sufijo `#lat,lon` solo aparece al desambiguar
dos sitios homónimos separados por más de 15 km, y hoy en producción no hay ninguno.

Por tanto el defecto NO es una clave inestable, sino que **la misma finca se nombra
con dos topónimos legítimos distintos** —el de la finca y el del término municipal—,
y ninguna normalización de cadena puede unir `Valladolid` con `Zamadueñas`.

Consecuencia: esto **no lo arregla reconstruir el grafo**, porque la reconstrucción
vuelve a aplicar la misma normalización por nombre y reproduce el problema intacto.
Medido sobre el snapshot 20260718: de 4.342 ensayos multienlazados, la
canonicalización automática resuelve 138 (3,2 %); 997 (23 %) son agregados sintéticos
BSL, que es un problema de modelado; y 3.207 (73,9 %) son alias reales que exigen el
criterio que codifica este script. La solución estructural es un registro de alias
persistente; este script es la corrección del grafo ya construido.

EFECTO. Los ensayos quedan enlazados a varios TrialSite con nombres distintos, lo
que rompe cualquier validación cruzada agrupada por sitio: al retener un sitio, el
ensayo sigue entrando en entrenamiento por su otro nombre.

ALCANCE DELIBERADAMENTE LIMITADO. Solo se fusionan grupos verificados uno a uno con
criterio agronómico del propietario. Quedan FUERA a propósito:

  · Juansenea (Doneztebe) - C1N0 … C3N2 y variantes. NO son sitios duplicados: son
    el mismo sitio con el código de tratamiento (corte × nitrógeno) metido en el
    nombre. Verificado: `management`, `productionSystem` y `metadata` están vacíos,
    así que el nombre del sitio es el ÚNICO registro del nivel de nitrógeno.
    Fusionarlos destruiría ese dato. Antes hay que extraer C/N a atributos del ensayo.

  · Casasola de Arión (2 nodos). Coordenadas a 25 km y clases climáticas que se
    contradicen (BSk vs Cfb). Solo 12 ensayos. Necesita decisión aparte.

Uso:
    python3 merge_duplicate_trial_sites.py            # dry-run
    python3 merge_duplicate_trial_sites.py --execute
"""
from __future__ import annotations

import argparse
import os
import sys

from neo4j import GraphDatabase

URI = os.getenv("NEO4J_URI", "")
USER = os.getenv("NEO4J_USER", "")
PASSWORD = os.getenv("NEO4J_PASSWORD", "")

if not (URI and USER and PASSWORD):
    raise SystemExit(
        "Faltan credenciales. Exporte NEO4J_URI, NEO4J_USER y NEO4J_PASSWORD."
    )

# Cada plan: nombre superviviente, nombres a absorber, y las propiedades que se
# fijan explícitamente en el superviviente tras la fusión.
MERGE_PLANS = [
    {
        "keep": "Zamadueñas (Valladolid)",
        "absorb": ["Valladolid", "Zamadueñas"],
        "reason": (
            "Finca Zamadueñas (ITACyL), término municipal de Valladolid. Cuatro nodos "
            "para el mismo lugar por coordenadas resueltas de forma distinta "
            "(41,50 / 41,65 / 41,6489 con lon -4,18 / 41,70). En Valladolid ciudad no "
            "hay ensayo: el nombre alude a la ciudad cercana."
        ),
        "props": {
            # Coordenada de la finca, al norte de la ciudad. ET0 ~1050 mm es lo
            # esperable en ese clima; el nodo 'Valladolid' traía 950, algo bajo.
            "latitude": 41.70,
            "longitude": -4.71,
            "annualET0Mm": 1050,
        },
    },
    {
        "keep": "Montes de Cierzo",
        "absorb": ["Tudela"],
        "reason": (
            "Finca Montes de Cierzo (INTIA), término municipal de Tudela. La latitud "
            "42,40 era un error de transcripción por transposición de dígitos: cae en "
            "Falces, no en Tudela. La correcta es 42,04, que coincide con la zona de "
            "cultivo de Tudela, mayoritariamente de regadío."
        ),
        "props": {
            "latitude": 42.04,
            "longitude": -1.60,
        },
    },
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    with driver.session() as s:
        before = s.run(
            "MATCH (ts:TrialSite) RETURN count(*) AS sitios"
        ).single()["sitios"]
        rels_before = s.run(
            "MATCH ()-[r:TRIAL_AT]->() RETURN count(r) AS c"
        ).single()["c"]
        print(f"TrialSite ahora: {before} | TRIAL_AT: {rels_before:,}\n")

        for plan in MERGE_PLANS:
            names = [plan["keep"]] + plan["absorb"]
            rows = s.run(
                """
                MATCH (ts:TrialSite) WHERE ts.name IN $names
                OPTIONAL MATCH (vt:VarietyTrial)-[:TRIAL_AT]->(ts)
                RETURN ts.name AS name, ts.siteKey AS key, count(vt) AS trials
                ORDER BY trials DESC
                """,
                names=names,
            ).data()
            print(f"→ {plan['keep']}")
            for r in rows:
                mark = "CONSERVAR" if r["name"] == plan["keep"] else "absorber "
                print(f"    {mark} {r['name'][:36]:38s} {r['trials']:5,} ensayos")
            print(f"    nodos: {len(rows)} → 1")

        if not args.execute:
            print("\nDRY-RUN — no se ha modificado nada. Repetir con --execute.")
            driver.close()
            return 0

        for plan in MERGE_PLANS:
            names = [plan["keep"]] + plan["absorb"]
            merged = s.run(
                """
                MATCH (ts:TrialSite) WHERE ts.name IN $names
                WITH collect(ts) AS nodes, [n IN collect(ts) WHERE n.name = $keep][0] AS keeper
                WITH [keeper] + [n IN nodes WHERE n.name <> $keep] AS ordered
                WHERE size(ordered) > 1
                // Rellenar en el superviviente lo que solo tengan los absorbidos,
                // antes de fusionar: 'discard' descarta las propiedades de los demás.
                WITH ordered, head(ordered) AS keeper, tail(ordered) AS losers
                WITH ordered, keeper,
                     apoc.map.mergeList([n IN losers | properties(n)]) AS lost
                SET keeper += apoc.map.merge(lost, properties(keeper))
                WITH ordered
                CALL apoc.refactor.mergeNodes(ordered,
                     {properties: 'discard', mergeRels: true}) YIELD node
                RETURN node.name AS name
                """,
                names=names, keep=plan["keep"],
            ).single()
            if merged:
                s.run(
                    "MATCH (ts:TrialSite {name: $keep}) SET ts += $props",
                    keep=plan["keep"], props=plan["props"],
                )
                print(f"fusionado: {plan['keep']}")

        after = s.run("MATCH (ts:TrialSite) RETURN count(*) AS c").single()["c"]
        rels_after = s.run(
            "MATCH ()-[r:TRIAL_AT]->() RETURN count(r) AS c"
        ).single()["c"]
        print(f"\nTrialSite: {before} → {after} ({after - before:+d})")
        print(f"TRIAL_AT:  {rels_before:,} → {rels_after:,} ({rels_after - rels_before:+,})")

        multi = s.run(
            """
            MATCH (vt:VarietyTrial)
            WHERE vt.yieldKgHa IS NOT NULL AND vt.yieldDerivationMethod IS NULL
              AND vt.aggregationScope = 'site'
            OPTIONAL MATCH (vt)-[:TRIAL_AT]->(ts:TrialSite)
            WITH vt, count(DISTINCT ts.name) AS n
            RETURN count(*) AS nucleo, sum(CASE WHEN n > 1 THEN 1 ELSE 0 END) AS multi
            """
        ).single()
        print(f"\nnúcleo de sitio: {multi['nucleo']:,} ensayos, "
              f"multienlazados restantes: {multi['multi']:,}")

        integ = s.run(
            "MATCH (vt:VarietyTrial) RETURN count(*) AS total, "
            "count(vt.yieldKgHa) AS rendimiento"
        ).single()
        print(f"integridad: {integ['total']:,} ensayos, "
              f"{integ['rendimiento']:,} con rendimiento")

    driver.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
