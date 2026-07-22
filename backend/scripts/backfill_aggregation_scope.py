"""Backfill `aggregationScope` on VarietyTrial: is this one site, or a mean of many?

Three separate defects turned out to be the same one: observations that are averages
over several places are stored exactly like single-site observations. Left unmarked
they corrupt any site-level analysis — a mean of five locations competes with the
five locations that compose it, and site-grouped cross-validation leaks.

Values:
  site        no evidence of aggregation. NOT a verification that the trial is
              single-site: it means no aggregation signal was found for it.
  national    Nationwide results (`trialLocation` starting with "Bundesweit").
  regional    Aggregates coarser than a site but narrower than a country:
              · ALL of BSL. Verified: the German corpus has no per-site location at
                all — every trial sits on one of three synthetic climate buckets the
                pipeline builds (BSL Deutschland Cfb / Dfb / Uebergang), which are
                not real places but climate-zone containers with a representative
                city coordinate.
              · LfL Bayern aggregate columns — "Mittel aus N Orten" (mean of several
                locations) and "nach Anbaugebieten" (growing region). Verified
                against the source PDFs: they have no site name because they never
                had one.
  unlocated   no TRIAL_AT link at all. Invisible to any geographic query.

Deliberately NOT deleted. 413 varieties — all of them from 2025 — have their only
measured yield in these rows: the newest cohort arrives as national aggregates before
the per-site detail is published. Dropping them would blind the recommender to the
varieties a grower is choosing between right now. They are data at a different scale,
not wrong data.

Usage:
    python3 backfill_aggregation_scope.py            # dry-run
    python3 backfill_aggregation_scope.py --execute
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

SYNTHETIC_PREFIX = "BSL Deutschland"

# Each rule: (label, Cypher predicate applied to `vt` with its site list `s`).
# Order matters — the first match wins, so the specific rules precede `site`.
# Expresión única de la regla, reutilizada por el recuento y por la escritura, para
# que no puedan divergir.
SCOPE_EXPR = """
                CASE
                  WHEN coalesce(vt.trialLocation, '') STARTS WITH 'Bundesweit'
                    THEN 'national'
                  WHEN vt.source_id = 'BSL' THEN 'regional'
                  WHEN vt.source_id = 'LFL-BAYERN' AND vt.trialLocation IS NULL
                    THEN 'regional'
                  WHEN size(s) = 0 THEN 'unlocated'
                  ELSE 'site'
                END"""

COUNT_QUERY = f"""
MATCH (vt:VarietyTrial)
OPTIONAL MATCH (vt)-[:TRIAL_AT]->(ts:TrialSite)
WITH vt, collect(DISTINCT ts.name) AS s
WITH {SCOPE_EXPR} AS scope
RETURN scope, count(*) AS n ORDER BY n DESC
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    with driver.session() as s:
        counts = {r["scope"]: r["n"]
                  for r in s.run(COUNT_QUERY, prefix=SYNTHETIC_PREFIX).data()}
        total = sum(counts.values())
        print(f"VarietyTrial: {total:,}")
        for k in ("site", "national", "regional", "unlocated"):
            n = counts.get(k, 0)
            print(f"  {k:12s} {n:7,}  ({100 * n / total:.1f} %)")

        # Qué parte del conjunto de evaluación queda marcada como agregado.
        pool = s.run(
            f"""
            MATCH (vt:VarietyTrial)
            WHERE vt.yieldKgHa IS NOT NULL AND vt.yieldDerivationMethod IS NULL
            OPTIONAL MATCH (vt)-[:TRIAL_AT]->(ts:TrialSite)
            WITH vt, collect(DISTINCT ts.name) AS s
            WITH {SCOPE_EXPR} AS scope
            RETURN scope, count(*) AS n ORDER BY n DESC
            """,
            prefix=SYNTHETIC_PREFIX,
        ).data()
        print("\nconjunto de evaluación (rendimiento medido, no derivado):")
        for r in pool:
            print(f"  {r['scope']:12s} {r['n']:7,}")

        if not args.execute:
            print("\nDRY-RUN — no se ha modificado nada. Repetir con --execute.")
            driver.close()
            return 0

        updated = s.run(
            f"""
            MATCH (vt:VarietyTrial)
            OPTIONAL MATCH (vt)-[:TRIAL_AT]->(ts:TrialSite)
            WITH vt, collect(DISTINCT ts.name) AS s
            SET vt.aggregationScope = {SCOPE_EXPR}
            RETURN count(vt) AS c
            """,
            prefix=SYNTHETIC_PREFIX,
        ).single()["c"]
        print(f"\nnodos actualizados: {updated:,}")

        final = s.run(
            "MATCH (vt:VarietyTrial) RETURN vt.aggregationScope AS scope, "
            "count(*) AS n ORDER BY n DESC"
        ).data()
        print("resultado en el grafo:")
        for r in final:
            print(f"  {str(r['scope']):12s} {r['n']:7,}")

        integrity = s.run(
            "MATCH (vt:VarietyTrial) RETURN count(*) AS total, "
            "count(vt.yieldKgHa) AS rendimiento, count(vt.cropCycle) AS ciclo"
        ).single()
        print(f"\nintegridad: {integrity['total']:,} ensayos, "
              f"{integrity['rendimiento']:,} con rendimiento, "
              f"{integrity['ciclo']:,} con ciclo")

    driver.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
