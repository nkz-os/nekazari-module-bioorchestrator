"""Backfill `cropCycle` on VarietyTrial nodes already in the graph.

The growing cycle (winter / spring / facultative) is a property of the variety, not
of the species: barley, wheat, rye and oats all have both types. It is present in
the sources but was discarded at ingest, so every node in the graph has it null.
Without it, per-crop means pool winter and spring populations — agronomically
meaningless — and season-aware rotation planning cannot work.

This does NOT re-ingest. It only SETs a property on existing nodes, matched by the
natural key (source, EPPO, variety, location, year). Safe while the uniqueness
constraint on mergeKey is still missing.

Two sources, with different confidence, recorded in `cropCycleSource`:

  BSL     `source_label`  — per-trial German label (Winterweichweizen, Sommergerste).
                            Explicit and per-observation.
  GENVCE  `article_group` — article-level `crop_group = cereales-invierno`. Coarse:
                            it marks the report as winter cereals, not each trial.

Keys whose source rows disagree (the same variety, site and year appearing as both
winter and spring) are left NULL: the graph lost the field that told them apart, so
guessing would fabricate data.

Usage:
    python3 backfill_crop_cycle.py                # dry-run, reports only
    python3 backfill_crop_cycle.py --execute
"""
from __future__ import annotations

import argparse
import collections
import glob
import json
import os
import sys

from neo4j import GraphDatabase

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

BSL_BUNDLES = "/home/g/Documents/nekazari/nkz-bsa-scraper/data/jsonld/*.jsonld"
GENVCE_BUNDLES = "/home/g/Documents/nekazari/nkz-genvce-scraper/data/jsonld/*.jsonld"
WINTER_CEREAL_GROUP = "cereales-invierno"

# The GENVCE article group says "winter cereals" about the REPORT, not about each
# trial in it, and those reports also carry trials of crops that have no winter
# form. Restrict the inference to species that genuinely have a winter type;
# anything else (maize above all — it is always spring-sown) keeps a null cycle.
WINTER_CEREAL_EPPO = {
    "TRZAX",  # bread wheat
    "TRZDU",  # durum wheat
    "TRZAW",  # spelt / winter wheat
    "HORVX",  # barley
    "SECCE",  # rye
    "AVESA",  # oats
    "TTLSS",  # triticale
}

URI = os.getenv("NEO4J_URI", "")
USER = os.getenv("NEO4J_USER", "")
PASSWORD = os.getenv("NEO4J_PASSWORD", "")

if not (URI and USER and PASSWORD):
    raise SystemExit(
        "Faltan credenciales. Exporte NEO4J_URI, NEO4J_USER y NEO4J_PASSWORD."
    )


def _key(source: str, eppo: str, variety: str, location: str, year) -> tuple:
    return (
        source,
        (eppo or "").replace("eppo:", "").upper(),
        (variety or "").strip().lower(),
        (location or "").strip().lower(),
        year,
    )


def _trials(pattern: str):
    for path in glob.glob(pattern):
        try:
            data = json.load(open(path, encoding="utf-8"))
        except Exception:
            continue
        for node in data.get("@graph", []):
            if "VarietyTrial" in str(node.get("@type", "")):
                yield node


def collect_cycles() -> tuple[dict, dict]:
    """natural key -> cycle, plus the count of keys dropped for disagreeing."""
    votes: dict[tuple, set] = collections.defaultdict(set)
    origin: dict[tuple, str] = {}

    from app.ingestion.bsl_ingester import BslIngester  # solo al leer bundles

    for node in _trials(BSL_BUNDLES):
        cycle = BslIngester._crop_cycle(node.get("crop_scientific"))
        if not cycle:
            continue
        k = _key("BSL", node.get("crop_eppo"), node.get("variety"),
                 node.get("trial_location"), node.get("year"))
        votes[k].add(cycle)
        origin[k] = "source_label"

    for node in _trials(GENVCE_BUNDLES):
        group = (node.get("metadata") or {}).get("crop_group")
        if group != WINTER_CEREAL_GROUP:
            continue
        k = _key("GENVCE", node.get("crop_eppo"), node.get("variety"),
                 node.get("trial_location"), node.get("year"))
        if k[1] not in WINTER_CEREAL_EPPO:
            continue  # e.g. maize inside a winter-cereals report
        votes[k].add("winter")
        origin.setdefault(k, "article_group")

    resolved = {k: next(iter(v)) for k, v in votes.items() if len(v) == 1}
    ambiguous = {k for k, v in votes.items() if len(v) > 1}
    return {k: (c, origin[k]) for k, c in resolved.items()}, ambiguous


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    ap.add_argument(
        "--dump-json", metavar="PATH",
        help="Resolve cycles from the local bundles and write them to PATH. "
             "No database access — run this where the scraper bundles live.",
    )
    ap.add_argument(
        "--from-json", metavar="PATH",
        help="Apply a mapping produced by --dump-json instead of reading the "
             "bundles. Use this inside the cluster, where the bundles are absent.",
    )
    args = ap.parse_args()

    if args.from_json:
        payload = json.load(open(args.from_json, encoding="utf-8"))
        resolved = {tuple(r["key"]): (r["cycle"], r["origin"]) for r in payload["rows"]}
        ambiguous = set(range(payload["ambiguous"]))  # count only
    else:
        resolved, ambiguous = collect_cycles()

    if args.dump_json:
        json.dump(
            {
                "rows": [
                    {"key": list(k), "cycle": c, "origin": o}
                    for k, (c, o) in resolved.items()
                ],
                "ambiguous": len(ambiguous),
            },
            open(args.dump_json, "w", encoding="utf-8"),
            ensure_ascii=False,
        )
        print(f"escrito {args.dump_json}: {len(resolved):,} claves, "
              f"{len(ambiguous):,} descartadas por desacuerdo")
        return 0

    by_origin = collections.Counter(o for _, o in resolved.values())
    by_cycle = collections.Counter(c for c, _ in resolved.values())

    print(f"claves resueltas desde las fuentes: {len(resolved):,}")
    print(f"  por ciclo:  {dict(by_cycle)}")
    print(f"  por origen: {dict(by_origin)}")
    print(f"claves descartadas por desacuerdo:  {len(ambiguous):,} "
          "(el grafo no puede distinguirlas — se dejan nulas)")

    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    with driver.session() as s:
        # Without this the MATCH below scans every VarietyTrial once per input row
        # (35k × 12k comparisons). The index is idempotent and worth keeping: the
        # (source, crop) pair is the natural entry point for per-source queries.
        s.run(
            "CREATE INDEX variety_trial_source_crop IF NOT EXISTS "
            "FOR (vt:VarietyTrial) ON (vt.source_id, vt.cropEppo)"
        )
        s.run("CALL db.awaitIndexes(300)")

        before = s.run(
            "MATCH (vt:VarietyTrial) RETURN count(vt) AS total, "
            "count(vt.cropCycle) AS with_cycle"
        ).single()
        print(f"\ngrafo ahora: {before['total']:,} ensayos, "
              f"{before['with_cycle']:,} con ciclo")

        rows = [
            {"source": k[0], "eppo": k[1], "variety": k[2], "location": k[3],
             "year": k[4], "cycle": c, "origin": o}
            for k, (c, o) in resolved.items()
        ]

        matched = s.run(
            """
            UNWIND $rows AS row
            MATCH (vt:VarietyTrial {source_id: row.source, cropEppo: row.eppo})
            WHERE toLower(trim(coalesce(vt.variety, ''))) = row.variety
              AND toLower(trim(coalesce(vt.trialLocation, ''))) = row.location
              AND vt.year = row.year
            RETURN count(vt) AS c
            """,
            rows=rows,
        ).single()["c"]
        print(f"nodos que emparejan en el grafo: {matched:,}")

        if not args.execute:
            print("\nDRY-RUN — no se ha modificado nada. Repetir con --execute.")
            driver.close()
            return 0

        updated = s.run(
            """
            UNWIND $rows AS row
            MATCH (vt:VarietyTrial {source_id: row.source, cropEppo: row.eppo})
            WHERE toLower(trim(coalesce(vt.variety, ''))) = row.variety
              AND toLower(trim(coalesce(vt.trialLocation, ''))) = row.location
              AND vt.year = row.year
            SET vt.cropCycle = row.cycle,
                vt.cropCycleSource = row.origin
            RETURN count(vt) AS c
            """,
            rows=rows,
        ).single()["c"]

        after = s.run(
            "MATCH (vt:VarietyTrial) RETURN count(vt) AS total, "
            "count(vt.cropCycle) AS with_cycle"
        ).single()
        dist = s.run(
            "MATCH (vt:VarietyTrial) WHERE vt.cropCycle IS NOT NULL "
            "RETURN vt.cropCycle AS cycle, vt.cropCycleSource AS origin, "
            "count(*) AS n ORDER BY n DESC"
        ).data()

        print(f"\nnodos actualizados: {updated:,}")
        print(f"grafo tras backfill: {after['total']:,} ensayos, "
              f"{after['with_cycle']:,} con ciclo "
              f"(+{after['with_cycle'] - before['with_cycle']:,})")
        for d in dist:
            print(f"  {d['cycle']:12s} {d['origin']:14s} {d['n']:,}")

    driver.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
