"""Backfill `effect` on RotationConstraint nodes in an existing graph.

WHY. `RotationConstraint` conflated two opposite agronomic relations with no
field telling them apart: restrictions ("do not follow within N years") and
benefits ("this is a good successor"). `intervalYears` does not separate them
either — `wheat->maize` is 1 because of Fusarium, `pea->wheat` is 1 because of
N fixation. Same number, opposite meaning.

The classification lives ONLY in data/phenology_sources.yaml (the seed source).
This script derives its map from that YAML — no second copy that could drift.
A pair present in the graph but absent from the YAML is reported, never guessed.

Usage:
    python3 backfill_rotation_effect.py            # dry-run
    python3 backfill_rotation_effect.py --execute
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import yaml
from neo4j import GraphDatabase

URI = os.getenv("NEO4J_URI", "")
USER = os.getenv("NEO4J_USER", "")
PASSWORD = os.getenv("NEO4J_PASSWORD", "")

if not (URI and USER and PASSWORD):
    raise SystemExit(
        "Faltan credenciales. Exporte NEO4J_URI, NEO4J_USER y NEO4J_PASSWORD."
    )

_YAML = Path(__file__).parent.parent / "data" / "phenology_sources.yaml"


def effect_map() -> dict[tuple[str, str], str]:
    """(cropA, cropB) -> effect, read from the canonical YAML. Single source."""
    data = yaml.safe_load(_YAML.read_text())
    out: dict[tuple[str, str], str] = {}
    for rc in data.get("rotation_constraints", []):
        out[(rc["crop_a"], rc["crop_b"])] = rc.get("effect", "restriction")
    return out


def apply(driver, execute: bool) -> dict:
    """Reconcile stored effect with the YAML. Returns a summary dict.

    Only writes rows whose stored effect differs from the YAML, so a second
    pass writes nothing. Pairs absent from the YAML are reported, not touched.
    """
    want_by_pair = effect_map()
    with driver.session() as s:
        rows = s.run(
            "MATCH (r:RotationConstraint) "
            "RETURN r.cropA AS a, r.cropB AS b, r.effect AS effect"
        ).data()

        pending, unknown, already = [], [], 0
        for r in rows:
            pair = (r["a"], r["b"])
            want = want_by_pair.get(pair)
            if want is None:
                unknown.append(pair)
            elif r["effect"] == want:
                already += 1
            else:
                pending.append((pair, want))

        written = 0
        if execute:
            for (a, b), want in pending:
                s.run(
                    "MATCH (r:RotationConstraint {cropA: $a, cropB: $b}) SET r.effect = $e",
                    a=a, b=b, e=want,
                )
                written += 1

    return {
        "total": len(rows),
        "already": already,
        "pending": len(pending),
        "written": written,
        "unknown": unknown,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    try:
        result = apply(driver, execute=args.execute)
    finally:
        driver.close()

    print(f"RotationConstraint en el grafo: {result['total']}")
    print(f"  ya correctos  : {result['already']}")
    print(f"  por escribir  : {result['pending']}")
    print(f"  sin clasificar: {len(result['unknown'])}")
    for a, b in result["unknown"]:
        print(f"    SIN CLASIFICAR {a} -> {b} — añadir al YAML y clasificar a mano, NO se toca")

    if not args.execute:
        print("\nDRY-RUN — no se ha modificado nada. Repetir con --execute.")
    else:
        print(f"\nescritos: {result['written']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
