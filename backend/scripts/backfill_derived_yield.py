"""Backfill derived yieldKgHa from yieldNoteS1 (BSL 1-9 scale) via empirical per-crop factors.

Calibration (No Guessing): factor[crop] = avg(yieldKgHa / yieldNoteS1) over MEASURED
trials (both fields present AND yieldDerivationMethod IS NULL). Application: note-only
trials get yieldKgHa = yieldNoteS1 * factor[crop], with full provenance.

Rules:
  - n_dual >= 20  -> direct factor (high confidence)
  - n_dual < 20   -> group median (cereal / oilseed_pulse), computed from direct crops
  - no dual + unclassified crop -> flagged in dry-run, SKIPPED in apply (fail-safe)
  - NEVER overwrites a measured yieldKgHa.

Idempotent: derived trials carry yieldDerivationMethod, so they are excluded from
calibration on re-run -> factors stable -> re-apply is a no-op.

Usage:
  python scripts/backfill_derived_yield.py             # dry-run (default)
  python scripts/backfill_derived_yield.py --apply     # persist
"""
from __future__ import annotations

import argparse
import os
import sys
from statistics import median

from neo4j import GraphDatabase

# Static crop -> group classification (agronomy, not derived).
# Crops present in the note-only set not listed here are flagged unclassified.
CROP_GROUPS = {
    # cereals
    "HORVX": "cereal", "TRZAX": "cereal", "TTLSS": "cereal", "SECCE": "cereal",
    "AVESA": "cereal", "TRZDU": "cereal", "ZEAMX": "cereal",
    # oilseed / pulse
    "BRSNN": "oilseed_pulse", "BRSNW": "oilseed_pulse", "PIBAR": "oilseed_pulse",
    "HELAN": "oilseed_pulse", "LUPAL": "oilseed_pulse", "LINUS": "oilseed_pulse",
}
MIN_DIRECT_N = 20


def connect() -> "GraphDatabase.driver":
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    pwd = os.getenv("NEO4J_PASSWORD", "bioorchestrator")
    driver = GraphDatabase.driver(uri, auth=(user, pwd))
    driver.verify_connectivity()
    print(f"[backfill] connected to {uri}")
    return driver


def calibrate(session) -> dict:
    """{crop: {factor, n}} from MEASURED dual trials only (yieldDerivationMethod IS NULL)."""
    rows = session.run("""
        MATCH (vt:VarietyTrial)
        WHERE vt.yieldKgHa IS NOT NULL
          AND vt.yieldNoteS1 IS NOT NULL
          AND vt.yieldDerivationMethod IS NULL
          AND vt.yieldNoteS1 > 0
        RETURN vt.cropEppo AS eppo,
               avg(toFloat(vt.yieldKgHa) / toFloat(vt.yieldNoteS1)) AS factor,
               count(*) AS n
    """)
    return {r["eppo"]: {"factor": r["factor"], "n": r["n"]} for r in rows}


def group_medians(cal: dict) -> dict:
    """Median factor per group, from high-confidence (n>=20) direct crops only."""
    buckets: dict[str, list[float]] = {}
    for crop, c in cal.items():
        g = CROP_GROUPS.get(crop)
        if g and c["n"] >= MIN_DIRECT_N:
            buckets.setdefault(g, []).append(c["factor"])
    return {g: median(v) for g, v in buckets.items() if v}


def resolve(crop: str, cal: dict, gmedians: dict) -> dict:
    """Resolve factor + source for a crop present in the note-only set."""
    c = cal.get(crop)
    g = CROP_GROUPS.get(crop)
    if c and c["n"] >= MIN_DIRECT_N:
        return {"factor": c["factor"], "n_dual": c["n"], "source": "direct", "group": g}
    if c and g and g in gmedians:
        return {"factor": gmedians[g], "n_dual": c["n"], "source": f"group_median({g})", "group": g}
    if not c and g and g in gmedians:
        return {"factor": gmedians[g], "n_dual": 0, "source": f"group_median({g},no_dual)", "group": g}
    return {"factor": None, "n_dual": (c["n"] if c else 0),
            "source": "UNCLASSIFIED_NO_FACTOR", "group": g}


def note_only_counts(session) -> dict:
    rows = session.run("""
        MATCH (vt:VarietyTrial)
        WHERE vt.yieldKgHa IS NULL AND vt.yieldNoteS1 IS NOT NULL
        RETURN vt.cropEppo AS eppo, count(*) AS n
    """)
    return {r["eppo"]: r["n"] for r in rows}


def coverage(session) -> tuple[int, int]:
    r = session.run("""
        MATCH (vt:VarietyTrial)
        RETURN count(vt) AS total, count(vt.yieldKgHa) AS with_yield
    """).single()
    return r["total"], r["with_yield"]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="persist (default: dry-run)")
    args = ap.parse_args()
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"[backfill] mode={mode}\n")

    driver = connect()
    with driver.session() as session:
        cal = calibrate(session)
        gmedians = group_medians(cal)
        note_only = note_only_counts(session)
        total, with_yield_before = coverage(session)

        print("=== group medians (from n>=20 direct crops) ===")
        for g, f in sorted(gmedians.items()):
            print(f"  {g}: {f:.0f} kg/ha per note")
        print()

        print("=== per-crop resolution (note-only trials to fill) ===")
        print(f"  {'crop':<8} {'group':<16} {'n_dual':>7} {'n_fill':>7} {'factor':>8}  source")
        plan, unclassified = [], []
        for crop in sorted(note_only, key=lambda c: -note_only[c]):
            r = resolve(crop, cal, gmedians)
            n_fill = note_only[crop]
            print(f"  {crop:<8} {str(r['group']):<16} {r['n_dual']:>7} {n_fill:>7} "
                  f"{(r['factor'] or 0):>8.0f}  {r['source']}")
            if r["factor"] is None:
                unclassified.append((crop, n_fill))
            else:
                plan.append((crop, r["factor"], r["n_dual"], r["group"], n_fill))
        total_fill = sum(p[4] for p in plan)
        print()
        print(f"=== summary ===")
        print(f"  total VarietyTrial:        {total}")
        print(f"  yieldKgHa BEFORE:          {with_yield_before} ({with_yield_before/total:.0%})")
        print(f"  would-fill (dry-run):      {total_fill}")
        projected = with_yield_before + total_fill
        print(f"  yieldKgHa AFTER (project): {projected} ({projected/total:.0%})")
        if unclassified:
            print(f"  ⚠️ UNCLASSIFIED (skipped, {sum(n for _,n in unclassified)} trials): "
                  + ", ".join(f"{c}({n})" for c, n in unclassified))

        if not args.apply:
            print("\n[backfill] dry-run only. Re-run with --apply to persist.")
            driver.close()
            return 0

        if unclassified:
            print("\n[backfill] ABORT: unclassified crops present — resolve classification first (No Guessing).")
            driver.close()
            return 2

        print("\n=== APPLY ===")
        total_set = 0
        for crop, factor, n_dual, group, _ in plan:
            res = session.run("""
                MATCH (vt:VarietyTrial)
                WHERE vt.yieldKgHa IS NULL
                  AND vt.yieldNoteS1 IS NOT NULL
                  AND vt.cropEppo = $crop
                SET vt.yieldKgHa = toFloat(vt.yieldNoteS1) * $factor,
                    vt.yieldDerivationMethod = 'bsl_note_empirical_factor',
                    vt.yieldDerivationFactor = $factor,
                    vt.yieldDerivationCalibrationN = $n_dual,
                    vt.yieldDerivationGroup = $group
                RETURN count(vt) AS set_n
            """, crop=crop, factor=factor, n_dual=n_dual, group=group).single()
            set_n = res["set_n"]
            total_set += set_n
            print(f"  {crop} (factor={factor:.0f}, group={group}): set {set_n}")

        _, with_yield_after = coverage(session)
        print(f"\n=== DONE ===")
        print(f"  set this run:              {total_set}")
        print(f"  yieldKgHa AFTER (actual):  {with_yield_after} ({with_yield_after/total:.0%})")
    driver.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
