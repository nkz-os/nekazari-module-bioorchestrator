"""
Validate that a source's JSON-LD can be fully normalised by the registry.

Checks:
  1. All EPPO codes in the data are mapped in EPPO_TO_SCIENTIFIC
  2. Trial locations are covered by LOCATION_NORMALIZATION
  3. Source-specific trait keys (if any) are registered in TRAIT_REGISTRY
  4. Required fields (cropEppo, variety, year) are present

Usage:
    python -m app.ingestion.validate_source GENVCE /path/to/trials.jsonld
    python -m app.ingestion.validate_source BSL /path/to/trials.jsonld --strict
"""

from __future__ import annotations

import argparse
import json
import sys

from app.ingestion.normalization_registry import (
    TRAIT_REGISTRY,
    DISEASE_REGISTRY,
    normalize_variety_name,
    normalize_location,
    eppo_to_scientific,
)


def validate_source(source_id: str, jsonld_path: str, strict: bool = False) -> int:
    """Validate a source JSON-LD against the normalisation registry.

    Returns the number of errors found (0 = all clear).
    """
    errors = 0
    warnings = 0

    print(f"Validating source: {source_id}")
    print(f"  File: {jsonld_path}")
    print()

    # ── Load JSON-LD ────────────────────────────────────────────────────
    with open(jsonld_path, encoding="utf-8") as f:
        data = json.load(f)
    graph = data.get("@graph", [])
    if not graph:
        print("  ❌ Empty or missing @graph")
        return 1

    # ── Extract nodes ───────────────────────────────────────────────────
    trials = [n for n in graph if n.get("@type") == "VarietyTrial"]
    sites = [n for n in graph if n.get("@type") == "TrialSite"]
    articles = [n for n in graph if n.get("@type") == "ArticleSource"]
    mgmt_trials = [n for n in graph if n.get("@type") == "ManagementTrial"]

    print(f"  VarietyTrial:     {len(trials)}")
    print(f"  ManagementTrial:  {len(mgmt_trials)}")
    print(f"  TrialSite:        {len(sites)}")
    print(f"  ArticleSource:    {len(articles)}")
    print()

    # ── 1. EPPO code coverage ──────────────────────────────────────────
    eppos: set[str] = set()
    for t in trials:
        e = t.get("crop_eppo") or t.get("cropEppo") or ""
        if e:
            eppos.add(e.replace("eppo:", "").replace("EPPO:", "").upper().strip())
    for m in mgmt_trials:
        e = m.get("crop_eppo") or m.get("cropEppo") or ""
        if e:
            eppos.add(e.replace("eppo:", "").replace("EPPO:", "").upper().strip())

    print(f"  Unique EPPO codes: {len(eppos)}")

    for code in sorted(eppos):
        if code:
            sci = eppo_to_scientific(code)
            if sci:
                print(f"    ✅ {code} → {sci}")
            else:
                print(f"    ❌ {code} → NOT MAPPED in EPPO_TO_SCIENTIFIC")
                errors += 1
    print()

    # ── 2. Trial locations ──────────────────────────────────────────────
    locs: set[str] = set()
    for t in trials:
        loc = t.get("trial_location") or t.get("trialLocation")
        if loc:
            locs.add(loc)
    for m in mgmt_trials:
        loc = m.get("trial_location") or m.get("trialLocation")
        if loc:
            locs.add(loc)

    unmapped_locs = []
    for loc in sorted(locs):
        info = normalize_location(loc)
        if info:
            print(f"    ✅ {loc[:50]:50s} → {info['name']}")
        else:
            unmapped_locs.append(loc)
            msg = f"    ⚠️  {loc[:50]:50s} → NOT MAPPED"
            if strict:
                print(msg.replace("⚠️", "❌"))
                errors += 1
            else:
                print(msg)
                warnings += 1
    print()

    # ── 3. Source-specific trait keys ───────────────────────────────────
    traits_found: set[str] = set()
    for t in trials:
        raw = t.get("agronomic_traits") or t.get("agronomicTraits")
        if raw:
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except json.JSONDecodeError:
                    pass
            if isinstance(raw, dict):
                traits_found.update(raw.keys())

    if traits_found:
        print(f"  Agronomic trait keys found in data: {len(traits_found)}")
        registered_source_keys = set()
        for canonical, config in TRAIT_REGISTRY.items():
            src_key = config["sources"].get(source_id)
            if src_key:
                registered_source_keys.add(src_key)

        for key in sorted(traits_found):
            if key in registered_source_keys:
                print(f"    ✅ {key} → registered")
            else:
                print(f"    ⚠️  {key} → NOT in TRAIT_REGISTRY for {source_id}")
                if strict:
                    errors += 1
                else:
                    warnings += 1
        print()

    # ── 4. Required fields ──────────────────────────────────────────────
    missing_req = 0
    for i, t in enumerate(trials):
        e = t.get("crop_eppo") or t.get("cropEppo")
        v = t.get("variety")
        y = t.get("year")
        if not e:
            missing_req += 1
        if not v:
            missing_req += 1
        if not y:
            missing_req += 1

    if missing_req:
        print(f"  ❌ {missing_req} missing required fields (cropEppo, variety, year)")
        errors += 1
    else:
        print(f"  ✅ All required fields present")
    print()

    # ── Summary ─────────────────────────────────────────────────────────
    if errors:
        print(f"  ❌ {errors} error(s) — fix before ingestion")
    if warnings:
        print(f"  ⚠️  {warnings} warning(s) — review recommended")
    if not errors and not warnings:
        print(f"  ✅ Source {source_id} is fully normalisable")
    print()

    return errors


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate a source JSON-LD against the normalisation registry",
    )
    parser.add_argument("source_id", help="Source identifier (e.g. BSL, GENVCE)")
    parser.add_argument("jsonld", help="Path to the JSON-LD file")
    parser.add_argument("--strict", action="store_true",
                        help="Treat warnings as errors")
    args = parser.parse_args()

    return validate_source(args.source_id, args.jsonld, strict=args.strict)


if __name__ == "__main__":
    sys.exit(main())
