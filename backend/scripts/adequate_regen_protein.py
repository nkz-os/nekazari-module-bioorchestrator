"""Adequacy layer for IFAPA/ITACYL/INTIA regen-protein curated bundles.

Reads ``curated_regen_protein.jsonld`` (from workspace ``curate_datasets.py``),
adds canonical ``source_id``, ``mergeKey``, site linkage, and year repair —
without re-scraping. Emits ``all_trials_adequate.jsonld`` for gate + ingest.

Usage:
    python -m scripts.adequate_regen_protein ifapa
    python -m scripts.adequate_regen_protein itacyl
    python -m scripts.adequate_regen_protein intia
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.ingestion.base_ingester import BaseIngester
from app.ingestion.validate_ingest_bundle import validate_bundle

CONTEXT_URL = "https://nkz.robotika.cloud/ngsi-ld/bioorchestrator-context.jsonld"

SCRAPERS: dict[str, dict[str, str]] = {
    "ifapa": {
        "dir": "nkz-ifapa-scraper",
        "legacy_source": "IFAPA-EXP",
        "canonical_source": "IFAPA",
    },
    "itacyl": {
        "dir": "nkz-itacyl-scraper",
        "legacy_source": "ITACYL-EXP",
        "canonical_source": "ITACYL",
    },
    "intia": {
        "dir": "nkz-intia-scraper",
        "legacy_source": "INTIA-EXP",
        "canonical_source": "INTIA-EXP",
    },
}

_YEAR_RANGE = re.compile(r":(\d{4})-(\d{4}):")
_YEAR_SINGLE = re.compile(r":(\d{4}):")


def _workspace_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _parse_year(node: dict) -> int | None:
    year = node.get("year")
    if isinstance(year, int) and not isinstance(year, bool) and year > 1900:
        return year
    nid = str(node.get("@id") or "")
    m = _YEAR_RANGE.search(nid)
    if m:
        return int(m.group(2))
    m = _YEAR_SINGLE.search(nid)
    if m:
        y = int(m.group(1))
        return y if y > 1900 else None
    return None


def _site_key(name: str) -> str:
    return str(name).lower().replace(" ", "-").replace(",", "").replace("(", "").replace(")", "")


def _merge_key_site(source_id: str, name: str, municipality: str = "") -> str:
    return (
        f"{source_id.lower()}|{name.strip().lower()}|{municipality.strip().lower()}"
    )


def _merge_key_trial(
    source_id: str,
    eppo: str,
    variety: str,
    location: str,
    year: int,
    irrigation: str = "unknown",
) -> str:
    return (
        f"{source_id}|eppo:{eppo}|{variety.strip().lower()}|"
        f"{location.strip().lower()}|{irrigation}|{year}|regen"
    )


def adequate_curated(curated_path: Path, *, canonical_source: str) -> dict:
    data = json.loads(curated_path.read_text(encoding="utf-8"))
    graph = data.get("@graph", [])
    if not isinstance(graph, list):
        raise ValueError("missing @graph")

    sites_out: dict[str, dict] = {}
    articles_out: dict[str, dict] = {}
    trials_out: list[dict] = []
    excluded_bad_year = 0

    for node in graph:
        ntype = node.get("@type")
        if ntype == "TrialSite":
            name = str(node.get("name") or "").strip()
            if not name:
                continue
            sid = node.get("@id") or f"urn:nkz:site:{_site_key(name)}"
            sites_out[sid] = {
                **node,
                "@id": sid,
                "source_id": canonical_source,
                "mergeKey": _merge_key_site(
                    canonical_source, name, str(node.get("municipality") or "")
                ),
            }
        elif ntype == "ArticleSource":
            aid = node.get("@id") or f"urn:nkz:source:{canonical_source.lower()}"
            articles_out[aid] = {
                **node,
                "@id": aid,
                "source_id": canonical_source,
                "source": canonical_source,
            }
        elif ntype == "VarietyTrial":
            year = _parse_year(node)
            if year is None:
                excluded_bad_year += 1
                continue

            eppo = BaseIngester._normalize_eppo(node.get("crop_eppo"))
            if not eppo:
                continue

            location = str(node.get("trial_location") or "").strip()
            if not location:
                continue

            variety = str(node.get("variety") or "genérico").strip()
            irr = str(node.get("irrigation_regime") or "unknown")
            site_id = f"urn:nkz:site:{_site_key(location)}"
            mk = _merge_key_trial(
                canonical_source, eppo, variety, location, year, irr
            )
            uid = (
                f"urn:nkz:{canonical_source.lower()}:trial:"
                f"{hashlib.md5(mk.encode()).hexdigest()[:16]}"
            )

            trial = {
                **{k: v for k, v in node.items() if k not in ("dataSource",)},
                "@id": uid,
                "source_id": canonical_source,
                "year": year,
                "mergeKey": mk,
                "ranking_eligible": False,
                "yield_data_type": "metadata_only",
                "refTrialSite": {"@id": site_id},
            }
            trials_out.append(trial)

            if site_id not in sites_out and location:
                sites_out[site_id] = {
                    "@id": site_id,
                    "@type": "TrialSite",
                    "name": location,
                    "source_id": canonical_source,
                    "mergeKey": _merge_key_site(canonical_source, location),
                }

    out_graph = list(articles_out.values()) + list(sites_out.values()) + trials_out
    return {
        "@context": data.get("@context") or CONTEXT_URL,
        "adequacy_meta": {
            "date": str(date.today()),
            "canonical_source_id": canonical_source,
            "excluded_bad_year": excluded_bad_year,
            "note": "Regen/protein cover-crop trials — metadata only, ranking_eligible false",
        },
        "@graph": out_graph,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Adequate regen-protein curated bundle")
    parser.add_argument("scraper", choices=sorted(SCRAPERS))
    parser.add_argument("--validate", action="store_true", help="Run ingest gate after write")
    args = parser.parse_args()

    cfg = SCRAPERS[args.scraper]
    root = _workspace_root()
    scraper_dir = root / cfg["dir"]
    curated = scraper_dir / "data" / "jsonld" / "curated_regen_protein.jsonld"
    output = scraper_dir / "data" / "jsonld" / "all_trials_adequate.jsonld"

    if not curated.is_file():
        print(f"curated bundle not found: {curated}", file=sys.stderr)
        return 1

    doc = adequate_curated(curated, canonical_source=cfg["canonical_source"])
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")

    vt = sum(1 for n in doc["@graph"] if n.get("@type") == "VarietyTrial")
    ts = sum(1 for n in doc["@graph"] if n.get("@type") == "TrialSite")
    print(f"Wrote {output} — VT={vt} sites={ts} source={cfg['canonical_source']}")

    if args.validate:
        report = validate_bundle(str(output))
        print(f"Gate: {'PASS' if report.ok else 'FAIL'} errors={len(report.errors())}")
        if not report.ok:
            for err in report.errors()[:15]:
                print(f"  {err.code} {err.node_id}: {err.message}")
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
