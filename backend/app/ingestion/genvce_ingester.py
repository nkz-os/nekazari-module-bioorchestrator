"""GENVCE → canonical node transformation (BaseIngester subclass).

Reads the GENVCE JSON-LD file and transforms it into canonical node dicts
with mergeKeys for idempotent ingestion and registry enrichment.

Usage:
    python -m app.ingestion.genvce_ingester \
        --jsonld ../nkz-genvce-scraper/data/trials.jsonld
"""

from __future__ import annotations

import argparse
import asyncio
import json

from app.ingestion.base_ingester import BaseIngester


EPPO_TO_SPECIES: dict[str, str] = {
    "TRZAX": "Triticum aestivum",
    "TRZAW": "Triticum aestivum",
    "TRZDU": "Triticum durum",
    "HORVX": "Hordeum vulgare",
    "ZEAMX": "Zea mays",
    "BRSNN": "Brassica napus",
    "HELAN": "Helianthus annuus",
    "GLXMA": "Glycine max",
    "PISSA": "Pisum sativum",
    "VICSA": "Vicia sativa",
    "LENCU": "Lens culinaris",
    "CIEAR": "Cicer arietinum",
    "AVESA": "Avena sativa",
    "SECCE": "Secale cereale",
    "TTLSS": "Triticosecale",
}


class GenvceIngester(BaseIngester):
    """Transform GENVCE JSON-LD to canonical node dicts."""

    SOURCE_ID = "GENVCE"

    async def _parse_nodes(self, data: dict) -> dict[str, list[dict]]:
        graph = data.get("@graph", [])
        sites: list[dict] = []
        articles: list[dict] = []
        variety_trials: list[dict] = []

        for node in graph:
            t = node.get("@type", "")
            if t == "TrialSite":
                sites.append(self._convert_site(node))
            elif t == "ArticleSource":
                articles.append(self._convert_article(node))
            elif t == "VarietyTrial":
                variety_trials.append(self._convert_trial(node))

        return {
            "trial_sites": sites,
            "article_sources": articles,
            "variety_trials": variety_trials,
            "management_trials": [],
        }

    # ── Converters ─────────────────────────────────────────────────────────

    def _convert_site(self, node: dict) -> dict:
        name = node.get("name")
        municipality = node.get("municipality")
        default_mk = (
            f"{self.SOURCE_ID.lower()}|"
            f"{str(name or '').strip().lower()}|"
            f"{str(municipality or '').strip().lower()}"
        )
        return {
            "name": name,
            "municipality": municipality,
            "region": node.get("region"),
            "agroclimatic_zone": node.get("agroclimatic_zone"),
            "latitude": node.get("latitude"),
            "longitude": node.get("longitude"),
            "elevationM": node.get("elevationM"),
            "climateClass": node.get("climateClass"),
            "annualRainfallMm": node.get("annualRainfallMm"),
            "soilType": node.get("soilType"),
            "soilTexture": node.get("soilTexture"),
            "soilPh": node.get("soilPh"),
            "source_id": self.SOURCE_ID,
            "mergeKey": node.get("mergeKey") or default_mk,
        }

    @staticmethod
    def _convert_article(node: dict) -> dict:
        return {
            "source_id": "GENVCE",
            "issueNumber": node.get("issue_number"),
            "articleTitle": node.get("article_title"),
            "articleAuthor": node.get("article_author"),
            "year": node.get("year"),
            "topic": node.get("topic"),
            "mergeKey": (
                f"genvce|{str(node.get('issue_number', ''))}|"
                f"{str(node.get('article_title', ''))[:80]}"
            ),
        }

    def _convert_trial(self, node: dict) -> dict:
        eppo = BaseIngester._normalize_eppo(node.get("crop_eppo"))
        irr = str(node.get("irrigation_regime") or "unknown")
        loc = str(node.get("trial_location") or "unknown").strip().lower()
        year = node.get("year") or 0
        variety = str(node.get("variety") or "").strip().lower()
        default_mk = (
            f"{self.SOURCE_ID}|eppo:{eppo or 'unknown'}|{variety}|{loc}|{irr}|{year}|genvce"
        )
        out = {
            "cropEppo": eppo,
            "cropScientific": EPPO_TO_SPECIES.get(eppo) if eppo else None,
            "variety": node.get("variety"),
            "year": node.get("year"),
            "yieldKgHa": node.get("yield_kg_ha"),
            "yieldRelativePct": node.get("yield_relative_pct"),
            "irrigationRegime": node.get("irrigation_regime"),
            "productionSystem": node.get("production_system"),
            "trialLocation": node.get("trial_location"),
            "agroclimaticZone": node.get("agroclimatic_zone"),
            "qualityParams": (
                json.dumps(node.get("quality_params"))
                if node.get("quality_params")
                else None
            ),
            "diseaseScores": (
                json.dumps(node.get("disease_scores"))
                if node.get("disease_scores")
                else None
            ),
            "confidence": node.get("confidence", "high"),
            "source_id": self.SOURCE_ID,
            "trial_id": node.get("@id", ""),
            "mergeKey": node.get("mergeKey") or default_mk,
            "rankingEligible": node.get("ranking_eligible"),
            "yieldDataType": node.get("yield_data_type"),
        }
        return {k: v for k, v in out.items() if v is not None}


# ── CLI ──────────────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="GENVCE → transform nodes")
    parser.add_argument("--jsonld", required=True, help="Path to GENVCE JSON-LD")
    parser.add_argument("--dry-run", action="store_true", default=True)
    parser.add_argument(
        "--no-dry-run",
        action="store_true",
        help="Actually run (will fail — reserved for unified ingestion)",
    )
    args = parser.parse_args()

    ingester = GenvceIngester()
    nodes = await ingester.transform(args.jsonld)

    print("[genvce_ingester] Transformed nodes:")
    for node_type, items in nodes.items():
        print(f"  {node_type}: {len(items)}")

    if not args.no_dry_run:
        print("Dry-run mode — no Neo4j writes.")
    else:
        await ingester.merge(nodes)


if __name__ == "__main__":
    asyncio.run(main())
