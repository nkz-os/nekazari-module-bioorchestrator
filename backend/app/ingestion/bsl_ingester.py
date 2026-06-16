"""BSL (Bundessortenamt Germany) → canonical node transformation.

Reads the BSL JSON-LD file (from nkz-bsa-scraper) and transforms it into
canonical node dicts with mergeKeys for idempotent ingestion.

BSL uses a 1-9 scoring scale (yield_note_s1/s2) for variety characterisation
instead of absolute yield. Agronomic traits and disease scores use German
terms in the JSON-LD — standardization happens at the unified pipeline level.

Usage:
    python -m app.ingestion.bsl_ingester \
        --jsonld ../nkz-bsa-scraper/data/jsonld/bsl_all.jsonld
"""

from __future__ import annotations

import argparse
import asyncio
import json

from app.ingestion.base_ingester import BaseIngester


EPPO_TO_SPECIES: dict[str, str] = {
    "HORVX": "Hordeum vulgare",
    "TRZAX": "Triticum aestivum",
    "TRZAW": "Triticum aestivum",
    "TRZDU": "Triticum durum",
    "ZEAMX": "Zea mays",
    "BRSNN": "Brassica napus",
    "AVESA": "Avena sativa",
    "SECCE": "Secale cereale",
    "TTLSS": "Triticosecale",
    "SOLTU": "Solanum tuberosum",
}


class BslIngester(BaseIngester):
    """Transform BSL (Bundessortenamt) JSON-LD to canonical node dicts."""

    SOURCE_ID = "BSL"

    async def _parse_nodes(self, data: dict) -> dict[str, list[dict]]:
        graph = data.get("@graph", [])
        sites: list[dict] = []
        articles: list[dict] = []
        variety_trials: list[dict] = []
        management_trials: list[dict] = []

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
            "management_trials": management_trials,
        }

    def _convert_site(self, node: dict) -> dict:
        return {
            "name": node.get("name"),
            "mergeKey": (
                f"{self.SOURCE_ID.lower()}|"
                f"{str(node.get('name', '')).strip().lower()}"
            ),
        }

    def _convert_article(self, node: dict) -> dict:
        return {
            "source_id": self.SOURCE_ID,
            "issueNumber": node.get("issue_number"),
            "articleTitle": node.get("article_title"),
            "year": node.get("year"),
            "topic": node.get("topic"),
            "mergeKey": (
                f"{self.SOURCE_ID.lower()}|"
                f"{str(node.get('issue_number', ''))}|"
                f"{str(node.get('article_title', ''))[:80]}"
            ),
        }

    def _convert_trial(self, node: dict) -> dict:
        eppo = BaseIngester._normalize_eppo(node.get("crop_eppo"))
        return {
            "cropEppo": eppo,
            "cropScientific": EPPO_TO_SPECIES.get(eppo) or node.get("crop_scientific"),
            "variety": node.get("variety"),
            "year": node.get("year"),
            "yieldNoteS1": node.get("yield_note_s1"),
            "yieldNoteS2": node.get("yield_note_s2"),
            "agronomicTraits": (
                json.dumps(node.get("agronomic_traits"))
                if node.get("agronomic_traits")
                else None
            ),
            "diseaseScores": (
                json.dumps(node.get("disease_scores"))
                if node.get("disease_scores")
                else None
            ),
            "trialLocation": node.get("trial_location"),
            "confidence": node.get("confidence", self._registry_entry.get("confidence_default", "medium")),
            "source_id": self.SOURCE_ID,
            "trial_id": node.get("@id", ""),
            "mergeKey": (
                f"{self.SOURCE_ID.lower()}|{eppo or 'unknown'}|"
                f"{str(node.get('variety', '')).strip().lower()}|"
                f"{str(node.get('trial_location', 'unknown')).strip().lower()}|"
                f"{str(node.get('year', 0))}"
            ),
        }


async def main():
    parser = argparse.ArgumentParser(description="BSL → transform nodes")
    parser.add_argument("--jsonld", required=True, help="Path to BSL JSON-LD")
    parser.add_argument("--dry-run", action="store_true", default=True)
    parser.add_argument("--no-dry-run", action="store_true", help="Actually run (will fail)")
    args = parser.parse_args()

    ingester = BslIngester()
    nodes = await ingester.transform(args.jsonld)
    print(f"[{ingester.SOURCE_ID.lower()}_ingester] Transformed nodes:")
    for node_type, items in nodes.items():
        print(f"  {node_type}: {len(items)}")
    if not args.no_dry_run:
        print("Dry-run mode — no Neo4j writes.")
    else:
        await ingester.merge(nodes)


if __name__ == "__main__":
    asyncio.run(main())
