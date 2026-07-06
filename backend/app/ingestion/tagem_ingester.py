"""TAGEM → canonical node transformation.

Reads the TAGEM JSON-LD file and transforms it into canonical node
dicts with mergeKeys and registry enrichment.
"""

from __future__ import annotations

import argparse
import asyncio

from app.ingestion.base_ingester import BaseIngester


class TagemIngester(BaseIngester):
    """Transform TAGEM JSON-LD to canonical node dicts."""

    SOURCE_ID = "TAGEM_TR_2012"

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
            "mergeKey": (
                f"{self.SOURCE_ID.lower()}|"
                f"{str(node.get('source', ''))[:80]}"
            ),
        }

    def _convert_trial(self, node: dict) -> dict:
        eppo = BaseIngester._normalize_eppo(node.get("crop_eppo"))
        return {
            "cropEppo": eppo,
            "cropScientific": node.get("crop_scientific"),
            "variety": node.get("variety"),
            "year": node.get("year"),
            "yieldKgHa": node.get("yield_kg_ha"),
            "trialLocation": node.get("trial_location"),
            "rankingEligible": node.get("ranking_eligible", False),
            "confidence": node.get("confidence", self._registry_entry.get("confidence_default", "medium")),
            "source_id": self.SOURCE_ID,
            "trial_id": node.get("@id", ""),
            "mergeKey": node.get("mergeKey") or (
                f"{self.SOURCE_ID.lower()}|{eppo or 'unknown'}|"
                f"{str(node.get('variety', '')).strip().lower()}|"
                f"{str(node.get('trial_location', 'unknown')).strip().lower()}|"
                f"{str(node.get('year', 0))}"
            ),
        }


async def main():
    parser = argparse.ArgumentParser(description="TAGEM → transform nodes")
    parser.add_argument("--jsonld", required=True, help="Path to JSON-LD")
    parser.add_argument("--dry-run", action="store_true", default=True)
    parser.add_argument("--no-dry-run", action="store_true", help="Actually run (will fail)")
    args = parser.parse_args()

    ingester = TagemIngester()
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
