"""EVENA → canonical node transformation.

Reads the EVENA JSON-LD file and transforms it into canonical node
dicts with mergeKeys and registry enrichment.
"""

from __future__ import annotations

import argparse
import asyncio

from app.ingestion.base_ingester import BaseIngester


class EvenaIngester(BaseIngester):
    """Transform EVENA JSON-LD to canonical node dicts."""

    SOURCE_ID = "EVENA"

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
        name = node.get("name")
        return {
            "name": name,
            "municipality": node.get("municipality") or name,
            "latitude": node.get("latitude"),
            "longitude": node.get("longitude"),
            "climateClass": node.get("climateClass") or node.get("climate_class"),
            "source_id": self.SOURCE_ID,
            "mergeKey": (
                f"{self.SOURCE_ID}|"
                f"{str(name or '').strip().lower()}|"
                f"{str(node.get('municipality') or name or '').strip().lower()}"
            ),
        }

    def _convert_article(self, node: dict) -> dict:
        title = node.get("article_title") or node.get("name") or ""
        year = node.get("year")
        return {
            "source_id": self.SOURCE_ID,
            "issueNumber": node.get("issue_number") or year,
            "articleTitle": title,
            "year": year,
            "mergeKey": (
                f"{self.SOURCE_ID.lower()}|"
                f"{str(year or '')}|"
                f"{str(title).strip().lower()[:80]}"
            ),
        }

    def _convert_trial(self, node: dict) -> dict:
        eppo = BaseIngester._normalize_eppo(node.get("crop_eppo"))
        qp = node.get("quality_params") or {}
        return {
            "cropEppo": eppo,
            "cropScientific": node.get("crop_scientific"),
            "variety": node.get("variety"),
            "year": node.get("year"),
            "yieldKgHa": node.get("yield_kg_ha"),
            "trialLocation": node.get("trial_location"),
            "rankingEligible": node.get("ranking_eligible", False),
            "yieldMetric": qp.get("yield_metric") or node.get("yield_metric"),
            "confidence": node.get("confidence", self._registry_entry.get("confidence_default", "medium")),
            "source_id": self.SOURCE_ID,
            "trial_id": node.get("@id", ""),
            "mergeKey": node.get("mergeKey") or (
                f"{self.SOURCE_ID}|{eppo or 'unknown'}|"
                f"{str(node.get('variety', '')).strip().lower()}|"
                f"{str(node.get('trial_location', 'unknown')).strip().lower()}|"
                f"{str(node.get('year', 0))}"
            ),
        }


async def main():
    parser = argparse.ArgumentParser(description="EVENA → transform nodes")
    parser.add_argument("--jsonld", required=True, help="Path to JSON-LD")
    parser.add_argument("--dry-run", action="store_true", default=True)
    parser.add_argument("--no-dry-run", action="store_true", help="Actually run (will fail)")
    args = parser.parse_args()

    ingester = EvenaIngester()
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
