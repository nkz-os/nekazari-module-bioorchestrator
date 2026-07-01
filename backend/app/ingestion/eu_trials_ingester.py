"""EU-TRIAL-REPORTS → canonical node transformation (BaseIngester subclass).

Reads the aggregated European institutional trial JSON-LD
(nkz-cpvo-scraper/data/jsonld/eu_trials_v1.jsonld) and transforms it into
canonical node dicts. Real trials (measured yieldKgHa, real trial sites) from
LfL, COBORU, SLU, Arvalis, CSIC, IFAPA, BRESOV and others.

The scraper emits snake_case fields + one TrialSite per distinct trial_location
(name matches trial_location exactly, so _merge_relationships can MATCH it and
create TRIAL_AT — without which trials orphan and are invisible to extrapolate).

Usage:
    python -m app.ingestion.eu_trials_ingester \
        --jsonld ../nkz-cpvo-scraper/data/jsonld/eu_trials_v1.jsonld --apply
"""

from __future__ import annotations

import argparse
import asyncio

from app.ingestion.base_ingester import BaseIngester


class EuTrialsIngester(BaseIngester):
    """Transform EU-TRIAL-REPORTS JSON-LD to canonical node dicts."""

    SOURCE_ID = "EU-TRIAL-REPORTS"

    async def _parse_nodes(self, data: dict) -> dict[str, list[dict]]:
        graph = data.get("@graph", [])
        sites: list[dict] = []
        articles: list[dict] = []
        variety_trials: list[dict] = []

        for node in graph:
            if not isinstance(node, dict):
                continue
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
            "climateClass": node.get("climateClass"),
            # name must match trial_location for the TRIAL_AT relationship
            "mergeKey": f"{self.SOURCE_ID.lower()}|{str(name or '').strip().lower()}|",
        }

    def _convert_article(self, node: dict) -> dict:
        title = node.get("article_title")
        return {
            "source_id": self.SOURCE_ID,
            "articleTitle": title,
            "year": node.get("year"),
            "mergeKey": f"{self.SOURCE_ID.lower()}||{str(title or '')[:80]}",
        }

    def _convert_trial(self, node: dict) -> dict:
        eppo = BaseIngester._normalize_eppo(node.get("crop_eppo"))
        variety = node.get("variety")
        location = node.get("trial_location")
        production = node.get("production_system")
        year = node.get("year")
        return {
            "cropEppo": eppo,
            "cropScientific": node.get("crop_scientific"),
            "variety": variety,
            "year": year,
            "yieldKgHa": node.get("yield_kg_ha"),
            "trialLocation": location,
            "climateClass": node.get("climate_class"),
            "locationCountry": node.get("country"),
            "productionSystem": production,
            "confidence": node.get(
                "confidence", self._registry_entry.get("confidence_default", "medium")
            ),
            "source_id": self.SOURCE_ID,
            "trial_id": node.get("@id", ""),
            # production_system in the key so organic vs conventional at the same
            # variety/location/year are NOT collapsed (mirrors crea's irrigation).
            "mergeKey": (
                f"{self.SOURCE_ID.lower()}|{eppo or 'unknown'}|"
                f"{str(variety or '').strip().lower()}|"
                f"{str(location or 'unknown').strip().lower()}|"
                f"{str(production or 'unknown')}|"
                f"{str(year or 0)}"
            ),
        }


async def main():
    parser = argparse.ArgumentParser(description="EU-TRIAL-REPORTS → transform nodes")
    parser.add_argument("--jsonld", required=True, help="Path to JSON-LD")
    parser.add_argument("--apply", action="store_true", help="persist (default: dry-run)")
    args = parser.parse_args()

    ingester = EuTrialsIngester()
    nodes = await ingester.transform(args.jsonld)
    print(f"[{ingester.SOURCE_ID.lower()}_ingester] Transformed nodes:")
    for node_type, items in nodes.items():
        print(f"  {node_type}: {len(items)}")
    if args.apply:
        stats = await ingester.merge(nodes)
        print(f"[{ingester.SOURCE_ID.lower()}_ingester] MERGE stats: {stats}")
    else:
        print("Dry-run — no Neo4j writes. Re-run with --apply to persist.")


if __name__ == "__main__":
    asyncio.run(main())
