"""INIAV-LVR → canonical node transformation (BaseIngester subclass).

Reads the INIAV-LVR JSON-LD file and transforms it into canonical node
dicts with mergeKeys and registry enrichment.

Usage:
    python -m app.ingestion.iniav_ingester \
        --jsonld ../nkz-iniav-scraper/data/trials.jsonld
"""

from __future__ import annotations

import argparse
import asyncio

from app.ingestion.base_ingester import BaseIngester


EPPO_TO_SPECIES: dict[str, str] = {
    "TRZAX": "Triticum aestivum",
    "TRZAW": "Triticum aestivum",
    "TRZDU": "Triticum durum",
    "HORVX": "Hordeum vulgare",
    "AVESA": "Avena sativa",
}


class IniavIngester(BaseIngester):
    """Transform INIAV-LVR JSON-LD to canonical node dicts."""

    SOURCE_ID = "INIAV-LVR"

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
            elif t == "ManagementTrial":
                management_trials.append(self._convert_management(node))

        return {
            "trial_sites": sites,
            "article_sources": articles,
            "variety_trials": variety_trials,
            "management_trials": management_trials,
        }

    def _convert_site(self, node: dict) -> dict:
        return {
            "name": node.get("name"),
            "municipality": node.get("municipality"),
            "latitude": node.get("latitude"),
            "longitude": node.get("longitude"),
            "climateClass": node.get("climateClass"),
            "soilType": node.get("soilType"),
            "annualRainfallMm": node.get("annualRainfallMm"),
            "mergeKey": (
                f"{self.SOURCE_ID.lower()}|"
                f"{str(node.get('name', '')).strip().lower()}|"
                f"{str(node.get('municipality', '')).strip().lower()}"
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
                f"{str(node.get('issue_number', ''))}|"
                f"{str(node.get('article_title', ''))[:80]}"
            ),
        }

    def _convert_trial(self, node: dict) -> dict:
        eppo = BaseIngester._normalize_eppo(node.get("crop_eppo"))
        irr = node.get("irrigation_regime") or "secano"
        loc = str(node.get("trial_location", "unknown")).strip().lower()
        year = node.get("year") or 0
        variety = str(node.get("variety", "")).strip().lower()
        default_mk = (
            f"{self.SOURCE_ID}|eppo:{eppo or 'unknown'}|{variety}|{loc}|{irr}|{year}"
        )
        out = {
            "cropEppo": eppo,
            "cropScientific": EPPO_TO_SPECIES.get(eppo or ""),
            "variety": node.get("variety"),
            "year": node.get("year"),
            "yieldKgHa": node.get("yield_kg_ha"),
            "irrigationRegime": node.get("irrigation_regime"),
            "trialLocation": node.get("trial_location"),
            "confidence": node.get("confidence", self._registry_entry.get("confidence_default", "medium")),
            "source_id": self.SOURCE_ID,
            "trial_id": node.get("@id", ""),
            "mergeKey": node.get("mergeKey") or default_mk,
            "rankingEligible": node.get("ranking_eligible"),
            "relativeYieldRank": node.get("relative_yield_rank"),
            "yieldDataType": node.get("yield_data_type"),
        }
        return {k: v for k, v in out.items() if v is not None}

    def _convert_management(self, node: dict) -> dict:
        return {
            "cropEppo": BaseIngester._normalize_eppo(node.get("crop_eppo")),
            "experimentType": node.get("experiment_type"),
            "treatment": node.get("treatment"),
            "resultMetric": node.get("result_metric"),
            "resultValue": node.get("result_value"),
            "confidence": node.get("confidence", self._registry_entry.get("confidence_default", "medium")),
            "source_id": self.SOURCE_ID,
            "trial_id": node.get("@id", ""),
            "mergeKey": (
                f"{self.SOURCE_ID.lower()}|"
                f"{str(node.get('experiment_type', ''))}|"
                f"{str(node.get('treatment', ''))[:60]}"
            ),
        }


async def main():
    parser = argparse.ArgumentParser(description="INIAV-LVR → transform nodes")
    parser.add_argument("--jsonld", required=True, help="Path to JSON-LD")
    parser.add_argument("--dry-run", action="store_true", default=True)
    parser.add_argument("--no-dry-run", action="store_true", help="Actually run (will fail)")
    args = parser.parse_args()

    ingester = IniavIngester()
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
