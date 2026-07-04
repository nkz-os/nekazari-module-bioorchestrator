"""IFAPA Las Torres almond (PRNDU) → canonical nodes (perennial VarietyTrial)."""
from __future__ import annotations

import argparse
import asyncio
import json

from app.ingestion.base_ingester import BaseIngester

EPPO_TO_SPECIES = {"PRNDU": "Prunus dulcis"}


class AlmondIfapaIngester(BaseIngester):
    SOURCE_ID = "IFAPA_ALMOND"

    async def _parse_nodes(self, data: dict) -> dict[str, list[dict]]:
        graph = data.get("@graph", [])
        sites, articles, trials = [], [], []
        for node in graph:
            t = node.get("@type", "")
            if t == "TrialSite":
                sites.append(self._convert_site(node))
            elif t == "ArticleSource":
                articles.append(self._convert_article(node))
            elif t == "VarietyTrial":
                trials.append(self._convert_trial(node))
        return {"trial_sites": sites, "article_sources": articles,
                "variety_trials": trials, "management_trials": []}

    def _convert_site(self, node: dict) -> dict:
        # Schema §2/§3.2: scrapers emit snake_case `climate_class`
        return {
            "name": node.get("name"),
            "municipality": node.get("municipality"),
            "latitude": node.get("latitude"),
            "longitude": node.get("longitude"),
            "climateClass": node.get("climate_class"),
            "mergeKey": (f"{self.SOURCE_ID.lower()}|"
                        f"{str(node.get('name', '')).strip().lower()}|"
                        f"{str(node.get('municipality', '')).strip().lower()}"),
        }

    def _convert_article(self, node: dict) -> dict:
        return {
            "source_id": self.SOURCE_ID,
            "issueNumber": node.get("issue_number"),
            "articleTitle": node.get("article_title"),
            "year": node.get("year"),
            "mergeKey": (f"{self.SOURCE_ID.lower()}|{node.get('issue_number', '')}|"
                        f"{str(node.get('article_title', ''))[:80]}"),
        }

    def _convert_trial(self, node: dict) -> dict:
        eppo = BaseIngester._normalize_eppo(node.get("crop_eppo"))
        # Schema §3.4: yield_metric disambiguates kernel/oil/fruit. Record it in
        # qualityParams so yieldKgHa is never an ambiguous number.
        quality = node.get("quality_params") or {}
        if node.get("yield_metric"):
            quality = {**quality, "yield_metric": node.get("yield_metric")}
        return {
            "cropEppo": eppo,
            "cropScientific": EPPO_TO_SPECIES.get(eppo),
            "variety": node.get("variety"),
            "rootstock": node.get("rootstock"),
            "scion": node.get("scion") or node.get("variety"),
            "trainingSystem": node.get("training_system"),
            "plantingYear": node.get("planting_year"),
            "plantingDensityTreesHa": node.get("planting_density_trees_ha"),
            "year": node.get("year"),
            "yieldKgHa": node.get("yield_kg_ha"),
            "yieldNoteS1": node.get("yield_note_s1"),
            "qualityParams": json.dumps(quality) if quality else None,
            "irrigationRegime": node.get("irrigation_regime"),
            "trialLocation": node.get("trial_location"),
            "confidence": node.get("confidence", self._registry_entry.get("confidence_default", "medium")),
            "source_id": self.SOURCE_ID,
            "trial_id": node.get("@id", ""),
            "mergeKey": (f"{self.SOURCE_ID.lower()}|{eppo or 'unknown'}|"
                        f"{str(node.get('variety', '')).strip().lower()}|"
                        f"{str(node.get('rootstock', '')).strip().lower()}|"
                        f"{str(node.get('trial_location', 'unknown')).strip().lower()}|"
                        f"{str(node.get('year', 0))}"),
        }


async def _main():
    p = argparse.ArgumentParser()
    p.add_argument("--jsonld", required=True)
    p.add_argument("--no-dry-run", action="store_true")
    a = p.parse_args()
    ing = AlmondIfapaIngester()
    nodes = await ing.transform(a.jsonld)
    for k, v in nodes.items():
        print(f"  {k}: {len(v)}")
    if a.no_dry_run:
        await ing.merge(nodes)


if __name__ == "__main__":
    asyncio.run(_main())
