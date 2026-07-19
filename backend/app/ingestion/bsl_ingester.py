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
    "BRSNW": "Brassica napus",  # winter rapeseed — cycle travels in cropCycle
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

    # Vocabulary for `cropCycle`. `facultative` (alternative varieties, sown either
    # in autumn or in spring) is part of the contract from the outset even though
    # BSL does not currently label any: adding a value later would force a
    # re-extraction of every source.
    CROP_CYCLES = ("winter", "spring", "facultative")

    @staticmethod
    def _crop_cycle(crop_scientific: str | None) -> str | None:
        """Growing cycle from the German crop label, or None if it carries none.

        BSL names the cycle in `crop_scientific` (Winterweichweizen, Sommergerste,
        Winterraps…) — 9,980 of 15,884 trials. `cropScientific` itself is
        overwritten below with the Latin binomial because the recommender matches
        crops on it, so without this the cycle would be lost at ingest. Losing it
        merges winter and spring populations of the same species into one mean,
        which is agronomically meaningless and breaks season-aware rotation
        planning. Crops with no seasonal prefix (maize, soy, triticale) get None —
        we do not guess.
        """
        if not crop_scientific:
            return None
        label = crop_scientific.strip().lower()
        if label.startswith("wechsel"):  # Wechselweizen: sown autumn OR spring
            return "facultative"
        if label.startswith("winter"):
            return "winter"
        if label.startswith("sommer"):
            return "spring"
        return None

    def _convert_trial(self, node: dict) -> dict:
        eppo = BaseIngester._normalize_eppo(node.get("crop_eppo"))
        cycle = self._crop_cycle(node.get("crop_scientific"))
        return {
            "cropEppo": eppo,
            "cropScientific": EPPO_TO_SPECIES.get(eppo) or node.get("crop_scientific"),
            "cropCycle": cycle,
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
            # The cycle is part of the trial's identity: winter and spring barley
            # of the same variety, site and year are different trials.
            "mergeKey": (
                f"{self.SOURCE_ID.lower()}|{eppo or 'unknown'}|"
                f"{str(node.get('variety', '')).strip().lower()}|"
                f"{cycle or 'nocycle'}|"
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
