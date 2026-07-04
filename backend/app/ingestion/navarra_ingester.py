"""
Navarra Agraria → canonical node transformation (BaseIngester subclass).

Reads the enriched JSON-LD file and transforms it into canonical node dicts
with mergeKeys for idempotent ingestion and registry enrichment.

Usage:
    python -m app.ingestion.navarra_ingester \
        --jsonld data/jsonld/all_trials_enriched.jsonld \
        --dry-run

Transforms:
    - :TrialSite
    - :ArticleSource
    - :VarietyTrial
    - :ManagementTrial
    - :HarvestData
"""

from __future__ import annotations

import json
from typing import Optional

from neo4j import AsyncDriver

from app.ingestion.base_ingester import BaseIngester, NEO4J_PASSWORD, NEO4J_URI, NEO4J_USER


# ═══════════════════════════════════════════════════════════════════════════════
# EPPO code → scientific name mapping (canonical, used for enrichment)
# ═══════════════════════════════════════════════════════════════════════════════

EPPO_SCIENTIFIC_NAMES: dict[str, str] = {
    "ALLPO": "Allium porrum",
    "AVESA": "Avena sativa",
    "BETVU": "Beta vulgaris",
    "BRPNA": "Brassica napus",
    "BRSNN": "Brassica napus",
    "CIEAR": "Cicer arietinum",
    "CPSAN": "Capsicum annuum",
    "CUMME": "Cucumis melo",
    "FRAAN": "Fragaria x ananassa",
    "GLXMA": "Glycine max",
    "HELAN": "Helianthus annuus",
    "HORVX": "Hordeum vulgare",
    "LENCU": "Lens culinaris",
    "LTHSA": "Lathyrus sativus",
    "LYPES": "Solanum lycopersicum",
    "MABSD": "Malus domestica",
    "MEDSA": "Medicago sativa",
    "ORYSA": "Oryza sativa",
    "PIBSX": "Pisum sativum",
    "PISSA": "Pisum sativum",
    "PRNAR": "Prunus armeniaca",
    "PRNDU": "Prunus dulcis",
    "SECCE": "Secale cereale",
    "SOLME": "Solanum melongena",
    "SOLTU": "Solanum tuberosum",
    "TRZAW": "Triticum aestivum",
    "TRZAX": "Triticum aestivum",
    "TRZDU": "Triticum durum",
    "TTLSS": "Triticosecale",
    "VICER": "Vicia ervilia",
    "VICFX": "Vicia faba",
    "VICSA": "Vicia sativa",
    "ZEAMX": "Zea mays",
}


def _resolve_scientific_name(eppo_code: str, raw_scientific: str | None) -> str | None:
    """Resolve a scientific name for a crop, preferring known values.

    Priority:
      1. Provided scientific name (if not null/empty/None/'(unknown)')
      2. EPPO code lookup in canonical mapping
      3. EPPO code itself as fallback
      4. None if nothing available
    """
    # Normalize raw value
    raw = str(raw_scientific).strip() if raw_scientific else ""
    if raw and raw not in ("", "None", "(unknown)", "null"):
        # If the "scientific name" is just the EPPO code, treat as unknown
        eppo = (eppo_code or "").strip().upper()
        if raw.upper() != eppo:
            return raw

    # Look up EPPO code
    eppo = (eppo_code or "").strip().upper()
    if eppo in EPPO_SCIENTIFIC_NAMES:
        return EPPO_SCIENTIFIC_NAMES[eppo]

    # Fallback: use EPPO code if it looks like a valid code
    if len(eppo) == 5 and eppo.isalpha():
        return None  # Will be resolved at query time or in UI

    return None


# ═══════════════════════════════════════════════════════════════════════════════
# MERGE keys — composite natural keys for idempotent ingestion
# ═══════════════════════════════════════════════════════════════════════════════

def _merge_key_trial_site(node: dict) -> str:
    """Unique key: source + name + municipality.

    Includes source to prevent cross-source collisions when
    different data providers reference the same physical site.
    """
    source = _resolve_source(node)
    name = (node.get("name") or "").strip().lower()
    muni = (node.get("municipality") or "").strip().lower()
    return f"{source}|{name}|{muni}"

def _merge_key_article_source(node: dict) -> str:
    """Unique key: source + issue_number + article_title."""
    source = (node.get("source") or "").strip().lower()
    issue = str(node.get("issue_number") or 0)
    title = (node.get("article_title") or "")[:80].strip().lower()
    return f"{source}|{issue}|{title}"

def _merge_key_variety_trial(node: dict) -> str:
    """Unique key: source + crop + variety + location + irrigation + year.

    Includes source prefix to guarantee cross-source uniqueness.
    Without it, two different data providers (e.g. GENVCE and NÉBIH)
    could collide on identical crop/variety/location/year combinations.
    """
    source = _resolve_source(node)
    crop = str(node.get("crop_eppo") or node.get("crop_scientific") or "unknown")
    variety = (node.get("variety") or "").strip().lower()
    location = (node.get("trial_location") or "unknown").strip().lower()
    irrigation = str(node.get("irrigation_regime") or "unknown")
    year = str(node.get("year") or 0)
    return f"{source}|{crop}|{variety}|{location}|{irrigation}|{year}"

def _merge_key_management_trial(node: dict) -> str:
    """Unique key: source + experiment + crop + treatment + @id.
    
    Includes source prefix and @id suffix to guarantee cross-source uniqueness.
    """
    source = _resolve_source(node)
    exp_type = str(node.get("experiment_type") or "")
    crop = str(node.get("crop_eppo") or "")
    treatment = (node.get("treatment") or "").strip().lower()[:60]
    uid = str(node.get("@id", ""))
    return f"{source}|{exp_type}|{crop}|{treatment}|{uid}"

def _merge_key_harvest_data(node: dict) -> str:
    """Unique key: source + campaign + crop + zone."""
    source = _resolve_source(node)
    campaign = str(node.get("campaign") or "")
    crop = str(node.get("crop_eppo") or "")
    zone = str(node.get("agroclimatic_zone") or "")
    uid = str(node.get("@id", ""))
    return f"{source}|{campaign}|{crop}|{zone}|{uid}"


# ═══════════════════════════════════════════════════════════════════════════════
# Ingester
# ═══════════════════════════════════════════════════════════════════════════════

class NavarraIngester(BaseIngester):
    """Navarra Agraria → canonical node transformation with registry enrichment."""

    SOURCE_ID = "NAVARRA-AGRARIA"

    def __init__(self, driver: Optional[AsyncDriver] = None) -> None:
        super().__init__(driver=driver)
        self._stats: dict[str, int] = {}

    async def ingest(self, jsonld_path: str, dry_run: bool = False) -> dict:
        """Transform JSON-LD and optionally MERGE into Neo4j."""
        nodes = await self.transform(jsonld_path)
        print(f"[navarra_ingester] Transformed {sum(len(v) for v in nodes.values())} nodes:")
        for key in ("trial_sites", "article_sources", "variety_trials", "management_trials"):
            print(f"  {key}: {len(nodes.get(key, []))}")

        if dry_run or not self._driver:
            return {"dry_run": True, "transformed": {k: len(v) for k, v in nodes.items()}}

        stats = await self.merge(nodes)
        return {"dry_run": False, "merged": stats}

    # ── Parse ──────────────────────────────────────────────────────────────

    async def _parse_nodes(self, data: dict) -> dict[str, list[dict]]:
        """Partition @graph by @type and map JSON-LD fields to canonical dicts."""
        graph = data.get("@graph", [])

        sites: list[dict] = []
        articles: list[dict] = []
        variety_trials: list[dict] = []
        management_trials: list[dict] = []
        unknown: list[str] = []

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
            elif t == "HarvestData":
                continue
            else:
                unknown.append(t)

        if unknown:
            print(f"[navarra_ingester] Unknown types: {set(unknown)}")

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
            "region": node.get("region"),
            "latitude": node.get("latitude"),
            "longitude": node.get("longitude"),
            "elevationM": node.get("elevationM"),
            "climateClass": node.get("climateClass") or node.get("climate_class"),
            "annualRainfallMm": node.get("annualRainfallMm"),
            "annualET0Mm": node.get("annualET0Mm"),
            "frostDaysPerYear": node.get("frostDaysPerYear"),
            "soilType": node.get("soilType"),
            "soilTexture": node.get("soilTexture"),
            "soilPh": node.get("soilPh"),
            "soilOrganicMatterPct": node.get("soilOrganicMatterPct"),
            "photoperiodSummerHours": node.get("photoperiodSummerHours"),
            "agroclimaticZone": node.get("agroclimatic_zone"),
            "source_id": node.get("source_id") or self.SOURCE_ID,
            "mergeKey": node.get("mergeKey") or _merge_key_trial_site(node),
        }

    def _convert_article(self, node: dict) -> dict:
        return {
            "source": node.get("source") or "Navarra Agraria",
            "issueNumber": node.get("issue_number"),
            "issuePeriod": node.get("issue_period"),
            "articleTitle": node.get("article_title"),
            "articleAuthor": node.get("article_author"),
            "year": node.get("year"),
            "topic": node.get("topic"),
            "source_id": node.get("source_id") or self.SOURCE_ID,
            "mergeKey": node.get("mergeKey") or _merge_key_article_source(node),
        }

    def _convert_trial(self, node: dict) -> dict:
        eppo = BaseIngester._normalize_eppo(node.get("crop_eppo"))
        eppo_bare = (eppo or "").replace("eppo:", "")
        qp = node.get("quality_params")
        ds = node.get("disease_scores")
        return {
            "cropEppo": eppo,
            "cropScientific": _resolve_scientific_name(eppo_bare, node.get("crop_scientific")),
            "variety": node.get("variety"),
            "year": node.get("year"),
            "yieldKgHa": node.get("yield_kg_ha"),
            "yieldRelativePct": node.get("yield_relative_pct"),
            "qualityParams": json.dumps(qp) if qp else None,
            "diseaseScores": json.dumps(ds) if ds else None,
            "irrigationRegime": node.get("irrigation_regime"),
            "trialLocation": node.get("trial_location"),
            "agroclimaticZone": node.get("agroclimatic_zone"),
            "productionSystem": node.get("production_system"),
            "confidence": node.get("confidence", self._registry_entry.get("confidence_default", "medium")),
            "source_id": node.get("source_id") or self.SOURCE_ID,
            "trial_id": node.get("@id", ""),
            "mergeKey": node.get("mergeKey") or _merge_key_variety_trial(node),
        }

    def _convert_management(self, node: dict) -> dict:
        return {
            "cropEppo": BaseIngester._normalize_eppo(node.get("crop_eppo")),
            "variety": node.get("variety"),
            "experimentType": node.get("experiment_type"),
            "treatment": node.get("treatment"),
            "treatmentDescription": node.get("treatment_description"),
            "resultMetric": node.get("result_metric"),
            "resultValue": node.get("result_value"),
            "resultUnit": node.get("result_unit"),
            "controlValue": node.get("control_value"),
            "controlUnit": node.get("control_unit"),
            "year": node.get("year"),
            "trialLocation": node.get("trial_location"),
            "confidence": node.get("confidence", self._registry_entry.get("confidence_default", "medium")),
            "source_id": node.get("source_id") or self.SOURCE_ID,
            "trial_id": node.get("@id", ""),
            "mergeKey": node.get("mergeKey") or _merge_key_management_trial(node),
        }


def _resolve_source(node: dict) -> str:
    """Extract source name from a JSON-LD node.

    Resolution order:
      1. node['source'] (direct field)
      2. node['metadata']['source'] (nested in metadata)
      3. node['@id'] (extract prefix like 'urn:nkz:nebih' or 'urn:nkz:iniav')
      4. 'unknown' (fallback)
    """
    source = node.get("source")
    if source:
        return source.strip().lower()

    meta = node.get("metadata")
    if isinstance(meta, dict) and meta.get("source"):
        return meta["source"].strip().lower()
    if isinstance(meta, str):
        try:
            m = json.loads(meta)
            if m.get("source"):
                return m["source"].strip().lower()
        except json.JSONDecodeError:
            pass

    # Fallback: extract from @id prefix
    nid = node.get("@id", "")
    parts = nid.split(":")
    if len(parts) >= 3:
        return parts[2].strip().lower()  # e.g. "urn:nkz:nebih:trial:..." -> "nebih"

    return "unknown"


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

async def main():
    import argparse

    parser = argparse.ArgumentParser(description="Navarra Agraria → transform / merge")
    parser.add_argument("--jsonld", required=True, help="Path to adequate JSON-LD bundle")
    parser.add_argument("--dry-run", action="store_true", default=True)
    parser.add_argument("--no-dry-run", action="store_true", help="MERGE into Neo4j (needs NEO4J_* env)")
    args = parser.parse_args()

    driver = None
    if args.no_dry_run:
        from neo4j import AsyncGraphDatabase
        driver = AsyncGraphDatabase.driver(
            NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)
        )

    ingester = NavarraIngester(driver=driver)
    try:
        stats = await ingester.ingest(args.jsonld, dry_run=not args.no_dry_run)
        print(json.dumps(stats, indent=2))
    finally:
        if driver is not None:
            await driver.close()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
