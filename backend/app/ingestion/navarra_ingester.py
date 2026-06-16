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

from app.ingestion.base_ingester import BaseIngester


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
        super().__init__()
        self._driver = driver
        self._stats: dict[str, int] = {}

    async def ingest(self, jsonld_path: str, dry_run: bool = False) -> dict:
        """Transform JSON-LD to canonical nodes.

        Args:
            jsonld_path: Path to enriched JSON-LD file.
            dry_run: If True, only print stats without Neo4j writes.

        Returns:
            Dict with per-type counts.
        """
        nodes = await self.transform(jsonld_path)
        sites = nodes["trial_sites"]
        articles = nodes["article_sources"]
        variety_trials = nodes["variety_trials"]
        management_trials = nodes["management_trials"]

        print(f"[navarra_ingester] Transformed {sum(len(v) for v in nodes.values())} nodes:")
        print(f"  TrialSites:        {len(sites)}")
        print(f"  ArticleSources:    {len(articles)}")
        print(f"  VarietyTrials:     {len(variety_trials)}")
        print(f"  ManagementTrials:  {len(management_trials)}")

        if dry_run or not self._driver:
            return {"dry_run": True, "transformed": {k: len(v) for k, v in nodes.items()}}

        # Neo4j MERGE — disabled until unified ingestion
        raise NotImplementedError(
            "Neo4j MERGE disabled until unified ingestion. "
            "Use dry_run=True to preview."
        )

    async def merge(self, nodes: dict[str, list[dict]]) -> dict[str, int]:
        """MERGE nodes into Neo4j — disabled until unified ingestion.

        Raises:
            NotImplementedError: Unified ingestion is pending.
        """
        raise NotImplementedError(
            "Unified ingestion pending. "
            "Use transform() to prepare nodes, then wait for the "
            "unified ingestion pipeline."
        )

    # ── Node MERGE operations ────────────────────────────────────────────────

    async def _merge_trial_sites(self, sites: list[dict]) -> int:
        count = 0
        async with self._driver.session() as s:
            for node in sites:
                merge_key = _merge_key_trial_site(node)
                source = _resolve_source(node)
                await s.run(
                    """
                    MERGE (ts:TrialSite {mergeKey: $merge_key})
                    SET ts.dataSource = $source,
                        ts.name = $name,
                        ts.municipality = $municipality,
                        ts.region = COALESCE($region, ts.region),
                        ts.agroclimaticZone = $zone,
                        ts.latitude = $lat,
                        ts.longitude = $lon,
                        ts.elevationM = $elevation,
                        ts.climateClass = $climate_class,
                        ts.annualRainfallMm = $rainfall,
                        ts.annualET0Mm = $et0,
                        ts.frostDaysPerYear = $frost_days,
                        ts.soilType = $soil_type,
                        ts.soilTexture = $soil_texture,
                        ts.soilPh = $soil_ph,
                        ts.soilOrganicMatterPct = $soil_om,
                        ts.photoperiodSummerHours = $photoperiod,
                        ts.updatedAt = datetime()
                    """,
                    merge_key=merge_key,
                    source=source,
                    name=node.get("name"),
                    municipality=node.get("municipality"),
                    region=node.get("region"),
                    zone=node.get("agroclimatic_zone"),
                    lat=node.get("latitude"),
                    lon=node.get("longitude"),
                    elevation=node.get("elevationM"),
                    climate_class=node.get("climateClass"),
                    rainfall=node.get("annualRainfallMm"),
                    et0=node.get("annualET0Mm"),
                    frost_days=node.get("frostDaysPerYear"),
                    soil_type=node.get("soilType"),
                    soil_texture=node.get("soilTexture"),
                    soil_ph=node.get("soilPh"),
                    soil_om=node.get("soilOrganicMatterPct"),
                    photoperiod=node.get("photoperiodSummerHours"),
                )
                count += 1
        return count

    async def _merge_article_sources(self, articles: list[dict]) -> int:
        count = 0
        async with self._driver.session() as s:
            for node in articles:
                merge_key = _merge_key_article_source(node)
                await s.run(
                    """
                    MERGE (as:ArticleSource {mergeKey: $merge_key})
                    SET as.source = $source,
                        as.issueNumber = $issue,
                        as.issuePeriod = $period,
                        as.articleTitle = $title,
                        as.articleAuthor = $author,
                        as.year = $year,
                        as.topic = $topic,
                        as.extractionModel = $model,
                        as.extractionDate = $ext_date,
                        as.updatedAt = datetime()
                    """,
                    merge_key=merge_key,
                    source=node.get("source"),
                    issue=node.get("issue_number"),
                    period=node.get("issue_period"),
                    title=node.get("article_title"),
                    author=node.get("article_author"),
                    year=node.get("year"),
                    topic=node.get("topic"),
                    model=node.get("extraction_model"),
                    ext_date=node.get("extraction_date"),
                )
                count += 1
        return count

    async def _merge_variety_trials(self, trials: list[dict]) -> int:
        count = 0
        async with self._driver.session() as s:
            for node in trials:
                merge_key = _merge_key_variety_trial(node)
                source = _resolve_source(node)
                await s.run(
                    """
                    MERGE (vt:VarietyTrial {mergeKey: $merge_key})
                    SET vt.dataSource = $source,
                        vt.cropEppo = $crop_eppo,
                        vt.cropScientific = $crop_sci,
                        vt.variety = $variety,
                        vt.agroclimaticZone = $zone,
                        vt.year = $year,
                        vt.yieldKgHa = $yield_val,
                        vt.yieldRelativePct = $yield_rel,
                        vt.qualityParams = $quality,
                        vt.diseaseScores = $disease,
                        vt.irrigationRegime = $irrigation,
                        vt.productionSystem = $production_system,
                        vt.trialLocation = $location,
                        vt.confidence = $confidence,
                        vt.pageInIssue = $page,
                        vt.tableNumber = $table,
                        vt.metadata = $metadata,
                        vt.updatedAt = datetime()
                    """,
                    merge_key=merge_key,
                    source=source,
                    crop_eppo=(node.get("crop_eppo") or "").replace("eppo:", ""),
                    crop_sci=_resolve_scientific_name(
                        (node.get("crop_eppo") or "").replace("eppo:", ""),
                        node.get("crop_scientific"),
                    ),
                    variety=node.get("variety"),
                    zone=node.get("agroclimatic_zone"),
                    year=node.get("year"),
                    yield_val=node.get("yield_kg_ha"),
                    yield_rel=node.get("yield_relative_pct"),
                    quality=json.dumps(node.get("quality_params")) if node.get("quality_params") else None,
                    disease=json.dumps(node.get("disease_scores")) if node.get("disease_scores") else None,
                    irrigation=node.get("irrigation_regime"),
                    production_system=node.get("production_system"),
                    location=node.get("trial_location"),
                    confidence=node.get("confidence"),
                    page=node.get("page_in_issue"),
                    table=str(node.get("table_number", "")) if node.get("table_number") else None,
                    metadata=json.dumps(node.get("metadata")) if node.get("metadata") else None,
                )
                count += 1
        return count

    async def _merge_management_trials(self, trials: list[dict]) -> int:
        count = 0
        async with self._driver.session() as s:
            for node in trials:
                merge_key = _merge_key_management_trial(node)
                source = _resolve_source(node)
                await s.run(
                    """
                    MERGE (mt:ManagementTrial {mergeKey: $merge_key})
                    SET mt.dataSource = $source,
                        mt.cropEppo = $crop_eppo,
                        mt.variety = $variety,
                        mt.experimentType = $exp_type,
                        mt.treatment = $treatment,
                        mt.treatmentDescription = $treatment_desc,
                        mt.resultMetric = $metric,
                        mt.resultValue = $value,
                        mt.resultUnit = $unit,
                        mt.controlValue = $ctrl_value,
                        mt.controlUnit = $ctrl_unit,
                        mt.year = $year,
                        mt.trialLocation = $location,
                        mt.confidence = $confidence,
                        mt.pageInIssue = $page,
                        mt.tableNumber = $table,
                        mt.metadata = $metadata,
                        mt.updatedAt = datetime()
                    """,
                    merge_key=merge_key,
                    source=source,
                    crop_eppo=(node.get("crop_eppo") or "").replace("eppo:", ""),
                    variety=node.get("variety"),
                    exp_type=node.get("experiment_type"),
                    treatment=node.get("treatment"),
                    treatment_desc=node.get("treatment_description"),
                    metric=node.get("result_metric"),
                    value=node.get("result_value"),
                    unit=node.get("result_unit"),
                    ctrl_value=node.get("control_value"),
                    ctrl_unit=node.get("control_unit"),
                    year=node.get("year"),
                    location=node.get("trial_location"),
                    confidence=node.get("confidence"),
                    page=node.get("page_in_issue"),
                    table=str(node.get("table_number", "")) if node.get("table_number") else None,
                    metadata=json.dumps(node.get("metadata")) if node.get("metadata") else None,
                )
                count += 1
        return count

    async def _merge_harvest_data(self, records: list[dict]) -> int:
        count = 0
        async with self._driver.session() as s:
            for node in records:
                merge_key = _merge_key_harvest_data(node)
                source = _resolve_source(node)
                await s.run(
                    """
                    MERGE (hd:HarvestData {mergeKey: $merge_key})
                    SET hd.dataSource = $source,
                        hd.cropEppo = $crop_eppo,
                        hd.agroclimaticZone = $zone,
                        hd.campaign = $campaign,
                        hd.areaHa = $area,
                        hd.totalProductionT = $production,
                        hd.avgYieldKgHa = $avg_yield,
                        hd.yieldVsPreviousPct = $vs_prev,
                        hd.confidence = $confidence,
                        hd.pageInIssue = $page,
                        hd.updatedAt = datetime()
                    """,
                    merge_key=merge_key,
                    source=source,
                    crop_eppo=(node.get("crop_eppo") or "").replace("eppo:", ""),
                    zone=node.get("agroclimatic_zone"),
                    campaign=node.get("campaign"),
                    area=node.get("area_ha"),
                    production=node.get("total_production_t"),
                    avg_yield=node.get("avg_yield_kg_ha"),
                    vs_prev=node.get("yield_vs_previous_pct"),
                    confidence=node.get("confidence"),
                    page=node.get("page_in_issue"),
                )
                count += 1
        return count

    # ── Relationship MERGE ───────────────────────────────────────────────────

    async def _merge_relationships(
        self,
        variety_trials: list[dict],
        management_trials: list[dict],
        harvest_data: list[dict],
    ) -> int:
        count = 0
        async with self._driver.session() as s:
            # VarietyTrial -[:TRIAL_AT]-> TrialSite
            for vt in variety_trials:
                location = (vt.get("trial_location") or "").strip().lower()
                if location:
                    await s.run(
                        """
                        MATCH (vt:VarietyTrial {mergeKey: $vt_key})
                        MATCH (ts:TrialSite)
                        WHERE toLower(ts.name) = $loc OR toLower(ts.municipality) = $loc
                        MERGE (vt)-[:TRIAL_AT]->(ts)
                        """,
                        vt_key=_merge_key_variety_trial(vt),
                        loc=location,
                    )
                    count += 1

                # VarietyTrial -[:SOURCED_FROM]-> ArticleSource
                meta = vt.get("metadata", {})
                if isinstance(meta, str):
                    try:
                        meta = json.loads(meta)
                    except json.JSONDecodeError:
                        meta = {}
                if meta:
                    source_key = _merge_key_article_source({
                        "source": meta.get("source", "Navarra Agraria"),
                        "issue_number": meta.get("issue_number"),
                        "article_title": meta.get("article_title", ""),
                    })
                    await s.run(
                        """
                        MATCH (vt:VarietyTrial {mergeKey: $vt_key})
                        MATCH (as:ArticleSource {mergeKey: $as_key})
                        MERGE (vt)-[:SOURCED_FROM]->(as)
                        """,
                        vt_key=_merge_key_variety_trial(vt),
                        as_key=source_key,
                    )
                    count += 1

                # VarietyTrial -[:TRIAL_OF]-> Species (when EPPO code matches)
                eppo = (vt.get("crop_eppo") or "").replace("eppo:", "")
                if eppo:
                    await s.run(
                        """
                        MATCH (vt:VarietyTrial {mergeKey: $vt_key})
                        MATCH (s:Species {eppoCode: $eppo})
                        MERGE (vt)-[:TRIAL_OF]->(s)
                        """,
                        vt_key=_merge_key_variety_trial(vt),
                        eppo=eppo,
                    )
                    count += 1

            # ManagementTrial -[:TRIAL_AT]-> TrialSite
            for mt in management_trials:
                location = (mt.get("trial_location") or "").strip().lower()
                if location:
                    await s.run(
                        """
                        MATCH (mt:ManagementTrial {mergeKey: $mt_key})
                        MATCH (ts:TrialSite)
                        WHERE toLower(ts.name) = $loc OR toLower(ts.municipality) = $loc
                        MERGE (mt)-[:TRIAL_AT]->(ts)
                        """,
                        mt_key=_merge_key_management_trial(mt),
                        loc=location,
                    )
                    count += 1

                meta = mt.get("metadata", {})
                if isinstance(meta, str):
                    try:
                        meta = json.loads(meta)
                    except json.JSONDecodeError:
                        meta = {}
                if meta:
                    source_key = _merge_key_article_source({
                        "source": meta.get("source", "Navarra Agraria"),
                        "issue_number": meta.get("issue_number"),
                        "article_title": meta.get("article_title", ""),
                    })
                    await s.run(
                        """
                        MATCH (mt:ManagementTrial {mergeKey: $mt_key})
                        MATCH (as:ArticleSource {mergeKey: $as_key})
                        MERGE (mt)-[:SOURCED_FROM]->(as)
                        """,
                        mt_key=_merge_key_management_trial(mt),
                        as_key=source_key,
                    )
                    count += 1

            # HarvestData -[:SOURCED_FROM]-> ArticleSource
            for hd in harvest_data:
                meta = hd.get("metadata", {})
                if isinstance(meta, str):
                    try:
                        meta = json.loads(meta)
                    except json.JSONDecodeError:
                        meta = {}
                if meta:
                    source_key = _merge_key_article_source({
                        "source": meta.get("source", "Navarra Agraria"),
                        "issue_number": meta.get("issue_number"),
                        "article_title": meta.get("article_title", ""),
                    })
                    await s.run(
                        """
                        MATCH (hd:HarvestData {mergeKey: $hd_key})
                        MATCH (as:ArticleSource {mergeKey: $as_key})
                        MERGE (hd)-[:SOURCED_FROM]->(as)
                        """,
                        hd_key=_merge_key_harvest_data(hd),
                        as_key=source_key,
                    )
                    count += 1

        return count

    # ── Parse ──────────────────────────────────────────────────────────────

    async def _parse_nodes(self, data: dict) -> dict[str, list[dict]]:
        """Partition @graph by @type and set mergeKey on each node.

        Args:
            data: Parsed JSON-LD dict (with @graph key).

        Returns:
            Canonical node dicts keyed by type.
        """
        graph = data.get("@graph", [])

        sites = []
        articles = []
        variety_trials = []
        management_trials = []
        harvest_data = []
        unknown = []

        for node in graph:
            t = node.get("@type", "")
            if t == "TrialSite":
                node["mergeKey"] = _merge_key_trial_site(node)
                sites.append(node)
            elif t == "ArticleSource":
                node["mergeKey"] = _merge_key_article_source(node)
                articles.append(node)
            elif t == "VarietyTrial":
                node["mergeKey"] = _merge_key_variety_trial(node)
                variety_trials.append(node)
            elif t == "ManagementTrial":
                node["mergeKey"] = _merge_key_management_trial(node)
                management_trials.append(node)
            elif t == "HarvestData":
                node["mergeKey"] = _merge_key_harvest_data(node)
                harvest_data.append(node)
            else:
                unknown.append(t)

        if unknown:
            print(f"[navarra_ingester] Unknown types: {set(unknown)}")

        return {
            "trial_sites": sites,
            "article_sources": articles,
            "variety_trials": variety_trials,
            "management_trials": management_trials,
            "harvest_data": harvest_data,
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

    parser = argparse.ArgumentParser(description="Navarra Agraria → transform nodes")
    parser.add_argument("--jsonld", required=True, help="Path to all_trials_enriched.jsonld")
    parser.add_argument("--dry-run", action="store_true", default=True)
    parser.add_argument("--no-dry-run", action="store_true", help="Actually run (will fail — reserved for unified ingestion)")
    args = parser.parse_args()

    ingester = NavarraIngester()
    stats = await ingester.ingest(args.jsonld, dry_run=not args.no_dry_run)
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
