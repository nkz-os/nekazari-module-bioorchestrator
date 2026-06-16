"""Abstract base class for all trial source ingesters.

Each scraper repo produces a JSON-LD file. The ingester transforms
that JSON-LD into canonical node dicts (TrialSite, ArticleSource,
VarietyTrial, ManagementTrial) with full legal metadata from the
source registry, then MERGEs them into Neo4j.

Usage:
    class GenvceIngester(BaseIngester):
        SOURCE_ID = "GENVCE"
        ...

    ingester = GenvceIngester()
    nodes = await ingester.transform("path/to/trials.jsonld")
    stats = await ingester.merge(nodes)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

from neo4j import AsyncDriver, AsyncGraphDatabase

from app.common.source_registry import get_source

logger = logging.getLogger(__name__)

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://bioorchestrator-neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "bioorchestrator")


class BaseIngester(ABC):
    """Abstract ingester that transforms JSON-LD to canonical node dicts
    and MERGEs them into Neo4j.

    Subclasses must set SOURCE_ID and implement _parse_nodes().

    The transform() method:
      1. Loads JSON-LD
      2. Calls _parse_nodes() to get domain-specific node dicts
      3. Enriches each ArticleSource with registry metadata
      4. Returns structured dict with mergeKey for idempotent ingestion

    The merge() method:
      1. MERGE TrialSite nodes (matched by mergeKey)
      2. MERGE ArticleSource nodes
      3. MERGE VarietyTrial nodes
      4. MERGE ManagementTrial nodes
      5. Creates TRIAL_AT and SOURCED_FROM relationships
      6. Skips nodes with skip_ingestion=True (e.g. CTIFL restricted data)
    """

    SOURCE_ID: str = ""

    def __init__(self, driver: Optional[AsyncDriver] = None) -> None:
        if not self.SOURCE_ID:
            raise ValueError(f"{type(self).__name__} must define SOURCE_ID")
        self._registry_entry = get_source(self.SOURCE_ID)
        self._driver = driver

    # ── Public API ────────────────────────────────────────────────────────────

    async def transform(self, jsonld_path: str) -> dict[str, list[dict]]:
        """Read JSON-LD, parse nodes, enrich with registry metadata.

        Args:
            jsonld_path: Path to the JSON-LD file produced by a scraper.

        Returns:
            Dict with keys: trial_sites, article_sources,
            variety_trials, management_trials. Each value is a list
            of canonical node dicts. Subclasses must set 'mergeKey'
            in _parse_nodes(). Registry metadata is added automatically.

        Raises:
            FileNotFoundError: If jsonld_path does not exist.
            ValueError: If JSON-LD is missing @graph.
        """
        data = self._load_jsonld(jsonld_path)
        nodes = await self._parse_nodes(data)
        self._enrich_article_sources(nodes.get("article_sources", []))
        return nodes

    async def merge(self, nodes: dict[str, list[dict]]) -> dict[str, int]:
        """MERGE all nodes into Neo4j. Idempotent.

        Args:
            nodes: Output from transform(). Keys: trial_sites,
                   article_sources, variety_trials, management_trials.

        Returns:
            Dict with counts: sites, articles, variety_trials,
            management_trials, relationships.

        Raises:
            RuntimeError: If no Neo4j driver is available (neither injected
                          via constructor nor via NEO4J_* env vars).
        """
        driver = await self._get_driver()
        stats: dict[str, int] = {
            "sites": 0,
            "articles": 0,
            "variety_trials": 0,
            "management_trials": 0,
            "relationships": 0,
        }

        try:
            stats["sites"] = await self._merge_trial_sites(driver, nodes.get("trial_sites", []))
            stats["articles"] = await self._merge_article_sources(driver, nodes.get("article_sources", []))
            stats["variety_trials"] = await self._merge_variety_trials(driver, nodes.get("variety_trials", []))
            stats["management_trials"] = await self._merge_management_trials(driver, nodes.get("management_trials", []))
            stats["relationships"] = await self._merge_relationships(
                driver,
                nodes.get("variety_trials", []),
                nodes.get("management_trials", []),
            )
        finally:
            # Only close driver if we created it ourselves
            if self._driver is None:
                await driver.close()

        logger.info(
            "[%s] MERGE complete: %d sites, %d articles, %d VT, %d MT, %d rels",
            self.SOURCE_ID, stats["sites"], stats["articles"],
            stats["variety_trials"], stats["management_trials"],
            stats["relationships"],
        )
        return stats

    @abstractmethod
    async def _parse_nodes(self, data: dict) -> dict[str, list[dict]]:
        """Parse JSON-LD into canonical node dicts.

        Must return dict with keys: trial_sites, article_sources,
        variety_trials, management_trials (empty lists for unused types).

        Args:
            data: Parsed JSON-LD dict (from _load_jsonld).

        Returns:
            Canonical node dicts partitioned by type.
        """

    # ── Neo4j MERGE helpers ──────────────────────────────────────────────────

    async def _get_driver(self) -> AsyncDriver:
        """Return a Neo4j driver (injected or created from env vars)."""
        if self._driver is not None:
            return self._driver

        if not NEO4J_URI or not NEO4J_USER or not NEO4J_PASSWORD:
            raise RuntimeError(
                f"[{self.SOURCE_ID}] Neo4j driver not configured. "
                "Set NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD env vars "
                "or inject a driver via the constructor."
            )

        logger.info("[%s] Creating Neo4j driver: %s", self.SOURCE_ID, NEO4J_URI)
        return AsyncGraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD),
        )

    async def _merge_trial_sites(self, driver: AsyncDriver, sites: list[dict]) -> int:
        """MERGE TrialSite nodes by mergeKey. Idempotent."""
        if not sites:
            return 0

        count = 0
        async with driver.session() as session:
            for node in sites:
                merge_key = self._get_merge_key(node)
                if not merge_key:
                    continue
                await session.run(
                    """
                    MERGE (ts:TrialSite {mergeKey: $merge_key})
                    SET ts.name = $name,
                        ts.municipality = $municipality,
                        ts.climateClass = $climate,
                        ts.soilType = $soil_type,
                        ts.soilTexture = $soil_texture,
                        ts.soilPh = $soil_ph,
                        ts.annualRainfallMm = $rainfall,
                        ts.annualET0Mm = $et0,
                        ts.elevationM = $elevation,
                        ts.frostDaysPerYear = $frost,
                        ts.latitude = $lat,
                        ts.longitude = $lon,
                        ts.photoperiodSummerHours = $photoperiod,
                        ts.source_id = $source,
                        ts.updatedAt = datetime()
                    """,
                    merge_key=merge_key,
                    name=node.get("name"),
                    municipality=node.get("municipality"),
                    climate=node.get("climateClass"),
                    soil_type=node.get("soilType"),
                    soil_texture=node.get("soilTexture"),
                    soil_ph=node.get("soilPh"),
                    rainfall=node.get("annualRainfallMm"),
                    et0=node.get("annualET0Mm"),
                    elevation=node.get("elevationM"),
                    frost=node.get("frostDaysPerYear"),
                    lat=node.get("latitude"),
                    lon=node.get("longitude"),
                    photoperiod=node.get("photoperiodSummerHours"),
                    source=node.get("source_id", self.SOURCE_ID),
                )
                count += 1
        return count

    async def _merge_article_sources(self, driver: AsyncDriver, articles: list[dict]) -> int:
        """MERGE ArticleSource nodes by mergeKey. Idempotent."""
        # Skip restricted articles (CTIFL)
        active = [a for a in articles if not a.get("skip_ingestion")]
        skipped = len(articles) - len(active)
        if skipped:
            logger.info("[%s] Skipping %d restricted article sources", self.SOURCE_ID, skipped)

        if not active:
            return 0

        count = 0
        async with driver.session() as session:
            for node in active:
                merge_key = self._get_merge_key(node)
                if not merge_key:
                    continue
                await session.run(
                    """
                    MERGE (as:ArticleSource {mergeKey: $merge_key})
                    SET as.source_id = $source,
                        as.issueNumber = $issue,
                        as.articleTitle = $title,
                        as.articleAuthor = $author,
                        as.year = $year,
                        as.topic = $topic,
                        as.institution = $institution,
                        as.licenseClass = $license,
                        as.useType = $use_type,
                        as.officialStatus = $official,
                        as.dataFormat = $data_format,
                        as.documentUrl = $doc_url,
                        as.confidence = $confidence,
                        as.updatedAt = datetime()
                    """,
                    merge_key=merge_key,
                    source=node.get("source_id", self.SOURCE_ID),
                    issue=node.get("issueNumber"),
                    title=node.get("articleTitle"),
                    author=node.get("articleAuthor"),
                    year=node.get("year"),
                    topic=node.get("topic"),
                    institution=node.get("institution"),
                    license=node.get("license_class"),
                    use_type=node.get("use_type"),
                    official=node.get("official_status"),
                    data_format=node.get("data_format"),
                    doc_url=node.get("document_url"),
                    confidence=node.get("confidence", "medium"),
                )
                count += 1
        return count

    async def _merge_variety_trials(self, driver: AsyncDriver, trials: list[dict]) -> int:
        """MERGE VarietyTrial nodes by mergeKey. Skip restricted entries."""
        active = [t for t in trials if not t.get("skip_ingestion")]
        skipped = len(trials) - len(active)
        if skipped:
            logger.info("[%s] Skipping %d restricted variety trials", self.SOURCE_ID, skipped)

        if not active:
            return 0

        count = 0
        async with driver.session() as session:
            for node in active:
                merge_key = self._get_merge_key(node)
                if not merge_key:
                    continue
                # Compute a content hash as suffix to guarantee uniqueness
                # across all sources, even when @id or mergeKey have collisions
                node_snapshot = {k: v for k, v in node.items()
                                if k not in ("mergeKey", "trial_id", "source_id")}
                content_hash = hashlib.md5(
                    json.dumps(node_snapshot, sort_keys=True, default=str).encode()
                ).hexdigest()[:12]
                unique_key = f"{merge_key}|{content_hash}"
                eppo_raw = node.get("cropEppo") or ""
                await session.run(
                    """
                    MERGE (vt:VarietyTrial {mergeKey: $unique_key})
                    SET vt.source_id = $source,
                        vt.mergeKey = $merge_key,
                        vt.cropEppo = $eppo,
                        vt.cropScientific = $sci_name,
                        vt.variety = $variety,
                        vt.year = $year,
                        vt.yieldKgHa = $yield_kg,
                        vt.yieldNoteS1 = $yield_s1,
                        vt.yieldNoteS2 = $yield_s2,
                        vt.yieldRelativePct = $yield_rel,
                        vt.qualityParams = $quality,
                        vt.diseaseScores = $disease,
                        vt.agronomicTraits = $agro_traits,
                        vt.irrigationRegime = $irrigation,
                        vt.trialLocation = $location,
                        vt.agroclimaticZone = $agro_zone,
                        vt.productionSystem = $production,
                        vt.confidence = $confidence,
                        vt.updatedAt = datetime()
                    """,
                    unique_key=unique_key,
                    merge_key=merge_key,
                    source=node.get("source_id", self.SOURCE_ID),
                    eppo=eppo_raw.replace("eppo:", ""),
                    sci_name=node.get("cropScientific"),
                    variety=node.get("variety"),
                    year=node.get("year"),
                    yield_kg=node.get("yieldKgHa"),
                    yield_s1=node.get("yieldNoteS1"),
                    yield_s2=node.get("yieldNoteS2"),
                    yield_rel=node.get("yieldRelativePct"),
                    quality=node.get("qualityParams"),
                    disease=node.get("diseaseScores"),
                    agro_traits=node.get("agronomicTraits"),
                    irrigation=node.get("irrigationRegime"),
                    location=node.get("trialLocation"),
                    agro_zone=node.get("agroclimaticZone"),
                    production=node.get("productionSystem"),
                    confidence=node.get("confidence", "medium"),
                )
                count += 1
        return count

    async def _merge_management_trials(self, driver: AsyncDriver, trials: list[dict]) -> int:
        """MERGE ManagementTrial nodes by mergeKey. Skip restricted entries."""
        active = [t for t in trials if not t.get("skip_ingestion")]
        skipped = len(trials) - len(active)
        if skipped:
            logger.info("[%s] Skipping %d restricted management trials", self.SOURCE_ID, skipped)

        if not active:
            return 0

        count = 0
        async with driver.session() as session:
            for node in active:
                merge_key = self._get_merge_key(node)
                if not merge_key:
                    continue
                eppo_raw = node.get("cropEppo") or ""
                await session.run(
                    """
                    MERGE (mt:ManagementTrial {mergeKey: $merge_key})
                    SET mt.source_id = $source,
                        mt.cropEppo = $eppo,
                        mt.variety = $variety,
                        mt.experimentType = $exp_type,
                        mt.treatment = $treatment,
                        mt.resultMetric = $metric,
                        mt.resultValue = $value,
                        mt.resultUnit = $unit,
                        mt.year = $year,
                        mt.trialLocation = $location,
                        mt.confidence = $confidence,
                        mt.updatedAt = datetime()
                    """,
                    merge_key=merge_key,
                    source=node.get("source_id", self.SOURCE_ID),
                    eppo=eppo_raw.replace("eppo:", ""),
                    variety=node.get("variety"),
                    exp_type=node.get("experimentType"),
                    treatment=node.get("treatment"),
                    metric=node.get("resultMetric"),
                    value=node.get("resultValue"),
                    unit=node.get("resultUnit"),
                    year=node.get("year"),
                    location=node.get("trialLocation"),
                    confidence=node.get("confidence", "medium"),
                )
                count += 1
        return count

    async def _merge_relationships(
        self,
        driver: AsyncDriver,
        variety_trials: list[dict],
        management_trials: list[dict],
    ) -> int:
        """Create TRIAL_AT and SOURCED_FROM relationships.

        Matches TrialSite by name (lowercase) and ArticleSource by mergeKey.
        """
        count = 0
        if not variety_trials and not management_trials:
            return count

        async with driver.session() as session:
            # ── VarietyTrial → TRIAL_AT → TrialSite ──────────────
            for vt in variety_trials:
                if vt.get("skip_ingestion"):
                    continue
                merge_key = self._get_merge_key(vt)
                if not merge_key:
                    continue
                # Content hash for relationship lookup (same as in _merge_variety_trials)
                node_snapshot = {k: v for k, v in vt.items()
                                if k not in ("mergeKey", "trial_id", "source_id")}
                content_hash = hashlib.md5(
                    json.dumps(node_snapshot, sort_keys=True, default=str).encode()
                ).hexdigest()[:12]
                unique_key = f"{merge_key}|{content_hash}"

                # Match by trial_location → TrialSite name
                location = (vt.get("trialLocation") or "").strip().lower()
                if location:
                    result = await session.run(
                        """
                        MATCH (vt:VarietyTrial {mergeKey: $unique_key})
                        MATCH (ts:TrialSite)
                        WHERE toLower(ts.name) = $loc
                           OR toLower(ts.municipality) = $loc
                        MERGE (vt)-[:TRIAL_AT]->(ts)
                        RETURN count(*) AS c
                        """,
                        unique_key=unique_key,
                        loc=location,
                    )
                    row = await result.single()
                    if row and row["c"] > 0:
                        count += 1

            # ── ManagementTrial → TRIAL_AT → TrialSite ───────────
            for mt in management_trials:
                if mt.get("skip_ingestion"):
                    continue
                mt_key = self._get_merge_key(mt)
                if not mt_key:
                    continue

                location = (mt.get("trialLocation") or "").strip().lower()
                if location:
                    result = await session.run(
                        """
                        MATCH (mt:ManagementTrial {mergeKey: $mt_key})
                        MATCH (ts:TrialSite)
                        WHERE toLower(ts.name) = $loc
                           OR toLower(ts.municipality) = $loc
                        MERGE (mt)-[:TRIAL_AT]->(ts)
                        RETURN count(*) AS c
                        """,
                        mt_key=mt_key,
                        loc=location,
                    )
                    row = await result.single()
                    if row and row["c"] > 0:
                        count += 1

        return count

    # ── Helpers ────────────────────────────────────────────────────────────

    def _enrich_article_sources(self, articles: list[dict]) -> None:
        """Add registry metadata to each ArticleSource dict.

        Args:
            articles: List of ArticleSource node dicts. Modified in place.
        """
        entry = self._registry_entry
        for art in articles:
            art["source_id"] = self.SOURCE_ID
            art["institution"] = entry["institution"]
            art["license_class"] = entry["license_class"]
            art["use_type"] = entry["use_type"]
            art["official_status"] = entry["official_status"]
            art["data_format"] = entry.get("data_format", "pdf-extracted")
            art["confidence"] = art.get("confidence") or entry.get("confidence_default", "medium")
            # Generate document_url if template exists
            template = entry.get("document_url_template")
            if template and art.get("issueNumber"):
                art["document_url"] = template.replace("{issue_number}", str(art["issueNumber"]))

    @staticmethod
    def _get_merge_key(node: dict) -> str | None:
        """Extract mergeKey from a node dict, ensuring it exists."""
        mk = node.get("mergeKey")
        if not mk:
            logger.warning("Node missing mergeKey: %s", str(node.get("variety", "")))
            return None
        return str(mk)

    @staticmethod
    def _load_jsonld(path: str) -> dict:
        """Load and validate JSON-LD file.

        Args:
            path: Path to the JSON-LD file.

        Returns:
            Parsed JSON-LD dict.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If @graph is missing from the JSON-LD.
        """
        filepath = Path(path)
        if not filepath.exists():
            raise FileNotFoundError(f"JSON-LD not found: {path}")
        with open(filepath, encoding="utf-8") as f:
            data: dict = json.load(f)
        if "@graph" not in data:
            raise ValueError(f"Not a valid JSON-LD file: missing @graph in {path}")
        return data

    @staticmethod
    def _normalize_eppo(eppo_code: str | None) -> str | None:
        """Normalize EPPO code: strip 'eppo:' prefix, uppercase.

        Args:
            eppo_code: Raw EPPO code (may include 'eppo:' prefix).

        Returns:
            Normalized 5-char uppercase EPPO code, or None if invalid.
        """
        if not eppo_code:
            return None
        code = eppo_code.replace("eppo:", "").replace("EPPO:", "").strip().upper()
        return code if len(code) == 5 else None
