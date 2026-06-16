"""Abstract base class for all trial source ingesters.

Each scraper repo produces a JSON-LD file. The ingester transforms
that JSON-LD into canonical node dicts (TrialSite, ArticleSource,
VarietyTrial, ManagementTrial) with full legal metadata from the
source registry.

Usage:
    class NavarraIngester(BaseIngester):
        SOURCE_ID = "NAVARRA-AGRARIA"
        ...

    ingester = NavarraIngester()
    nodes = await ingester.transform("path/to/trials.jsonld")
    # Later, when unified ingestion is ready:
    # await ingester.merge(nodes)
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from app.common.source_registry import get_source


class BaseIngester(ABC):
    """Abstract ingester that transforms JSON-LD to canonical node dicts.

    Subclasses must set SOURCE_ID and implement _parse_nodes().

    The transform() method:
      1. Loads JSON-LD
      2. Calls _parse_nodes() to get domain-specific node dicts
      3. Enriches each ArticleSource with registry metadata
      4. Returns structured dict with mergeKey for idempotent ingestion
    """

    SOURCE_ID: str = ""

    def __init__(self) -> None:
        if not self.SOURCE_ID:
            raise ValueError(f"{type(self).__name__} must define SOURCE_ID")
        self._registry_entry = get_source(self.SOURCE_ID)

    async def transform(self, jsonld_path: str) -> dict[str, list[dict]]:
        """Read JSON-LD, parse nodes, enrich with registry metadata.

        Args:
            jsonld_path: Path to the JSON-LD file produced by a scraper.

        Returns:
            Dict with keys: trial_sites, article_sources,
            variety_trials, management_trials. Each value is a list
            of canonical node dicts with mergeKey and registry metadata.

        Raises:
            FileNotFoundError: If jsonld_path does not exist.
            ValueError: If JSON-LD is missing @graph.
        """
        data = self._load_jsonld(jsonld_path)
        nodes = await self._parse_nodes(data)
        self._enrich_article_sources(nodes.get("article_sources", []))
        return nodes

    async def merge(self, nodes: dict[str, list[dict]]) -> dict[str, int]:
        """MERGE nodes into Neo4j. NOT YET IMPLEMENTED.

        Raises:
            NotImplementedError: Always — unified ingestion is pending
                until the parallel agent finishes data unification.
        """
        raise NotImplementedError(
            "Unified ingestion pending — see Fase 0. "
            "Use transform() to prepare nodes, then wait for the "
            "unified ingestion pipeline."
        )

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
            if template and art.get("issue_number"):
                art["document_url"] = template.replace("{issue_number}", str(art["issue_number"]))

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
