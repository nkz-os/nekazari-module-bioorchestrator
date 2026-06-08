"""Crop reference data — carbon, operations, nutrient requirements.

Multi-layer resolver:
1. Neo4j AgriKnowledge → n_fixation_kg_ha (real measurements from INTIA/IFAPA/ITACyL)
2. EPPO API → family → auto-detect Fabaceae (legume) vs Poaceae (cereal)
3. EcoCrop CSV → growing_season_days (FAO GAEZ)
4. Static IPCC 2019 Tier 1 table (fallback)

Used by compare-crops and rotation-plan endpoints.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from cachetools import TTLCache

from app.services.ecocrop_loader import get_growing_season_days

logger = logging.getLogger(__name__)

# ── Static IPCC reference table (fallback) ──────────────────────────────────

CROP_REFERENCE: dict[str, dict] = {
    # Cereals
    "TRZAX": {"carbon_fixed_tco2e_ha": 2.8, "operations_count": 7, "n_requirement_kg_ha": 180, "n_fixation_kg_ha": 0, "growing_season_days": 210},
    "TRZDU": {"carbon_fixed_tco2e_ha": 2.5, "operations_count": 6, "n_requirement_kg_ha": 160, "n_fixation_kg_ha": 0, "growing_season_days": 200},
    "TRZAS": {"carbon_fixed_tco2e_ha": 2.3, "operations_count": 6, "n_requirement_kg_ha": 140, "n_fixation_kg_ha": 0, "growing_season_days": 210},
    "HORVX": {"carbon_fixed_tco2e_ha": 2.3, "operations_count": 6, "n_requirement_kg_ha": 140, "n_fixation_kg_ha": 0, "growing_season_days": 190},
    "HORVW": {"carbon_fixed_tco2e_ha": 2.2, "operations_count": 5, "n_requirement_kg_ha": 130, "n_fixation_kg_ha": 0, "growing_season_days": 180},
    "AVESA": {"carbon_fixed_tco2e_ha": 2.0, "operations_count": 5, "n_requirement_kg_ha": 120, "n_fixation_kg_ha": 0, "growing_season_days": 180},
    "SECCE": {"carbon_fixed_tco2e_ha": 2.1, "operations_count": 5, "n_requirement_kg_ha": 110, "n_fixation_kg_ha": 0, "growing_season_days": 200},
    "ZEAXX": {"carbon_fixed_tco2e_ha": 4.0, "operations_count": 8, "n_requirement_kg_ha": 250, "n_fixation_kg_ha": 0, "growing_season_days": 180},
    "ZEAMX": {"carbon_fixed_tco2e_ha": 4.2, "operations_count": 8, "n_requirement_kg_ha": 260, "n_fixation_kg_ha": 0, "growing_season_days": 180},
    "SORVU": {"carbon_fixed_tco2e_ha": 3.5, "operations_count": 6, "n_requirement_kg_ha": 150, "n_fixation_kg_ha": 0, "growing_season_days": 170},
    "ORYSA": {"carbon_fixed_tco2e_ha": 2.8, "operations_count": 9, "n_requirement_kg_ha": 160, "n_fixation_kg_ha": 0, "growing_season_days": 180},
    # Oilseeds
    "BRSNW": {"carbon_fixed_tco2e_ha": 2.2, "operations_count": 6, "n_requirement_kg_ha": 180, "n_fixation_kg_ha": 0, "growing_season_days": 280},
    "BRSNN": {"carbon_fixed_tco2e_ha": 2.0, "operations_count": 5, "n_requirement_kg_ha": 170, "n_fixation_kg_ha": 0, "growing_season_days": 260},
    "HELAN": {"carbon_fixed_tco2e_ha": 1.8, "operations_count": 5, "n_requirement_kg_ha": 100, "n_fixation_kg_ha": 0, "growing_season_days": 150},
    # Root & tuber crops
    "SOLTU": {"carbon_fixed_tco2e_ha": 1.8, "operations_count": 9, "n_requirement_kg_ha": 200, "n_fixation_kg_ha": 0, "growing_season_days": 160},
    "BETVU": {"carbon_fixed_tco2e_ha": 2.5, "operations_count": 8, "n_requirement_kg_ha": 180, "n_fixation_kg_ha": 0, "growing_season_days": 210},
    # Legumes
    "CIEAR": {"carbon_fixed_tco2e_ha": 0.9, "operations_count": 4, "n_requirement_kg_ha": 30, "n_fixation_kg_ha": 80, "growing_season_days": 150},
    "PIBSX": {"carbon_fixed_tco2e_ha": 1.2, "operations_count": 4, "n_requirement_kg_ha": 20, "n_fixation_kg_ha": 100, "growing_season_days": 140},
    "PIBAR": {"carbon_fixed_tco2e_ha": 1.1, "operations_count": 4, "n_requirement_kg_ha": 15, "n_fixation_kg_ha": 110, "growing_season_days": 140},
    "VICFX": {"carbon_fixed_tco2e_ha": 1.5, "operations_count": 4, "n_requirement_kg_ha": 20, "n_fixation_kg_ha": 130, "growing_season_days": 160},
    "LENCU": {"carbon_fixed_tco2e_ha": 0.7, "operations_count": 3, "n_requirement_kg_ha": 15, "n_fixation_kg_ha": 60, "growing_season_days": 130},
    "GLXMA": {"carbon_fixed_tco2e_ha": 2.5, "operations_count": 5, "n_requirement_kg_ha": 30, "n_fixation_kg_ha": 180, "growing_season_days": 160},
    "VICSA": {"carbon_fixed_tco2e_ha": 1.0, "operations_count": 3, "n_requirement_kg_ha": 10, "n_fixation_kg_ha": 110, "growing_season_days": 150},
    "VICVI": {"carbon_fixed_tco2e_ha": 1.2, "operations_count": 3, "n_requirement_kg_ha": 10, "n_fixation_kg_ha": 120, "growing_season_days": 160},
    "VICER": {"carbon_fixed_tco2e_ha": 0.9, "operations_count": 3, "n_requirement_kg_ha": 15, "n_fixation_kg_ha": 100, "growing_season_days": 140},
    "LTHSA": {"carbon_fixed_tco2e_ha": 0.8, "operations_count": 3, "n_requirement_kg_ha": 10, "n_fixation_kg_ha": 90, "growing_season_days": 130},
    "LUPAL": {"carbon_fixed_tco2e_ha": 1.3, "operations_count": 3, "n_requirement_kg_ha": 10, "n_fixation_kg_ha": 100, "growing_season_days": 160},
    "LUPAN": {"carbon_fixed_tco2e_ha": 1.4, "operations_count": 3, "n_requirement_kg_ha": 10, "n_fixation_kg_ha": 120, "growing_season_days": 170},
    "PHVUX": {"carbon_fixed_tco2e_ha": 1.0, "operations_count": 5, "n_requirement_kg_ha": 20, "n_fixation_kg_ha": 50, "growing_season_days": 140},
    # Forage
    "MEDSA": {"carbon_fixed_tco2e_ha": 3.0, "operations_count": 5, "n_requirement_kg_ha": 10, "n_fixation_kg_ha": 200, "growing_season_days": 250},
    "MEDLU": {"carbon_fixed_tco2e_ha": 2.5, "operations_count": 4, "n_requirement_kg_ha": 10, "n_fixation_kg_ha": 170, "growing_season_days": 220},
    "TRFPR": {"carbon_fixed_tco2e_ha": 2.8, "operations_count": 4, "n_requirement_kg_ha": 10, "n_fixation_kg_ha": 150, "growing_season_days": 240},
    # Vegetables
    "SOLLC": {"carbon_fixed_tco2e_ha": 0.8, "operations_count": 10, "n_requirement_kg_ha": 180, "n_fixation_kg_ha": 0, "growing_season_days": 150},
    "CAPAN": {"carbon_fixed_tco2e_ha": 0.6, "operations_count": 8, "n_requirement_kg_ha": 140, "n_fixation_kg_ha": 0, "growing_season_days": 160},
    "CUMSA": {"carbon_fixed_tco2e_ha": 0.5, "operations_count": 7, "n_requirement_kg_ha": 100, "n_fixation_kg_ha": 0, "growing_season_days": 120},
    "CUCUM": {"carbon_fixed_tco2e_ha": 0.4, "operations_count": 7, "n_requirement_kg_ha": 90, "n_fixation_kg_ha": 0, "growing_season_days": 110},
    "ALLCE": {"carbon_fixed_tco2e_ha": 0.7, "operations_count": 7, "n_requirement_kg_ha": 150, "n_fixation_kg_ha": 0, "growing_season_days": 180},
    # Perennial / fruits / nuts
    "BRUn":  {"carbon_fixed_tco2e_ha": 5.2, "operations_count": 6, "n_requirement_kg_ha": 200, "n_fixation_kg_ha": 0, "growing_season_days": 250},
    "OLEU":  {"carbon_fixed_tco2e_ha": 3.5, "operations_count": 5, "n_requirement_kg_ha": 100, "n_fixation_kg_ha": 0, "growing_season_days": 365},
    "VITIS": {"carbon_fixed_tco2e_ha": 3.0, "operations_count": 10, "n_requirement_kg_ha": 80, "n_fixation_kg_ha": 0, "growing_season_days": 240},
    "PRMDO": {"carbon_fixed_tco2e_ha": 4.0, "operations_count": 8, "n_requirement_kg_ha": 90, "n_fixation_kg_ha": 0, "growing_season_days": 210},
    "PRNAR": {"carbon_fixed_tco2e_ha": 3.8, "operations_count": 6, "n_requirement_kg_ha": 120, "n_fixation_kg_ha": 0, "growing_season_days": 220},
    "CITRU": {"carbon_fixed_tco2e_ha": 3.2, "operations_count": 7, "n_requirement_kg_ha": 150, "n_fixation_kg_ha": 0, "growing_season_days": 365},
    "PRMPE": {"carbon_fixed_tco2e_ha": 2.8, "operations_count": 7, "n_requirement_kg_ha": 80, "n_fixation_kg_ha": 0, "growing_season_days": 200},
}

DEFAULT_REFERENCE = {"carbon_fixed_tco2e_ha": 1.5, "operations_count": 5, "n_requirement_kg_ha": 100, "n_fixation_kg_ha": 0, "growing_season_days": 180}

# ── Caches ──────────────────────────────────────────────────────────────────

_eppo_taxonomy_cache: TTLCache[str, Optional[dict]] = TTLCache(maxsize=200, ttl=86400 * 7)
_agriknowledge_cache: TTLCache[str, Optional[float]] = TTLCache(maxsize=200, ttl=86400)

EPPO_API_KEY = os.getenv("EPPO_API_KEY", "")
EPPO_BASE = "https://api.eppo.int/gd/v2"


# ── Live data fetchers ──────────────────────────────────────────────────────

async def _fetch_eppo_taxonomy(eppo_code: str) -> Optional[dict]:
    """Fetch taxonomy from EPPO API v2. Returns {family, scientific_name, life_cycle} or None."""
    cached = _eppo_taxonomy_cache.get(eppo_code)
    if cached is not None:
        return cached if cached else None

    if not EPPO_API_KEY:
        logger.debug("EPPO_API_KEY not set — skipping taxonomy lookup")
        return None

    try:
        import httpx
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{EPPO_BASE}/taxons/taxon/{eppo_code}/taxonomy",
                headers={"X-Api-Key": EPPO_API_KEY},
            )
            if resp.status_code == 200:
                data = resp.json()
                result = {
                    "family": data.get("family", ""),
                    "scientific_name": data.get("scientificName", ""),
                    "life_cycle": data.get("lifeCycle", ""),
                }
                _eppo_taxonomy_cache[eppo_code] = result
                return result
            elif resp.status_code == 404:
                _eppo_taxonomy_cache[eppo_code] = {}
                return None
            else:
                logger.warning("EPPO taxonomy returned %d for %s", resp.status_code, eppo_code)
    except Exception as e:
        logger.warning("EPPO taxonomy lookup failed for %s: %s", eppo_code, e)

    _eppo_taxonomy_cache[eppo_code] = {}
    return None


async def _fetch_agriknowledge_n_fixation(eppo_code: str) -> Optional[float]:
    """Query Neo4j :AgriKnowledge nodes for measured N fixation. Returns kg/ha or None."""
    cached = _agriknowledge_cache.get(eppo_code)
    if cached is not None:
        return cached if cached else None

    try:
        from neo4j import AsyncGraphDatabase

        neo4j_uri = os.getenv("NEO4J_URI", "bolt://bioorchestrator-neo4j:7687")
        neo4j_user = os.getenv("NEO4J_USER", "neo4j")
        neo4j_password = os.getenv("NEO4J_PASSWORD", "bioorchestrator")

        driver = AsyncGraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        async with driver.session() as session:
            result = await session.run(
                """
                MATCH (ak:AgriKnowledge {speciesEppo: $eppo, parameter: 'n_fixation_kg_ha'})
                RETURN avg(ak.value) AS avg_n, count(ak) AS cnt
                """,
                eppo=eppo_code,
            )
            record = await result.single()
            await driver.close()
            if record and record["cnt"] > 0:
                val = float(record["avg_n"])
                _agriknowledge_cache[eppo_code] = val
                return val
    except Exception as e:
        logger.warning("AgriKnowledge N fixation query failed for %s: %s", eppo_code, e)

    _agriknowledge_cache[eppo_code] = None
    return None


# ── Main resolver ───────────────────────────────────────────────────────────

async def get_crop_ref(eppo: str) -> dict:
    """Get reference data for a crop EPPO code, resolving from live sources.

    Resolution order:
    1. Neo4j AgriKnowledge → n_fixation_kg_ha (real measurements)
    2. EPPO API → family → auto-detect Fabaceae (legume) vs Poaceae (cereal)
    3. EcoCrop CSV → growing_season_days (FAO GAEZ)
    4. Static IPCC 2019 Tier 1 table or DEFAULT_REFERENCE (fallback)

    Returns dict with keys: carbon_fixed_tco2e_ha, operations_count,
    n_requirement_kg_ha, n_fixation_kg_ha, growing_season_days.
    May include extra diagnostic keys: n_fixation_source, growing_season_source.
    """
    base = CROP_REFERENCE.get(eppo, dict(DEFAULT_REFERENCE))
    result = dict(base)
    source_info: dict[str, str] = {}

    # Layer 1: AgriKnowledge N fixation (real measurements)
    n_fix = await _fetch_agriknowledge_n_fixation(eppo)
    if n_fix is not None:
        result["n_fixation_kg_ha"] = round(n_fix, 1)
        source_info["n_fixation"] = "AgriKnowledge (measured)"

    # Layer 2: EPPO taxonomy → auto-detect legume/cereal
    tax = await _fetch_eppo_taxonomy(eppo)
    if tax:
        family = tax.get("family", "").lower()
        sci_name = tax.get("scientific_name", "")

        # Auto-detect N fixation for Fabaceae if not already set by AgriKnowledge
        if "fabaceae" in family and "n_fixation" not in source_info:
            result["n_fixation_kg_ha"] = max(result.get("n_fixation_kg_ha", 0), 60)
            result["n_requirement_kg_ha"] = 15
            source_info["n_fixation"] = "EPPO taxonomy (Fabaceae)"

        if "poaceae" in family and "n_fixation" not in source_info:
            result["n_fixation_kg_ha"] = 0
            source_info["n_fixation"] = "EPPO taxonomy (Poaceae)"

        # Layer 3: EcoCrop growing season from scientific name
        if sci_name:
            gsd = get_growing_season_days(sci_name)
            if gsd:
                result["growing_season_days"] = gsd
                source_info["growing_season"] = "EcoCrop (FAO)"

    # Ensure all required keys exist
    required_keys = (
        "carbon_fixed_tco2e_ha", "operations_count",
        "n_requirement_kg_ha", "n_fixation_kg_ha", "growing_season_days",
    )
    for key in required_keys:
        if key not in result:
            result[key] = DEFAULT_REFERENCE[key]

    return result


def get_crop_ref_sync(eppo: str) -> dict:
    """Synchronous fallback — returns static IPCC data without live sources."""
    return CROP_REFERENCE.get(eppo, dict(DEFAULT_REFERENCE))
