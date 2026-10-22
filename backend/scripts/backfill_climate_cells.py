"""
Warm the global ClimateCell cache for every parcel of the given tenants.

Reads AgriParcel centroids from Orion-LD (read-only), deduplicates them by CHELSA
30" grid cell and computes the cells that are not cached yet. The only thing
written is ClimateCell nodes (global, no tenant or parcel data), via
GraphDAO.parcel_climate(wait=True). Idempotent: cached cells are skipped.

Usage:
    python -m scripts.backfill_climate_cells --tenant <id> [--tenant <id> ...] [--dry-run]
    python -m scripts.backfill_climate_cells --all-installed [--dry-run]

--all-installed reads tenant_installed_modules (module_id='bioorchestrator') via
POSTGRES_URL.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from nkz_platform_sdk.orion import OrionClient

from app.core.config import settings
from app.graph.dao import GraphDAO, _geometry_centroid
from app.services.chelsa_climate import cell_key
from neo4j import AsyncGraphDatabase

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("backfill_climate_cells")

CELL_TIMEOUT_S = 90  # bound for one CHELSA cell read (same as enrich_trial_sites_chelsa)

AGRI_PARCEL_TYPE = "https://saref.etsi.org/saref4agri/AgriParcel"
PAGE_SIZE = 100
MODULE_ID = "bioorchestrator"


def _list_installed_tenants_sync(postgres_url: str) -> list[str]:
    import psycopg2

    conn = psycopg2.connect(postgres_url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT tenant_id FROM tenant_installed_modules "
                "WHERE module_id = %s AND is_enabled ORDER BY tenant_id",
                (MODULE_ID,),
            )
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


async def list_installed_tenants() -> list[str]:
    postgres_url = os.getenv("POSTGRES_URL")
    if not postgres_url:
        raise SystemExit("POSTGRES_URL is required for --all-installed")
    return await asyncio.to_thread(_list_installed_tenants_sync, postgres_url)


def parcel_centroid(entity: dict) -> tuple[float, float] | None:
    """(lat, lon) of an AgriParcel from keyValues or normalized `location`."""
    location = entity.get("location")
    if not isinstance(location, dict):
        return None
    geometry = location.get("value", location)
    if not isinstance(geometry, dict):
        return None
    point = _geometry_centroid(geometry.get("coordinates"))
    if point is None:
        return None
    lon, lat = point
    return lat, lon


async def iter_tenant_parcels(tenant: str):
    """Yield AgriParcel entities of a tenant, paginated (read-only)."""
    orion = OrionClient(tenant)
    try:
        offset = 0
        while True:
            page = await orion.query_entities(
                type=AGRI_PARCEL_TYPE,
                attrs="location",
                options="keyValues",
                limit=PAGE_SIZE,
                offset=offset,
            )
            for entity in page:
                yield entity
            if len(page) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
    finally:
        await orion.close()


async def collect_cells(tenants: list[str]) -> tuple[int, dict[str, tuple[float, float]]]:
    """Return (parcel count, {cell_key: (lat, lon)}) across tenants."""
    parcels = 0
    cells: dict[str, tuple[float, float]] = {}
    for index, tenant in enumerate(tenants, start=1):
        tenant_parcels = 0
        async for entity in iter_tenant_parcels(tenant):
            tenant_parcels += 1
            point = parcel_centroid(entity)
            if point is None:
                continue
            cells.setdefault(cell_key(*point), point)
        parcels += tenant_parcels
        logger.info("tenant_index=%d parcels=%d", index, tenant_parcels)
    return parcels, cells


async def backfill(dao: GraphDAO, tenants: list[str], dry_run: bool) -> dict[str, int]:
    parcels, cells = await collect_cells(tenants)
    to_compute: dict[str, tuple[float, float]] = {}
    cached = 0
    for key, point in cells.items():
        if await dao.get_climate_cell(key) is not None:
            cached += 1
        else:
            to_compute[key] = point
    counts = {
        "parcels": parcels,
        "cells": len(cells),
        "cached": cached,
        "to_compute": len(to_compute),
    }
    print(
        "parcels={parcels} cells={cells} cached={cached} to_compute={to_compute}".format(**counts)
    )
    if dry_run:
        return counts

    computed = failed = 0
    for key, (lat, lon) in to_compute.items():
        started = time.monotonic()
        data = await dao.parcel_climate(lat, lon, wait=True, timeout_s=CELL_TIMEOUT_S)
        seconds = time.monotonic() - started
        if data is None:
            failed += 1
        else:
            computed += 1
        logger.info(
            "cell key=%s koppen=%s seconds=%.1f",
            key,
            (data or {}).get("koppen") or "none",
            seconds,
        )
    counts.update(computed=computed, failed=failed)
    print(f"computed={computed} failed={failed}")
    return counts


async def _main(args: argparse.Namespace) -> int:
    tenants = list(dict.fromkeys(args.tenant)) if args.tenant else await list_installed_tenants()
    if not tenants:
        logger.error("no tenants to process")
        return 1
    driver = AsyncGraphDatabase.driver(
        settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password)
    )
    try:
        counts = await backfill(GraphDAO(driver), tenants, args.dry_run)
    finally:
        await driver.close()
    return 1 if counts.get("failed") else 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--tenant", action="append", help="tenant id (repeatable)")
    group.add_argument("--all-installed", action="store_true", help="tenants with the module installed")
    parser.add_argument("--dry-run", action="store_true", help="print counts only")
    args = parser.parse_args()
    sys.exit(asyncio.run(_main(args)))


if __name__ == "__main__":
    main()
