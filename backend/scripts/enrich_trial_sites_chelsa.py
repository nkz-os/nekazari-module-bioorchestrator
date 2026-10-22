"""
Add CHELSA climate properties to TrialSite nodes (expand-only).

New properties only: climateClassChelsa, annualTempCChelsa, annualRainfallMmChelsa,
annualET0MmChelsa, coldestMonthMinCChelsa, climateChelsaCellKey,
climateChelsaComputedAt. Existing properties are never read back into a SET, so
legacy values (climateClass, annualRainfallMm, ...) are untouched.

Safe by default: without --execute nothing is written and a CSV diff
(name,stored,chelsa,changed) is printed.

--execute requires --backup <path>: a JSON dump of every TrialSite about to be
touched is written and fsync'd first; if that fails nothing is written. An
existing backup file is never overwritten. Idempotent: sites whose
climateChelsaCellKey equals the current cell key are skipped.

Restore: the change is additive, so rolling back is
    MATCH (ts:TrialSite) REMOVE ts.climateClassChelsa, ts.annualTempCChelsa,
        ts.annualRainfallMmChelsa, ts.annualET0MmChelsa, ts.coldestMonthMinCChelsa,
        ts.climateChelsaCellKey, ts.climateChelsaComputedAt
The backup also records name, latitude and longitude per node; elementId is not
stable over time, so match by those if the graph was rebuilt since.

--dry-run writes nothing at all: cells are read with chelsa_climate.read_cell and
the ClimateCell cache is not touched. --execute goes through
GraphDAO.parcel_climate(wait=True, timeout_s=...), which also caches ClimateCell nodes.
Both modes bound one cell read by the same CELL_TIMEOUT_S (inside read_cell).

Sites whose cell has koppen and rainfall but no ET0 are reported as "incomplete" (not
written, not an error): sea/coastal cells can have climate but no PET (petmean nodata).
Read/driver failures and cells without koppen or rainfall count as "failed" (exit 1).

Usage:
    python -m scripts.enrich_trial_sites_chelsa                      # dry-run
    python -m scripts.enrich_trial_sites_chelsa --execute --backup /path/backup.json
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.core.config import settings
from app.graph.dao import GraphDAO
from app.services.chelsa_climate import cell_key, read_cell
from neo4j import AsyncGraphDatabase

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("enrich_trial_sites_chelsa")

CONCURRENCY = 2
CELL_TIMEOUT_S = 90

READ_QUERY = (
    "MATCH (ts:TrialSite) "
    "WHERE ts.latitude IS NOT NULL AND ts.longitude IS NOT NULL "
    "RETURN elementId(ts) AS id, properties(ts) AS props ORDER BY ts.name"
)

# Only the new Chelsa properties are ever named here.
WRITE_QUERY = (
    "MATCH (ts:TrialSite) WHERE elementId(ts) = $id "
    "AND ts.latitude = $lat AND ts.longitude = $lon "
    "SET ts.climateClassChelsa = $climateClassChelsa, "
    "ts.annualTempCChelsa = $annualTempCChelsa, "
    "ts.annualRainfallMmChelsa = $annualRainfallMmChelsa, "
    "ts.annualET0MmChelsa = $annualET0MmChelsa, "
    "ts.coldestMonthMinCChelsa = $coldestMonthMinCChelsa, "
    "ts.climateChelsaCellKey = $climateChelsaCellKey, "
    "ts.climateChelsaComputedAt = $climateChelsaComputedAt "
    "RETURN count(ts) AS n"
)


def _coords(props: dict) -> tuple[float, float] | None:
    try:
        return float(props["latitude"]), float(props["longitude"])
    except (KeyError, TypeError, ValueError):
        return None


def write_backup(path: str, sites: list[dict]) -> None:
    """Dump the current properties of `sites` to `path`; fsync before returning."""
    payload = [
        {
            "id": s["id"],
            "name": s["props"].get("name"),
            "latitude": s["props"].get("latitude"),
            "longitude": s["props"].get("longitude"),
            "properties": s["props"],
        }
        for s in sites
    ]
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=1, default=str)
        fh.flush()
        os.fsync(fh.fileno())
    dir_fd = os.open(os.path.dirname(os.path.abspath(path)), os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


async def _compute(dao: GraphDAO, sem: asyncio.Semaphore, site: dict, execute: bool) -> dict | None:
    lat, lon = _coords(site["props"])  # validated by caller
    async with sem:
        try:
            if not execute:
                return await read_cell(lat, lon, timeout_s=CELL_TIMEOUT_S)
            # The single time bound is read_cell's timeout_s, forwarded by the DAO.
            return await dao.parcel_climate(lat, lon, wait=True, timeout_s=CELL_TIMEOUT_S)
        except Exception as exc:  # noqa: BLE001 - one bad cell must not stop the run
            logger.warning("site=%s cell failed: %s", site["props"].get("name"), type(exc).__name__)
            return None


async def enrich(driver, dao: GraphDAO, sites: list[dict], *, execute: bool, backup: str | None) -> dict:
    counts = {"sites": len(sites), "skipped": 0, "no_coords": 0, "computed": 0,
              "failed": 0, "incomplete": 0, "written": 0}
    todo: list[dict] = []
    for site in sites:
        point = _coords(site["props"])
        if point is None:
            counts["no_coords"] += 1
        elif site["props"].get("climateChelsaCellKey") == cell_key(*point):
            counts["skipped"] += 1
        else:
            todo.append(site)

    sem = asyncio.Semaphore(CONCURRENCY)
    results = await asyncio.gather(*(_compute(dao, sem, s, execute) for s in todo))

    ready: list[tuple[dict, dict]] = []
    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(["name", "stored", "chelsa", "changed"])
    for site, data in zip(todo, results):
        name = site["props"].get("name")
        if data is None or data.get("koppen") is None or data.get("annual_rainfall_mm") is None:
            logger.warning("site=%s failed cell, not written", name)
            counts["failed"] += 1
            continue
        if data.get("annual_et0_mm") is None:
            logger.warning("site=%s incomplete cell (no ET0), not written", name)
            counts["incomplete"] += 1
            continue
        counts["computed"] += 1
        stored = site["props"].get("climateClass")
        chelsa = data.get("koppen")
        writer.writerow([site["props"].get("name"), stored or "", chelsa or "", stored != chelsa])
        ready.append((site, data))

    if not execute:
        return counts
    if not ready:
        return counts
    if not backup:
        raise ValueError("--backup is required with --execute")
    write_backup(backup, [s for s, _ in ready])  # raises -> nothing is written

    computed_at = datetime.now(timezone.utc).isoformat()
    for site, data in ready:
        lat, lon = _coords(site["props"])
        async with driver.session() as session:
            res = await session.run(
                WRITE_QUERY,
                id=site["id"],
                lat=site["props"]["latitude"],
                lon=site["props"]["longitude"],
                climateClassChelsa=data.get("koppen"),
                annualTempCChelsa=data.get("annual_temp_c"),
                annualRainfallMmChelsa=data.get("annual_rainfall_mm"),
                annualET0MmChelsa=data.get("annual_et0_mm"),
                coldestMonthMinCChelsa=data.get("coldest_month_min_c"),
                climateChelsaCellKey=cell_key(lat, lon),
                climateChelsaComputedAt=computed_at,
            )
            record = await res.single()
        if record is None or record["n"] != 1:
            logger.error("site=%s write matched no node, not written", site["props"].get("name"))
            counts["failed"] += 1
            continue
        counts["written"] += 1
    return counts


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--dry-run", action="store_true", help="default; print the CSV diff only")
    parser.add_argument("--execute", action="store_true", help="write the new properties")
    parser.add_argument("--backup", help="JSON backup path (required with --execute)")
    args = parser.parse_args(argv)
    if args.execute and args.dry_run:
        parser.error("--execute and --dry-run are mutually exclusive")
    if args.execute and not args.backup:
        parser.error("--execute requires --backup <path>")
    if args.backup and not args.execute:
        logger.warning("--backup has no effect without --execute (dry-run writes nothing)")
    return args


async def fetch_sites(driver) -> list[dict]:
    async with driver.session() as session:
        result = await session.run(READ_QUERY)
        return [{"id": r["id"], "props": dict(r["props"])} async for r in result]


def exit_code(counts: dict) -> int:
    """Non-zero only for real failures; incomplete (no ET0) cells are expected."""
    return 1 if counts["failed"] else 0


async def _main(args: argparse.Namespace) -> int:
    driver = AsyncGraphDatabase.driver(
        settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password)
    )
    try:
        sites = await fetch_sites(driver)
        counts = await enrich(
            driver, GraphDAO(driver), sites, execute=args.execute, backup=args.backup
        )
    finally:
        await driver.close()
    logger.info("mode=%s %s", "execute" if args.execute else "dry-run", counts)
    return exit_code(counts)


def main() -> None:
    sys.exit(asyncio.run(_main(parse_args())))


if __name__ == "__main__":
    main()
