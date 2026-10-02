#!/usr/bin/env python3
"""One-off operator tool: compare computed Koppen class vs CHELSA kg2 vs stored class.

Usage: python scripts/validate_koppen_kg2.py sites.json [--limit N] [--seed S] [--concurrency 1]

Input: JSON list of {name, latitude, longitude, climate_class}, or a dict holding
that list. Output: CSV on stdout, summary on stderr. Read-only; the kg2 agreement
is informational and never affects the exit code (non-zero only on input/IO errors).
"""

from __future__ import annotations

import argparse
import asyncio
import collections
import csv
import json
import logging
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services import chelsa_climate as cc

log = logging.getLogger("validate_koppen_kg2")

KG2_PATH = "bioclim/kg2/1981-2010/CHELSA_kg2_1981-2010_V.2.1.tif"
READ_TIMEOUT_S = 90
MAX_CONCURRENCY = 4


def load_sites(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    if isinstance(data, dict):
        data = next((v for v in data.values() if isinstance(v, list)), None)
    if not isinstance(data, list):
        raise TypeError("input must be a JSON list or a dict containing a list")
    return [s for s in data if s.get("latitude") is not None and s.get("longitude") is not None]


def sample_kg2(lat: float, lon: float) -> int | None:
    import rasterio

    c_lat, c_lon = cc.cell_center(lat, lon)
    env = rasterio.Env(GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR")
    with env, rasterio.open("/vsicurl/" + cc.CHELSA_BASE + KG2_PATH) as ds:
        return int(next(ds.sample([(c_lon, c_lat)]))[0])


async def process(site: dict, sem: asyncio.Semaphore) -> tuple:
    async with sem:
        cell = None
        code = None
        try:
            cell = await cc.read_cell(site["latitude"], site["longitude"], timeout_s=READ_TIMEOUT_S)
        except Exception as exc:  # noqa: BLE001 - report and continue
            log.warning("read_cell failed for %s: %s", site.get("name"), exc)
        try:
            code = await asyncio.to_thread(sample_kg2, site["latitude"], site["longitude"])
        except Exception as exc:  # noqa: BLE001
            log.warning("kg2 sample failed for %s: %s", site.get("name"), exc)
        return site.get("name"), (cell or {}).get("koppen"), code, site.get("climate_class")


def report(rows: list[tuple]) -> None:
    by_code: dict = collections.defaultdict(collections.Counter)
    for _, computed, code, _ in rows:
        if code is not None and computed is not None:
            by_code[code][computed] += 1
    print("kg2 code -> computed class:", file=sys.stderr)
    for code, cnt in sorted(by_code.items()):
        print(f"  {code}: {dict(cnt)}", file=sys.stderr)
    total = sum(sum(c.values()) for c in by_code.values())
    agree = sum(c.most_common(1)[0][1] for c in by_code.values())
    if total:
        print(f"kg2 agreement (informational): {agree}/{total} = {agree / total:.2%}", file=sys.stderr)
    else:
        print("kg2 agreement: no comparable sites", file=sys.stderr)
    stored = [(n, k, s, c) for n, k, c, s in rows if s and k]
    match = sum(1 for _, k, s, _ in stored if k == s)
    if stored:
        print(f"computed vs stored: {match}/{len(stored)} = {match / len(stored):.2%}", file=sys.stderr)
    print("disagreements (computed != stored):", file=sys.stderr)
    for n, k, s, c in stored:
        if k != s:
            print(f"  {n}: computed={k} stored={s} kg2={c}", file=sys.stderr)


async def run(sites: list[dict], concurrency: int) -> list[tuple]:
    sem = asyncio.Semaphore(concurrency)
    return list(await asyncio.gather(*(process(s, sem) for s in sites)))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("sites_file")
    ap.add_argument("--limit", type=int, default=None, help="random sample size")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--concurrency", type=int, default=1, help=f"1..{MAX_CONCURRENCY}")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(levelname)s %(message)s")
    if not 1 <= args.concurrency <= MAX_CONCURRENCY:
        ap.error(f"--concurrency must be 1..{MAX_CONCURRENCY}")
    try:
        sites = load_sites(args.sites_file)
    except (OSError, ValueError, TypeError) as exc:
        log.error("cannot read input: %s", exc)
        return 2
    if args.limit is not None and args.limit < len(sites):
        sites = random.Random(args.seed).sample(sites, args.limit)
    log.info("processing %d sites, concurrency %d", len(sites), args.concurrency)
    rows = asyncio.run(run(sites, args.concurrency))
    writer = csv.writer(sys.stdout)
    writer.writerow(["name", "computed", "kg2_code", "stored_class"])
    writer.writerows(rows)
    report(rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
