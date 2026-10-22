"""Enrich the sites of a bundle (plan task 8): CHELSA climate and a country check, FIELD sites only.

* An aggregate or region (a zone, a stratum, a network average) is never enriched: it has no place,
  so no coordinates, no Köppen class, no CHELSA cell. It is skipped explicitly and counted, even if a
  registry ever gave it coordinates.
* A field site with registry coordinates gets the CHELSA cell of those coordinates (the same layers,
  Köppen peel and limits as ``app.services.chelsa_climate``). A field site without coordinates is
  counted as ``no_coordinates`` and left alone (never guessed).
* Results are cached in a local JSON keyed by CHELSA cell, so a rebuild is deterministic and offline;
  only successful reads are cached. ``offline=True`` never touches the network (a miss is a gap).
* ``country_at`` checks the registry country against the coordinates; a mismatch is reported, never
  overwritten (the registry is the authority, a mismatch is a registry question).

The values are written under the property names ``dao.py`` reads: ``climateClassChelsa`` and friends
(read through ``coalesce(ts.climateClassChelsa, ts.climateClass)``), plus ``climateClass`` for the
queries that read only that. Three more properties ``dao.py`` reads directly are derived here, each from a
source named on the node: ``annualRainfallMm`` / ``annualET0Mm`` (the same CHELSA cell values; ``climateSource``
names them) and ``photoperiodSummerHours`` (astronomical day length at the summer solstice from the site's
latitude, ``photoperiodSource``). No timestamp is stored: the graph export must be deterministic.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from collections import Counter
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from app.services.chelsa_climate import SOURCE_LABEL, cell_key
from app.services.country_lookup import country_at
from app.services.environment import summer_solstice_photoperiod

from . import identity
from .model import SiteRow

logger = logging.getLogger(__name__)

PHOTOPERIOD_SOURCE = "astronomical day length at the summer solstice from the site latitude"
CONCURRENCY = 2
CELL_TIMEOUT_S = 90.0

ClimateReader = Callable[[float, float], Awaitable["dict[str, Any] | None"]]

_CACHE_FIELDS = ("koppen", "annual_temp_c", "annual_rainfall_mm", "annual_et0_mm", "coldest_month_min_c")


@dataclass(frozen=True)
class SiteEnrichment:
    site_key: str
    cell_key: str
    koppen: str
    annual_temp_c: float | None
    annual_rainfall_mm: float
    annual_et0_mm: float | None
    coldest_month_min_c: float | None
    source: str
    photoperiod_summer_hours: float


@dataclass(frozen=True)
class EnrichReport:
    enriched: dict[str, SiteEnrichment]  # site key -> values
    counts: dict[str, int]
    country_mismatch: dict[str, str]  # site key -> "registry XX, coordinates YY"
    failed: tuple[str, ...] = field(default=())

    @property
    def ok(self) -> bool:
        return not self.failed


def load_cache(path: Path | None) -> dict[str, dict[str, Any]]:
    if path is None or not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise TypeError(f"climate cache {path} is not a JSON object")
    return data


def save_cache(path: Path, cache: Mapping[str, Mapping[str, Any]]) -> None:
    """Deterministic (sorted keys) and atomic: a half-written cache is never left behind."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(cache, sort_keys=True, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    os.replace(tmp, path)


async def _default_reader(lat: float, lon: float) -> dict[str, Any] | None:
    from app.services.chelsa_climate import read_cell

    return await read_cell(lat, lon, timeout_s=CELL_TIMEOUT_S)


def _usable(data: Mapping[str, Any] | None) -> bool:
    return bool(data) and data.get("koppen") is not None and data.get("annual_rainfall_mm") is not None


async def enrich_sites(
    sites: Sequence[SiteRow], *, cache_path: Path | None = None, reader: ClimateReader | None = None,
    offline: bool = False, concurrency: int = CONCURRENCY,
) -> EnrichReport:
    """CHELSA climate for the field sites with coordinates; every other site is skipped, counted by reason."""
    counts: Counter[str] = Counter()
    mismatch: dict[str, str] = {}
    todo: dict[str, SiteRow] = {}  # cell key -> a site of it
    wanted: dict[str, list[SiteRow]] = {}
    for site in sorted(sites, key=identity.site_key):
        counts["sites"] += 1
        if site.site_kind != "field":
            counts["skipped_aggregate"] += 1  # no place, so no climate; never from coordinates it might carry
            continue
        if site.latitude is None or site.longitude is None:
            counts["no_coordinates"] += 1
            continue
        found = country_at(site.latitude, site.longitude)
        if found is not None and found != site.country:
            mismatch[identity.site_key(site)] = f"registry {site.country}, coordinates {found}"
        key = cell_key(site.latitude, site.longitude)
        wanted.setdefault(key, []).append(site)
        todo.setdefault(key, site)

    cache = load_cache(cache_path)
    missing = {k: s for k, s in todo.items() if not _usable(cache.get(k))}
    counts["cells"] = len(todo)
    counts["from_cache"] = len(todo) - len(missing)
    failed: list[str] = []
    if missing and not offline:
        read = reader or _default_reader
        sem = asyncio.Semaphore(concurrency)

        async def one(key: str, site: SiteRow) -> tuple[str, dict[str, Any] | None]:
            async with sem:
                try:
                    return key, await read(site.latitude, site.longitude)  # type: ignore[arg-type]
                except Exception as exc:  # noqa: BLE001 - one bad cell must not stop the others
                    logger.warning("kg enrich cell=%s failed: %s", key, type(exc).__name__)
                    return key, None

        for key, data in await asyncio.gather(*(one(k, s) for k, s in sorted(missing.items()))):
            if _usable(data):
                cache[key] = {f: data.get(f) for f in _CACHE_FIELDS} | {"source": data.get("source") or SOURCE_LABEL}
                counts["fetched"] += 1
            else:
                counts["failed"] += 1
        if counts["fetched"] and cache_path is not None:
            save_cache(cache_path, cache)
    enriched: dict[str, SiteEnrichment] = {}
    for key, group in sorted(wanted.items()):
        data = cache.get(key)
        if not _usable(data):
            failed.extend(identity.site_key(s) for s in group)
            counts["without_climate"] += len(group)
            continue
        for site in group:
            enriched[identity.site_key(site)] = SiteEnrichment(
                site_key=identity.site_key(site), cell_key=key, koppen=data["koppen"],
                annual_temp_c=data.get("annual_temp_c"), annual_rainfall_mm=data["annual_rainfall_mm"],
                annual_et0_mm=data.get("annual_et0_mm"), coldest_month_min_c=data.get("coldest_month_min_c"),
                source=data.get("source") or SOURCE_LABEL,
                photoperiod_summer_hours=summer_solstice_photoperiod(site.latitude))
    counts["enriched"] = len(enriched)
    logger.info("kg enrich counts=%s country_mismatch=%d", dict(counts), len(mismatch))
    return EnrichReport(enriched=enriched, counts=dict(counts), country_mismatch=mismatch, failed=tuple(sorted(failed)))


# Never matches an aggregate: the site must be a field site, and the write must hit exactly one node.
_WRITE = """
UNWIND $rows AS r
MATCH (n:TrialSite {siteKey: r.siteKey}) WHERE n.siteKind = 'field'
SET n.climateClass = r.koppen, n.climateClassChelsa = r.koppen,
    n.annualTempCChelsa = r.annualTempC, n.annualRainfallMmChelsa = r.annualRainfallMm,
    n.annualET0MmChelsa = r.annualET0Mm, n.coldestMonthMinCChelsa = r.coldestMonthMinC,
    n.climateChelsaCellKey = r.cellKey, n.climateSource = r.source,
    n.annualRainfallMm = r.annualRainfallMm, n.annualET0Mm = r.annualET0Mm,
    n.photoperiodSummerHours = r.photoperiodHours, n.photoperiodSource = $photoperiodSource
RETURN count(n) AS matched
"""


async def apply_enrichment(report: EnrichReport, driver: Any, *, database: str | None = None) -> int:
    """Write the enrichment to the graph; returns the sites written. Idempotent (plain SET)."""
    rows = [
        {"siteKey": e.site_key, "koppen": e.koppen, "annualTempC": e.annual_temp_c,
         "annualRainfallMm": e.annual_rainfall_mm, "annualET0Mm": e.annual_et0_mm,
         "coldestMonthMinC": e.coldest_month_min_c, "cellKey": e.cell_key, "source": e.source,
         "photoperiodHours": e.photoperiod_summer_hours}
        for _, e in sorted(report.enriched.items())
    ]
    if not rows:
        return 0
    async with driver.session(database=database) as session:
        async def work(tx: Any) -> int:
            result = await tx.run(_WRITE, rows=rows, photoperiodSource=PHOTOPERIOD_SOURCE)
            record = await result.single()
            await result.consume()
            return int(record["matched"]) if record is not None else 0

        matched = await session.execute_write(work)
    if matched != len(rows):
        raise RuntimeError(f"enrichment matched {matched} of {len(rows)} field sites in the graph")
    return matched


__all__ = ["EnrichReport", "SiteEnrichment", "apply_enrichment", "enrich_sites", "load_cache", "save_cache"]
