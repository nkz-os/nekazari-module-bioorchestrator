"""Seasonal NET irrigation need of a crop at a parcel: P10/P50/P90 over the climatology years.

net need of a year = sum over 10-day periods of max(0, ETc - Pe), ETc = Kc(day) * ET0(day), Pe the
effective rain of the period (method and citation in data/fao56_stage_lengths.yaml). In the field,
without irrigation efficiency. Pure functions; the weather comes from planning.climatology.

Kc curve (FAO-56 eq. 66): Kc_ini constant until the end of `initial`, linear Kc_ini -> Kc_mid through
`development`, Kc_mid constant through `mid-season`, linear Kc_mid -> Kc_end through `late-season`.
The stored Kc of `development` is not a node of the curve. d1/d2 are cumulative days after sowing.
"""
from __future__ import annotations

import calendar
import functools
import logging
import math
import os
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import yaml

from app.core.knowledge import KnowledgeSource
from app.planning.climatology import Climatology

logger = logging.getLogger(__name__)

STAGES = ("initial", "development", "mid-season", "late-season")
MIN_YEARS = 5
MIN_COVERAGE = 0.95
MAX_SEASON_DAYS = 365
STAGE_TTL_S = 600.0

# Generic parameter of a stage: the seeder stores an unset cultivar / management / climate zone as
# these markers; cultivar, deficit-irrigation and contributed rows are other things.
_STAGES_QUERY = """
MATCH (sp:Species)-[:HAS_STAGE]->(st:PhenologyStage)-[:HAS_PARAMETER]->(p:PhenologyParams)
WHERE st.name IN $stages
  AND coalesce(p.cultivar, '__generic__') = '__generic__'
  AND coalesce(p.management, '__standard__') = '__standard__'
  AND coalesce(p.climateZone, '__any__') = '__any__'
  AND coalesce(p.status, '') <> 'pending_review'
RETURN sp.name AS species, st.name AS stage, p.kc AS kc, p.d1 AS d1, p.d2 AS d2,
       coalesce(p.sourceShort, p.sourceMethod) AS source
"""


@dataclass(frozen=True)
class PeMethod:
    """USDA-SCS effective rain of a 10-day period (mm), the form used by FAO CROPWAT 8.0."""
    id: str
    citation: str
    period_days: int
    ref_mm: float
    low_slope: float
    break_mm: float
    offset_mm: float
    high_slope: float

    def pe(self, rain_mm: float) -> float:
        if rain_mm <= 0:
            return 0.0
        if rain_mm <= self.break_mm:
            return rain_mm * (self.ref_mm - self.low_slope * rain_mm) / self.ref_mm
        return self.offset_mm + self.high_slope * rain_mm

    def pe_array(self, rain_mm: np.ndarray) -> np.ndarray:
        low = rain_mm * (self.ref_mm - self.low_slope * rain_mm) / self.ref_mm
        high = self.offset_mm + self.high_slope * rain_mm
        return np.where(rain_mm <= 0, 0.0, np.where(rain_mm <= self.break_mm, low, high))


@functools.lru_cache(maxsize=1)
def load_pe_method() -> PeMethod:
    path = Path(os.getenv("DATA_DIR", "./data")) / "fao56_stage_lengths.yaml"
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))["pe_method"]
    return PeMethod(id=raw["id"], citation=" ".join(str(raw["citation"]).split()),
                    period_days=int(raw["period_days"]), ref_mm=float(raw["ref_mm"]),
                    low_slope=float(raw["low_slope"]), break_mm=float(raw["break_mm"]),
                    offset_mm=float(raw["offset_mm"]), high_slope=float(raw["high_slope"]))


# ── Stages ────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class StageSet:
    """Validated FAO stages of a species: Kc per stage, (d1, d2) per stage, and the Kc source."""
    kc: dict[str, float]
    lengths: dict[str, tuple[float, float]]
    kc_source: str


def _num(v) -> float | None:
    if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v):
        return None
    return float(v)


def stage_set(rows: list[dict]) -> tuple[StageSet | None, str | None]:
    """(stages, None) or (None, reason): `no_kc` when a stage has no usable single Kc, `no_stage_lengths`
    when the limits are missing, not increasing or not contiguous. `rows` are the generic parameters of ONE species."""
    by_stage: dict[str, list[dict]] = {s: [] for s in STAGES}
    for r in rows:
        if r.get("stage") in by_stage:
            by_stage[r["stage"]].append(r)
    kc: dict[str, float] = {}
    lengths: dict[str, tuple[float, float]] = {}
    for s in STAGES:
        if len(by_stage[s]) != 1 or (_num(by_stage[s][0].get("kc")) or 0.0) <= 0:
            return None, "no_kc"
        kc[s] = _num(by_stage[s][0]["kc"])
    prev_end = None
    for s in STAGES:
        d1, d2 = _num(by_stage[s][0].get("d1")), _num(by_stage[s][0].get("d2"))
        if d1 is None or d2 is None or d1 < 0 or d2 <= d1 or (prev_end is not None and d1 != prev_end):
            return None, "no_stage_lengths"
        lengths[s] = (d1, d2)
        prev_end = d2
    sources = sorted({str(by_stage[s][0]["source"]) for s in STAGES if by_stage[s][0].get("source")})
    return StageSet(kc, lengths, "; ".join(sources) or "unknown"), None


def kc_curve(stages: dict[str, float], lengths: dict[str, tuple[float, float]]) -> list[float]:
    """Kc per day after sowing (index 0 = day 1), up to the end of `late-season`.

    `stages` maps stage name -> Kc, `lengths` stage name -> (d1, d2) in cumulative days after sowing.
    """
    kc_ini, kc_mid, kc_end = stages["initial"], stages["mid-season"], stages["late-season"]
    dev_start, dev_end = lengths["development"]
    late_start, late_end = lengths["late-season"]
    ini_end = lengths["initial"][1]
    mid_end = lengths["mid-season"][1]
    out = []
    for day in range(1, int(math.ceil(late_end)) + 1):
        if day <= ini_end:
            k = kc_ini
        elif day <= dev_end:
            k = kc_ini + (day - dev_start) / (dev_end - dev_start) * (kc_mid - kc_ini)
        elif day <= mid_end:
            k = kc_mid
        else:
            k = kc_mid + (day - late_start) / (late_end - late_start) * (kc_end - kc_mid)
        out.append(k)
    return out


_stage_cache: dict[str, tuple[float, dict[str, list[dict]]]] = {}


def clear_stage_cache() -> None:
    _stage_cache.clear()


async def load_stage_rows(source: KnowledgeSource, now: float | None = None) -> dict[str, list[dict]]:
    """Generic FAO stage parameters of every species, one graph query, cached per graph."""
    t = time.monotonic() if now is None else now
    hit = _stage_cache.get(source.graph_id)
    if hit is not None and t - hit[0] < STAGE_TTL_S:
        return hit[1]
    async with source.driver.session() as session:
        rows = await (await session.run(_STAGES_QUERY, stages=list(STAGES))).data()
    by_species: dict[str, list[dict]] = {}
    for r in rows:
        if r.get("species"):
            by_species.setdefault(r["species"], []).append(r)
    _stage_cache[source.graph_id] = (t, by_species)
    return by_species


# ── Need ──────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class NeedResult:
    p10: float
    p50: float
    p90: float
    n_years: int
    first_year: int
    last_year: int
    sowing_doy: int


def percentile(sorted_values: list[float], q: float) -> float:
    """Linear-interpolation percentile (q in 0..100) of an ascending list."""
    if len(sorted_values) == 1:
        return sorted_values[0]
    pos = (len(sorted_values) - 1) * q / 100.0
    lo = int(math.floor(pos))
    hi = min(lo + 1, len(sorted_values) - 1)
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (pos - lo)


def _gather(clim: Climatology, sow: date, n_days: int) -> tuple[np.ndarray, np.ndarray]:
    """ET0 and rain of the n_days from `sow` (NaN where missing), crossing the year boundary if needed."""
    et_parts, p_parts = [], []
    year, start, remaining = sow.year, sow.timetuple().tm_yday - 1, n_days
    while remaining > 0:
        take = min(remaining, (366 if calendar.isleap(year) else 365) - start)
        et0, rain = clim.year_series(year)
        et_parts.append(np.frombuffer(et0, dtype=np.float32)[start:start + take])
        p_parts.append(np.frombuffer(rain, dtype=np.float32)[start:start + take])
        remaining -= take
        year, start = year + 1, 0
    return np.concatenate(et_parts).astype(np.float64), np.concatenate(p_parts).astype(np.float64)


def _season_need(kc: np.ndarray, sow: date, clim: Climatology, pe: PeMethod) -> float | None:
    """Net need (mm) of the season sown on `sow`, or None when under MIN_COVERAGE of its days have data.

    A missing day is skipped; the sums of a 10-day block are then scaled to the block's EXPECTED days
    (a full period, or what is left of the season), so a gap does not lower the need.
    """
    n_days = len(kc)
    et0, rain = _gather(clim, sow, n_days)
    valid = np.isfinite(et0) & np.isfinite(rain)
    if valid.sum() < MIN_COVERAGE * n_days:
        return None
    period = pe.period_days
    block = np.arange(n_days) // period
    n_blocks = int(block[-1]) + 1
    etc = np.bincount(block, weights=np.where(valid, kc * et0, 0.0), minlength=n_blocks)
    prec = np.bincount(block, weights=np.where(valid, rain, 0.0), minlength=n_blocks)
    got = np.bincount(block, weights=valid.astype(np.float64), minlength=n_blocks)
    expected = np.minimum(period, n_days - np.arange(n_blocks) * period)
    use = got > 0
    # Pe is defined for a full period: scale the valid days to one, apply it, scale back to the expected days.
    g = np.where(use, period / np.where(use, got, 1.0), 0.0)
    need = np.maximum(0.0, etc * g - pe.pe_array(prec * g)) * expected / period
    return float(need[use].sum())


def irrigation_need(kc: list[float], sowing_doy: int, clim: Climatology, pe_method: PeMethod) -> NeedResult | None:
    """P10/P50/P90 of the yearly net need, or None (no season, or under MIN_YEARS valid years).

    A year is the HARVEST year; a season that crosses New Year (autumn sowing) is sown in the year before
    and needs both years in the climatology. The sowing day is a day-of-year of a non-leap year.
    The result is memoized on the climatology object (it dies with the cache entry).
    """
    n = len(kc)
    if n == 0 or n > MAX_SEASON_DAYS or not 1 <= sowing_doy <= 366:
        return None
    doy = min(sowing_doy, 365)
    memo = clim.__dict__.setdefault("_need_memo", {})
    key = (tuple(kc), doy, pe_method.id)
    if key in memo:
        return memo[key]
    crosses = doy + n - 1 > 365
    have = set(clim.years)
    kc_arr = np.asarray(kc, dtype=np.float64)
    ref = date(2001, 1, 1) + timedelta(days=doy - 1)
    needs: dict[int, float] = {}
    for year in clim.years:
        sow_year = year - 1 if crosses else year
        if sow_year not in have:
            continue
        value = _season_need(kc_arr, date(sow_year, ref.month, ref.day), clim, pe_method)
        if value is not None:
            needs[year] = value
    result = None
    if len(needs) >= MIN_YEARS:
        ordered = sorted(needs.values())
        result = NeedResult(p10=percentile(ordered, 10), p50=percentile(ordered, 50), p90=percentile(ordered, 90),
                            n_years=len(needs), first_year=min(needs), last_year=max(needs), sowing_doy=sowing_doy)
    memo[key] = result
    return result


# ── Declared allotment ────────────────────────────────────────────────────────

ALLOTMENT_UNIT = "MMT"


def read_allotment(raw, *, parcel_id: str = "", tenant_id: str = "") -> float | None:
    """Allotment in mm from the parcel's `irrigationAllotment` Property, or None (with a log when it is
    present but unusable: another unit, non-numeric, negative, or without unit)."""
    if raw is None:
        return None
    unit = raw.get("unitCode") if isinstance(raw, dict) else None
    value = _num(raw.get("value")) if isinstance(raw, dict) else None
    if unit != ALLOTMENT_UNIT:
        logger.warning("irrigation allotment ignored: unitCode=%r, expected %s tenant=%s parcel=%s", unit, ALLOTMENT_UNIT, tenant_id, parcel_id)
        return None
    if value is None or value < 0:
        logger.warning("irrigation allotment ignored: value is not a non-negative number tenant=%s parcel=%s", tenant_id, parcel_id)
        return None
    return value
