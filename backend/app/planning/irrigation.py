"""Seasonal NET irrigation need of a crop at a parcel: P10/P50/P90 over the climatology years.

net need of a year = sum over 10-day periods of max(0, ETc - Pe), ETc = Kc(day) * ET0(day), Pe the
effective rain of the period (method and citation in data/fao56_stage_lengths.yaml). In the field,
without irrigation efficiency. Pure functions; the weather comes from planning.climatology.

Kc and stage lengths come ONLY from data/fao56_stage_lengths.yaml (FAO-56 Tables 11 and 12, each value
cited there); a crop that is not in it has no stage lengths. Kc curve (FAO-56 eq. 66): Kc_ini constant
through `initial` (which starts on the sowing day), linear Kc_ini -> Kc_mid through `development`, Kc_mid
constant through `mid-season`, linear Kc_mid -> Kc_end through `late-season`.
"""
from __future__ import annotations

import calendar
import functools
import logging
import math
import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import yaml

from app.planning.climatology import Climatology
from app.species_registry import resolve_species

logger = logging.getLogger(__name__)

STAGES = ("initial", "development", "mid-season", "late-season")
MIN_YEARS = 5
MIN_COVERAGE = 0.95
MAX_SEASON_DAYS = 365


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


def _parameters_path() -> Path:
    return Path(os.getenv("DATA_DIR", "./data")) / "fao56_stage_lengths.yaml"


@functools.lru_cache(maxsize=1)
def load_pe_method() -> PeMethod:
    raw = yaml.safe_load(_parameters_path().read_text(encoding="utf-8"))["pe_method"]
    return PeMethod(id=raw["id"], citation=" ".join(str(raw["citation"]).split()),
                    period_days=int(raw["period_days"]), ref_mm=float(raw["ref_mm"]),
                    low_slope=float(raw["low_slope"]), break_mm=float(raw["break_mm"]),
                    offset_mm=float(raw["offset_mm"]), high_slope=float(raw["high_slope"]))


# ── Stages ────────────────────────────────────────────────────────────────────

YAML_STAGES = ("initial", "development", "mid_season", "late_season")


@dataclass(frozen=True)
class CropStages:
    """FAO-56 stage lengths (days, Table 11) and Kc ini/mid/end (Table 12) of one crop."""
    eppo: str
    lengths: tuple[int, int, int, int]
    kc_ini: float
    kc_mid: float
    kc_end: float
    kc_source: str

    def bounds(self) -> dict[str, tuple[int, int]]:
        """Cumulative (start, end) days after sowing per stage; `initial` starts on day 0."""
        out, start = {}, 0
        for name, length in zip(STAGES, self.lengths, strict=True):
            out[name] = (start, start + length)
            start += length
        return out


def _num(v) -> float | None:
    if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v):
        return None
    return float(v)


def _text(v) -> str:
    return " ".join(str(v).split()) if isinstance(v, str) else ""


def _crop_stages(eppo: str, raw) -> CropStages:
    """One crop entry, or ValueError: four whole-day lengths > 0, three Kc > 0, both table sources cited."""
    raw = raw if isinstance(raw, dict) else {}  # not a mapping: fails on the first missing field below
    lengths_raw = raw.get("lengths_days") or {}
    lengths = []
    for name in YAML_STAGES:
        v = lengths_raw.get(name)
        if isinstance(v, bool) or not isinstance(v, int) or v <= 0:
            raise ValueError(f"{eppo}: stage length {name} must be a whole number of days > 0")
        lengths.append(v)
    kc_raw = raw.get("kc") or {}
    kc = {}
    for name in ("ini", "mid", "end"):
        v = _num(kc_raw.get(name))
        if v is None or v <= 0:
            raise ValueError(f"{eppo}: Kc {name} must be a number > 0")
        kc[name] = v
    src = raw.get("source") or {}
    t11, t12 = src.get("stage_lengths") or {}, src.get("kc") or {}
    if t11.get("table") != 11 or t12.get("table") != 12:
        raise ValueError(f"{eppo}: stage lengths must cite FAO-56 Table 11 and Kc Table 12")
    for part, fields in ((t11, ("row", "plant_date", "region", "rule", "url")), (t12, ("row", "ini_row", "url"))):
        if not all(_text(part.get(f)) for f in fields):
            raise ValueError(f"{eppo}: source of table {part.get('table')} needs {', '.join(fields)}")
    if t11["rule"] not in ("mediterranean_or_spain", "first_row"):
        raise ValueError(f"{eppo}: unknown row rule {t11['rule']!r}")
    kc_source = (f"FAO-56 Table 11 '{_text(t11['row'])}' ({_text(t11['plant_date'])}, {_text(t11['region'])}); "
                 f"Table 12 '{_text(t12['row'])}'")
    if _text(t12["ini_row"]) != _text(t12["row"]):
        kc_source += f", Kc ini '{_text(t12['ini_row'])}'"
    return CropStages(eppo, tuple(lengths), kc["ini"], kc["mid"], kc["end"], kc_source)


@functools.lru_cache(maxsize=1)
def load_stage_table() -> dict[str, CropStages]:
    """Species slug -> CropStages from the FAO-56 YAML. Raises when the file is missing or an entry is
    invalid (the caller reports `kc_unavailable`), or when a key does not resolve to a registry species."""
    crops = yaml.safe_load(_parameters_path().read_text(encoding="utf-8")).get("crops")
    if not isinstance(crops, dict) or not crops:
        raise ValueError("fao56_stage_lengths.yaml: no crops")
    table: dict[str, CropStages] = {}
    for eppo, raw in crops.items():
        slug = resolve_species(str(eppo))
        if slug is None:
            raise ValueError(f"fao56_stage_lengths.yaml: {eppo} is not a registry species")
        if slug in table:
            raise ValueError(f"fao56_stage_lengths.yaml: {eppo} repeats species {slug}")
        table[slug] = _crop_stages(str(eppo), raw)
    return table


def kc_curve(crop: CropStages) -> list[float]:
    """Kc per day after sowing (index 0 = day 1), up to the end of `late-season`."""
    b = crop.bounds()
    ini_end = b["initial"][1]
    dev_start, dev_end = b["development"]
    mid_end = b["mid-season"][1]
    late_start, late_end = b["late-season"]
    out = []
    for day in range(1, late_end + 1):
        if day <= ini_end:
            k = crop.kc_ini
        elif day <= dev_end:
            k = crop.kc_ini + (day - dev_start) / (dev_end - dev_start) * (crop.kc_mid - crop.kc_ini)
        elif day <= mid_end:
            k = crop.kc_mid
        else:
            k = crop.kc_mid + (day - late_start) / (late_end - late_start) * (crop.kc_end - crop.kc_mid)
        out.append(k)
    return out


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
    lo = math.floor(pos)
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
