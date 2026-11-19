"""Population scans of the quality gate: what a single row cannot show, only the rows together.

Every scan returns warnings only (``Hit`` = rule, group, example) and never changes a row. They look at the
derived yield copy of the units (``UnitRow.yield_kg_ha``) and never mix:

- tiers: a unit whose site is a ``field`` site against the others (aggregate, region or no site), and
- production systems: organic units against the rest (``evidence_policy`` rule 10).

Thresholds were calibrated by defect injection on the graph export of 2026-10-20: a scaled, cloned or mixed
copy of real rows was injected 200 times with a fixed seed, and each threshold is the most sensitive one with at
most 1 % false alarms and at most 50 warnings per source and tier. Logarithms use ``max(yield, 1)`` so a zero
yield never raises.
"""
from __future__ import annotations

import math
import statistics
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Protocol

from app.graph.evidence_policy import is_organic
from app.graph.site_canonicalization import haversine_km

from .model import SiteRow, UnitRow

Hit = tuple[str, str, str]  # (rule, group, example)

MIN_REFERENCE_ENVIRONMENTS = 5  # environments behind a crop's reference median
SCALE_ENVIRONMENT_LOG10 = 0.5  # an environment about x3 off its crop
MIN_BLOCK_UNITS = 5  # units on each side of a crop-year block comparison
SCALE_BLOCK_LOG10 = 0.3  # a whole crop-year about x2 off the other years
MIN_CLONED_VALUES = 5  # yields two studies must share, all of them, to read as a copy
MIN_BIMODAL_VALUES = 12
MIN_BIMODAL_SIDE = 4  # values on each side of the gap
BIMODAL_GAP = 1.5  # widest gap between sorted log-yields, in robust spreads (1.4826 * MAD)
MAD_TO_SD = 1.4826
NEAR_SITES_M = 200.0  # below this the gate already warns (field_sites_closer_than_200m)
ALIAS_SITES_KM = 2.0
COPY_DECIMALS = 1


class OtherSources(Protocol):
    """Read-only view of the units of the OTHER sources a graph holds."""

    def other_source_yields(self, source_id: str) -> Iterable[tuple[str, int | None, str | None, float, str]]: ...


def _tier(unit: UnitRow, sites: Mapping[str, SiteRow]) -> str:
    site = sites.get(unit.site_key) if unit.site_key else None
    return "field" if site is not None and site.site_kind == "field" else "regional"


def _system(unit: UnitRow) -> str:
    return "organic" if is_organic(unit.production_system) else "conventional"


def _log10(value: float) -> float:
    return math.log10(max(value, 1.0))


def _scale_environment(rows: Sequence[tuple[UnitRow, str, str]]) -> list[Hit]:
    env_values: dict[tuple[Any, ...], list[float]] = defaultdict(list)
    for unit, tier, system in rows:
        env_values[(unit.crop_eppo, tier, system, unit.irrigation_regime or "", unit.site_key or "",
                    unit.year)].append(unit.yield_kg_ha)
    means = {key: statistics.fmean(values) for key, values in env_values.items()}
    by_regime: dict[tuple[Any, ...], list[float]] = defaultdict(list)
    by_crop: dict[tuple[Any, ...], list[float]] = defaultdict(list)
    for key, mean in means.items():
        by_regime[key[:4]].append(mean)
        by_crop[key[:3]].append(mean)
    hits = []
    for key in sorted(means, key=repr):
        reference = by_regime[key[:4]] if len(by_regime[key[:4]]) >= MIN_REFERENCE_ENVIRONMENTS else by_crop[key[:3]]
        if len(reference) < MIN_REFERENCE_ENVIRONMENTS:
            continue
        median = statistics.median(reference)
        score = abs(_log10(means[key]) - _log10(median))
        if score >= SCALE_ENVIRONMENT_LOG10:
            crop, tier, site, year = key[0], key[1], key[4], key[5]
            hits.append(("scale_environment", crop,
                         f"env:{crop}|{site}|{year}|{tier} mean={means[key]:.0f} crop median={median:.0f}"))
    return hits


def _scale_block(rows: Sequence[tuple[UnitRow, str, str]]) -> list[Hit]:
    by_crop: dict[tuple[str, str, str], dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    for unit, tier, system in rows:
        if unit.year is not None:
            by_crop[(unit.crop_eppo, tier, system)][unit.year].append(unit.yield_kg_ha)
    hits = []
    for (crop, tier, system) in sorted(by_crop):
        years = by_crop[(crop, tier, system)]
        for year in sorted(years):
            block = years[year]
            rest = [value for other, values in years.items() if other != year for value in values]
            if len(block) < MIN_BLOCK_UNITS or len(rest) < MIN_BLOCK_UNITS:
                continue
            block_median, rest_median = statistics.median(block), statistics.median(rest)
            if abs(_log10(block_median) - _log10(rest_median)) >= SCALE_BLOCK_LOG10:
                hits.append(("scale_block", f"{crop}|{year}|{tier}|{system}",
                             (f"block:{crop}|{year}|{tier}|{system} median={block_median:.0f} "
                              f"other years={rest_median:.0f}")))
    return hits


def _by_study(rows: Sequence[tuple[UnitRow, str, str]]) -> dict[tuple[str, str, str, str], list[float]]:
    studies: dict[tuple[str, str, str, str], list[float]] = defaultdict(list)
    for unit, tier, system in rows:
        if unit.study_key is not None:
            studies[(unit.crop_eppo, tier, system, unit.study_key)].append(unit.yield_kg_ha)
    return studies


def _cloned(studies: Mapping[tuple[str, str, str, str], list[float]]) -> list[Hit]:
    by_signature: dict[tuple[Any, ...], list[str]] = defaultdict(list)
    for (crop, tier, system, study), values in studies.items():
        if len(values) >= MIN_CLONED_VALUES:
            by_signature[(crop, tier, system, tuple(sorted(round(v, COPY_DECIMALS) for v in values)))].append(study)
    hits = []
    for (crop, _tier, _system, signature), study_keys in sorted(by_signature.items(), key=repr):
        if len(study_keys) < 2:
            continue
        for study in sorted(study_keys):
            hits.append(("cloned_studies", crop,
                         f"study:{study} shares its {len(signature)} yields with {len(study_keys) - 1} other study(ies)"))
    return hits


def _bimodal(studies: Mapping[tuple[str, str, str, str], list[float]]) -> list[Hit]:
    hits = []
    for key in sorted(studies):
        crop, study, values = key[0], key[3], studies[key]
        if len(values) < MIN_BIMODAL_VALUES:
            continue
        logs = sorted(math.log(max(v, 1.0)) for v in values)
        median = statistics.median(logs)
        spread = MAD_TO_SD * statistics.median(abs(v - median) for v in logs)
        if spread <= 0:
            continue
        gaps = [logs[i + 1] - logs[i] for i in range(MIN_BIMODAL_SIDE - 1, len(logs) - MIN_BIMODAL_SIDE)]
        if gaps and max(gaps) / spread >= BIMODAL_GAP:
            hits.append(("bimodal_study", crop,
                         f"study:{study} {len(values)} yields split by a gap of {max(gaps) / spread:.1f} spreads"))
    return hits


def scan_population(units: Sequence[UnitRow], sites: Mapping[str, SiteRow]) -> list[Hit]:
    """Scale (environment, crop-year block), cloned studies and bimodal studies, over the units with a yield."""
    rows = [(unit, _tier(unit, sites), _system(unit)) for unit in units if unit.yield_kg_ha is not None]
    if not rows:
        return []
    studies = _by_study(rows)
    return [*_scale_environment(rows), *_scale_block(rows), *_cloned(studies), *_bimodal(studies)]


def scan_site_aliases(sites: Mapping[str, SiteRow], registries: Any) -> list[Hit]:
    """Two located field sites (bundle vs bundle or vs the registry) 200 m to 2 km apart: maybe one place."""
    located = [(site_id, site.latitude, site.longitude) for site_id, site in sorted(sites.items())
               if site.site_kind == "field" and site.latitude is not None and site.longitude is not None]
    registered = [(entry.id, entry.latitude, entry.longitude) for entry in registries.sites
                  if entry.site_kind == "field" and entry.latitude is not None and entry.longitude is not None]
    seen: set[tuple[str, str]] = set()
    hits = []
    for site_id, lat, lon in located:
        for other_id, other_lat, other_lon in located + registered:
            pair = tuple(sorted((site_id, other_id)))
            if other_id == site_id or pair in seen:
                continue
            seen.add(pair)
            metres = haversine_km(lat, lon, other_lat, other_lon) * 1000.0
            if NEAR_SITES_M <= metres <= ALIAS_SITES_KM * 1000.0:
                hits.append(("field_sites_within_2km", f"{pair[0]} ~ {pair[1]}", f"site:{site_id} {metres:.0f} m"))
    return hits


def scan_cross_source(units: Sequence[UnitRow], others: OtherSources) -> list[Hit]:
    """The same yield of the same variety, crop and year in another source of the graph: maybe one trial twice."""
    mine = {}
    source_id = None
    for unit in units:
        source_id = source_id or unit.source_id
        if unit.yield_kg_ha is not None and unit.variety_key is not None:
            mine.setdefault((unit.crop_eppo, unit.year, unit.variety_key, round(unit.yield_kg_ha, COPY_DECIMALS)),
                            unit)
    if not mine or source_id is None:
        return []
    hits = []
    for crop, year, variety_key, value, other_source in others.other_source_yields(source_id):
        unit = mine.get((crop, year, variety_key, round(value, COPY_DECIMALS)))
        if unit is not None:
            hits.append(("cross_source_duplicate", other_source,
                         f"variety:{variety_key[:12]} {crop} {year} yield={value:g} also in {other_source}"))
    return sorted(hits)


__all__ = ["Hit", "OtherSources", "scan_cross_source", "scan_population", "scan_site_aliases"]
