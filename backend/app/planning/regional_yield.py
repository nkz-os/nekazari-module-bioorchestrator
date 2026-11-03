"""Expected regional yield from Eurostat statistics: pure, no I/O.

A statistic of the parcel's NUTS2 region (rainfed and irrigated together, EU standard humidity),
never a forecast for the parcel. It has its own basis and is never averaged with trial evidence.
"""
from __future__ import annotations

import statistics

from app.planning.estimates import _valid_yield, percentiles
from app.planning.models import Percentiles, RegionalMoisture, RegionalYield
from app.services.eurostat_yields import PlausibleRange, _range_check

REGION_SERIES_SHORT = "region_series_short"
NATIONAL_SERIES_SHORT = "national_series_short"


def _series(rows: list[dict], plausible: PlausibleRange | None) -> dict[int, float]:
    """year -> yield (t/ha): valid yields only, without the years above the plausible maximum."""
    out: dict[int, float] = {}
    for r in rows:
        value = r.get("yieldTHa")
        year = r.get("year")
        if not _valid_yield(value) or not isinstance(year, int) or isinstance(year, bool):
            continue
        if _range_check(value, plausible) == "above_max":
            continue
        out[year] = float(value)
    return out


def regional_yield(region_rows: list[dict], country_rows: list[dict], *, window: int = 10,
                   min_years: int = 5, plausible: PlausibleRange | None = None,
                   region_name: dict[str, str] | None = None) -> tuple[RegionalYield | None, str | None]:
    """P10/P50/P90 of the region's last `window` valid years, and its index against the country.

    Returns ``(None, "region_series_short")`` under `min_years` valid years. The index is the
    region P50 over the country P50 on the SAME years; with fewer than `min_years` country
    years in them it is None (reason ``national_series_short``, returned beside the figure).
    """
    region = _series(region_rows, plausible)
    years = sorted(region)[-window:]
    if len(years) < min_years:
        return None, REGION_SERIES_SHORT
    pct = percentiles([region[y] for y in years])
    country = _series(country_rows, plausible)
    shared = [y for y in years if y in country]
    index = None
    reason = NATIONAL_SERIES_SHORT
    if len(shared) >= min_years:
        base = statistics.median(country[y] for y in shared)
        region_p50 = statistics.median(region[y] for y in shared)
        if base > 0:
            index, reason = round(region_p50 / base, 3), None
    humidity = [r["humidityPct"] for r in region_rows
                if r.get("year") in years and isinstance(r.get("humidityPct"), (int, float))]
    nuts = next((r["nuts"] for r in region_rows if r.get("nuts")), "")
    return RegionalYield(
        nuts=nuts, region_name=region_name, first_year=years[0], last_year=years[-1],
        n_years=len(years), percentiles=Percentiles(**{k: round(v, 2) for k, v in pct.items()}),
        moisture=RegionalMoisture(pct=statistics.median(humidity) if humidity else None),
        relative_index=index,
    ), reason
