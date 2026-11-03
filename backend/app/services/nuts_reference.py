"""Cached reference lookups shared by the balance API and the planning fit.

Crop yield plausibility ranges, the INE province -> NUTS 2 table and the NUTS 2 region names.
"""
from __future__ import annotations

import functools
import logging

from app.kg.registries import DEFAULT_REGISTRIES_PATH, Registries, load_registries
from app.services.eurostat_yields import PlausibleRange
from app.services.nuts_regions import load_province_table, load_region_names

logger = logging.getLogger(__name__)


@functools.lru_cache(maxsize=1)
def _registries() -> Registries:
    return load_registries(DEFAULT_REGISTRIES_PATH)


def plausible_range(eppo: str) -> PlausibleRange | None:
    """The grain-yield envelope of a crop from the BioOrch ranges, in t/ha; None when it has none.

    The registry states kg/ha; any other unit is not converted (no guessing) and gives no range.
    """
    found = _registries().range_for(eppo, "crop_yield", {"purpose": "grain"})
    if found is None:
        return None
    if found.unit != "kg/ha":
        logger.warning("yield range %s has unit %r, expected kg/ha: no plausibility check",
                       found.id, found.unit)
        return None
    return PlausibleRange(min_t_ha=found.min / 1000, max_t_ha=found.max / 1000,
                          range_id=found.id, status=found.status)


@functools.lru_cache(maxsize=1)
def province_table() -> dict[str, str]:
    return load_province_table()


_names_failure_logged = False


def names_or_none() -> dict[str, str] | None:
    """Region names, or None when the snapshot cannot be read: names are decoration.

    The failure is logged once per process, not per call (a fit asks once per crop).
    """
    global _names_failure_logged
    try:
        return load_region_names()
    except Exception as exc:  # noqa: BLE001
        if not _names_failure_logged:
            _names_failure_logged = True
            logger.error("NUTS region names unavailable: %s: %s", type(exc).__name__, exc)
        return None
