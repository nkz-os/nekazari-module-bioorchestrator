"""EcoCrop data loader — in-memory cache of FAO EcoCrop CSV.

Loads once at module import. Provides growing_cycle_days lookups by scientific name.
CSV path: data/raw/ecocrop.csv (from FAO GAEZ EcoCrop export).
"""

import csv
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_ecocrop_cache: dict[str, dict] = {}
_loaded = False

DEFAULT_CSV_PATH = Path(__file__).parent.parent.parent.parent / "data" / "raw" / "ecocrop.csv"


def _load_ecocrop() -> None:
    global _loaded, _ecocrop_cache
    if _loaded:
        return
    _loaded = True

    csv_path = DEFAULT_CSV_PATH
    if not csv_path.exists():
        logger.warning("EcoCrop CSV not found at %s — growing_season_days will use defaults", csv_path)
        return

    try:
        with csv_path.open("r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                sci_name = (row.get("ScientificName") or row.get("scientific_name") or "").strip().lower()
                if not sci_name:
                    continue
                cycle_min = _safe_int(row.get("CycleLow") or row.get("cycle_min"))
                cycle_max = _safe_int(row.get("CycleHigh") or row.get("cycle_max"))
                life_form = (row.get("LifeForm") or row.get("life_form") or "").strip().lower()
                family = (row.get("Family") or row.get("family") or "").strip()
                if cycle_min or cycle_max:
                    _ecocrop_cache[sci_name] = {
                        "cycle_min": cycle_min,
                        "cycle_max": cycle_max,
                        "life_form": life_form,
                        "family": family,
                    }
        logger.info("EcoCrop loaded: %d species", len(_ecocrop_cache))
    except Exception as e:
        logger.warning("Failed to load EcoCrop CSV: %s", e)


def _safe_int(val) -> Optional[int]:
    if val is None or val == "" or val == "NA":
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def get_ecocrop_data(scientific_name: str) -> Optional[dict]:
    """Look up EcoCrop data by scientific name. Returns None if not found."""
    if not _loaded:
        _load_ecocrop()
    return _ecocrop_cache.get(scientific_name.strip().lower())


def get_growing_season_days(scientific_name: str) -> Optional[int]:
    """Get growing cycle days (midpoint of min/max). Returns None if not found."""
    data = get_ecocrop_data(scientific_name)
    if not data:
        return None
    cmin = data.get("cycle_min")
    cmax = data.get("cycle_max")
    if cmin and cmax:
        return (cmin + cmax) // 2
    return cmin or cmax
