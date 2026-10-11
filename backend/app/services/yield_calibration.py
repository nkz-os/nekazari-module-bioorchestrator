"""Expected yield in the parcel's region and the moisture basis of reported yields.

The simulated yield is the water-limited ceiling: AquaCrop simulates water, not
nitrogen, pests or management. The expected yield scales it by a gap factor fitted
per region against official yields, and brackets it with the errors measured on
years left out of the fit (method and figures: the calibration file's ``sources`` and
``validation``). It is never computed outside the calibrated crop, regime and regions.

Moisture: the calibration compares with official yields at commercial moisture, so
crops with a sourced commercial moisture are reported on that basis
(dry yield / (1 - moisture)); the others stay as AquaCrop's dry matter.
"""

from __future__ import annotations

import bisect
import functools
import json
import statistics
from pathlib import Path
from typing import Any

# backend/data locally; /app/data in the image (Dockerfile: COPY backend/data/ data/).
DATA_DIR = Path(__file__).resolve().parents[2] / "data"
# The official wheat yields mix bread and durum wheat; both are simulated as WheatGDD.
CALIBRATION_FILES = {"wheat": "wheat_es_province_calibration.json",
                     "durum_wheat": "wheat_es_province_calibration.json"}
CALIBRATED_IRRIGATION = "rainfed"

_LLOVERAS_2017 = ("Lloveras J., Petit X., Yakoub A., Vida Rural 433 (2017) 42-47: commercial "
                  "wheat and barley fields, harvest at 13 % grain moisture")
COMMERCIAL_MOISTURE = {"wheat": (13.0, _LLOVERAS_2017), "durum_wheat": (13.0, _LLOVERAS_2017),
                       "barley": (13.0, _LLOVERAS_2017)}
_DRY = {"moisture": "dry_matter", "grain_moisture_pct": None, "source": None}


def yield_basis(crop_slug: str) -> dict:
    moisture = COMMERCIAL_MOISTURE.get(crop_slug)
    if moisture is None:
        return dict(_DRY)
    return {"moisture": "commercial", "grain_moisture_pct": moisture[0], "source": moisture[1]}


def to_basis(value: Any, basis: dict) -> Any:
    """A dry yield (float, {p10, p50, p90} or None) on ``basis``."""
    if value is None or basis["moisture"] != "commercial":
        return value
    factor = 1.0 / (1.0 - basis["grain_moisture_pct"] / 100.0)
    if isinstance(value, dict):
        return {k: (v * factor if v is not None else None) for k, v in value.items()}
    return value * factor


@functools.cache
def _calibration(crop_slug: str) -> dict | None:
    name = CALIBRATION_FILES.get(crop_slug)
    if name is None:
        return None
    return json.loads((DATA_DIR / name).read_text(encoding="utf-8"))


def _province(crop_slug: str, region: dict | None) -> dict | None:
    cal = _calibration(crop_slug)
    if cal is None or region is None or region.get("scheme") != cal["region_scheme"]:
        return None
    return cal["provinces"].get(region.get("code"))


def calibrated_phenology(crop_slug: str, irrigation: str, region: dict | None) -> dict[str, float] | None:
    """Phenology overrides validated for this crop, regime and region, else None (built-in crop)."""
    if irrigation != CALIBRATED_IRRIGATION:
        return None
    prov = _province(crop_slug, region)
    return dict(prov["phenology"]) if prov else None


def _lookup(table: dict, pred_kg: float) -> tuple[float, float]:
    b = bisect.bisect_right(table["edges"], pred_kg)
    return max(0.0, pred_kg + table["r10"][b]), max(0.0, pred_kg + table["r90"][b])


def expected_yield(crop_slug: str, irrigation: str, region: dict | None, ceiling_t_ha: Any) -> dict:
    """``{"status": "ok", ...}`` or ``{"status": "unavailable", "reason": ...}``.

    ``ceiling_t_ha``: the water-limited yield on the commercial basis, a float, or
    {p10, p50, p90} in season. In season the range is the union of the ranges of the
    three scenarios: the bounds are not monotonic in the prediction (above ~3 t/ha the
    model over-predicts, so a higher prediction can carry a lower upper bound).
    """
    cal = _calibration(crop_slug)
    if cal is None:
        return {"status": "unavailable", "reason": "crop_not_calibrated"}
    if irrigation != CALIBRATED_IRRIGATION:
        return {"status": "unavailable", "reason": "irrigated"}
    if region is None or region.get("scheme") != cal["region_scheme"]:
        return {"status": "unavailable", "reason": "outside_calibrated_regions"}
    prov = cal["provinces"].get(region.get("code"))
    if prov is None:
        return {"status": "unavailable", "reason": "region_not_calibrated"}
    f = prov["gap_factor"]
    scenarios = ([ceiling_t_ha[k] for k in ("p10", "p50", "p90")] if isinstance(ceiling_t_ha, dict)
                 else [ceiling_t_ha])
    bounds = [_lookup(cal["range"], f * v * 1000.0) for v in scenarios]
    low, high = min(b[0] for b in bounds), max(b[1] for b in bounds)
    return {
        "status": "ok",
        "basis": "gap_corrected",
        "range_t_ha": {"low": round(low / 1000.0, 2), "high": round(high / 1000.0, 2)},
        "coverage": round(statistics.mean(cal["range"]["coverage_by_block"]), 2),
        "gap_factor": f,
        "region": dict(region),
        "calibration": {"version": cal["version"], "source": cal["sources"]["yields"],
                        "validation": {"rmse_kg_ha": round(cal["validation"]["rmse_kg_ha"]),
                                       "anomaly_r_median": round(cal["validation"]["anomaly_r_median"], 2)}},
    }
