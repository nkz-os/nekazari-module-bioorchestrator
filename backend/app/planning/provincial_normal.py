"""Provincial normal: the mean official yield of a province, as an estimate (contract 2.12).

Only for rainfed bread wheat in Spain with a resolved province that has enough years of
official statistics. The band is the measured spread of the errors of that mean (method and
figures: the data file's ``validation``). It is the normal of the province, not a forecast
of the season. Pure: the only I/O is the cached data file; anything out of scope gives None.
"""

from __future__ import annotations

import functools
import json
import logging
from pathlib import Path
from typing import Any

from app.common.source_registry import get_attributions

logger = logging.getLogger(__name__)

PROVIDER_ID = "provincial_normal"
SOURCE_ID = "mapa_anuario_estadistica"
DATA_FILE = "wheat_es_province_normal.json"
# backend/data locally; /app/data in the image.
DATA_DIR = Path(__file__).resolve().parents[2] / "data"

VALIDATION_SCOPE = "normal of the province, not a forecast of this season"


@functools.cache
def _normal_data() -> dict | None:
    try:
        return json.loads((DATA_DIR / DATA_FILE).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        # Safe state: no normal is offered.
        logger.exception("provincial_normal_unavailable file=%s", DATA_FILE)
        return None


def _validation_metric(data: dict) -> str:
    """Metric line from the file's measured ``validation`` block and ``coverage``."""
    v = data["validation"]
    return (f"RMSE {v['rmse_t_ha']:.2f} t/ha out of sample (provincial statistics, "
            f"{v['n']} province-years {v['years']}); band covers {round(data['coverage'] * 100)} %")


def normal_estimate(crop_eppo: str, regime: str | None, country: str | None,
                    region: dict | None) -> dict[str, Any] | None:
    """``Estimate``-shaped dict for the province of the parcel, or None when out of scope."""
    data = _normal_data()
    if data is None or region is None:
        return None
    if (crop_eppo != data["crop"] or regime != data["regime"] or country != data["country"]
            or region.get("scheme") != data["region_scheme"]):
        return None
    prov = data["provinces"].get(region.get("code"))
    if prov is None or prov["n_years"] < data["min_years"]:
        return None
    mean = prov["mean_t_ha"]
    p10 = round(max(0.0, mean + data["band_t_ha"]["p10"]), 2)
    p90 = round(mean + data["band_t_ha"]["p90"], 2)
    return {
        "provider_id": PROVIDER_ID, "provider_version": data["version"], "basis": "provincial_normal",
        "unit": "t/ha",
        "percentiles": {"p10": p10, "p50": round(mean, 2), "p90": p90},
        "range": {"low": p10, "high": p90},
        "coverage": data["coverage"],
        "moisture": {"basis": "commercial", "pct": data["grain_moisture_pct"]},
        "n": prov["n_years"], "n_kind": "years",
        "validation": {"status": "validated", "metric": _validation_metric(data), "scope": VALIDATION_SCOPE},
        "warnings": [], "attributions": get_attributions([SOURCE_ID]), "graph_id": None,
    }
