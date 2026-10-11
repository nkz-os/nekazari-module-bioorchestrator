"""Recommender conditions of a parcel environment (shared by recommend and planning)."""
from __future__ import annotations

CLIMATE_KEYS = ("annual_rainfall_mm", "annual_et0_mm", "coldest_month_min_c", "annual_temp_c")


def conditions_from_environment(env: dict, *, climate_class: str | None = None, soil_type: str | None = None,
                                irrigation_regime: str | None = None, management: str = "any",
                                season: str = "all", purpose: str = "main", crops: list[str] | None = None,
                                top_n: int = 10, frost_margin_c: float | None = None) -> dict:
    soil = env.get("soil") or {}
    has_soil = bool(soil.get("data_available"))
    conditions = {
        "climate_class": climate_class or env.get("climate_class"),
        "soil_type": soil_type or (soil.get("wrb_type") if has_soil else None),
        "soil_ph": soil.get("ph") if has_soil else None,
        "soil_texture": soil.get("texture") if has_soil else None,
        "irrigation_regime": irrigation_regime or (env.get("irrigation") or {}).get("inferred"),
        "management": management,
        "season": season,
        "purpose": purpose,
        "crops": crops,
        "top_n": top_n,
        "frost_margin_c": frost_margin_c,
        "country": env.get("country"),
    }
    centroid = env.get("centroid") or {}
    if centroid.get("lat") is not None and centroid.get("lon") is not None:
        # The parcel point selects the GGCMI crop calendar cell (typical sowing day).
        # Added only when known, so a pointless parcel keys and ids like the conditions route.
        conditions["lat"], conditions["lon"] = centroid["lat"], centroid["lon"]
    detail = env.get("climate_detail") or {}
    for key in CLIMATE_KEYS:
        conditions[key] = detail.get(key)
    return conditions
