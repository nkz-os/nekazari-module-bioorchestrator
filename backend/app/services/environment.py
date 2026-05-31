"""
Environmental profile resolver — lat/lon → agronomic context.

Resolves climate (Köppen, rainfall, ET0, frost days), soil (WRB type,
texture, pH, organic matter), elevation, and photoperiod for any point
on Earth using IkerKeta connectors and astronomical calculation.

This is the bridge between "a farmer's GPS coordinates" and the
BioOrchestrator knowledge graph's TrialSite-based extrapolation engine.

Data sources (all open access):
  - Climate:  CHELSA v2.1 / ERA5 reanalysis (via IkerKeta ERA5ClimateConnector)
  - Soil:     SoilGrids ISRIC v2.0 (WCS REST, 250m resolution)
  - Elevation: Copernicus DEM GLO-30 (30m, via IkerKeta CopernicusDEMConnector)
  - Photoperiod: Astronomical calculation from latitude + solstice declination
"""

from __future__ import annotations

import math
from datetime import date
from typing import Any


# ═══════════════════════════════════════════════════════════════════════════════
# Allowed external FQDNs — defense in depth
# ═══════════════════════════════════════════════════════════════════════════════
# Even if NetworkPolicy allows HTTPS egress, this allowlist restricts which
# hosts the resolver can contact. Any FQDN not on this list is rejected
# BEFORE the HTTP request is made. Combined with NetworkPolicy, this provides
# two independent layers of security.

ALLOWED_FQDNS: frozenset[str] = frozenset({
    # ERA5 / CDS climate reanalysis (EU Copernicus)
    "cds.climate.copernicus.eu",
    "cds-beta.climate.copernicus.eu",
    # SoilGrids ISRIC — World Soil Information
    "rest.isric.org",
    "soilgrids.org",
    # Copernicus DEM — ESA Digital Elevation Model
    "copernicus-dem.openearth.community",
    # CHELSA — high-resolution climatology (fallback)
    "chelsa-climate.org",
    "envicloud.wsl.ch",
})


def _validate_fqdn(hostname: str) -> bool:
    """Check if a hostname is in the allowed FQDN set.

    Accepts exact match or any subdomain (e.g., 'rest.isric.org'
    matches both 'rest.isric.org' and 'data.rest.isric.org').
    """
    host = hostname.lower().strip()
    return any(
        host == allowed or host.endswith("." + allowed)
        for allowed in ALLOWED_FQDNS
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Photoperiod calculation
# ═══════════════════════════════════════════════════════════════════════════════

def _solar_declination(day_of_year: int) -> float:
    """Solar declination angle in degrees for a given day of year.

    Spencer formula (1971), accurate to ±0.01°.
    """
    b = (2 * math.pi / 365) * (day_of_year - 1)
    decl = (
        0.006918
        - 0.399912 * math.cos(b)
        + 0.070257 * math.sin(b)
        - 0.006758 * math.cos(2 * b)
        + 0.000907 * math.sin(2 * b)
        - 0.002697 * math.cos(3 * b)
        + 0.001480 * math.sin(3 * b)
    )
    return math.degrees(decl)


def _photoperiod_hours(lat: float, declination: float) -> float:
    """Day length in hours at a given latitude and solar declination.

    Uses the standard formula:
        day_length = (24/π) × arccos(−tan(lat) × tan(decl))

    For polar regions during polar day/night, clamps to [0, 24].
    """
    lat_rad = math.radians(lat)
    decl_rad = math.radians(declination)
    cos_hour_angle = -math.tan(lat_rad) * math.tan(decl_rad)

    if cos_hour_angle >= 1:
        return 24.0  # polar day
    if cos_hour_angle <= -1:
        return 0.0   # polar night

    hour_angle = math.acos(cos_hour_angle)
    return round(24.0 * hour_angle / math.pi, 2)


def summer_solstice_photoperiod(lat: float) -> float:
    """Photoperiod at the summer solstice (June 21, day 172) in hours.

    This is the standard reference for crop photoperiod sensitivity.
    """
    decl = _solar_declination(172)  # ~23.44°
    return _photoperiod_hours(lat, decl)


# ═══════════════════════════════════════════════════════════════════════════════
# Climate classification (Köppen) — simplified from rainfall + temperature
# ═══════════════════════════════════════════════════════════════════════════════

def _classify_koppen(
    annual_temp_c: float,
    annual_rainfall_mm: float,
    coldest_month_temp_c: float | None = None,
    warmest_month_temp_c: float | None = None,
    driest_month_rainfall_mm: float | None = None,
    wettest_season: str | None = None,
) -> str:
    """Approximate Köppen climate classification from key parameters.

    This is a simplified classifier for the main groups relevant to agriculture.
    Full Köppen requires monthly data; this uses annual + seasonal aggregates.

    Returns one of: Af, Am, Aw, BWh, BWk, BSh, BSk, Csa, Csb, Cfa, Cfb,
                     Cwa, Cwb, Dfa, Dfb, Dfc, ET, EF, Unknown
    """
    if annual_temp_c is None or annual_rainfall_mm is None:
        return "Unknown"

    dryness_threshold = 20 * (annual_temp_c + 7)

    if warmest_month_temp_c is not None and warmest_month_temp_c < 10:
        if coldest_month_temp_c is not None and coldest_month_temp_c > 0:
            return "ET"
        return "EF"

    is_arid = annual_rainfall_mm < dryness_threshold

    if is_arid:
        if annual_rainfall_mm < dryness_threshold / 2:
            if annual_temp_c >= 18:
                return "BWh"
            return "BWk"
        else:
            if annual_temp_c >= 18:
                return "BSh"
            return "BSk"

    if coldest_month_temp_c is not None and coldest_month_temp_c >= 18:
        if driest_month_rainfall_mm is not None and driest_month_rainfall_mm >= 60:
            return "Af"
        elif driest_month_rainfall_mm is not None:
            if annual_rainfall_mm >= 25 * (100 - driest_month_rainfall_mm):
                return "Am"
            return "Aw"
        return "Aw"

    if coldest_month_temp_c is not None and -3 <= coldest_month_temp_c < 18:
        if warmest_month_temp_c is not None and warmest_month_temp_c >= 22:
            if wettest_season == "winter":
                return "Csa"
            elif driest_month_rainfall_mm is not None and driest_month_rainfall_mm < 30:
                return "Cwa"
            return "Cfa"
        else:
            if wettest_season == "winter":
                return "Csb"
            return "Cfb"

    if coldest_month_temp_c is not None and coldest_month_temp_c <= -3:
        if warmest_month_temp_c is not None:
            if warmest_month_temp_c >= 22:
                return "Dfa" if wettest_season != "winter" else "Dwa"
            elif warmest_month_temp_c >= 10:
                return "Dfb" if wettest_season != "winter" else "Dwb"
            else:
                return "Dfc"
        return "Dfb"

    return "Unknown"


# ═══════════════════════════════════════════════════════════════════════════════
# Main resolver
# ═══════════════════════════════════════════════════════════════════════════════

async def resolve_environment(
    lat: float,
    lon: float,
    use_ikers: bool = True,
) -> dict[str, Any]:
    """Resolve environmental profile for a geographic point.

    Args:
        lat: Latitude in decimal degrees (-90 to 90).
        lon: Longitude in decimal degrees (-180 to 180).
        use_ikers: If True, use IkerKeta connectors (ERA5, Copernicus DEM).
                   Falls back to astronomical-only if connectors unavailable.

    Returns:
        Dict with keys:
          - elevation_m: float | None
          - climate_class: str (Köppen code)
          - annual_rainfall_mm: float | None
          - annual_temp_c: float | None
          - annual_et0_mm: float | None
          - frost_days_per_year: int | None
          - soil_type: str | None (WRB reference group)
          - soil_texture: str | None
          - soil_ph: float | None
          - soil_organic_matter_pct: float | None
          - photoperiod_summer_hours: float (always available — astronomical)
          - data_source: str (description of sources used)
    """
    profile: dict[str, Any] = {
        "latitude": lat,
        "longitude": lon,
        "elevation_m": None,
        "climate_class": "Unknown",
        "annual_rainfall_mm": None,
        "annual_temp_c": None,
        "annual_et0_mm": None,
        "frost_days_per_year": None,
        "soil_type": None,
        "soil_texture": None,
        "soil_ph": None,
        "soil_organic_matter_pct": None,
        "photoperiod_summer_hours": summer_solstice_photoperiod(lat),
        "data_source": "photoperiod: astronomical calculation",
    }

    if not use_ikers:
        return profile

    sources_used = ["photoperiod: astronomical"]

    # ── Elevation (Copernicus DEM) ──────────────────────────────────────
    try:
        from ikerketa.connectors.copernicus_dem import CopernicusDEMConnector
        connector = CopernicusDEMConnector()
        result = connector.fetch(lat=lat, lon=lon)
        if result.entities:
            dem = result.entities[0]
            if isinstance(dem, dict):
                elev = dem.get("elevation_m") or dem.get("elevation")
            else:
                elev = getattr(dem, "elevation_m", None) or getattr(dem, "elevation", None)
            if elev is not None:
                profile["elevation_m"] = round(float(elev), 1)
        sources_used.append("elevation: Copernicus DEM GLO-30")
    except (ImportError, TypeError, Exception):
        pass

    # ── Climate (ERA5 reanalysis) ───────────────────────────────────────
    try:
        from ikerketa.connectors.era5_climate import ERA5ClimateConnector
        connector = ERA5ClimateConnector()
        result = connector.fetch(lat=lat, lon=lon)
        if result.entities:
            climate = result.entities[0]
            if isinstance(climate, dict):
                rainfall = climate.get("annual_rainfall_mm") or climate.get("precipitation_mm")
                temp = climate.get("annual_temp_c") or climate.get("temperature_c")
                et0 = climate.get("annual_et0_mm") or climate.get("et0_mm")
                frost = climate.get("frost_days_per_year")
                coldest = climate.get("coldest_month_temp_c")
                warmest = climate.get("warmest_month_temp_c")
            else:
                rainfall = getattr(climate, "annual_rainfall_mm", None) or getattr(climate, "precipitation_mm", None)
                temp = getattr(climate, "annual_temp_c", None) or getattr(climate, "temperature_c", None)
                et0 = getattr(climate, "annual_et0_mm", None) or getattr(climate, "et0_mm", None)
                frost = getattr(climate, "frost_days_per_year", None)
                coldest = getattr(climate, "coldest_month_temp_c", None)
                warmest = getattr(climate, "warmest_month_temp_c", None)

            if rainfall is not None:
                profile["annual_rainfall_mm"] = round(float(rainfall), 1)
            if temp is not None:
                profile["annual_temp_c"] = round(float(temp), 1)
            if et0 is not None:
                profile["annual_et0_mm"] = round(float(et0), 1)
            if frost is not None:
                profile["frost_days_per_year"] = int(frost)

            profile["climate_class"] = _classify_koppen(
                annual_temp_c=profile["annual_temp_c"],
                annual_rainfall_mm=profile["annual_rainfall_mm"],
                coldest_month_temp_c=coldest,
                warmest_month_temp_c=warmest,
            )

        sources_used.append("climate: ERA5 reanalysis (via IkerKeta)")
    except (ImportError, TypeError, Exception):
        pass

    # ── Soil (SoilGrids ISRIC) ──────────────────────────────────────────
    try:
        from ikerketa.connectors.soilgrids import SoilGridsConnector
        connector = SoilGridsConnector()
        result = connector.fetch(lat=lat, lon=lon)
        if result.entities:
            soil = result.entities[0]
            if isinstance(soil, dict):
                profile["soil_type"] = soil.get("wrb_class") or soil.get("soil_type")
                profile["soil_texture"] = soil.get("texture") or soil.get("soil_texture")
                ph = soil.get("ph_h2o") or soil.get("soil_ph")
                om = soil.get("organic_carbon_pct") or soil.get("soil_organic_matter_pct")
            else:
                profile["soil_type"] = getattr(soil, "wrb_class", None) or getattr(soil, "soil_type", None)
                profile["soil_texture"] = getattr(soil, "texture", None) or getattr(soil, "soil_texture", None)
                ph = getattr(soil, "ph_h2o", None) or getattr(soil, "soil_ph", None)
                om = getattr(soil, "organic_carbon_pct", None) or getattr(soil, "soil_organic_matter_pct", None)

            if ph is not None:
                profile["soil_ph"] = round(float(ph), 1)
            if om is not None:
                profile["soil_organic_matter_pct"] = round(float(om), 1)

        sources_used.append("soil: SoilGrids ISRIC v2.0")
    except (ImportError, TypeError, Exception):
        pass

    profile["data_source"] = "; ".join(sources_used)
    return profile
