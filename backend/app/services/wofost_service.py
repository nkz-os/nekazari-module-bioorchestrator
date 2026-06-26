"""WOFOST crop simulation service — wraps PCSE for BioOrchestrator.

Provides run_wofost_simulation() which:
  1. Fetches weather from timeseries-reader (backed by weather-worker)
  2. Fetches soil texture from Soil module → pedotransfer
  3. Fetches sowing date from field-operations (AgriParcelOperation)
  4. Loads crop parameters from graph (PhenologyParams + defaults)
  5. Runs PCSE WOFOST simulation
  6. Returns biomass, LAI, and yield projection

Requires: pcse (pip install pcse)
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

logger = logging.getLogger(__name__)

# PCSE is optional — simulation fails gracefully if not installed
try:
    import pcse  # noqa: F401
    HAS_PCSE = True
except ImportError:
    HAS_PCSE = False


# ── Default WOFOST crop parameters for key crops ──
# These are WOFOST-standard values from PCSE's built-in crop files.
# TSUM1 = GDD from emergence to anthesis (°C·day)
# TSUM2 = GDD from anthesis to maturity (°C·day)
# AMAXTB = maximum CO2 assimilation rate (kg/ha/hr) per DVS
# SLATB = specific leaf area (ha/kg) per DVS

DEFAULT_CROP_PARAMS = {
    "wheat": {
        "TSUM1": 1100, "TSUM2": 900,
        "TDWI": 210, "LAIEM": 0.15, "RGRLAI": 0.008,
        "SPAN": 35, "TBASE": 0, "TEFFMX": 30,
        "AMAXTB": [(0.0, 35.0), (1.0, 40.0), (1.5, 35.0), (2.0, 5.0)],
        "SLATB": [(0.0, 0.0027), (1.0, 0.0020), (2.0, 0.0015)],
    },
    "barley": {
        "TSUM1": 950, "TSUM2": 800,
        "TDWI": 200, "LAIEM": 0.15, "RGRLAI": 0.009,
        "SPAN": 31, "TBASE": 0, "TEFFMX": 30,
        "AMAXTB": [(0.0, 35.0), (1.0, 38.0), (1.5, 35.0), (2.0, 5.0)],
        "SLATB": [(0.0, 0.0030), (1.0, 0.0022), (2.0, 0.0015)],
    },
    "maize": {
        "TSUM1": 800, "TSUM2": 950,
        "TDWI": 25, "LAIEM": 0.0025, "RGRLAI": 0.03,
        "SPAN": 42, "TBASE": 8, "TEFFMX": 40,
        "AMAXTB": [(0.0, 40.0), (1.0, 60.0), (1.5, 50.0), (2.0, 5.0)],
        "SLATB": [(0.0, 0.0030), (1.0, 0.0023), (2.0, 0.0018)],
    },
    "potato": {
        "TSUM1": 800, "TSUM2": 1100,
        "TDWI": 150, "LAIEM": 0.006, "RGRLAI": 0.01,
        "SPAN": 35, "TBASE": 4, "TEFFMX": 30,
        "AMAXTB": [(0.0, 40.0), (1.0, 50.0), (1.5, 45.0), (2.0, 5.0)],
        "SLATB": [(0.0, 0.0030), (1.0, 0.0025), (2.0, 0.0020)],
    },
    "sunflower": {
        "TSUM1": 900, "TSUM2": 950,
        "TDWI": 18, "LAIEM": 0.006, "RGRLAI": 0.03,
        "SPAN": 40, "TBASE": 6, "TEFFMX": 38,
        "AMAXTB": [(0.0, 40.0), (1.0, 55.0), (1.5, 50.0), (2.0, 5.0)],
        "SLATB": [(0.0, 0.0030), (1.0, 0.0023), (2.0, 0.0018)],
    },
    "soybean": {
        "TSUM1": 900, "TSUM2": 1000,
        "TDWI": 20, "LAIEM": 0.006, "RGRLAI": 0.03,
        "SPAN": 35, "TBASE": 6, "TEFFMX": 35,
        "AMAXTB": [(0.0, 35.0), (1.0, 45.0), (1.5, 40.0), (2.0, 5.0)],
        "SLATB": [(0.0, 0.0030), (1.0, 0.0025), (2.0, 0.0020)],
    },
    "rice": {
        "TSUM1": 1100, "TSUM2": 950,
        "TDWI": 30, "LAIEM": 0.0025, "RGRLAI": 0.008,
        "SPAN": 38, "TBASE": 10, "TEFFMX": 38,
        "AMAXTB": [(0.0, 30.0), (1.0, 45.0), (1.5, 40.0), (2.0, 5.0)],
        "SLATB": [(0.0, 0.0030), (1.0, 0.0022), (2.0, 0.0015)],
    },
}


def get_crop_params(crop_slug: str, graph_params: dict | None = None) -> dict:
    """Get WOFOST crop parameters, optionally overriding from graph data.

    Args:
        crop_slug: Canonical crop name (e.g. 'wheat', 'maize')
        graph_params: Optional overrides from Neo4j PhenologyParams
                      (may contain tsum1, tsum2, etc.)

    Returns:
        WOFOST-compatible crop parameter dict
    """
    params = dict(DEFAULT_CROP_PARAMS.get(crop_slug, DEFAULT_CROP_PARAMS.get("wheat", {})))

    if graph_params:
        # Override TSUM from graph GDD stage data
        # Graph has stage_detection with gdd_min/gdd_max per stage
        if graph_params.get("tsum1"):
            params["TSUM1"] = float(graph_params["tsum1"])
        if graph_params.get("tsum2"):
            params["TSUM2"] = float(graph_params["tsum2"])
        if graph_params.get("tbase"):
            params["TBASE"] = float(graph_params["tbase"])

    return params


def run_wofost_simulation(
    crop_slug: str,
    sowing_date: date,
    weather_data: list[dict],
    soil_hydraulic_props: dict,
    crop_params_override: dict | None = None,
    emergence_date: date | None = None,
    max_duration_days: int = 365,
) -> dict:
    """Run a WOFOST simulation and return projections.

    Args:
        crop_slug: Canonical crop name
        sowing_date: Date of sowing (from field-operations)
        weather_data: List of {date, tmin, tmax, precip, radiation, wind, vapour_pressure}
        soil_hydraulic_props: From pedotransfer (theta_sat, theta_fc, theta_wp, k_sat)
        crop_params_override: Optional param overrides from graph
        emergence_date: Optional observed emergence date (from satellite/field)
        max_duration_days: Maximum simulation length

    Returns:
        Dict with projected yield, biomass, LAI, and per-stage results
    """
    if not HAS_PCSE:
        return _run_fallback_simulation(
            crop_slug, sowing_date, weather_data, soil_hydraulic_props,
            crop_params_override, emergence_date, max_duration_days,
        )

    # ── Full PCSE simulation ──
    try:
        from pcse.base import ParameterProvider
        from pcse.models import Wofost71_WLP_FD
        import os

        crop_params = get_crop_params(crop_slug, crop_params_override)

        # Build weather in PCSE format
        # PCSE expects: [DAY, IRRAD (kJ/m²/day), TMIN, TMAX, VAP (kPa), WIND (m/s), RAIN (mm/day)]
        wdp = _build_weather_for_pcse(weather_data, sowing_date)

        # Build soil
        sdp = _build_soil_for_pcse(soil_hydraulic_props)

        # Build crop parameter file
        crop_fp = _build_crop_file(crop_params)

        try:
            parameters = ParameterProvider(
                cropdata=crop_fp, sitedata=sdp, soildata=None
            )
            wofost = Wofost71_WLP_FD(parameters, wdp, None)

            # Run simulation
            wofost.run_till_terminate()

            # Collect output
            output = []
            for step in wofost.get_output():
                output.append({
                    "day": step["DAY"],
                    "dvs": round(step.get("DVS", 0), 3),
                    "lai": round(step.get("LAI", 0), 2),
                    "tagp": round(step.get("TAGP", 0), 1),  # total above-ground biomass (kg/ha)
                    "twso": round(step.get("TWSO", 0), 1),  # storage organ weight (kg/ha)
                    "twlv": round(step.get("TWLV", 0), 1),  # leaf weight
                    "twst": round(step.get("TWST", 0), 1),  # stem weight
                    "wso": round(step.get("WSO", 0), 1),    # storage organ yield
                    "tran": round(step.get("TRAN", 0), 1),  # transpiration
                    "evap": round(step.get("EVAP", 0), 1),  # soil evaporation
                    "tra": round(step.get("TRA", 0), 1),    # actual transpiration
                })

            last = output[-1] if output else {}
            return {
                "model": "WOFOST 7.1 (PCSE)",
                "method": "mechanistic",
                "simulated_yield_kg_ha": round(last.get("wso", 0), 1),
                "total_biomass_kg_ha": round(last.get("tagp", 0), 1),
                "max_lai": round(max(s["lai"] for s in output) if output else 0, 2),
                "days_simulated": len(output),
                "daily_output": output,
                "harvest_dvs": round(last.get("dvs", 0), 3),
            }

        finally:
            os.unlink(crop_fp)

    except Exception as e:
        logger.warning("PCSE simulation failed: %s — falling back", e)
        return _run_fallback_simulation(
            crop_slug, sowing_date, weather_data, soil_hydraulic_props,
            crop_params_override, emergence_date, max_duration_days,
        )


def _run_fallback_simulation(
    crop_slug: str,
    sowing_date: date,
    weather_data: list[dict],
    soil_hydraulic_props: dict,
    crop_params_override: dict | None = None,
    emergence_date: date | None = None,
    max_duration_days: int = 365,
) -> dict:
    """Simplified FAO-33 simulation when PCSE is unavailable.

    Uses the same Ky × water stress methodology as Phase B, but with
    daily weather and soil hydraulic parameters for better accuracy.
    """
    crop_params = get_crop_params(crop_slug, crop_params_override)
    tsum1 = crop_params.get("TSUM1", 1100)
    tsum2 = crop_params.get("TSUM2", 900)
    tbase = crop_params.get("TBASE", 0)

    awc_mm = soil_hydraulic_props.get("awc_mm_per_metre", 140)
    soil_water = awc_mm * 0.7  # start at 70% AWC

    # Simplified FAO-33 stages
    stages = [
        {"name": "initial", "ky": 0.4, "kc": 0.35, "gdd_range": (0, tsum1 * 0.2)},
        {"name": "development", "ky": 0.55, "kc": 0.70, "gdd_range": (tsum1 * 0.2, tsum1)},
        {"name": "mid-season", "ky": 0.65, "kc": 1.15, "gdd_range": (tsum1, tsum1 + tsum2 * 0.6)},
        {"name": "late-season", "ky": 0.4, "kc": 0.40, "gdd_range": (tsum1 + tsum2 * 0.6, tsum1 + tsum2)},
    ]

    cum_gdd = 0.0
    stress_factor = 1.0
    daily_results = []

    for day_offset, w in enumerate(weather_data[:max_duration_days]):
        tavg = (w.get("tmin", 10) + w.get("tmax", 20)) / 2
        gdd_today = max(0, tavg - tbase)
        cum_gdd += gdd_today

        # Determine current stage
        current_stage = None
        for st in stages:
            if st["gdd_range"][0] <= cum_gdd < st["gdd_range"][1]:
                current_stage = st
                break
        if current_stage is None:
            if cum_gdd >= stages[-1]["gdd_range"][1]:
                break  # mature
            current_stage = stages[0]

        kc = current_stage["kc"]
        ky = current_stage["ky"]
        precip = w.get("precip", 0) or 0
        et0 = w.get("eto", 3.5) or 3.5
        etc = kc * et0

        # Water balance
        soil_water = min(awc_mm, soil_water + precip - etc)
        soil_water = max(0, soil_water)

        # Stress
        if etc > 0 and awc_mm > 0:
            relative_water = soil_water / awc_mm
            if relative_water < 0.5:  # MAD threshold
                ks = relative_water / 0.5
                stress_today = 1.0 - ky * (1.0 - ks)
                stress_today = max(0.3, min(1.0, stress_today))
                stress_factor *= stress_today
            else:
                stress_today = 1.0

        daily_results.append({
            "day": day_offset + 1,
            "gdd_cum": round(cum_gdd, 1),
            "stage": current_stage["name"],
            "kc": kc,
            "ky": ky,
            "eto": round(et0, 1),
            "etc": round(etc, 1),
            "precip": round(precip, 1),
            "soil_water_mm": round(soil_water, 1),
            "ks": round(stress_today, 3) if etc > 0 else 1.0,
        })

    return {
        "model": "FAO-33 simplified (PCSE unavailable)",
        "method": "empirical",
        "projected_yield_kg_ha": None,  # caller multiplies by potential
        "total_stress_factor": round(stress_factor, 3),
        "cumulative_gdd": round(cum_gdd, 1),
        "tsum_emergence_to_anthesis": tsum1,
        "tsum_anthesis_to_maturity": tsum2,
        "base_temperature": tbase,
        "days_simulated": len(daily_results),
        "daily_output": daily_results,
    }


def _build_weather_for_pcse(weather_data: list[dict], start_date: date) -> Any:
    """Build PCSE weather data provider from timeseries-reader format."""
    from pcse.util import WOFOST71SiteDataProvider
    import tempfile
    import os
    import csv

    # PCSE expects a CSV with columns: DAY, IRRAD, TMIN, TMAX, VAP, WIND, RAIN
    fd, path = tempfile.mkstemp(suffix=".csv", text=True)
    with os.fdopen(fd, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["DAY", "IRRAD", "TMIN", "TMAX", "VAP", "WIND", "RAIN"])
        for i, w in enumerate(weather_data):
            day = (start_date + timedelta(days=i)).strftime("%Y%m%d")
            # Convert W/m² to kJ/m²/day: multiply by 0.0864
            rad_w_m2 = w.get("radiation_w_m2", 200) or 200
            irrad = rad_w_m2 * 0.0864
            tmin = w.get("tmin", 10)
            tmax = w.get("tmax", 25)
            vap = w.get("vapour_pressure_kpa", 1.5) or 1.5
            wind = w.get("wind_speed_ms", 2.0) or 2.0
            rain = w.get("precip", 0) or 0
            writer.writerow([day, round(irrad, 1), round(tmin, 1), round(tmax, 1),
                            round(vap, 2), round(wind, 1), round(rain, 1)])

    return WOFOST71SiteDataProvider(WAV=path, latitude=42.0, lon=-1.5, elev=400)


def _build_soil_for_pcse(props: dict) -> dict:
    """Build PCSE soil parameters from pedotransfer output."""
    return {
        "SMFCF": props.get("theta_fc", 0.28),  # field capacity (m³/m³)
        "SM0": props.get("theta_sat", 0.45),    # saturated (m³/m³)
        "SMW": props.get("theta_wp", 0.12),      # wilting point (m³/m³)
        "KSUB": props.get("k_sat_mm_d", 250),    # K_sat (mm/day)
        "SMLIM": props.get("theta_wp", 0.12),    # initial moisture (start at WP)
        "RDMSOL": 100,  # rooting depth (cm) — default 1m
        "NOTINF": 0,     # no infiltration limit
        "IFUNRN": 0,     # no runoff
        "SSMAX": 0,      # no surface storage
        "SSI": 0,         # no surface storage initial
        "SOPE": 0.02,    # slope (2%)
    }


def _build_crop_file(crop_params: dict) -> str:
    """Build a temporary PCSE CABO crop file from parameters."""
    import tempfile
    import os

    fd, path = tempfile.mkstemp(suffix=".crop", text=True)
    with os.fdopen(fd, "w") as f:
        f.write(f"TSUM1 = {crop_params.get('TSUM1', 1100)}\n")
        f.write(f"TSUM2 = {crop_params.get('TSUM2', 900)}\n")
        f.write(f"TDWI = {crop_params.get('TDWI', 210)}\n")
        f.write(f"LAIEM = {crop_params.get('LAIEM', 0.15)}\n")
        f.write(f"RGRLAI = {crop_params.get('RGRLAI', 0.008)}\n")
        f.write(f"SPAN = {crop_params.get('SPAN', 35)}\n")
        f.write(f"TBASE = {crop_params.get('TBASE', 0)}\n")
        f.write(f"TEFFMX = {crop_params.get('TEFFMX', 30)}\n")
        # AMAXTB pairs
        amax = crop_params.get("AMAXTB", [(0, 35), (1, 40), (2, 5)])
        for dvs, val in amax:
            f.write(f"AMAXTB = {dvs},{val}\n")
        # SLATB pairs
        slat = crop_params.get("SLATB", [(0, 0.0027), (1, 0.0020), (2, 0.0015)])
        for dvs, val in slat:
            f.write(f"SLATB = {dvs},{val}\n")
    return path
