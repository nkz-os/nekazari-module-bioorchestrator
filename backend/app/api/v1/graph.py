"""Graph API endpoints — health, stats, and phenology params."""

from __future__ import annotations

import os
from typing import Annotated

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from neo4j import AsyncDriver

from app.core.dependencies import get_neo4j_driver
from app.graph.dao import GraphDAO

router = APIRouter()

DriverDep = Annotated[AsyncDriver, Depends(get_neo4j_driver)]


def _get_tenant_id(request: Request) -> str:
    """Extract tenant_id from auth middleware state."""
    return getattr(request.state, "tenant_id", "")


@router.get("/health")
async def graph_health(driver: DriverDep):
    """Check Neo4j connectivity."""
    dao = GraphDAO(driver)
    return await dao.health_check()


@router.get("/stats")
async def graph_stats(driver: DriverDep, tenant_id: str = Depends(_get_tenant_id)):
    """Return node and relationship counts for the knowledge graph."""
    dao = GraphDAO(driver)
    return await dao.get_stats(tenant_id=tenant_id or None)


@router.get("/species")
async def list_species(driver: DriverDep):
    """List all species with variety counts and data availability."""
    dao = GraphDAO(driver)
    crops = await dao.get_crop_catalog()
    return {"species": crops, "total": len(crops)}


@router.get("/phenology-params")
async def phenology_params(
    driver: DriverDep,
    species: str = Query(
        default="olive",
        description="Species name, scientific name, or AGROVOC URI (e.g. 'olive', 'Olea europaea')",
    ),
    stage: str | None = Query(
        default=None,
        description="Phenological stage (e.g. 'vegetative', 'pit_hardening', 'veraison'). "
                    "If omitted, returns the default parameter set for the species.",
    ),
    cultivar: str | None = Query(
        default=None,
        description="Cultivar/variety for context-aware matching (e.g. 'Picual', 'Tempranillo')",
    ),
    management: str | None = Query(
        default=None,
        description="Irrigation management: deficit_irrigation, regulated_deficit_irrigation, "
                    "or null for full irrigation / rainfed",
    ),
    lat: float | None = Query(
        default=None, ge=-90, le=90,
        description="Latitude for geographic context matching",
    ),
    lon: float | None = Query(
        default=None, ge=-180, le=180,
        description="Longitude for geographic context matching",
    ),
    gdd: float | None = Query(
        default=None, ge=0,
        description="Growing Degree Days accumulated since season start. "
                    "If provided and stage is not given, auto-detects phenological stage.",
    ),
    tenant_id: str = Depends(_get_tenant_id),
):
    """Return phenology parameters with scientific provenance.

    Context-aware cascade matching:
      1. Exact: species + stage + cultivar + management
      2. Management-only: species + stage + management
      3. Generic: species + stage (best available default)
      4. Species-only: any stage default

    Returns:
        - Core values: kc, d1, d2, mds_ref with confidence intervals
        - Scientific provenance: DOI, author, year, institution, method, conditions
        - Stage detection: base temperature, GDD thresholds
        - Alternatives: other published values for comparison
        - Match level: how closely the result matched the requested context

    404 if species is not in the knowledge graph.
    """
    dao = GraphDAO(driver)
    params = await dao.get_phenology_params(
        species=species,
        stage=stage,
        cultivar=cultivar,
        management=management,
        lat=lat,
        lon=lon,
        gdd=gdd,
        tenant_id=tenant_id or None,
    )

    if params is None:
        detail = f"No phenology data for species={species}"
        if stage:
            detail += f" stage={stage}"
        raise HTTPException(status_code=404, detail=detail)

    params["dataAvailable"] = {
        "kc": params.get("kc") is not None,
        "d1": params.get("d1") is not None,
        "d2": params.get("d2") is not None,
        "mds": params.get("mds_ref") is not None,
    }

    return params


@router.post("/phenology-params/contribute")
async def contribute_phenology_params(
    driver: DriverDep,
    species: str = Query(..., description="Species name"),
    stage: str = Query(..., description="Phenological stage"),
    kc: float = Query(..., description="Crop coefficient"),
    d1: float | None = Query(None, description="NWSB baseline"),
    d2: float | None = Query(None, description="Max stress baseline"),
    mds_ref: float | None = Query(None, description="Reference MDS (µm)"),
    cultivar: str | None = Query(None, description="Cultivar/variety"),
    management: str | None = Query(None, description="Irrigation management"),
    doi: str | None = Query(None, description="DOI of the publication"),
    author: str | None = Query(None, description="Author name(s)"),
    conditions: str | None = Query(None, description="Experimental conditions"),
    contact_email: str | None = Query(None, description="Contact email for review"),
):
    """Submit a contributed phenology parameter for scientific review.

    Creates a PhenologyParams node with status='pending_review'.
    An administrator can later approve and merge into the main parameter set.
    """
    dao = GraphDAO(driver)
    result = await dao.contribute_phenology(
        species=species,
        stage=stage,
        kc=kc,
        d1=d1,
        d2=d2,
        mds_ref=mds_ref,
        cultivar=cultivar,
        management=management,
        doi=doi,
        author=author,
        conditions=conditions,
        contact_email=contact_email,
    )
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result.get("detail", "Unknown error"))
    return result


# ── Heat Tolerance ────────────────────────────────────────────────────────────


@router.get("/heat-tolerance")
async def heat_tolerance(
    driver: DriverDep,
    species: str = Query(..., description="Species name"),
):
    """Return heat/frost damage thresholds for a species."""
    dao = GraphDAO(driver)
    data = await dao.get_heat_tolerance(species)
    if data is None:
        raise HTTPException(status_code=404, detail=f"No heat tolerance data for {species}")
    return data


# ── Nutrient Profile ──────────────────────────────────────────────────────────


@router.get("/nutrient-profile")
async def nutrient_profile(
    driver: DriverDep,
    species: str = Query(..., description="Species name"),
    stage: str | None = Query(None, description="Phenological stage filter"),
):
    """Return NPK uptake curve per phenological stage."""
    dao = GraphDAO(driver)
    data = await dao.get_nutrient_profile(species, stage)
    if data is None:
        raise HTTPException(status_code=404, detail=f"No nutrient data for {species}")
    return data


# ── Soil Suitability ──────────────────────────────────────────────────────────


@router.get("/soil-suitability")
async def soil_suitability(
    driver: DriverDep,
    species: str = Query(..., description="Species name"),
):
    """Return soil requirements (pH, texture, drainage, depth, salinity)."""
    dao = GraphDAO(driver)
    data = await dao.get_soil_suitability(species)
    if data is None:
        raise HTTPException(status_code=404, detail=f"No soil data for {species}")
    return data


# ── Rotation Constraints ──────────────────────────────────────────────────────


@router.get("/rotation-constraints")
async def rotation_constraints(
    driver: DriverDep,
    crop: str = Query(..., description="Crop name to check rotation for"),
):
    """Return rotation constraints for a crop."""
    dao = GraphDAO(driver)
    return await dao.get_rotation_constraints(crop)


# ── Recommendations ───────────────────────────────────────────────────────────


@router.get("/recommendations/next-crop")
async def recommend_next_crop(
    driver: DriverDep,
    previous_crop: str = Query(..., description="Previous crop grown on the parcel"),
):
    """Suggest next crop based on rotation rules, excluding constrained crops."""
    dao = GraphDAO(driver)
    crops = await dao.recommend_next_crop(previous_crop)
    return {"previous_crop": previous_crop, "suggested_crops": crops}


# ── Protected Area Check (Natura 2000 proxy) ─────────────────────────────────


@router.get("/protected-area-check")
async def protected_area_check(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
):
    """Check if a point is within a Natura 2000 protected area."""
    try:
        from ikerketa.connectors.natura2000 import Natura2000Connector
        connector = Natura2000Connector()
        result = connector.fetch(lat=lat, lon=lon)
        if result.entities:
            return result.entities[0] if len(result.entities) == 1 else result.entities
        return {"in_protected_area": False}
    except ImportError:
        return {"error": "Natura 2000 connector not available"}
    except Exception as e:
        return {"error": str(e)}


@router.get("/varieties")
async def variety_catalogue(
    species: str = Query(..., description="Crop species scientific name"),
):
    """Return varieties for a species from Orion-LD via hasSubCrop."""
    from app.ingestion.uri import agri_crop_uri
    from app.ingestion.orion import OrionIngestionClient

    orion = OrionIngestionClient()
    parent_uri = agri_crop_uri(species)

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                f"{orion.base}/ngsi-ld/v1/entities/{parent_uri}",
                headers={"Accept": "application/ld+json"},
            )
            resp.raise_for_status()
            parent = resp.json()
    except Exception:
        return {"varieties": [], "error": "Parent species not found in catalog"}

    sub_crop = parent.get("hasSubCrop", {})
    variety_uris = []
    if isinstance(sub_crop, dict) and sub_crop.get("type") == "Relationship":
        uris = sub_crop.get("object", [])
        if isinstance(uris, str):
            variety_uris = [uris]
        else:
            variety_uris = uris

    return {"varieties": [{"uri": u} for u in variety_uris]}


@router.get("/pesticides")
async def pesticide_validation(crop: str = Query(..., description="Crop name")):
    """Return authorized active substances for a crop from EU Pesticides DB."""
    try:
        from ikerketa.connectors.eu_pesticides import EUPesticidesConnector
        connector = EUPesticidesConnector()
        result = connector.fetch()
        substances = [s for s in result.entities if crop.lower() in (s.get("crop", "")).lower()]
        return {"substances": substances[:20]}
    except ImportError:
        return {"substances": []}
    except Exception as e:
        return {"substances": [], "error": str(e)}


@router.get("/pollinators")
async def pollinator_occurrences(lat: float = Query(...), lon: float = Query(...)):
    """Return pollinator species near a location from GBIF."""
    try:
        from ikerketa.connectors.gbif_pollinators import GBIFPollinatorsConnector
        connector = GBIFPollinatorsConnector()
        result = connector.fetch(lat=lat, lon=lon)
        species = list({e["species"]: e for e in result.entities}.values())
        return {"pollinators": species[:10]}
    except ImportError:
        return {"pollinators": []}
    except Exception as e:
        return {"pollinators": [], "error": str(e)}


@router.get("/terrain")
async def terrain_data(lat: float = Query(...), lon: float = Query(...)):
    """Fetch elevation and slope from Copernicus DEM for a point."""
    try:
        from ikerketa.connectors.copernicus_dem import CopernicusDEMConnector
        connector = CopernicusDEMConnector()
        result = connector.fetch(lat=lat, lon=lon)
        return result.entities[0] if result.entities else {"error": "No DEM data"}
    except ImportError:
        return {"error": "Copernicus DEM connector not available", "elevation_m": None}
    except Exception as e:
        return {"error": str(e)}


@router.get("/climate-reference")
async def climate_reference(lat: float = Query(...), lon: float = Query(...)):
    """Fetch ERA5 climate reanalysis reference for a point."""
    try:
        from ikerketa.connectors.era5_climate import ERA5ClimateConnector
        connector = ERA5ClimateConnector()
        result = connector.fetch(lat=lat, lon=lon)
        return result.entities[0] if result.entities else {"error": "No climate data"}
    except ImportError:
        return {"error": "ERA5 connector not available"}
    except Exception as e:
        return {"error": str(e)}


@router.get("/recommendations/simulate")
async def simulate_scenario(
    driver: DriverDep,
    baseline_crop: str = Query(..., description="Current crop"),
    scenario_crop: str = Query(..., description="Proposed alternative crop"),
):
    """Compare two agronomic scenarios: rotation, soil, fertilizer delta."""
    dao = GraphDAO(driver)
    result = await dao.simulate_scenario(baseline_crop, scenario_crop)
    result["baseline_data_gaps"] = await _compute_data_gaps(dao, baseline_crop)
    result["scenario_data_gaps"] = await _compute_data_gaps(dao, scenario_crop)
    return result


async def _compute_data_gaps(dao: GraphDAO, crop_name: str) -> list[str]:
    """Return list of missing data types for a crop."""
    gaps = []
    async with dao._driver.session() as session:
        result = await session.run("""
            MATCH (c:AgriCrop)
            WHERE toLower(c.name) CONTAINS toLower($name)
               OR toLower(c.scientificName) CONTAINS toLower($name)
            OPTIONAL MATCH (c)-[:HAS_PARAMETER]->(p:PhenologyParams)
            OPTIONAL MATCH (c)-[:HAS_HEAT_TOLERANCE]->(ht:CropHeatTolerance)
            OPTIONAL MATCH (c)-[:HAS_NUTRIENT_PROFILE]->(np:CropNutrientProfile)
            RETURN count(DISTINCT p) > 0 as has_phenology,
                   count(DISTINCT ht) > 0 as has_thermal,
                   count(DISTINCT np) > 0 as has_npk
        """, name=crop_name)
        record = await result.single()
        if record:
            if not record["has_phenology"]:
                gaps.extend(["d1", "d2", "mds"])
            if not record["has_thermal"]:
                gaps.append("thermal")
            if not record["has_npk"]:
                gaps.append("npk")
    return gaps


@router.get("/recommendations/fertilizer")
async def recommend_fertilizer(
    driver: DriverDep,
    species: str = Query(..., description="Crop species"),
    stage: str = Query("vegetative", description="Phenological stage"),
    soil_n: float = Query(0, description="Soil nitrogen level (kg/ha)"),
    soil_p: float = Query(0, description="Soil phosphorus level (kg/ha)"),
    soil_k: float = Query(0, description="Soil potassium level (kg/ha)"),
):
    """Return NPK fertilizer recommendations based on crop demand and soil levels."""
    dao = GraphDAO(driver)
    data = await dao.recommend_fertilizer(species, stage, soil_n, soil_p, soil_k)
    if data is None:
        raise HTTPException(status_code=404, detail=f"No nutrient data for {species}/{stage}")
    return data


# ═══════════════════════════════════════════════════════════════════════════════
# Agriculture Domain — Variety Trials & Extrapolation
# ═══════════════════════════════════════════════════════════════════════════════


@router.get("/agriculture/variety-trials")
async def agriculture_variety_trials(
    driver: DriverDep,
    crop: str | None = Query(
        default=None,
        description="Crop filter: EPPO code (e.g. 'TRZAX'), scientific name, or common name",
    ),
    climate_class: str | None = Query(
        default=None,
        description="Köppen climate class filter (e.g. 'BSk', 'Cfb')",
    ),
    soil_type: str | None = Query(
        default=None,
        description="WRB soil type filter (e.g. 'Calcisol', 'Fluvisol')",
    ),
    soil_texture: str | None = Query(
        default=None,
        description="Soil texture filter (e.g. 'franco-arcilloso')",
    ),
    irrigation_regime: str | None = Query(
        default=None,
        description="Irrigation regime filter (e.g. 'secano', 'regadío')",
    ),
    min_yield_kg_ha: float | None = Query(
        default=None,
        description="Minimum yield threshold (kg/ha)",
    ),
    min_rainfall_mm: float | None = Query(
        default=None,
        description="Minimum annual rainfall (mm)",
    ),
    max_rainfall_mm: float | None = Query(
        default=None,
        description="Maximum annual rainfall (mm)",
    ),
    limit: int = Query(default=50, le=200, description="Max results"),
):
    """Ranked variety trial results with environmental context.

    Returns varieties sorted by yield (descending) with their TrialSite
    environmental metadata (climate, soil, rainfall, elevation, frost days).

    Example queries:
      - /agriculture/variety-trials?crop=TRZAX&climate_class=BSk
        → Best wheat varieties in semi-arid climates
      - /agriculture/variety-trials?crop=LYPES&irrigation_regime=secano&min_yield_kg_ha=50000
        → Rainfed tomato varieties exceeding 50 t/ha
    """
    dao = GraphDAO(driver)
    trials = await dao.get_variety_trials(
        crop=crop,
        climate_class=climate_class,
        soil_type=soil_type,
        soil_texture=soil_texture,
        irrigation_regime=irrigation_regime,
        min_yield_kg_ha=min_yield_kg_ha,
        min_rainfall_mm=min_rainfall_mm,
        max_rainfall_mm=max_rainfall_mm,
        limit=limit,
    )
    return {
        "trials": trials,
        "total": len(trials),
        "filters_applied": {
            k: v for k, v in {
                "crop": crop,
                "climate_class": climate_class,
                "soil_type": soil_type,
                "soil_texture": soil_texture,
                "irrigation_regime": irrigation_regime,
                "min_yield_kg_ha": min_yield_kg_ha,
                "rainfall_range_mm": f"{min_rainfall_mm}-{max_rainfall_mm}" if min_rainfall_mm or max_rainfall_mm else None,
            }.items() if v is not None
        },
    }


@router.get("/agriculture/similar-sites")
async def agriculture_similar_sites(
    driver: DriverDep,
    reference_site: str | None = Query(
        default=None,
        description="Name of a known TrialSite to use as environmental reference (e.g. 'Cadreita')",
    ),
    climate_class: str | None = Query(
        default=None,
        description="Köppen climate class (used if no reference_site given)",
    ),
    soil_type: str | None = Query(
        default=None,
        description="WRB soil type (used if no reference_site given)",
    ),
    rainfall_min: float | None = Query(
        default=None,
        description="Minimum annual rainfall in mm",
    ),
    rainfall_max: float | None = Query(
        default=None,
        description="Maximum annual rainfall in mm",
    ),
    limit: int = Query(default=20, le=50, description="Max results"),
):
    """Find TrialSites with similar environmental conditions.

    If reference_site is provided, its climate/soil/rainfall are used as filters.
    Otherwise, explicit filters must be given.

    This is the building block for environmental extrapolation:
      "Which trial locations have similar conditions to my farm?"
    """
    dao = GraphDAO(driver)
    sites = await dao.get_similar_sites(
        reference_site=reference_site,
        climate_class=climate_class,
        soil_type=soil_type,
        rainfall_min=rainfall_min,
        rainfall_max=rainfall_max,
        limit=limit,
    )
    return {
        "sites": sites,
        "total": len(sites),
        "reference": reference_site or {
            "climate_class": climate_class,
            "soil_type": soil_type,
            "rainfall_range_mm": f"{rainfall_min}-{rainfall_max}",
        },
    }


@router.get("/agriculture/extrapolate")
async def agriculture_extrapolate(
    driver: DriverDep,
    crop: str = Query(
        ...,
        description="Crop to extrapolate: EPPO code (e.g. 'TRZAX'), scientific name, or common name",
    ),
    reference_site: str | None = Query(
        default=None,
        description="TrialSite name to use as target environment (e.g. 'Cadreita'). "
                    "Cannot be combined with lat/lon.",
    ),
    lat: float | None = Query(
        default=None, ge=-90, le=90,
        description="Latitude of target location (WGS84). Auto-resolves climate/soil via ERA5 + SoilGrids.",
    ),
    lon: float | None = Query(
        default=None, ge=-180, le=180,
        description="Longitude of target location (WGS84). Must be paired with lat.",
    ),
    climate_class: str | None = Query(
        default=None,
        description="Köppen climate class of the target environment (manual override)",
    ),
    soil_type: str | None = Query(
        default=None,
        description="WRB soil type of the target environment (manual override)",
    ),
    irrigation_regime: str | None = Query(
        default=None,
        description="Irrigation regime: 'secano' or 'regadío'",
    ),
    rainfall_min: float | None = Query(
        default=None,
        description="Minimum annual rainfall of the target environment (mm)",
    ),
    rainfall_max: float | None = Query(
        default=None,
        description="Maximum annual rainfall of the target environment (mm)",
    ),
    top_n: int = Query(default=10, le=30, description="Number of top varieties to return"),
    filter_soil_suitability: bool = Query(
        default=False,
        description="Filter out varieties incompatible with target soil (pH, texture)",
    ),
):
    """Extrapolate best crop varieties for a target environment.

    Three ways to specify the target environment:

    1. **By known TrialSite name** (fastest):
       ?crop=TRZAX&reference_site=Cadreita
       → Uses pre-enriched Cadreita data (BSk, Fluvisol calcáreo, ~400mm)

    2. **By GPS coordinates** (dynamic — works anywhere on Earth):
       ?crop=TRZAX&lat=38.88&lon=-6.97
       → Resolves climate (ERA5), soil (SoilGrids), elevation (Copernicus DEM)
         and photoperiod (astronomical) on-the-fly, then finds similar TrialSites

    3. **By explicit environmental filters** (manual):
       ?crop=TRZAX&climate_class=BSk&soil_type=Calcisol&rainfall_min=300&rainfall_max=500

    Returns:
      - target_environment: resolved environmental profile
      - similar_sites: TrialSites with matching conditions
      - ranked_varieties: best varieties by mean yield with stats
    """
    dao = GraphDAO(driver)

    # ── Dynamic geolocation path ────────────────────────────────────────
    if lat is not None and lon is not None:
        if reference_site:
            raise HTTPException(
                status_code=400,
                detail="Cannot specify both lat/lon and reference_site. Choose one.",
            )

        from app.services.environment import resolve_environment
        env = await resolve_environment(lat, lon, use_ikers=True)

        # Convert resolved environment to filters for the DAO
        climate_class = env.get("climate_class")
        soil_type = env.get("soil_type")

        rainfall = env.get("annual_rainfall_mm")
        if rainfall is not None:
            rainfall_min = rainfall_min or (rainfall - 200)
            rainfall_max = rainfall_max or (rainfall + 200)

        # Store resolved env for response
        resolved_env = env
    elif reference_site:
        resolved_env = None  # DAO will look it up
    else:
        resolved_env = None  # Explicit filters only

    result = await dao.extrapolate_varieties(
        crop=crop,
        reference_site=reference_site,
        climate_class=climate_class,
        soil_type=soil_type,
        irrigation_regime=irrigation_regime,
        rainfall_min=rainfall_min,
        rainfall_max=rainfall_max,
        top_n=top_n,
        filter_soil_suitability=filter_soil_suitability,
    )

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    # Inject resolved environment if dynamic path was used
    if resolved_env:
        result["resolved_environment"] = resolved_env

    return result


@router.get("/agriculture/trial-sites")
async def agriculture_trial_sites(
    driver: DriverDep,
):
    """Return all TrialSites with trial count summaries."""
    dao = GraphDAO(driver)
    sites = await dao.get_trial_sites_summary()
    return {"sites": sites, "total": len(sites)}


@router.get("/agriculture/crops")
async def agriculture_crops(
    driver: DriverDep,
):
    """Return distinct crops available in VarietyTrial data with counts."""
    dao = GraphDAO(driver)
    crops = await dao.get_available_crops()
    return {"crops": crops, "total": len(crops)}


@router.get("/agriculture/regenerative-sequence")
async def agriculture_regenerative_sequence(
    driver: DriverDep,
    climate_class: str = Query(
        ...,
        description="Köppen climate class (e.g. 'Csa', 'BSk', 'Cfb')",
    ),
    target_protein: str = Query(
        default="VICFX",
        description="EPPO code of target protein crop: VICFX (faba), PIBAR (pea), CIEAR (chickpea), LENCU (lentil), GLXMA (soy)",
    ),
    soil_type: str | None = Query(
        default=None,
        description="WRB soil type (e.g. 'Calcisol', 'Luvisol')",
    ),
    management: str = Query(
        default="any",
        description="Management context: 'organic', 'conventional', or 'any'",
    ),
    parcel_id: str | None = Query(
        default=None,
        description="Optional AgriParcel URN for real soil AWC data from Soil module",
    ),
):
    """Plan a regenerative cover-crop-to-protein-crop sequence.

    Given a climate zone and target protein crop, returns the best cover crop
    for roller-crimper termination with expected biomass, nitrogen dynamics,
    GDD timeline, estimated dates, water balance risk, and protein crop
    variety ranking from European trial data.

    Uses European agronomic data from INTIA Navarra, JRC MARS Bulletins,
    and Legumes Translated (H2020).

    Calculation methodology follows FAO-56 (Allen et al. 1998) for water
    balance, Clark (2007) for cover crop N dynamics, and Peoples et al.
    (2021) for legume N fixation rates. Full audit trail in response.

    Example:
      /agriculture/regenerative-sequence?climate_class=Csa&target_protein=VICFX
      /agriculture/regenerative-sequence?climate_class=Csa&target_protein=VICFX&parcel_id=urn:ngsi-ld:AgriParcel:123
      /agriculture/regenerative-sequence?climate_class=BSk&target_protein=CIEAR&management=organic

    Management modes:
      - 'organic': Only organic + low_input params. If variety data is conventional,
        applies 20% yield reduction estimate (Seufert et al. 2012).
      - 'conventional': Conventional + integrated params.
      - 'any': All data sources merged (default).
    """
    if management not in ("organic", "conventional", "any"):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid management mode: '{management}'. Use 'organic', 'conventional', or 'any'.",
        )

    dao = GraphDAO(driver)
    result = await dao.get_regenerative_sequence(
        climate_class=climate_class,
        target_protein=target_protein,
        soil_type=soil_type,
        management=management,
        parcel_id=parcel_id,
    )

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return result


@router.get("/agriculture/crop-context")
async def agriculture_crop_context(
    driver: DriverDep,
    request: Request,
    parcel_id: str = Query(
        ...,
        description="AgriParcel URN (e.g. 'urn:ngsi-ld:AgriParcel:parcela-42')",
    ),
    gdd: float | None = Query(
        default=None, ge=0,
        description="Growing Degree Days accumulated for stage auto-detection",
    ),
):
    """Return full calibrated agronomic context for a parcel."""
    tenant_id = getattr(request.state, "tenant_id", "")
    dao = GraphDAO(driver)
    result = await dao.get_crop_context(
        parcel_id=parcel_id,
        tenant_id=tenant_id,
        gdd=gdd,
    )
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.get("/agriculture/yield-potential")
async def agriculture_yield_potential(
    driver: DriverDep,
    request: Request,
    variety: str = Query(..., description="Variety name (e.g. 'LG_AURUS')"),
    crop: str = Query(..., description="EPPO code or species (e.g. 'TRZAX')"),
    climate_class: str | None = Query(default=None, description="Köppen climate class"),
    soil_type: str | None = Query(default=None, description="WRB soil type"),
    parcel_id: str | None = Query(default=None, description="Optional parcel URN for yield gap"),
):
    """Compute expected yield and yield gap for a variety."""
    tenant_id = getattr(request.state, "tenant_id", "")
    dao = GraphDAO(driver)
    result = await dao.get_yield_potential(
        variety=variety,
        crop=crop,
        climate_class=climate_class,
        soil_type=soil_type,
        parcel_id=parcel_id,
        tenant_id=tenant_id,
    )
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# F4: Crop-Health Integration — Endpoints
# ═══════════════════════════════════════════════════════════════════════════════


@router.post("/agriculture/assign-crop")
async def agriculture_assign_crop(
    driver: DriverDep,
    request: Request,
):
    """Assign a crop variety to a parcel. Writes to Orion-LD.

    Request body (JSON):
    {
      "parcel_id": "urn:ngsi-ld:AgriParcel:parcela-42",
      "variety_uri": "urn:ngsi-ld:AgriCropVariety:LG_AURUS",
      "crop_uri": "urn:ngsi-ld:AgriCrop:TRZAX",
      "management": "organic",
      "season_start": "2026-10-15",
      "season_end": "2027-06-30"
    }

    To clear assignment, send only parcel_id with no other fields.
    """
    body = await request.json()
    parcel_id = body.get("parcel_id")

    if not parcel_id:
        raise HTTPException(status_code=400, detail="parcel_id is required")

    # Clear assignment if no crop specified
    if not body.get("crop_uri"):
        dao = GraphDAO(driver)
        result = await dao.clear_crop_assignment(
            parcel_id=parcel_id,
            tenant_id=getattr(request.state, "tenant_id", ""),
        )
        return result

    # Validate required fields
    required = ["variety_uri", "crop_uri", "management", "season_start", "season_end"]
    missing = [f for f in required if not body.get(f)]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required fields: {', '.join(missing)}",
        )

    management = body["management"]
    if management not in ("organic", "conventional"):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid management: '{management}'. Use 'organic' or 'conventional'.",
        )

    dao = GraphDAO(driver)
    result = await dao.assign_crop_to_parcel(
        parcel_id=parcel_id,
        crop_uri=body["crop_uri"],
        variety_uri=body["variety_uri"],
        management=management,
        season_start=body["season_start"],
        season_end=body["season_end"],
        tenant_id=getattr(request.state, "tenant_id", ""),
    )
    return result
