"""Graph API endpoints — health, stats, and phenology params."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from neo4j import AsyncDriver

from nkz_platform_sdk.agronomy import (
    AgronomicValue, Source, confidence_from_match,
)
from nkz_platform_sdk.orion import OrionClient

from app.core.dependencies import get_neo4j_driver
from app.graph.dao import GraphDAO
from app.species_registry import get_species_info, resolve_species

router = APIRouter()

DriverDep = Annotated[AsyncDriver, Depends(get_neo4j_driver)]

_PHENO_FIELDS = ("kc", "d1", "d2", "mds_ref", "ky")


def _phenology_agronomic(params: dict) -> dict:
    """Build the additive `agronomic` map for phenology-params (P1 contract)."""
    is_default = bool(params.get("is_default"))
    match_level = params.get("match_level") or "none"
    prov = params.get("provenance") or {}
    source = Source(
        short=prov.get("short") or "default",
        doi=prov.get("doi"),
        institution=prov.get("institution"),
    )
    base_conf = confidence_from_match(match_level, is_default)
    out: dict[str, dict] = {}
    for field in _PHENO_FIELDS:
        value = params.get(field)
        if value is None:
            out[field] = AgronomicValue(
                value=None, source=Source(short="default"),
                confidence="low", notes=[f"Sin {field} para esta especie/estadio"],
            ).model_dump()
        else:
            out[field] = AgronomicValue(
                value=value, source=source, confidence=base_conf,
            ).model_dump()
    return out


_WATER_FIELDS = ("irrigation_required_mm", "etc_weekly_mm", "kc")
_WATER_BOILERPLATE = "All data sources available"


def _water_budget_agronomic(result: dict) -> dict:
    """Build the additive `agronomic` map for water-budget (P1 contract).

    Reuses the dao's aggregate confidence (already weakest-link over AWC/ET0/
    Kc inputs). The human reason rides in `notes` — it never blocks the
    irrigation suggestion.
    """
    conf = result.get("confidence") or "low"
    raw_note = result.get("confidence_notes") or ""
    notes = (
        [] if not raw_note or raw_note == _WATER_BOILERPLATE
        else [n.strip() for n in raw_note.split(";") if n.strip()]
    )
    source = Source(short="FAO-56 water balance")
    out: dict[str, dict] = {}
    for field in _WATER_FIELDS:
        out[field] = AgronomicValue(
            value=result.get(field), source=source,
            confidence=conf, notes=list(notes),
        ).model_dump()
    return out


def _get_tenant_id(request: Request) -> str:
    """Resolve tenant_id for this request.

    The `/agriculture/` prefix is auth-exempt (SKIP_AUTH_PREFIXES), so the auth
    middleware never sets `request.state.tenant_id`. Fall back to the
    `X-Tenant-ID` header (injected by the api-gateway) so parcel-scoped queries
    hit the parcel's tenant instead of the default/catalog tenant.
    """
    return getattr(request.state, "tenant_id", "") or request.headers.get("X-Tenant-ID", "")


@router.get("/health")
async def graph_health(driver: DriverDep):
    """Check Neo4j connectivity."""
    dao = GraphDAO(driver)
    return await dao.health_check()


@router.get("/stats")
async def graph_stats(driver: DriverDep):
    """Return node and relationship counts for the knowledge graph."""
    dao = GraphDAO(driver)
    return await dao.get_stats()


@router.get("/species")
async def list_species(driver: DriverDep):
    """List all species with variety counts and data availability."""
    dao = GraphDAO(driver)
    crops = await dao.get_all_species()
    return crops


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

    params["agronomic"] = _phenology_agronomic(params)
    return params


@router.get("/phenology-stages")
async def phenology_stages(
    driver: DriverDep,
    species: str = Query(
        ...,
        description="Species name or scientific name (e.g. 'olive', 'Olea europaea')",
    ),
):
    """Full ordered phenology stage table for a species (for crop-health).

    Returns all PhenologyStage nodes for the species ordered ascending by
    gddMin. Returns an empty `stages` list (not 404) when the species has
    no stage nodes — the caller falls back to its own default table.
    """
    dao = GraphDAO(driver)
    stages = await dao.get_phenology_stages(species)
    return {"species": species, "stages": stages}


@router.get("/action-rules")
async def list_action_rules(driver: DriverDep,
                            species: str | None = Query(None),
                            stage: str | None = Query(None),
                            role: str | None = Query(None)):
    return await GraphDAO(driver).get_action_rules(species=species, stage=stage, role=role)


@router.get("/action-rules/{rule_id}")
async def get_action_rule_route(rule_id: str, driver: DriverDep):
    rule = await GraphDAO(driver).get_action_rule(rule_id)
    if rule is None:
        raise HTTPException(status_code=404, detail="Rule not found")
    return rule


@router.post("/action-rules")
async def create_action_rule_route(driver: DriverDep, request: Request):
    body = await request.json()
    if not body.get("id") or not body.get("category"):
        raise HTTPException(status_code=400, detail="id and category are required")
    return await GraphDAO(driver).create_action_rule(body)


@router.put("/action-rules/{rule_id}")
async def update_action_rule_route(rule_id: str, driver: DriverDep, request: Request):
    return await GraphDAO(driver).update_action_rule(rule_id, await request.json())


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
    from app.core.config import settings

    parent_uri = agri_crop_uri(species)
    orion = OrionClient(
        settings.catalog_tenant,
        base_url=settings.orion_ld_url,
        context_url=settings.context_url,
    )
    try:
        parent = await orion.get_entity(parent_uri)
    except Exception:
        return {"varieties": [], "error": "Parent species not found in catalog"}
    finally:
        await orion.close()

    sub_crop = parent.get("hasSubCrop", {})
    variety_uris = []
    if isinstance(sub_crop, dict) and sub_crop.get("type") == "Relationship":
        uris = sub_crop.get("object", [])
        if isinstance(uris, str):
            variety_uris = [uris]
        else:
            variety_uris = uris

    return {"varieties": [{"uri": u} for u in variety_uris]}


@router.get("/agriculture/crop-name")
async def crop_name(
    eppo: str = Query(..., description="EPPO crop code, e.g. TRZAX"),
    lang: str = Query("es", description="Language for the common name"),
):
    """Resolve an EPPO crop code to its common name (single source: species_registry)."""
    slug = resolve_species(eppo.strip().upper())
    if not slug:
        raise HTTPException(status_code=404, detail=f"Unknown crop: {eppo}")
    info = get_species_info(slug) or {}
    name = (info.get("common_names") or {}).get(lang)
    if not name:
        raise HTTPException(status_code=404, detail=f"No '{lang}' name for {slug}")
    return {"eppo": eppo.strip().upper(), "slug": slug, "name": name}


@router.get("/agriculture/advisories")
async def list_advisories(request: Request, parcel_id: str = Query(...)):
    """CropAdvisory recommendations for a parcel (bioorch-owned, read from the broker)."""
    tenant_id = _get_tenant_id(request)
    client = OrionClient(tenant_id)
    try:
        rows = await client.query_entities(
            type="CropAdvisory",
            q=f'hasAgriParcel=="{parcel_id}"',
            limit=100, options="keyValues",
        )
    finally:
        await client.close()
    return {"parcel_id": parcel_id, "advisories": rows}


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
    parcel_id: str | None = Query(
        default=None,
        description=(
            "AgriParcel URN (e.g. 'urn:ngsi-ld:AgriParcel:XXX') for weather-adjusted "
            "recommendations. When provided, reads weatherStats from Orion-LD and "
            "adjusts variety scores for drought, heat, and frost stress."
        ),
    ),
    tenant_id: str = Query(
        default="",
        description="Tenant namespace for multi-tenancy (default: from auth context).",
    ),
    request: Request = None,
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

    # Resolve tenant: prefer gateway header over query param
    if not tenant_id and request is not None:
        tenant_id = _get_tenant_id(request)

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
        parcel_id=parcel_id,
        tenant_id=tenant_id,
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


@router.get("/agriculture/graph-stats")
async def agriculture_graph_stats(
    driver: DriverDep,
):
    """Quality-metrics snapshot of the trials sub-graph (Task 0.1).

    Public, read-only regression gate: trials, yield coverage, orphans,
    TRIAL_AT ratio, TrialSite dup-name groups, sites without climate,
    trials-per-climate, and distinct source ids. Run before/after any
    hygiene/canonicalization mutation and diff.
    """
    dao = GraphDAO(driver)
    return await dao.graph_quality_stats()


@router.get("/agriculture/backtest-report")
async def agriculture_backtest_report(
    driver: DriverDep,
):
    """Accuracy backtest of the variety advisor (Task C.3) — the falsifiability gate.

    Public, read-only. Leave-one-site-out cross-validation over MEASURED trials
    (`yieldKgHa` present, `yieldDerivationMethod` null): each site is held out and
    its variety ranking predicted from the rest via `extrapolate_varieties`, then
    compared to what was observed there. Reports median absolute yield error,
    top-3 variety rank-overlap, and coverage — overall and per crop / per climate.
    Run before/after any ranking change (C.1/C.2/C.4) to prove no accuracy regression.
    """
    from app.eval.backtest import Backtester

    dao = GraphDAO(driver)
    return await Backtester(dao).run()


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


@router.get("/agriculture/parcel-environment")
async def agriculture_parcel_environment(
    driver: DriverDep,
    request: Request,
    parcel_id: str = Query(
        ...,
        description="AgriParcel URN (e.g. 'urn:ngsi-ld:AgriParcel:parcela-42')",
    ),
):
    """Resolve parcel environmental profile WITHOUT requiring assigned crop.

    Returns climate class, soil, irrigation inference, area, and centroid.
    Used by CropPlanner planning phase — contrast with crop-context which
    requires AgriParcel.hasAgriCrop.
    """
    tenant_id = _get_tenant_id(request)
    dao = GraphDAO(driver)
    result = await dao.get_parcel_environment(
        parcel_id=parcel_id,
        tenant_id=tenant_id,
    )
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.get("/agriculture/suggest-crops")
async def agriculture_suggest_crops(
    driver: DriverDep,
    request: Request,
    parcel_id: str = Query(..., description="AgriParcel URN"),
    season_slot: str = Query("all", description="winter | summer | all"),
    management: str = Query("any", description="organic | conventional | any"),
    irrigation_regime: str | None = Query(None, description="secano | regadío | any"),
    top_n: int = Query(15, le=30, description="Max suggestions to return"),
    seed_price: float = Query(1.0, description="Seed cost €/ha"),
    harvest_price: float = Query(1.0, description="Harvest price"),
    price_unit: str = Query("eur_per_t", description="eur_per_kg | eur_per_t"),
    operation_cost: float = Query(1.0, description="Cost per field operation €"),
):
    """Suggest best crops for a parcel ranked by composite score.

    Orchestrates get_parcel_environment + get_available_crops +
    extrapolate_varieties + economics. No new Neo4j relationship types.
    """
    tenant_id = _get_tenant_id(request)
    dao = GraphDAO(driver)
    result = await dao.suggest_crops_for_parcel(
        parcel_id=parcel_id,
        tenant_id=tenant_id,
        season_slot=season_slot,
        management=management,
        irrigation_regime=irrigation_regime,
        top_n=top_n,
        seed_price=seed_price,
        harvest_price=harvest_price,
        price_unit=price_unit,
        operation_cost=operation_cost,
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
    tenant_id = _get_tenant_id(request)
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
    tenant_id = _get_tenant_id(request)
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


@router.get("/agriculture/water-budget")
async def agriculture_water_budget(
    driver: DriverDep,
    request: Request,
    parcel_id: str = Query(..., description="AgriParcel URN"),
    week_start: str | None = Query(default=None, description="ISO date for week start (default: today)"),
):
    """Calculate weekly irrigation requirement for a parcel."""
    tenant_id = _get_tenant_id(request)
    dao = GraphDAO(driver)
    result = await dao.get_water_budget(
        parcel_id=parcel_id, tenant_id=tenant_id, week_start=week_start,
    )
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    result["agronomic"] = _water_budget_agronomic(result)
    return result


@router.get("/agriculture/yield-projection")
async def agriculture_yield_projection(
    driver: DriverDep,
    request: Request,
    parcel_id: str = Query(..., description="AgriParcel URN"),
    initial_yield_kg_ha: float | None = Query(
        default=None,
        description="Override initial yield estimate (kg/ha). If omitted, derived from variety trials.",
    ),
):
    """Project current-season yield with FAO-33 water stress correction.

    Combines the initial yield estimate from variety trials (Phase A)
    with accumulated water stress per growth stage using the FAO-33
    methodology: Y = Y_potential × Π(1 - Ky × (1 - ETa/ETc)).

    Returns the projected yield, cumulative stress factor, and per-stage
    breakdown of water stress contributions.
    """
    tenant_id = _get_tenant_id(request)
    dao = GraphDAO(driver)
    result = await dao.get_yield_projection(
        parcel_id=parcel_id, tenant_id=tenant_id,
        initial_yield_kg_ha=initial_yield_kg_ha,
    )
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.post("/agriculture/wofost-simulation")
async def agriculture_wofost_simulation(
    driver: DriverDep,
    request: Request,
    parcel_id: str = Query(..., description="AgriParcel URN"),
    crop_slug: str | None = Query(
        default=None,
        description="Canonical crop slug (e.g. 'wheat', 'maize'). Auto-detected if omitted.",
    ),
    sowing_date: str | None = Query(
        default=None,
        description="Sowing date (YYYY-MM-DD). Auto-detected from field-operations if omitted.",
    ),
):
    """Run WOFOST/PCSE mechanistic simulation for a parcel.

    Fetches all inputs automatically:
      - Weather from timeseries-reader (backed by weather-worker Open-Meteo)
      - Soil hydraulic properties via pedotransfer (Saxton-Rawls) from AgriSoil texture
      - Sowing date from field-operations AgriParcelOperation(sowing)
      - Crop parameters from Neo4j graph (PhenologyParams) with PCSE defaults

    Falls back to FAO-33 simplified simulation if PCSE is not installed.
    Returns daily LAI, biomass, and yield projection.
    """
    tenant_id = _get_tenant_id(request)
    dao = GraphDAO(driver)
    result = await dao.run_wofost_simulation(
        parcel_id=parcel_id,
        tenant_id=tenant_id,
        crop_slug=crop_slug,
        sowing_date_str=sowing_date,
    )
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.get("/agriculture/compare-crops")
async def agriculture_compare_crops(
    driver: DriverDep, request: Request,
    parcel_id: str = Query(..., description="AgriParcel URN"),
    crops: str = Query(..., description="Comma-separated EPPO codes (e.g. TRZAX,HORVX)"),
    seed_price: float = Query(1, description="Seed cost €/ha"),
    harvest_price: float = Query(1, description="Harvest price €/t"),
    operation_cost: float = Query(1, description="Cost per field operation €"),
):
    """Compare multiple crops on a parcel — agronomic, environmental, economic."""
    crop_list = [c.strip() for c in crops.split(",") if c.strip()]
    if not crop_list:
        raise HTTPException(status_code=400, detail="At least one crop required")
    dao = GraphDAO(driver)
    result = await dao.compare_crops(
        parcel_id=parcel_id, crops=crop_list,
        seed_price=seed_price, harvest_price=harvest_price, operation_cost=operation_cost,
        tenant_id=_get_tenant_id(request),
    )
    return result


@router.get("/agriculture/rotation-plan")
async def agriculture_rotation_plan(
    driver: DriverDep, request: Request,
    parcel_id: str = Query(..., description="AgriParcel URN"),
    years: int = Query(4, ge=2, le=6, description="Planning horizon in years"),
    seed_price: float = Query(1, description="Seed cost €/ha"),
    harvest_price: float = Query(1, description="Harvest price €/t"),
    operation_cost: float = Query(1, description="Cost per field operation €"),
    starting_crop: str | None = Query(None, description="EPPO code for year 1 crop"),
    management: str = Query("any", description="organic | conventional | any"),
):
    """Generate multi-year rotation plan with carbon, N, pest, and PAC tracking."""
    dao = GraphDAO(driver)
    result = await dao.rotation_plan(
        parcel_id=parcel_id, years=years,
        seed_price=seed_price, harvest_price=harvest_price, operation_cost=operation_cost,
        tenant_id=_get_tenant_id(request),
        starting_crop=starting_crop,
        management=management,
    )
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.post("/agriculture/rotation-optimize")
async def agriculture_rotation_optimize(
    driver: DriverDep,
    request: Request,
):
    """Priority-driven rotation optimizer with cover crops."""
    body = await request.json()
    tenant_id = _get_tenant_id(request)
    dao = GraphDAO(driver)
    result = await dao.optimize_rotation(
        parcel_id=body.get("parcel_id", ""),
        years=body.get("years", 4),
        constraints=body.get("constraints"),
        priorities=body.get("priorities"),
        locked_years=body.get("locked_years"),
        seed_price=body.get("seed_price", 1.0),
        harvest_price=body.get("harvest_price", 1.0),
        price_unit=body.get("price_unit", "eur_per_t"),
        operation_cost=body.get("operation_cost", 1.0),
        tenant_id=tenant_id,
    )
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
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
            tenant_id=_get_tenant_id(request),
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
        tenant_id=_get_tenant_id(request),
    )
    return result


@router.post("/agriculture/crop-plan")
async def agriculture_commit_crop_plan(driver: DriverDep, request: Request):
    """Commit a multi-segment crop plan (rotation) for a parcel/season."""
    body = await request.json()
    parcel_id = body.get("parcel_id")
    season = body.get("season")
    segments = body.get("segments")
    if not parcel_id or not season or not isinstance(segments, list) or not segments:
        raise HTTPException(status_code=400, detail="parcel_id, season and non-empty segments[] are required")
    dao = GraphDAO(driver)
    return await dao.create_crop_plan(
        parcel_id=parcel_id, season=season, segments=segments,
        tenant_id=_get_tenant_id(request),
    )


@router.get("/agriculture/crop-plan")
async def agriculture_get_crop_plan(
    driver: DriverDep, request: Request,
    parcel_id: str = Query(..., description="AgriParcel URN"),
    season: str = Query(..., description="Campaign id, e.g. 2026"),
):
    """Read the committed plan (ordered segments + status) for a parcel/season."""
    dao = GraphDAO(driver)
    return await dao.get_crop_plan(
        parcel_id=parcel_id, season=season,
        tenant_id=_get_tenant_id(request),
    )


@router.post("/agriculture/crop-plan/{parcel_id:path}/segments/{seq}/advance")
async def agriculture_advance_segment(parcel_id: str, seq: int, driver: DriverDep, request: Request):
    """Manual advance: record the REAL sowing date, activate the segment, demote the prior."""
    body = await request.json()
    planting_date = body.get("planting_date")
    season = body.get("season")
    if not planting_date:
        raise HTTPException(status_code=400, detail="planting_date (real sowing date) is required")
    if not season:
        raise HTTPException(status_code=400, detail="season is required")
    dao = GraphDAO(driver)
    return await dao.advance_segment(
        parcel_id=parcel_id, season=season, seq=seq, planting_date=planting_date,
        tenant_id=_get_tenant_id(request),
    )


@router.get("/agriculture/alerts")
async def agriculture_alerts(
    request: Request,
    driver: DriverDep,
    parcel_id: str = Query(..., description="AgriParcel URN"),
):
    """Return active alerts for a parcel from the crop:events Redis Stream."""
    dao = GraphDAO(driver)
    result = await dao.get_alerts(parcel_id=parcel_id)
    return result


@router.get("/agriculture/organic-inputs")
async def agriculture_organic_inputs(
    request: Request,
    driver: DriverDep,
    crop: str = Query(..., description="EPPO code (e.g. 'TRZAX')"),
):
    """Return authorized organic inputs for a crop's pests (FiBL)."""
    dao = GraphDAO(driver)
    return await dao.get_organic_inputs(eppo=crop)


@router.get("/agriculture/sources")
async def agriculture_sources(request: Request):
    """Return data source health summary."""
    return {
        "total": 0,
        "ready": 0,
        "unavailable": 0,
        "by_domain": {},
        "sources": [],
    }


# ── Reference Data ─────────────────────────────────────────────────────────


@router.get("/reference/climate-classes")
async def reference_climate_classes(
    driver: DriverDep,
):
    """Return unique K\u00f6ppen climate classes available in the knowledge graph."""
    dao = GraphDAO(driver)
    classes = await dao.get_climate_classes()
    return {"climate_classes": classes, "total": len(classes)}


@router.get("/reference/soil-types")
async def reference_soil_types(
    driver: DriverDep,
):
    """Return unique WRB soil types available in the knowledge graph."""
    dao = GraphDAO(driver)
    types = await dao.get_soil_types()
    return {"soil_types": types, "total": len(types)}
