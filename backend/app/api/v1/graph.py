"""Graph API endpoints — health, stats, and phenology params."""

from __future__ import annotations

from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from nkz_platform_sdk.agronomy import (
    AgronomicValue,
    Source,
    confidence_from_match,
)
from nkz_platform_sdk.orion import OrionClient

from app.api.v1.attribution import attach_attributions, rows_source_ids
from app.common.source_registry import all_attributions
from app.core.dependencies import (
    get_neo4j_driver,
    require_contributor,
    require_platform_admin,
)
from app.graph.dao import GraphDAO
from app.services.sim_errors import SimulationError
from app.services.sim_jobs import SimJobs, shared_sim_jobs
from app.species_registry import get_species_info, resolve_species
from neo4j import AsyncDriver

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


def _get_tenant_id(request: Request) -> str:
    """Tenant of the verified caller, as set by NKZAuthMiddleware.

    Never read from headers, query params or the parcel URN: those come from the
    caller and would let one tenant read another's parcels on this direct-ingress
    service. Tenant-scoped requests are rejected by the middleware before reaching
    here when no identity was verified.
    """
    return getattr(request.state, "tenant_id", "") or ""


def _require_tenant_id(request: Request) -> str:
    """Verified tenant for a parcel-scoped handler; 401 when there is none."""
    tenant_id = _get_tenant_id(request)
    if not tenant_id:
        raise HTTPException(status_code=401, detail="No verified tenant")
    return tenant_id


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
                    "If omitted (and no gdd), returns the default parameter set of the species' "
                    "mid-season stage (FAO-56 reference), else of the first stage in the order "
                    "initial, development, late-season; the choice never depends on storage order.",
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


@router.post("/action-rules", dependencies=[Depends(require_platform_admin)])
async def create_action_rule_route(driver: DriverDep, request: Request):
    body = await request.json()
    if not body.get("id") or not body.get("category"):
        raise HTTPException(status_code=400, detail="id and category are required")
    return await GraphDAO(driver).create_action_rule(body)


@router.put("/action-rules/{rule_id}", dependencies=[Depends(require_platform_admin)])
async def update_action_rule_route(rule_id: str, driver: DriverDep, request: Request):
    return await GraphDAO(driver).update_action_rule(rule_id, await request.json())


@router.post("/phenology-params/contribute")
async def contribute_phenology_params(
    driver: DriverDep,
    user: Annotated[dict, Depends(require_contributor)],
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
    Requires TechnicalConsultant, TenantAdmin or PlatformAdmin; the verified
    subject and tenant are recorded on the node (contact_email is free text).
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
        contributed_by=user["sub"],
        contributor_tenant=user.get("tenant_id"),
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
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)}


@router.get("/varieties")
async def variety_catalogue(
    species: str = Query(..., description="Crop species scientific name"),
):
    """Return varieties for a species from Orion-LD via hasSubCrop."""
    from app.core.config import settings
    from app.ingestion.uri import agri_crop_uri

    parent_uri = agri_crop_uri(species)
    orion = OrionClient(
        settings.catalog_tenant,
        base_url=settings.orion_ld_url,
        context_url=settings.context_url,
    )
    try:
        parent = await orion.get_entity(parent_uri)
    except Exception:  # noqa: BLE001
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
    except Exception as e:  # noqa: BLE001
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
    except Exception as e:  # noqa: BLE001
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
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)}


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
    management: Literal["any", "conventional", "organic"] = Query(
        default="any",
        description="Production system: organic trials are never pooled with conventional ones. "
                    "'organic' lists only organic trials; 'any' and 'conventional' leave them out.",
    ),
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
        management=management,
    )
    return attach_attributions({
        "trials": trials,
        "total": len(trials),
        "filters_applied": {
            k: v for k, v in {
                "crop": crop,
                "climate_class": climate_class,
                "soil_type": soil_type,
                "soil_texture": soil_texture,
                "irrigation_regime": irrigation_regime,
                "management": management if management == "organic" else None,
                "min_yield_kg_ha": min_yield_kg_ha,
                "rainfall_range_mm": f"{min_rainfall_mm}-{max_rainfall_mm}" if min_rainfall_mm or max_rainfall_mm else None,
            }.items() if v is not None
        },
    }, rows_source_ids(trials))


@router.get("/agriculture/trial-sites")
async def agriculture_trial_sites(
    driver: DriverDep,
):
    """Return all TrialSites with trial count summaries."""
    dao = GraphDAO(driver)
    sites = await dao.get_trial_sites_summary()
    return attach_attributions(
        {"sites": sites, "total": len(sites)},
        (sid for s in sites for sid in s.get("source_ids") or []),
    )


@router.get("/agriculture/crops")
async def agriculture_crops(
    driver: DriverDep,
):
    """Return distinct crops available in VarietyTrial data with counts."""
    dao = GraphDAO(driver)
    crops = await dao.get_available_crops()
    return attach_attributions(
        {"crops": crops, "total": len(crops)},
        (sid for c in crops for sid in c.get("source_ids") or []),
    )


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
    stats = await dao.graph_quality_stats()
    return attach_attributions(stats, stats.get("source_ids") or [])


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
    tenant_id = _require_tenant_id(request)
    dao = GraphDAO(driver)
    result = await dao.get_parcel_environment(
        parcel_id=parcel_id,
        tenant_id=tenant_id,
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
    tenant_id = _require_tenant_id(request)
    dao = GraphDAO(driver)
    result = await dao.get_crop_context(
        parcel_id=parcel_id,
        tenant_id=tenant_id,
        gdd=gdd,
    )
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.post("/agriculture/crop-simulation")
async def agriculture_crop_simulation(
    driver: DriverDep,
    request: Request,
    parcel_id: str = Query(..., description="AgriParcel URN"),
    crop_slug: str | None = Query(
        default=None,
        description="Canonical crop slug (e.g. 'wheat', 'maize'). Taken from the parcel if omitted.",
    ),
    sowing_date: str | None = Query(
        default=None,
        description=("Sowing date (YYYY-MM-DD). Today or up to a year ahead runs a preseason ensemble. "
                     "Taken from the crop cycle or the latest completed sowing if omitted."),
    ),
    irrigation: str = Query(
        default="rainfed",
        description="'rainfed' or 'full' (irrigated to field capacity): the scenario of the headline yield.",
    ),
    engine: str = Query(default="aquacrop", description="Simulation engine."),
    sowing_method: str | None = Query(
        default=None,
        description=("'direct' (sown on the previous crop's stubble: simulated with the residue mulch) or "
                     "'conventional'. Taken from the recorded sowing if omitted; bare soil without it."),
    ),
):
    """Simulate the parcel's crop season (yield, water stress, daily canopy).

    Inputs come from real data only: soil from the Soil module, daily parcel
    weather (plus the weather archive for earlier days), crop and sowing from
    the parcel and its operations. A season still in progress, or a sowing date today
    or ahead (status "preseason"), is projected to harvest with a climatological
    ensemble (P10/P50/P90). Input problems answer
    422 and upstream outages 503, both as ``{"detail": {"code", "message"}}``.
    """
    tenant_id = _require_tenant_id(request)
    dao = GraphDAO(driver)
    try:
        return await dao.run_crop_simulation(
            parcel_id=parcel_id,
            tenant_id=tenant_id,
            crop_slug=crop_slug,
            sowing_date_str=sowing_date,
            irrigation=irrigation,
            engine=engine,
            sowing_method=sowing_method,
        )
    except SimulationError as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail()) from e


@router.post("/agriculture/crop-simulation/jobs", status_code=202)
async def agriculture_crop_simulation_job_start(
    driver: DriverDep,
    request: Request,
    parcel_id: str = Query(..., description="AgriParcel URN"),
    crop_slug: str | None = Query(default=None, description="As in crop-simulation."),
    sowing_date: str | None = Query(default=None, description="As in crop-simulation."),
    irrigation: str = Query(default="rainfed", description="As in crop-simulation."),
    engine: str = Query(default="aquacrop", description="Simulation engine."),
    sowing_method: str | None = Query(default=None, description="As in crop-simulation."),
):
    """Start (or join) an asynchronous crop simulation; poll it by ``job_id``.

    Same inputs as ``crop-simulation``. An identical request on the same day returns
    the same job, finished jobs answering at once. Body: ``{job_id, state, done,
    total, result, error}`` with state queued | running | done | failed. A tenant with
    too many unfinished jobs gets 429 ``too_many_jobs``.
    """
    tenant_id = _require_tenant_id(request)
    dao = GraphDAO(driver)
    from app.services.crop_simulation import (
        check_sowing_method,  # lazy, as the DAO imports it
    )

    try:
        check_sowing_method(sowing_method)
    except SimulationError as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail()) from e
    params = {"parcel_id": parcel_id, "crop_slug": crop_slug, "sowing_date": sowing_date,
              "irrigation": irrigation, "engine": engine}
    if sowing_method is not None:  # absent keeps the identity of requests made before the parameter
        params["sowing_method"] = sowing_method

    async def run(progress):
        return await dao.run_crop_simulation(
            parcel_id=parcel_id, tenant_id=tenant_id, crop_slug=crop_slug,
            sowing_date_str=sowing_date, irrigation=irrigation, engine=engine, progress=progress,
            sowing_method=sowing_method)

    try:
        return SimJobs.view(await shared_sim_jobs().start(tenant_id, params, run))
    except SimulationError as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail()) from e


@router.get("/agriculture/crop-simulation/jobs/{job_id}")
async def agriculture_crop_simulation_job_status(request: Request, job_id: str):
    """State and progress of a simulation job; ``result`` is the crop-simulation answer."""
    tenant_id = _require_tenant_id(request)
    job = shared_sim_jobs().get(job_id, tenant_id)
    if job is None:
        raise HTTPException(status_code=404, detail={"code": "job_not_found",
                                                     "message": "No such simulation job."})
    return SimJobs.view(job)


# ═══════════════════════════════════════════════════════════════════════════════
# F4: Crop-Health Integration — Endpoints
# ═══════════════════════════════════════════════════════════════════════════════


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
        tenant_id=_require_tenant_id(request),
    )


@router.get("/agriculture/crop-plan")
async def agriculture_get_crop_plan(
    driver: DriverDep, request: Request,
    parcel_id: str = Query(..., description="AgriParcel URN"),
    season: str | None = Query(None, description="Campaign id, e.g. 2026. Omit for all seasons."),
):
    """Read the committed plan (ordered segments + status) for a parcel/season."""
    dao = GraphDAO(driver)
    return await dao.get_crop_plan(
        parcel_id=parcel_id, season=season,
        tenant_id=_require_tenant_id(request),
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
        tenant_id=_require_tenant_id(request),
    )


@router.get("/agriculture/organic-inputs")
async def agriculture_organic_inputs(
    request: Request,
    driver: DriverDep,
    crop: str = Query(..., description="EPPO code (e.g. 'TRZAX')"),
):
    """Return authorized organic inputs for a crop's pests (FiBL)."""
    dao = GraphDAO(driver)
    return await dao.get_organic_inputs(eppo=crop)


@router.get("/agriculture/sources/attributions")
async def agriculture_source_attributions():
    """Mandatory attribution of every source that requires one (public, read-only)."""
    attributions = all_attributions()
    return {"attributions": attributions, "total": len(attributions)}


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
