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
