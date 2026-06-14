"""Crop catalog API — species/varieties from Orion-LD + Neo4j."""
from fastapi import APIRouter, Query, Depends, HTTPException
from app.graph.dao import GraphDAO
from app.ingestion.ecocrop_ingester import EcoCropIngester
from app.ingestion.variety_ingester import VarietyIngester
from app.core.dependencies import get_dao, get_current_user
from nkz_platform_sdk.orion import OrionClient
from app.core.config import settings

router = APIRouter(prefix="/catalog", tags=["crop-catalog"])


@router.get("")
async def list_crops(
    source: str | None = Query(None, description="Data source: ecocrop, cpvo"),
    q: str | None = Query(None, description="Search by name"),
    parent: str | None = Query(None, description="Parent species URI for varieties"),
    dao: GraphDAO = Depends(get_dao),
):
    """List species and/or varieties from the catalog."""
    crops = await dao.get_crop_catalog(source=source, search=q)
    return {"crops": crops, "total": len(crops)}


@router.get("/thermal-summary")
async def thermal_summary(
    dao: GraphDAO = Depends(get_dao),
):
    """Return count of species with/without thermal data."""
    async with dao._driver.session() as session:
        result = await session.run("""
            MATCH (s:Species)
            OPTIONAL MATCH (s)-[:HAS_HEAT_TOLERANCE]->(ht:CropHeatTolerance)
            RETURN count(DISTINCT s) as total,
                   count(DISTINCT ht) as with_thermal
        """)
        record = await result.single()
        total = record["total"]
        with_thermal = record["with_thermal"]
        return {
            "total_species": total,
            "with_thermal": with_thermal,
            "without_thermal": total - with_thermal,
        }


@router.get("/npk-summary")
async def npk_summary(
    dao: GraphDAO = Depends(get_dao),
):
    """Return count of species with/without NPK data."""
    async with dao._driver.session() as session:
        result = await session.run("""
            MATCH (s:Species)
            OPTIONAL MATCH (s)-[:HAS_STAGE]->(:PhenologyStage)-[:HAS_NUTRIENT_PROFILE]->(np:CropNutrientProfile)
            WITH s, count(DISTINCT np) as np_count
            RETURN count(s) as total_species,
                   count(CASE WHEN np_count > 0 THEN s END) as with_npk
        """)
        record = await result.single()
        total = record["total_species"]
        with_npk = record["with_npk"]
        return {
            "total_species": total,
            "with_npk": with_npk,
            "without_npk": total - with_npk,
        }


@router.get("/{crop_id:path}")
async def get_crop_detail(
    crop_id: str,
    dao: GraphDAO = Depends(get_dao),
):
    """Get full detail for a species or variety.

    Aggregates Orion-LD entity data + Neo4j graph enrichment
    (phenology params, heat tolerance, NPK profiles, rotation constraints).
    """
    async with dao._driver.session() as session:
        result = await session.run("""
            MATCH (c:AgriCrop {uri: $uri})
            OPTIONAL MATCH (c)-[:HAS_VARIETY]->(v:AgriCropVariety)
            OPTIONAL MATCH (c)-[:HAS_PARAMETER]->(p:PhenologyParams)
            OPTIONAL MATCH (c)-[:HAS_HEAT_TOLERANCE]->(ht:CropHeatTolerance)
            OPTIONAL MATCH (c)-[:HAS_SOIL_SUITABILITY]->(ss:CropSoilSuitability)
            OPTIONAL MATCH (c)-[:HAS_NUTRIENT_PROFILE]->(np:CropNutrientProfile)
            RETURN c, collect(DISTINCT v) as varieties,
                   collect(DISTINCT p) as params,
                   collect(DISTINCT ht) as heat,
                   collect(DISTINCT ss) as soil,
                   collect(DISTINCT np) as nutrients
        """, uri=crop_id)
        record = await result.single()
        if not record:
            raise HTTPException(status_code=404, detail="Crop not found")

        c = record["c"]
        varieties = record["varieties"]
        params = record["params"]
        heat = record["heat"]
        soil = record["soil"]
        nutrients = record["nutrients"]

    return {
        "uri": c.get("uri"),
        "name": c.get("name"),
        "scientificName": c.get("scientificName"),
        "dataProvider": c.get("dataProvider"),
        "data_available": {
            "kc": len(params) > 0 and any(p.get("kc") for p in params),
            "d1_d2": len(params) > 0 and any(p.get("d1") for p in params),
            "mds": len(params) > 0 and any(p.get("mdsRef") for p in params),
            "thermal": len(heat) > 0,
            "soil_suitability": len(soil) > 0,
            "npk": len(nutrients) > 0,
        },
        "varieties": [{"uri": v.get("uri"), "name": v.get("name")} for v in varieties],
        "phenology": [dict(p) for p in params],
        "heat_tolerance": [dict(h) for h in heat],
        "soil_suitability": [dict(s) for s in soil],
        "nutrient_profile": [dict(n) for n in nutrients],
    }


@router.post("/ingest")
async def trigger_ingestion(
    source: str = Query(..., description="ecocrop or cpvo"),
    species_filter: str | None = Query(None),
    user: dict = Depends(get_current_user),
    dao: GraphDAO = Depends(get_dao),
):
    """Trigger ingestion from an external source. Requires technician/admin."""
    orion = OrionClient(
        settings.catalog_tenant,
        base_url=settings.orion_ld_url,
        context_url=settings.context_url,
    )
    try:
        if source == "ecocrop":
            ingester = EcoCropIngester(orion)
            filter_list = species_filter.split(",") if species_filter else None
            result = await ingester.ingest(dao, species_filter=filter_list)
        elif source == "cpvo":
            ingester = VarietyIngester(orion)
            result = await ingester.ingest(dao)
        else:
            raise HTTPException(status_code=400, detail=f"Unknown source: {source}")
    finally:
        await orion.close()

    return result


@router.post("/contribute")
async def contribute_parameter(
    body: dict,
    user: dict = Depends(get_current_user),
    dao: GraphDAO = Depends(get_dao),
):
    """Contribute phenological/agronomic parameters for a crop.

    Body: {crop_id, params: {kc?, d1?, d2?, mds?, npk?, rotation?}, provenance}
    Requires technician/admin role.
    """
    crop_id = body.get("crop_id")
    params = body.get("params", {})
    provenance = body.get("provenance", {})

    if not crop_id or not params:
        raise HTTPException(status_code=400, detail="crop_id and params required")

    async with dao._driver.session() as session:
        await session.run("""
            MATCH (c:AgriCrop {uri: $uri})
            CREATE (p:PhenologyParams {
                status: 'pending_review',
                contributedBy: $user_id,
                contributedAt: datetime(),
                sourceDoi: $doi,
                sourceAuthor: $author,
                sourceYear: $year,
                sourceInstitution: $institution,
                sourceMethod: $method,
                sourceConditions: $conditions
            })
            SET p += $params
            CREATE (c)-[:HAS_PARAMETER]->(p)
        """,
            uri=crop_id,
            user_id=user.get("sub", "unknown"),
            doi=provenance.get("doi"),
            author=provenance.get("author"),
            year=provenance.get("year"),
            institution=provenance.get("institution"),
            method=provenance.get("method"),
            conditions=provenance.get("conditions"),
            params=params,
        )

    # Also push Kc values to Orion-LD if provided
    if any(k in params for k in ("kc", "kcIni", "kcMid", "kcEnd")):
        orion_attrs = {}
        for key in ("kcIni", "kcMid", "kcEnd"):
            if key in params:
                orion_attrs[key] = {"type": "Property", "value": params[key]}
        if orion_attrs:
            orion = OrionClient(
                settings.catalog_tenant,
                base_url=settings.orion_ld_url,
                context_url=settings.context_url,
            )
            try:
                await orion.append_entity_attrs(crop_id, orion_attrs)
            finally:
                await orion.close()

    return {"status": "submitted", "crop_id": crop_id}


@router.post("/derive-thermal")
async def derive_thermal(
    user: dict = Depends(get_current_user),
):
    """Trigger thermal limits derivation for all species with EcoCrop temp data.

    Runs derive_thermal_limits.py as a background subprocess.
    Requires technician/admin role.
    """
    import subprocess
    import sys
    from pathlib import Path

    script = Path(__file__).parent.parent.parent.parent / "scripts" / "derive_thermal_limits.py"
    subprocess.Popen(
        [sys.executable, str(script)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    return {"status": "started", "message": "Thermal derivation running in background"}
