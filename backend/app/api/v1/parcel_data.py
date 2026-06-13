"""Per-parcel data aggregation endpoints (vegetation, soil)."""
from datetime import datetime as dt

from fastapi import APIRouter, HTTPException, Query, Request
from nkz_platform_sdk.orion import OrionClient

from app.services.timescale import compute_trend, query_vegetation_timeseries

router = APIRouter(prefix="/parcel", tags=["parcel-data"])

VALID_INDICES = ("ndvi", "evi", "savi", "gndvi", "ndre", "ndwi")
VALID_PERIODS = ("1m", "3m", "6m", "1y", "season")


def _veg_unavailable(index: str, period: str) -> dict:
    return {
        "available": False,
        "index": index,
        "period": period,
        "observations": [],
        "current": None,
        "trend": None,
        "count": 0,
        "message": "El modulo vegetation-health no ha procesado esta parcela todavia.",
    }


@router.get("/{parcel_id}/vegetation")
async def parcel_vegetation(
    parcel_id: str,
    request: Request,
    index: str = Query("ndvi"),
    period: str = Query("3m"),
):
    """Get vegetation index trend for a parcel from TimescaleDB."""
    if index not in VALID_INDICES:
        raise HTTPException(status_code=400, detail=f"Invalid index: {index}. Valid: {VALID_INDICES}")
    if period not in VALID_PERIODS:
        raise HTTPException(status_code=400, detail=f"Invalid period: {period}. Valid: {VALID_PERIODS}")

    tenant_id = getattr(request.state, "tenant_id", "") or request.headers.get("X-Tenant-ID", "")
    orion = OrionClient(tenant_id)
    parcel_urn = f"urn:ngsi-ld:AgriParcel:{parcel_id}"

    try:
        try:
            entities = await orion.query_entities(type="VegetationIndex", q=f'hasAgriParcel=="{parcel_urn}"', limit=1)
        except Exception:
            return _veg_unavailable(index, period)

        if not entities:
            return _veg_unavailable(index, period)

        entity_id = entities[0].get("id", "")

        since = None
        if period == "season":
            try:
                seasons = await orion.query_entities(
                    type="AgriCropSeason", q=f'hasAgriParcel=="{parcel_urn}"', limit=1
                )
                if seasons:
                    start_raw = seasons[0].get("startDate", {})
                    if isinstance(start_raw, dict):
                        start_val = start_raw.get("value")
                        if start_val:
                            since = dt.fromisoformat(start_val.replace("Z", "+00:00"))
            except Exception:
                since = None

        attr_name = f"{index}Mean"
        observations = query_vegetation_timeseries(entity_id, attr_name, period, since)

        current_value = observations[-1]["value"] if observations else None
        trend = compute_trend(observations)

        return {
            "available": True,
            "index": index,
            "period": period,
            "observations": observations,
            "current": current_value,
            "trend": trend,
            "count": len(observations),
            "source": "Sentinel-2 L2A (ESA Copernicus)",
            "processor": "vegetation-health v2.0.0",
        }
    finally:
        await orion.close()


@router.get("/{parcel_id}/soil")
async def parcel_soil(parcel_id: str, request: Request):
    """Get soil horizons for a parcel from Orion-LD (AgriSoilExtended)."""
    tenant_id = getattr(request.state, "tenant_id", "") or request.headers.get("X-Tenant-ID", "")
    orion = OrionClient(tenant_id)
    parcel_urn = f"urn:ngsi-ld:AgriParcel:{parcel_id}"

    try:
        try:
            entities = await orion.query_entities(type="AgriSoilExtended", q=f'hasAgriParcel=="{parcel_urn}"', limit=1)
        except Exception:
            return {"available": False, "message": "El modulo soil no ha procesado esta parcela."}

        if not entities:
            return {
                "available": False,
                "message": "El modulo soil no ha procesado esta parcela. Los datos de suelo requieren definir horizontes y seleccionar proveedores de datos.",
            }

        entity = entities[0]

        horizons_raw = entity.get("horizons", {})
        horizons = horizons_raw.get("value", []) if isinstance(horizons_raw, dict) else []

        hydro_group = entity.get("hydrologicGroup", {})
        if isinstance(hydro_group, dict):
            hydro_group = hydro_group.get("value")

        return {
            "available": True,
            "entityId": entity.get("id"),
            "horizons": horizons,
            "hydrologicGroup": hydro_group,
            "source": "SoilGrids 2.0 + LUCAS 2018",
        }
    finally:
        await orion.close()
