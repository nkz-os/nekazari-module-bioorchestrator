"""Capability Registry HTTP endpoints — /catalog, /parcel/{id}, /attribute/{type}/{name}."""
from __future__ import annotations

import os
from collections import defaultdict
from typing import Annotated, Any

import httpx
from fastapi import APIRouter, Depends, HTTPException
from neo4j import AsyncDriver

from app.core.dependencies import get_neo4j_driver
from app.graph.capability_dao import CapabilityDao

router = APIRouter()


def _jsonify(obj: Any) -> Any:
    """Recursively convert Neo4j temporal types to ISO strings for JSON serialization."""
    if hasattr(obj, "iso_format"):
        return obj.iso_format()
    if isinstance(obj, dict):
        return {k: _jsonify(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_jsonify(i) for i in obj]
    return obj


def get_capability_dao(driver: Annotated[AsyncDriver, Depends(get_neo4j_driver)]) -> CapabilityDao:
    """Build a CapabilityDao around the shared async driver. Driver lifecycle is app-managed."""
    return CapabilityDao(driver)


DaoDep = Annotated[CapabilityDao, Depends(get_capability_dao)]


@router.get("/catalog")
async def get_catalog(dao: DaoDep) -> dict[str, list[dict]]:
    """All capabilities installed in the tenant, grouped by entityType."""
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in await dao.list_catalog():
        grouped[row["entityType"]].append(row)
    return dict(grouped)


@router.get("/attribute/{entity_type}/{attribute_name}")
async def get_attribute_detail(entity_type: str, attribute_name: str, dao: DaoDep) -> dict:
    """Full metadata for a single capability."""
    detail = await dao.get_attribute_detail(entity_type, attribute_name)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"{entity_type}/{attribute_name} not in registry")
    return _jsonify(detail)


@router.get("/parcel/{parcel_id}")
async def get_parcel_capabilities(parcel_id: str, dao: DaoDep) -> dict:
    """For a specific parcel: declared capabilities + current entities fetched from Orion-LD."""
    catalog = await dao.list_catalog()
    entity_types = sorted({c["entityType"] for c in catalog})

    orion = os.environ.get("ORION_BASE_URL", "http://orion-ld-service:1026")
    context = os.environ.get("CONTEXT_URL", "http://api-gateway-service:5000/ngsi-ld-context.json")

    entities_by_type: dict[str, list[dict]] = {}
    async with httpx.AsyncClient(timeout=10) as c:
        for et in entity_types:
            r = await c.get(
                f"{orion}/ngsi-ld/v1/entities",
                params={
                    "type": et,
                    "q": f'refAgriParcel=="urn:ngsi-ld:AgriParcel:{parcel_id}"',
                },
                headers={
                    "Accept": "application/json",
                    "Link": (
                        f'<{context}>; rel="http://www.w3.org/ns/json-ld#context";'
                        ' type="application/ld+json"'
                    ),
                },
            )
            if r.status_code == 200:
                entities_by_type[et] = r.json()

    return {
        "parcelId": parcel_id,
        "capabilities": catalog,
        "currentEntities": entities_by_type,
    }
