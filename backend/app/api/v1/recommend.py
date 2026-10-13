"""Crop recommendation endpoints (conditions, evidence, parcel)."""

from __future__ import annotations

from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.api.v1.attribution import (
    attach_attributions,
    rows_source_ids,
)
from app.api.v1.graph import DriverDep
from app.graph import zone_match
from app.graph.dao import GraphDAO
from app.graph.evidence_sites import evidence_sites

router = APIRouter()

_TENANT_PARAMS = ("parcel_id", "tenant_id")
_EPPO_PATTERN = r"^[A-Za-z0-9]{2,6}$"
_KOPPEN_PATTERN = r"^[A-Z][A-Za-z]{0,2}$"
_COUNTRY_PATTERN = r"^[A-Z]{2}$"
_MAX_TEXT = 64

Irrigation = Literal["secano", "regadío"]
Management = Literal["any", "conventional", "organic"]
Season = Literal["all", "autumn", "spring", "summer"]
Similarity = Literal["koppen", "vector_v2_fallback"]
Purpose = Literal["main", "forage"]
Tier = Literal["field", "regional"]

def _reject_tenant_params(request: Request) -> None:
    """Conditions endpoints are tenant-free; a parcel/tenant selector is a client mistake."""
    for name in _TENANT_PARAMS:
        if name in request.query_params:
            raise HTTPException(
                status_code=422,
                detail=f"{name} is not accepted here",
            )


class _Conditions:
    """Shared query parameters of the conditions and evidence endpoints."""

    def __init__(
        self,
        request: Request,
        climate_class: str = Query(..., pattern=_KOPPEN_PATTERN, max_length=_MAX_TEXT),
        soil_type: str | None = Query(None, max_length=_MAX_TEXT),
        soil_ph: float | None = Query(None, ge=3, le=10),
        soil_texture: str | None = Query(None, max_length=_MAX_TEXT),
        irrigation_regime: Irrigation | None = None,
        management: Management = "any",
        season: Season = "all",
        purpose: Purpose = "main",
        annual_rainfall_mm: float | None = Query(None, ge=0, le=5000),
        annual_et0_mm: float | None = Query(None, ge=0, le=3000),
        coldest_month_min_c: float | None = Query(None, ge=-60, le=40),
        annual_temp_c: float | None = Query(None, ge=-30, le=40),
        frost_margin_c: float | None = Query(None, ge=0, le=15),
    ):
        _reject_tenant_params(request)
        self.climate_class = climate_class
        self.soil_type = soil_type
        self.soil_ph = soil_ph
        self.soil_texture = soil_texture
        self.irrigation_regime = irrigation_regime
        self.management = management
        self.season = season
        self.purpose = purpose
        self.annual_rainfall_mm = annual_rainfall_mm
        self.annual_et0_mm = annual_et0_mm
        self.coldest_month_min_c = coldest_month_min_c
        self.annual_temp_c = annual_temp_c
        self.frost_margin_c = frost_margin_c

    def as_dict(self) -> dict:
        return dict(vars(self))


CondDep = Annotated[_Conditions, Depends()]


@router.get(
    "/agriculture/recommend/evidence",
    description=(
        "Trials behind a recommendation. `similarity` must match the recommendation's "
        "`trust.similarity`: `vector_v2_fallback` needs all four numeric climate inputs. "
        "`purpose` must match the request; `tier` the recommendation's `evidence.tier` "
        "(`regional` lists the aggregate-site trials and needs `similarity=koppen`). "
        "`country` (ISO 3166 alpha-2, optional) must match the recommendation's: it scopes the "
        "regional aggregate sites; without it only climate-matched aggregates are listed. "
        "`zone` (optional, with country ES) is the recommendation's `evidence.zone_match.zone_id`: the list is "
        "the parcel's own GENVCE zone when it has numeric trials of the crop, else the country level. "
        "Distinct trials only; each item names its tier."
    ),
)
async def recommend_evidence(
    driver: DriverDep,
    cond: CondDep,
    crop: str = Query(..., pattern=_EPPO_PATTERN),
    variety: str | None = Query(None, max_length=_MAX_TEXT),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=50),
    similarity: Similarity = "koppen",
    tier: Tier = "field",
    country: str | None = Query(
        None, pattern=_COUNTRY_PATTERN,
        description="ISO 3166 alpha-2; scopes the aggregate sites of tier=regional",
    ),
    zone: str | None = Query(
        None, pattern=zone_match.ZONE_ID_PATTERN, max_length=4000,
        description="opaque zone id of the recommendation's `evidence.zone_match.zone_id` (no coordinates)",
    ),
):
    from app.graph import agroclimatic
    from app.graph.dao import _irrigation_uri

    dao = GraphDAO(driver)
    if tier == "regional" and similarity != "koppen":
        raise HTTPException(status_code=422, detail="tier=regional needs similarity=koppen")
    if similarity == "vector_v2_fallback":
        rain, et0 = cond.annual_rainfall_mm, cond.annual_et0_mm
        cold, temp = cond.coldest_month_min_c, cond.annual_temp_c
        if agroclimatic.feature_vector_v2(rain, et0, cold, temp) is None:
            raise HTTPException(
                status_code=422,
                detail="similarity=vector_v2_fallback needs annual_rainfall_mm, annual_et0_mm (> 0), "
                "coldest_month_min_c and annual_temp_c",
            )
    sites = await evidence_sites(dao, cond.as_dict(), similarity=similarity, tier=tier, country=country)
    evidence = await dao.list_trial_evidence(
        crop=crop.strip().upper(),
        similar_sites=[s["name"] for s in sites],
        variety=variety,
        irrigation_uri=_irrigation_uri(cond.irrigation_regime),
        management=cond.management,
        page=page,
        page_size=page_size,
        purpose=cond.purpose,
        tier=tier,
        zone_ctx=(zone_match.context_from_zone_id(zone, cond.irrigation_regime)
                  if tier == "regional" and zone and zone_match.is_zone_country(country) else None),
    )
    return attach_attributions(evidence, rows_source_ids(evidence.get("items")))

