"""Crop recommendation endpoints (conditions, evidence, parcel)."""

from __future__ import annotations

import re
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.api.v1.graph import DriverDep, _require_tenant_id
from app.graph.dao import GraphDAO

router = APIRouter()

_TENANT_PARAMS = ("parcel_id", "tenant_id")
_CLIMATE_KEYS = ("annual_rainfall_mm", "annual_et0_mm", "coldest_month_min_c", "annual_temp_c")
_MAX_CROPS = 4
_EPPO_RE = re.compile(r"^[A-Z0-9]{2,6}$")
_EPPO_PATTERN = r"^[A-Za-z0-9]{2,6}$"
_KOPPEN_PATTERN = r"^[A-Z][A-Za-z]{0,2}$"
_MAX_TEXT = 64

Irrigation = Literal["secano", "regadío"]
Management = Literal["any", "conventional", "organic"]
Season = Literal["all", "autumn", "spring", "summer"]
Similarity = Literal["koppen", "vector_v2_fallback"]

_MANAGEMENT_DOC = (
    "management: only `organic` changes the computation (yields scaled by a 0.8 factor, "
    "recorded in `assumptions`); `any` and `conventional` use the trial data as-is."
)


def _reject_tenant_params(request: Request) -> None:
    """Conditions endpoints are tenant-free; a parcel/tenant selector is a client mistake."""
    for name in _TENANT_PARAMS:
        if name in request.query_params:
            raise HTTPException(
                status_code=422,
                detail=f"{name} is not accepted here; use /api/graph/recommend/parcel/{{id}}",
            )


def _parse_crops(raw: str | None) -> list[str] | None:
    if raw is None or not raw.strip():
        return None
    out: list[str] = []
    for part in raw.split(","):
        code = part.strip().upper()
        if not code:
            continue
        if not _EPPO_RE.match(code):
            raise HTTPException(status_code=422, detail=f"invalid crop code: {code[:16]}")
        if code not in out:
            out.append(code)
            if len(out) > _MAX_CROPS:
                raise HTTPException(status_code=422, detail=f"at most {_MAX_CROPS} crops")
    return out or None


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
        self.annual_rainfall_mm = annual_rainfall_mm
        self.annual_et0_mm = annual_et0_mm
        self.coldest_month_min_c = coldest_month_min_c
        self.annual_temp_c = annual_temp_c
        self.frost_margin_c = frost_margin_c

    def as_dict(self) -> dict:
        return dict(vars(self))


CondDep = Annotated[_Conditions, Depends()]


@router.get("/agriculture/recommend", description=_MANAGEMENT_DOC)
async def recommend_for_conditions(
    driver: DriverDep,
    cond: CondDep,
    crops: str | None = None,
    top_n: int = Query(10, ge=1, le=30),
):
    conditions = cond.as_dict()
    conditions["crops"] = _parse_crops(crops)
    conditions["top_n"] = top_n
    return await GraphDAO(driver).recommend_for_conditions(conditions)


@router.get(
    "/agriculture/recommend/evidence",
    description=(
        "Trials behind a recommendation. `similarity` must match the recommendation's "
        "`trust.similarity`: `vector_v2_fallback` needs all four numeric climate inputs."
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
):
    from app.graph import agroclimatic
    from app.graph.dao import _irrigation_uri

    dao = GraphDAO(driver)
    if similarity == "vector_v2_fallback":
        rain, et0 = cond.annual_rainfall_mm, cond.annual_et0_mm
        cold, temp = cond.coldest_month_min_c, cond.annual_temp_c
        if agroclimatic.feature_vector_v2(rain, et0, cold, temp) is None:
            raise HTTPException(
                status_code=422,
                detail="similarity=vector_v2_fallback needs annual_rainfall_mm, annual_et0_mm (> 0), "
                "coldest_month_min_c and annual_temp_c",
            )
        # Same site lookup as the v2 fallback of recommend_for_conditions.
        sites = await dao.get_similar_sites(
            climate_class=cond.climate_class, soil_type=cond.soil_type,
            rainfall_min=None, rainfall_max=None, limit=50,
            target_features={"rainfall": rain, "et0": et0, "coldest_min": cold, "annual_temp": temp},
            vector_version="v2",
        )
    else:
        sites = await dao.get_similar_sites(
            climate_class=cond.climate_class, soil_type=cond.soil_type, limit=50
        )
    return await dao.list_trial_evidence(
        crop=crop.strip().upper(),
        similar_sites=[s["name"] for s in sites],
        variety=variety,
        irrigation_uri=_irrigation_uri(cond.irrigation_regime),
        page=page,
        page_size=page_size,
    )


@router.get("/recommend/parcel/{parcel_id:path}", description=_MANAGEMENT_DOC)
async def recommend_for_parcel(
    request: Request,
    parcel_id: str,
    driver: DriverDep,
    climate_class: str | None = Query(None, pattern=_KOPPEN_PATTERN, max_length=_MAX_TEXT),
    soil_type: str | None = Query(None, max_length=_MAX_TEXT),
    irrigation_regime: Irrigation | None = None,
    management: Management = "any",
    season: Season = "all",
    crops: str | None = None,
    top_n: int = Query(10, ge=1, le=30),
    frost_margin_c: float | None = Query(None, ge=0, le=15),
):
    tenant = _require_tenant_id(request)
    dao = GraphDAO(driver)
    env = await dao.get_parcel_environment(parcel_id, tenant)
    if "error" in env:
        raise HTTPException(status_code=404, detail=env["error"])

    overrides = {
        "climate_class": climate_class,
        "soil_type": soil_type,
        "irrigation_regime": irrigation_regime,
    }
    used = [k for k, v in overrides.items() if v]
    inputs_used = dict(env.get("inputs_used") or {})
    inputs_used["user_override"] = used
    detail = env.get("climate_detail") or {}
    if climate_class and any(detail.get(k) is not None for k in _CLIMATE_KEYS):
        # Overridden class but the parcel's own numbers are still sent: show the mix.
        inputs_used["climate_numbers_from_parcel"] = True
    env = {**env, "inputs_used": inputs_used}

    resolved_climate = climate_class or env.get("climate_class")
    if not resolved_climate:
        return {"status": "needs_climate", "parcel_environment": env}

    soil = env.get("soil") or {}
    has_soil = bool(soil.get("data_available"))
    conditions = {
        "climate_class": resolved_climate,
        "soil_type": soil_type or (soil.get("wrb_type") if has_soil else None),
        "soil_ph": soil.get("ph") if has_soil else None,
        "soil_texture": soil.get("texture") if has_soil else None,
        "irrigation_regime": irrigation_regime
        or (env.get("irrigation") or {}).get("inferred"),
        "management": management,
        "season": season,
        "crops": _parse_crops(crops),
        "top_n": top_n,
        "frost_margin_c": frost_margin_c,
    }
    for key in _CLIMATE_KEYS:
        conditions[key] = detail.get(key)
    result = await dao.recommend_for_conditions(conditions)
    return {**result, "parcel_environment": env}
