"""Write a parcel's plan for one campaign as planned AgriCrop cycles (C5).

Only planned cycles are written. ``hasAgriCrop`` on the parcel is left to the platform's
reconciler, which sets it when a sowing happens. An annual must end in the campaign; a
herbaceous perennial (alfalfa) must start in it and may end up to MAX_PERENNIAL_YEARS later,
and while it lasts no other campaign can plan over it (nor can it be planned over later ones).
A campaign with a started cycle (anything other than planned or cancelled) is refused: what
happened in the field is changed from field operations, not from the plan. Rewriting a plan
deletes and recreates its planned cycles (same deterministic ids) and deletes the planned
cycles the new plan no longer has: they were intentions that never started.

Every cycle of the parcel is read once and placed in campaigns by its DATES (the same rule the
platform's cycle resolver applies), not only by ``cropSeason``: a cycle written by the legacy
assign-crop flow has no season, and a perennial's season is just the one it was sown in.
"""
from __future__ import annotations

from datetime import date
from itertools import pairwise

import httpx
from nkz_platform_sdk.orion import OrionClient

from app.graph.crop_plan import build_segment_entity
from app.planning.history import campaign_of_start, iso_date, plain
from app.planning.models import PlanIn
from app.planning.service import PARCEL_URN_RE, InvalidParcelId, UnknownCrop, crop_code
from app.species_registry import get_lifecycle, resolve_species

_REWRITABLE = {"planned", "cancelled"}
_CLOSED = {"harvested", "terminated"}
_START_KEYS = ("actualPlantingDate", "plantingDate", "sowingWindowStart")
_END_KEYS = ("actualTerminationDate", "harvestDate", "terminationDate", "expectedTerminationDate")
MAX_PERENNIAL_YEARS = 10
PAGE = 200


class PlanError(Exception):
    def __init__(self, code: str, status: int = 422):
        super().__init__(code)
        self.code = code
        self.status = status


def _lifecycle(crop: str) -> str | None:
    slug = resolve_species(crop)
    return get_lifecycle(slug) if slug else None


def _status(entity: dict) -> str | None:
    return plain(entity.get("status"))


def _first_date(entity: dict, keys: tuple[str, ...]) -> date | None:
    for key in keys:
        found = iso_date(entity.get(key))
        if found:
            return found
    return None


def _span(entity: dict) -> tuple[int, int | None] | None:
    """Campaigns a cycle occupies, as (first, last); ``last`` None = open-ended; None = cannot be placed.

    An annual without an end occupies the campaign it starts in; only a perennial that is not
    closed stays open-ended. An end that falls before the campaign its start opens (a short cycle
    across the August boundary) still occupies that campaign, so the span never comes out empty.
    """
    begin = _first_date(entity, _START_KEYS)
    if begin is None:
        return None
    first = campaign_of_start(begin)
    end = _first_date(entity, _END_KEYS)
    if end is not None:
        return first, max(first, end.year)
    if plain(entity.get("cropLifecycle")) == "perennial" and _status(entity) not in _CLOSED:
        return first, None
    return first, first


def _covers(span: tuple[int, int | None], campaign: int) -> bool:
    first, last = span
    return first <= campaign and (last is None or campaign <= last)


def _intersects(span: tuple[int, int | None], lo: int, hi: int) -> bool:
    first, last = span
    return first <= hi and (last is None or last >= lo)


def validate_plan(plan: PlanIn, today: date) -> None:
    if plan.campaign < today.year:
        raise PlanError("campaign_past")
    spans = []
    for cycle in plan.cycles:
        end = cycle.expected_termination
        if _lifecycle(cycle.crop) == "perennial":
            if campaign_of_start(cycle.sowing_start) != plan.campaign:
                raise PlanError("perennial_start_outside_campaign")
            if end.year < plan.campaign:
                raise PlanError("dates_inconsistent")
            if end.year > plan.campaign + MAX_PERENNIAL_YEARS:
                raise PlanError("perennial_too_long")
        elif end.year != plan.campaign:
            raise PlanError("termination_outside_campaign")
        if cycle.sowing_start > end or (cycle.sowing_end and not cycle.sowing_start <= cycle.sowing_end <= end):
            raise PlanError("dates_inconsistent")
        spans.append((cycle.sowing_start, end))
    spans.sort()
    for (_, end_a), (start_b, _) in pairwise(spans):
        if start_b < end_a:  # same day allowed: roller-crimp the cover and sow the main crop
            raise PlanError("cycles_overlap")


def segments_from_plan(plan: PlanIn) -> list[dict]:
    segments = []
    for cycle in sorted(plan.cycles, key=lambda c: c.sowing_start):
        try:
            crop = crop_code(cycle.crop)
        except UnknownCrop as e:
            raise PlanError("unknown_crop") from e
        segments.append({
            "crop": crop, "role": cycle.role, "variety": cycle.variety,
            "sowing_window": [cycle.sowing_start.isoformat(), (cycle.sowing_end or cycle.sowing_start).isoformat()],
            "expected_termination": cycle.expected_termination.isoformat(),
            "termination_method": cycle.termination_method,
            "irrigation_regime": plan.regime, "management": plan.management,
        })
    return segments


async def _require_parcel(client: OrionClient, parcel_urn: str) -> None:
    try:
        await client.get_entity(parcel_urn, options="keyValues")
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            raise PlanError("parcel_not_found", 404) from e
        raise


async def _parcel_cycles(client: OrionClient, parcel_urn: str) -> list[dict]:
    cycles: list[dict] = []
    while True:
        page = await client.query_entities(
            type="AgriCrop", q=f'hasAgriParcel=="{parcel_urn}"', limit=PAGE, offset=len(cycles), options="keyValues")
        cycles.extend(page)
        if len(page) < PAGE:
            return cycles


def _check_conflicts(cycles: list[dict], plan: PlanIn) -> list[dict]:
    """Refuse a plan that collides with what the parcel already has; return this campaign's cycles."""
    season = str(plan.campaign)
    mine: list[dict] = []
    others: list[tuple[dict, tuple[int, int | None]]] = []
    for cycle in cycles:
        if str(plain(cycle.get("cropSeason"))) == season:
            mine.append(cycle)
        elif _status(cycle) != "cancelled" and (span := _span(cycle)) is not None:
            others.append((cycle, span))
    if any(_status(c) not in _REWRITABLE for c in mine):
        raise PlanError("campaign_started", 409)
    if any(plain(c.get("cropLifecycle")) == "perennial" and _covers(span, plan.campaign) for c, span in others):
        raise PlanError("perennial_occupies", 409)
    if any(_status(c) != "planned" and _covers(span, plan.campaign) for c, span in others):
        raise PlanError("campaign_started", 409)
    ends = [c.expected_termination.year for c in plan.cycles if _lifecycle(c.crop) == "perennial"]
    if ends and any(_intersects(span, plan.campaign + 1, max(ends)) for _, span in others):
        raise PlanError("perennial_overlaps_plans", 409)
    return mine


async def write_plan(*, tenant_id: str, parcel_urn: str, plan: PlanIn, today: date) -> dict:
    # The id goes into an Orion path and a ``q`` filter: never trust the caller to have checked it.
    if not isinstance(parcel_urn, str) or not PARCEL_URN_RE.fullmatch(parcel_urn):
        raise InvalidParcelId("invalid parcel id")
    validate_plan(plan, today)
    segments = segments_from_plan(plan)
    season = str(plan.campaign)
    client = OrionClient(tenant_id=tenant_id)
    try:
        await _require_parcel(client, parcel_urn)
        existing = _check_conflicts(await _parcel_cycles(client, parcel_urn), plan)
        existing_ids = {e["id"] for e in existing}
        written = []
        for seq, segment in enumerate(segments):
            entity = build_segment_entity(tenant_id, parcel_urn, season, seq, segment)
            if entity["id"] in existing_ids:
                await client.delete_entity(entity["id"])
            await client.create_entity(entity)
            written.append(entity["id"])
        removed = []
        for e in existing:
            if e["id"] not in written and _status(e) == "planned":
                await client.delete_entity(e["id"])
                removed.append(e["id"])
        return {"parcel_id": parcel_urn, "campaign": plan.campaign, "written": written, "removed": removed}
    finally:
        await client.close()
