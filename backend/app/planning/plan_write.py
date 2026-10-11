"""Write a parcel's plan for one campaign as planned AgriCrop cycles (C5).

Only planned cycles are written. ``hasAgriCrop`` on the parcel is left to the platform's
reconciler, which sets it when a sowing happens. An annual must end in the campaign; a
herbaceous perennial (alfalfa) must start in it and may end up to MAX_PERENNIAL_YEARS later,
and while it lasts no other campaign can plan over it (nor can it be planned over later ones).
A campaign with a started cycle (anything other than planned or cancelled) is refused: what
happened in the field is changed from field operations, not from the plan. Rewriting a plan
deletes and recreates its planned cycles (same deterministic ids) and deletes the planned
cycles the new plan no longer has: they were intentions that never started.

Every cycle of the parcel is read once. A cycle of another season (or of none: the legacy
assign-crop flow writes no season) conflicts with the plan only if its DATE INTERVAL overlaps one
of the plan's cycles: a catch crop that ended last autumn does not block the next campaign, and a
perennial that ends in March does not block a maize sown in April. A cycle without an end ends
at the close of the campaign it started in, unless it is a perennial that is not closed.
"""
from __future__ import annotations

import re
from datetime import date
from itertools import pairwise

import httpx
from nkz_platform_sdk.orion import OrionClient

from app.graph.crop_plan import build_segment_entity
from app.planning.history import campaign_of_start, iso_date, plain
from app.planning.models import PlanIn
from app.planning.service import PARCEL_URN_RE, InvalidParcelId, UnknownCrop, crop_code
from app.species_registry import get_lifecycle, is_woody, resolve_species

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


def _interval(entity: dict) -> tuple[date, date] | None:
    """[start, end] a cycle occupies; None = it has no start and cannot be placed.

    A cycle without an end (or with one before its start: bad data must not shrink it) ends at
    Dec 31 of the campaign it started in; a perennial that is not closed has no end at all.
    """
    begin = _first_date(entity, _START_KEYS)
    if begin is None:
        return None
    end = _first_date(entity, _END_KEYS)
    if end is not None and end >= begin:
        return begin, end
    if plain(entity.get("cropLifecycle")) == "perennial" and _status(entity) not in _CLOSED:
        return begin, date.max
    return begin, date(campaign_of_start(begin), 12, 31)


def validate_plan(plan: PlanIn, today: date) -> None:
    if plan.campaign < today.year:
        raise PlanError("campaign_past")
    spans = []
    for cycle in plan.cycles:
        end = cycle.expected_termination
        slug = resolve_species(cycle.crop)
        if slug and is_woody(slug):
            # Trees and vines are a plantation decision, never a campaign choice.
            raise PlanError("woody_not_campaign_crop")
        if _lifecycle(cycle.crop) == "perennial":
            if campaign_of_start(cycle.sowing_start) != plan.campaign:
                raise PlanError("perennial_start_outside_campaign")
            if end.year < plan.campaign:
                raise PlanError("dates_inconsistent")
            if end.year > plan.campaign + MAX_PERENNIAL_YEARS:
                raise PlanError("perennial_too_long")
        elif end.year != plan.campaign:
            raise PlanError("termination_outside_campaign")
        elif campaign_of_start(cycle.sowing_start) < plan.campaign - 1:
            # An annual opens at most the previous campaign (autumn sowing).
            raise PlanError("dates_inconsistent")
        if cycle.sowing_start > end or (cycle.sowing_end and not cycle.sowing_start <= cycle.sowing_end <= end):
            raise PlanError("dates_inconsistent")
        spans.append((cycle.sowing_start, end))
    spans.sort()
    for (_, end_a), (start_b, _) in pairwise(spans):
        if start_b < end_a:  # same day allowed: roller-crimp the cover and sow the main crop
            raise PlanError("cycles_overlap")


# A cover or catch crop may be a species the registry does not know (phacelia): it is written by name.
# The name goes into an Orion body, so it is restricted to letters, digits, space, dot, dash and brackets.
_RAW_SPECIES_RE = re.compile(r"[^\W\d_][\w .()\-]{1,63}")
_RAW_SPECIES_ROLES = frozenset({"cover_crop", "catch_crop"})


def raw_species(name: str) -> str:
    if not isinstance(name, str) or not _RAW_SPECIES_RE.fullmatch(name):
        raise PlanError("unknown_crop")
    return name


def segments_from_plan(plan: PlanIn) -> list[dict]:
    segments = []
    for cycle in sorted(plan.cycles, key=lambda c: c.sowing_start):
        try:
            crop = crop_code(cycle.crop)
        except UnknownCrop as e:
            if cycle.role not in _RAW_SPECIES_ROLES:
                raise PlanError("unknown_crop") from e
            crop = raw_species(cycle.crop)
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


def _overlap_code(other: dict, new_has_perennial: bool) -> str:
    if plain(other.get("cropLifecycle")) == "perennial":
        return "perennial_occupies"
    if new_has_perennial:
        return "perennial_overlaps_plans"
    if _status(other) != "planned":
        return "campaign_started"
    return "cycles_overlap"


_CONFLICT_PRIORITY = ("perennial_occupies", "perennial_overlaps_plans", "campaign_started", "cycles_overlap")


def _check_conflicts(cycles: list[dict], plan: PlanIn) -> list[dict]:
    """Refuse a plan that collides with what the parcel already has; return this campaign's cycles.

    This season's cycles are rewritten (planned) or refuse the plan (started). Any other cycle
    of the parcel conflicts only if its date interval overlaps one of the plan's; touching on
    the same day is allowed (roller-crimp the cover, then sow).
    """
    season = str(plan.campaign)
    mine: list[dict] = []
    codes: set[str] = set()
    new_has_perennial = any(_lifecycle(c.crop) == "perennial" for c in plan.cycles)
    new_intervals = [(c.sowing_start, c.expected_termination) for c in plan.cycles]
    for cycle in cycles:
        if str(plain(cycle.get("cropSeason"))) == season:
            mine.append(cycle)
            continue
        if _status(cycle) == "cancelled" or (span := _interval(cycle)) is None:
            continue
        if any(span[0] < end and start < span[1] for start, end in new_intervals):
            codes.add(_overlap_code(cycle, new_has_perennial))
    if any(_status(c) not in _REWRITABLE for c in mine):
        raise PlanError("campaign_started", 409)
    for code in _CONFLICT_PRIORITY:
        if code in codes:
            raise PlanError(code, 409)
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
