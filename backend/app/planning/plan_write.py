"""Write a parcel's plan for one campaign as planned AgriCrop cycles (C5).

Only planned cycles are written. ``hasAgriCrop`` on the parcel is left to the platform's
reconciler, which sets it when a sowing happens. An annual must end in the campaign; a
herbaceous perennial (alfalfa) must start in it and may end up to MAX_PERENNIAL_YEARS later,
and while it lasts no other campaign can plan over it. A campaign with a started cycle (anything
other than planned or cancelled) is refused: what happened in the field is changed from
field operations, not from the plan. Rewriting a plan deletes and recreates its planned
cycles (same deterministic ids) and deletes the planned cycles the new plan no longer has:
they were intentions that never started.
"""
from __future__ import annotations

from datetime import date
from itertools import pairwise

from nkz_platform_sdk.orion import OrionClient

from app.graph.crop_plan import build_segment_entity
from app.planning.history import campaign_of_start, iso_date, plain
from app.planning.models import PlanIn
from app.planning.service import UnknownCrop, crop_code
from app.species_registry import get_lifecycle, resolve_species

_REWRITABLE = {"planned", "cancelled"}
MAX_PERENNIAL_YEARS = 10


class PlanError(Exception):
    def __init__(self, code: str, status: int = 422):
        super().__init__(code)
        self.code = code
        self.status = status


def _lifecycle(crop: str) -> str | None:
    slug = resolve_species(crop)
    return get_lifecycle(slug) if slug else None


def _occupies(entity: dict, campaign: int) -> bool:
    """A perennial cycle (another campaign's plan or a sown one) still on the field in ``campaign``."""
    if plain(entity.get("status")) == "cancelled":
        return False
    start = iso_date(entity.get("plantingDate")) or iso_date(entity.get("sowingWindowStart"))
    end = iso_date(entity.get("terminationDate")) or iso_date(entity.get("expectedTerminationDate"))
    if start is None:
        return False
    return campaign_of_start(start) <= campaign and (end is None or campaign <= end.year)


def validate_plan(plan: PlanIn, today: date) -> None:
    if plan.campaign < today.year:
        raise PlanError("campaign_past")
    spans = []
    for cycle in plan.cycles:
        end = cycle.expected_termination
        if _lifecycle(cycle.crop) == "perennial":
            if campaign_of_start(cycle.sowing_start) != plan.campaign:
                raise PlanError("perennial_start_outside_campaign")
            if not plan.campaign <= end.year <= plan.campaign + MAX_PERENNIAL_YEARS:
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


async def write_plan(*, tenant_id: str, parcel_urn: str, plan: PlanIn, today: date) -> dict:
    validate_plan(plan, today)
    segments = segments_from_plan(plan)
    season = str(plan.campaign)
    client = OrionClient(tenant_id=tenant_id)
    try:
        existing = await client.query_entities(
            type="AgriCrop", q=f'hasAgriParcel=="{parcel_urn}";cropSeason=="{season}"', limit=50, options="keyValues")
        if any(plain(e.get("status")) not in _REWRITABLE for e in existing):
            raise PlanError("campaign_started", 409)
        perennials = await client.query_entities(
            type="AgriCrop", q=f'hasAgriParcel=="{parcel_urn}";cropLifecycle=="perennial"', limit=50, options="keyValues")
        if any(_occupies(p, plan.campaign) for p in perennials if plain(p.get("cropSeason")) != season):
            raise PlanError("perennial_occupies", 409)
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
            if e["id"] not in written and plain(e.get("status")) == "planned":
                await client.delete_entity(e["id"])
                removed.append(e["id"])
        return {"parcel_id": parcel_urn, "campaign": plan.campaign, "written": written, "removed": removed}
    finally:
        await client.close()
