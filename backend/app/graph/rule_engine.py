"""Pure evaluation of action-rule conditions against an observed context.

bioorch evaluates action rules (Contract 2) — crop-health only observes and
writes phenologyStage to the broker. Missing context field → clause is False
(fail-safe: never fire a management recommendation on absent data).
"""
from __future__ import annotations
import logging
from datetime import date, datetime, timezone

logger = logging.getLogger(__name__)

_MISSING = object()


def _cmp(op: str, actual, expected) -> bool:
    if actual is _MISSING:
        return False
    try:
        if op == "eq":
            return actual == expected
        if op == "lte":
            return actual <= expected
        if op == "gte":
            return actual >= expected
        if op == "gt":
            return actual > expected
        if op == "lt":
            return actual < expected
        if op == "in":
            return actual in expected
        if op == "nin":
            return actual not in expected
    except TypeError:
        return False
    return False


def evaluate_conditions(tree: dict, context: dict) -> bool:
    """True iff every clause under `all` matches. Empty/absent tree → True."""
    clauses = (tree or {}).get("all", [])
    for clause in clauses:
        actual = context.get(clause["field"], _MISSING)
        if not _cmp(clause["op"], actual, clause.get("value")):
            return False
    return True


def flatten_context(crop: dict, observed: dict, *, today: date | None = None) -> dict:
    ctx = dict(observed)
    ctx["crop.role"] = crop.get("role")
    ctx["crop.status"] = crop.get("status")
    ctx["crop.species"] = crop.get("species")
    ctx["crop.termination_method"] = crop.get("terminationMethod")
    sow = crop.get("sowingWindowStart")
    if sow:
        try:
            d = date.fromisoformat(str(sow)[:10])
            ctx["crop.days_until_sowing_window"] = (d - (today or date.today())).days
        except ValueError:
            pass
    # drop keys whose value is None so a missing crop field never satisfies eq(None)
    return {k: v for k, v in ctx.items() if v is not None}


def _render(template: str, context: dict) -> str:
    out = template
    for k, v in context.items():
        out = out.replace("{" + k + "}", str(v))
    return out


def build_advisory(rule: dict, context: dict, tenant_id: str, parcel_id: str,
                   crop_id: str, stage: str, *, now: str | None = None) -> dict:
    action = rule.get("action", {})
    parcel_short = parcel_id.split(":")[-1]
    now = now or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    adv = {
        "id": f"urn:ngsi-ld:CropAdvisory:{tenant_id}:{parcel_short}:{rule['id']}:{stage}",
        "type": "CropAdvisory",
        "hasAgriParcel": {"type": "Relationship", "object": parcel_id},
        "hasAgriCrop": {"type": "Relationship", "object": crop_id},
        "ruleId": {"type": "Property", "value": rule["id"]},
        "description": {"type": "Property", "value": _render(action.get("description_template", ""), context)},
        "phenologyStage": {"type": "Property", "value": stage},
        "status": {"type": "Property", "value": "open"},
        "dateCreated": {"type": "Property", "value": {"@type": "DateTime", "@value": now}},
    }
    # Only include these when non-None: Orion-LD rejects a Property with value:null,
    # which would drop the whole advisory silently (a malformed rule must not do that).
    if action.get("operation_type") is not None:
        adv["operationType"] = {"type": "Property", "value": action["operation_type"]}
    if action.get("urgency") is not None:
        adv["urgency"] = {"type": "Property", "value": action["urgency"]}
    if context.get("crop.species") is not None:
        adv["cropSpecies"] = {"type": "Property", "value": context["crop.species"]}
    if action.get("window_days") is not None:
        adv["windowDays"] = {"type": "Property", "value": action["window_days"]}
    if rule.get("source_doi"):
        adv["sourceDoi"] = {"type": "Property", "value": rule["source_doi"]}
    if rule.get("source_short"):
        adv["sourceShort"] = {"type": "Property", "value": rule["source_short"]}
    return adv


async def evaluate(dao, orion, tenant_id: str, parcel_id: str, observed: dict) -> list[dict]:
    """Fetch the parcel's assigned crop, evaluate action rules, upsert advisories.

    Returns the advisories produced (idempotent upsert; empty if no crop/no match).
    """
    parcel = await orion.get_entity(parcel_id, options="keyValues")
    crop_id = parcel.get("hasAgriCrop") or parcel.get("refAgriCrop")
    if not crop_id:
        return []
    crop = await orion.get_entity(crop_id, options="keyValues")
    stage = observed.get("phenology.current_stage")
    context = flatten_context(crop, observed)
    rules = await dao.get_action_rules(species=crop.get("species"))
    advisories = []
    for rule in rules:
        if evaluate_conditions(rule.get("conditions", {}), context):
            adv = build_advisory(rule, context, tenant_id, parcel_id, crop_id, stage)
            advisories.append(adv)
            logger.info("action-rule matched: rule=%s parcel=%s stage=%s urgency=%s",
                        rule["id"], parcel_id, stage, rule.get("action", {}).get("urgency"))
    if advisories:
        result = await orion.upsert_entities_batch(advisories)
        if result.get("errors"):
            logger.warning("advisory upsert reported errors: parcel=%s errors=%s",
                           parcel_id, result["errors"])
    return advisories
