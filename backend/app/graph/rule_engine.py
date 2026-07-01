"""Pure evaluation of action-rule conditions against an observed context.

bioorch evaluates action rules (Contract 2) — crop-health only observes and
writes phenologyStage to the broker. Missing context field → clause is False
(fail-safe: never fire a management recommendation on absent data).
"""
from __future__ import annotations

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


from datetime import date, datetime, timezone


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
        "operationType": {"type": "Property", "value": action.get("operation_type")},
        "description": {"type": "Property", "value": _render(action.get("description_template", ""), context)},
        "urgency": {"type": "Property", "value": action.get("urgency")},
        "phenologyStage": {"type": "Property", "value": stage},
        "cropSpecies": {"type": "Property", "value": context.get("crop.species")},
        "status": {"type": "Property", "value": "open"},
        "dateCreated": {"type": "Property", "value": {"@type": "DateTime", "@value": now}},
    }
    if action.get("window_days") is not None:
        adv["windowDays"] = {"type": "Property", "value": action["window_days"]}
    if rule.get("source_doi"):
        adv["sourceDoi"] = {"type": "Property", "value": rule["source_doi"]}
    if rule.get("source_short"):
        adv["sourceShort"] = {"type": "Property", "value": rule["source_short"]}
    return adv
