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
