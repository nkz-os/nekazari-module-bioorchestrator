"""Rotation rules against a parcel's crop history (pure).

ASSUMPTION: ``RotationConstraint.intervalYears`` is the minimum number of campaigns between
crop A and crop B. B planned in campaign T after A in campaign t is restricted while
T - t <= interval; it is allowed again from t + interval + 1. The existing scenario check
words it the same way ("Minimum interval: N years"). Confirm against the constraint
sources before trusting edge years.
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass


@dataclass(frozen=True)
class Constraint:
    crop_a: str
    crop_b: str
    effect: str
    interval_years: int
    reason: str | None
    source: str | None


@dataclass(frozen=True)
class PastCrop:
    species: str
    campaign: int


def rotation_reasons(
    history: list[PastCrop], candidate: str, campaign: int, constraints: Iterable[Constraint]
) -> list[dict]:
    """Restrictions the candidate breaks and benefits it gets, as Reason dicts."""
    rules = [c for c in constraints if c.crop_b == candidate]
    found: dict[tuple[str, str], dict] = {}
    for past in sorted(history, key=lambda p: p.campaign):
        gap = campaign - past.campaign
        if gap < 1:
            continue
        for rule in rules:
            if rule.crop_a != past.species:
                continue
            if rule.effect == "restriction" and gap <= rule.interval_years:
                found[("rotation_restriction", past.species)] = {
                    "code": "rotation_restriction", "gate": "rotation", "params": {
                        "previous": past.species, "previous_campaign": past.campaign,
                        "interval_years": rule.interval_years,
                        "allowed_from": past.campaign + rule.interval_years + 1,
                        "source": rule.source}}
            elif rule.effect == "benefit" and gap == 1:
                found[("rotation_benefit", past.species)] = {
                    "code": "rotation_benefit", "gate": "rotation",
                    "params": {"previous": past.species, "source": rule.source}}
    return [found[k] for k in sorted(found)]
