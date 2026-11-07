"""Yield estimates for a planned crop: one contract, many providers.

Every provider answers with the same shape for the SAME inputs (resolved once by the
caller), declares its basis and validation status, and is never averaged with another.
``unvalidated`` estimates are not returned; a provider that cannot answer says why in
``unavailable``.

AquaCrop is asynchronous (see ``aquacrop_provider`` and the ``/estimates/aquacrop`` endpoint):
this listing only says whether it will answer (``async``) or why it will not.
"""
from __future__ import annotations

import math
from collections import defaultdict
from statistics import median, quantiles

from app.planning.aquacrop_provider import VALIDATED
from app.planning.history import (
    _history_cycles,
    _without_shadowed_declarations,
    cycle_yield_kg_ha,
    species_slug,
)
from app.species_registry import catalog_code, get_species_info

PROVIDER_PARCEL_HISTORY = "parcel_history"
PARCEL_HISTORY_VERSION = "1.0"
PROVIDER_TRIALS = "trial_evidence"
PROVIDER_AQUACROP = "aquacrop"
DISAGREEMENT_RATIO = 1.5  # P50 max/min among same-basis model estimates (owner, 2026-10-13: kept, revisit with use)
_OBSERVED_BASES = frozenset({"trial_evidence", "parcel_history"})


def estimate_providers(eppo: str | None, *, has_trials: bool, regime: str | None, country: str | None) -> list[str]:
    """Providers that can answer for this crop on this parcel. AquaCrop only inside its validated scope.

    AquaCrop still needs a sowing date to run: listed here means it is offered, not that it answered.
    """
    providers = [PROVIDER_TRIALS] if has_trials else []
    if eppo and (eppo, regime, country) in VALIDATED:
        providers.append(PROVIDER_AQUACROP)
    return providers


def percentiles(values: list[float]) -> dict | None:
    if not values:
        return None
    if len(values) == 1:
        return {"p10": values[0], "p50": values[0], "p90": values[0]}
    deciles = quantiles(values, n=10, method="inclusive")
    return {"p10": deciles[0], "p50": median(values), "p90": deciles[8]}


def _valid_yield(value) -> bool:
    # bool is an int subclass: True would otherwise count as a yield of 1 kg/ha.
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value > 0


def usable_rows(rows: list[dict], variety: str | None) -> list[dict]:
    """The trial rows behind an estimate: the asked variety (any when None) with a valid yield.

    One filter for the number and for its attributions, so a source is credited only when
    one of its rows feeds the estimate.
    """
    return [r for r in rows if _valid_yield(r.get("yield_kg_ha")) and (variety is None or r.get("variety") == variety)]


def trial_estimate(rows: list[dict], variety: str | None, attributions: list[dict], graph_id: str,
                   policy_version: str | None) -> dict | None:
    tonnes = [r["yield_kg_ha"] / 1000 for r in usable_rows(rows, variety)]
    pct = percentiles(tonnes)
    if pct is None:
        return None
    return {"provider_id": PROVIDER_TRIALS, "provider_version": policy_version or "unknown",
            "basis": "trial_evidence", "unit": "t/ha", "percentiles": {k: round(v, 2) for k, v in pct.items()},
            "n": len(tonnes), "n_kind": "trials", "validation": {"status": "not_applicable"},
            "moisture": {"basis": "as_reported", "pct": None},
            "warnings": [], "attributions": attributions, "graph_id": graph_id}


def parcel_history_estimate(cycles_payload: dict | None, crop_eppo: str,
                            area_ha: float | None = None) -> dict | None:
    """Empirical yield distribution from this parcel's own closed past cycles of the crop."""
    target_code = catalog_code(crop_eppo)
    tonnes: list[float] = []
    for cycle in _without_shadowed_declarations(_history_cycles(cycles_payload)):
        if (cycle.get("role") or "main_crop") != "main_crop":
            continue
        slug = species_slug(cycle.get("species"))
        eppo = (get_species_info(slug) or {}).get("eppo_code") if slug else None
        if not eppo or catalog_code(eppo) != target_code:
            continue
        yield_kg_ha, _ = cycle_yield_kg_ha(cycle, area_ha=area_ha)
        if _valid_yield(yield_kg_ha):
            tonnes.append(float(yield_kg_ha) / 1000.0)
    pct = percentiles(tonnes)
    if pct is None:
        return None
    return {
        "provider_id": PROVIDER_PARCEL_HISTORY,
        "provider_version": PARCEL_HISTORY_VERSION,
        "basis": "parcel_history",
        "unit": "t/ha",
        "percentiles": {k: round(v, 2) for k, v in pct.items()},
        "n": len(tonnes),
        "n_kind": "years",
        "validation": {"status": "not_applicable"},
        "moisture": {"basis": "as_reported", "pct": None},
        "warnings": [],
        "attributions": [],
        "graph_id": None,
    }


def aquacrop_status(gate_reason: str | None) -> dict:
    """The AquaCrop entry of ``unavailable`` in /estimates: ``async`` when it will answer (poll it)."""
    return {"provider_id": PROVIDER_AQUACROP, "reason": gate_reason or "async"}


def disagreement(estimates: list[dict]) -> bool:
    """Same-basis AND same-moisture model medians that differ by more than DISAGREEMENT_RATIO.

    Range-only figures have no median and empirical observations (trials, parcel history) are never compared with models.
    """
    groups: dict[tuple, list[float]] = defaultdict(list)
    for e in estimates:
        if e["basis"] in _OBSERVED_BASES or not e.get("percentiles"):
            continue
        moisture = e.get("moisture") or {}
        groups[(e["basis"], moisture.get("basis"), moisture.get("pct"))].append(e["percentiles"]["p50"])
    return any(len(v) >= 2 and max(v) > DISAGREEMENT_RATIO * min(v) for v in groups.values())


def visible(estimates: list[dict]) -> list[dict]:
    return [e for e in estimates if e["validation"]["status"] != "unvalidated"]
