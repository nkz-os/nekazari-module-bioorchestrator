"""Yield estimates for a planned crop: one contract, many providers.

Every provider answers with the same shape for the SAME inputs (resolved once by the
caller), declares its basis and validation status, and is never averaged with another.
``unvalidated`` estimates are not returned; a provider that cannot answer says why in
``unavailable``.

AquaCrop simulates sown campaigns only: its endpoint rejects a future sowing date. Until it
offers a pre-sowing mode (planned sowing day over the historical years), planning lists it as
unavailable with ``preseason_not_supported``.
"""
from __future__ import annotations

from collections import defaultdict
from statistics import median, quantiles

PROVIDER_TRIALS = "trial_evidence"
PROVIDER_AQUACROP = "aquacrop"
DISAGREEMENT_RATIO = 1.5  # ASSUMPTION: P50 max/min among same-basis model estimates; to review


def percentiles(values: list[float]) -> dict | None:
    if not values:
        return None
    if len(values) == 1:
        return {"p10": values[0], "p50": values[0], "p90": values[0]}
    deciles = quantiles(values, n=10, method="inclusive")
    return {"p10": deciles[0], "p50": median(values), "p90": deciles[8]}


def trial_estimate(rows: list[dict], variety: str | None, attributions: list[dict], graph_id: str,
                   policy_version: str | None) -> dict | None:
    tonnes = [r["yield_kg_ha"] / 1000 for r in rows
              if isinstance(r.get("yield_kg_ha"), (int, float)) and r["yield_kg_ha"] > 0
              and (variety is None or r.get("variety") == variety)]
    pct = percentiles(tonnes)
    if pct is None:
        return None
    return {"provider_id": PROVIDER_TRIALS, "provider_version": policy_version or "unknown",
            "basis": "trial_evidence", "unit": "t/ha", "percentiles": {k: round(v, 2) for k, v in pct.items()},
            "n": len(tonnes), "n_kind": "trials", "validation": {"status": "not_applicable"},
            "warnings": [], "attributions": attributions, "graph_id": graph_id}


def aquacrop_status() -> dict:
    return {"provider_id": PROVIDER_AQUACROP, "reason": "preseason_not_supported"}


def disagreement(estimates: list[dict]) -> bool:
    by_basis: dict[str, list[float]] = defaultdict(list)
    for e in estimates:
        if e["basis"] != "trial_evidence":
            by_basis[e["basis"]].append(e["percentiles"]["p50"])
    return any(len(v) >= 2 and min(v) > 0 and max(v) / min(v) > DISAGREEMENT_RATIO for v in by_basis.values())


def visible(estimates: list[dict]) -> list[dict]:
    return [e for e in estimates if e["validation"]["status"] != "unvalidated"]
