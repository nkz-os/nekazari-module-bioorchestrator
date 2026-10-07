"""Per-crop yield cutoffs that derive an irrigation regime where the source states none.

Two things live here and nothing else: :func:`classify_yield`, the two-band rule the contract engine
applies, and :func:`calibrate`, the way the cutoffs in ``irrigation_thresholds.yaml`` are produced from
the rows whose regime the source itself states. The values are data (the registry file); this module
neither reads nor writes it.

The rule (method ``yield_threshold_v1``): ``yield <= low`` is rainfed, ``yield >= high`` is irrigated,
anything in between has no regime.

Calibration, per crop and on the labelled rows only (units with a yield whose regime the source
states, never the rows the cutoffs are then applied to):

* fewer than :data:`MIN_LABELLED` labelled rows in either regime: no cutoff (nothing is invented);
* ``low`` is the largest multiple of :data:`STEP_KG_HA` such that at most :data:`EPSILON` of the
  irrigated rows are at or below it, ``high`` the smallest multiple such that at most
  :data:`EPSILON` of the rainfed rows are at or above it, so each regime is misjudged on at most
  :data:`EPSILON` of its own labelled rows whatever the mix of regimes in the sample;
* when the two tails do not meet (the regimes are separated with room to spare) the cut is the middle
  of the gap with a one-step undecided band; no positive ``low``: no cutoff;
* the rows the bands decide must be judged right in at least ``1 - MAX_DECIDED_ERROR`` of the cases,
  or the yields do not separate the regimes for that crop: no cutoff.

The four parameters are assumptions of the calibration, recorded with every result.
"""
from __future__ import annotations

import bisect
import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Literal

EPSILON = 0.05
STEP_KG_HA = 100.0
MIN_LABELLED = 15
MAX_DECIDED_ERROR = 0.20
# Guard for writing the registry (leave-one-year-out): the cutoffs must hold on years they were not fitted on.
MAX_HELDOUT_ERROR = 0.05  # wrong judgements over all held-out labelled rows (an undecided row counts as a row)
MIN_HELDOUT_YEARS = 3  # held-out years that must be judged, per regime class
QUANTILE_LEVELS = {"p05": 0.05, "p25": 0.25, "p50": 0.50, "p75": 0.75, "p95": 0.95}

Regime = Literal["rainfed", "irrigated"]


def classify_yield(yield_kg_ha: float, low_kg_ha: float, high_kg_ha: float) -> Regime | None:
    """The regime the two bands give a yield, or None between them (cutoffs belong to their bands)."""
    if yield_kg_ha <= low_kg_ha:
        return "rainfed"
    if yield_kg_ha >= high_kg_ha:
        return "irrigated"
    return None


def quantile(values: Sequence[float], level: float) -> float:
    """Nearest-rank quantile (no interpolation: the result is an observed value)."""
    if not values:
        raise ValueError("quantile of no values")
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, max(0, math.ceil(level * len(ordered)) - 1))]


def quantiles(values: Sequence[float]) -> dict[str, float]:
    return {name: quantile(values, level) for name, level in QUANTILE_LEVELS.items()} if values else {}


@dataclass(frozen=True)
class Calibration:
    """The outcome for one crop: the cutoffs (or why there are none) and how they judge the labelled rows."""

    crop: str
    n_rainfed: int
    n_irrigated: int
    rainfed_quantiles: dict[str, float]
    irrigated_quantiles: dict[str, float]
    low_kg_ha: float | None
    high_kg_ha: float | None
    reason: str | None  # why there is no cutoff
    # probability that a random irrigated labelled row out-yields a random rainfed one (0.5 = no separation)
    separation: float | None = None
    irrigated_judged_rainfed: int | None = None
    rainfed_judged_irrigated: int | None = None
    n_ambiguous: int | None = None

    @property
    def calibrated(self) -> bool:
        return self.low_kg_ha is not None

    @property
    def n_labelled(self) -> int:
        return self.n_rainfed + self.n_irrigated

    @property
    def n_wrong(self) -> int | None:
        if not self.calibrated:
            return None
        return (self.irrigated_judged_rainfed or 0) + (self.rainfed_judged_irrigated or 0)

    @property
    def error_rate(self) -> float | None:
        """Wrong judgements over ALL labelled rows (an ambiguous row is not wrong, it is undecided)."""
        return None if self.n_wrong is None or not self.n_labelled else self.n_wrong / self.n_labelled

    @property
    def n_decided(self) -> int | None:
        return None if self.n_wrong is None else self.n_labelled - (self.n_ambiguous or 0)

    @property
    def decided_error_rate(self) -> float | None:
        """Wrong judgements over the labelled rows the bands decide."""
        return None if not self.n_decided else (self.n_wrong or 0) / self.n_decided


def _tail_low(irrigated: Sequence[float], epsilon: float, step: float) -> float:
    """The largest grid value with at most ``epsilon`` of the irrigated rows at or below it."""
    allowed = math.floor(epsilon * len(irrigated))
    ordered = sorted(irrigated)
    # the (allowed + 1)-th smallest irrigated value must stay above `low`
    limit = ordered[allowed]
    low = math.floor(limit / step) * step
    return low - step if low >= limit else low


def _tail_high(rainfed: Sequence[float], epsilon: float, step: float) -> float:
    """The smallest grid value with at most ``epsilon`` of the rainfed rows at or above it."""
    allowed = math.floor(epsilon * len(rainfed))
    ordered = sorted(rainfed, reverse=True)
    limit = ordered[allowed]  # the (allowed + 1)-th largest rainfed value must stay below `high`
    high = math.ceil(limit / step) * step
    return high + step if high <= limit else high


def separation(rainfed: Sequence[float], irrigated: Sequence[float]) -> float | None:
    """P(irrigated yield > rainfed yield) over all labelled pairs, ties counting half (a Mann-Whitney U)."""
    if not rainfed or not irrigated:
        return None
    ordered = sorted(rainfed)
    wins = 0.0
    for y in irrigated:
        below = bisect.bisect_left(ordered, y)
        equal = bisect.bisect_right(ordered, y) - below
        wins += below + 0.5 * equal
    return wins / (len(rainfed) * len(irrigated))


def calibrate(
    crop: str, rainfed: Iterable[float], irrigated: Iterable[float], *, epsilon: float = EPSILON,
    step: float = STEP_KG_HA, min_labelled: int = MIN_LABELLED, max_decided_error: float = MAX_DECIDED_ERROR,
) -> Calibration:
    """Cutoffs for one crop from the yields (kg/ha) of its rows with a source-stated regime."""
    rain = [float(y) for y in rainfed]
    irr = [float(y) for y in irrigated]
    base = {
        "crop": crop, "n_rainfed": len(rain), "n_irrigated": len(irr),
        "rainfed_quantiles": quantiles(rain), "irrigated_quantiles": quantiles(irr),
        "separation": separation(rain, irr),
    }
    if len(rain) < min_labelled or len(irr) < min_labelled:
        missing = [name for name, n in (("rainfed", len(rain)), ("irrigated", len(irr))) if n < min_labelled]
        return Calibration(**base, low_kg_ha=None, high_kg_ha=None, reason=(
            f"fewer than {min_labelled} labelled rows with a yield in {' and '.join(missing)} "
            f"(rainfed {len(rain)}, irrigated {len(irr)}): nothing to calibrate on, no cutoff is invented"))
    low = _tail_low(irr, epsilon, step)
    high = _tail_high(rain, epsilon, step)
    if low >= high:
        # the two tails do not meet: the regimes are separated with room to spare. Any cut inside the gap
        # keeps both bounds, so take the middle one with the narrowest undecided band (one step wide).
        low = math.floor((low + high) / 2 / step) * step
        high = low + step
    if low <= 0:
        return Calibration(**base, low_kg_ha=None, high_kg_ha=None, reason=(
            f"the irrigated labelled yields reach down to {min(irr):g} kg/ha: no positive cutoff keeps the error "
            f"of the irrigated rows within {epsilon:.0%}"))
    wrong_irrigated = sum(1 for y in irr if y <= low)
    wrong_rainfed = sum(1 for y in rain if y >= high)
    ambiguous = sum(1 for y in rain + irr if low < y < high)
    decided = len(rain) + len(irr) - ambiguous
    decided_error = (wrong_irrigated + wrong_rainfed) / decided if decided else 1.0
    if decided_error > max_decided_error:
        return Calibration(**base, low_kg_ha=None, high_kg_ha=None, reason=(
            f"the bands judge {decided_error:.0%} of the {decided} labelled rows they decide wrongly (more than "
            f"{max_decided_error:.0%}): the yields do not separate rainfed from irrigated for this crop"))
    return Calibration(
        **base, low_kg_ha=low, high_kg_ha=high, reason=None, irrigated_judged_rainfed=wrong_irrigated,
        rainfed_judged_irrigated=wrong_rainfed, n_ambiguous=ambiguous)


@dataclass(frozen=True)
class LeaveYearOut:
    """Out-of-sample check of one crop: each year is judged by cutoffs calibrated on the other years."""

    crop: str
    years_rainfed: int  # held-out years with at least one judged rainfed row
    years_irrigated: int
    n_judged: int  # held-out labelled rows judged by cutoffs fitted without their year
    n_wrong: int
    n_ambiguous: int
    n_unjudged: int  # rows of years whose training set could not be calibrated, or rows with no year

    @property
    def error_rate(self) -> float | None:
        return None if not self.n_judged else self.n_wrong / self.n_judged

    def refusal(
        self, *, max_error: float = MAX_HELDOUT_ERROR, min_years: int = MIN_HELDOUT_YEARS,
    ) -> str | None:
        """Why these cutoffs may not be written to the registry, or None when they hold out of sample."""
        reasons = []
        for name, years in (("rainfed", self.years_rainfed), ("irrigated", self.years_irrigated)):
            if years < min_years:
                reasons.append(f"held-out data covers {years} year(s) of {name} rows, fewer than {min_years}")
        if self.error_rate is None:
            reasons.append("no held-out row could be judged")
        elif self.error_rate > max_error:
            reasons.append(f"held-out misclassification {self.error_rate:.1%} exceeds {max_error:.1%}")
        return f"{self.crop}: " + "; ".join(reasons) if reasons else None


def leave_one_year_out(
    crop: str, rows: Iterable[tuple[int | None, float, Regime]], *, epsilon: float = EPSILON, step: float = STEP_KG_HA,
    min_labelled: int = MIN_LABELLED, max_decided_error: float = MAX_DECIDED_ERROR,
) -> LeaveYearOut:
    """Calibrate on all years but one and judge the rows of the left-out year; repeat for every year."""
    labelled = list(rows)
    years = sorted({year for year, _, _ in labelled if year is not None})
    judged = wrong = ambiguous = 0
    covered: dict[str, set[int]] = {"rainfed": set(), "irrigated": set()}
    for year in years:
        train = [(y, r) for yr, y, r in labelled if yr != year]
        test = [(y, r) for yr, y, r in labelled if yr == year]
        fit = calibrate(
            crop, (y for y, r in train if r == "rainfed"), (y for y, r in train if r == "irrigated"),
            epsilon=epsilon, step=step, min_labelled=min_labelled, max_decided_error=max_decided_error)
        if not fit.calibrated:
            continue
        for value, regime in test:
            band = classify_yield(value, fit.low_kg_ha, fit.high_kg_ha)  # type: ignore[arg-type]
            judged += 1
            covered[regime].add(year)
            if band is None:
                ambiguous += 1
            elif band != regime:
                wrong += 1
    return LeaveYearOut(
        crop=crop, years_rainfed=len(covered["rainfed"]), years_irrigated=len(covered["irrigated"]),
        n_judged=judged, n_wrong=wrong, n_ambiguous=ambiguous, n_unjudged=len(labelled) - judged)


__all__ = [
    "EPSILON",
    "MAX_DECIDED_ERROR",
    "MAX_HELDOUT_ERROR",
    "MIN_HELDOUT_YEARS",
    "MIN_LABELLED",
    "STEP_KG_HA",
    "Calibration",
    "LeaveYearOut",
    "calibrate",
    "classify_yield",
    "leave_one_year_out",
    "quantile",
    "quantiles",
    "separation",
]
