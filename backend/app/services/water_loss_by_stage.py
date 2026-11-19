"""Water loss by growth stage.

Yield gained if water had not been short in that growth stage alone (the gain includes water
left in the soil for later stages).

For every member (one with observed weather for a finished season, one per analog year for a
season in progress) the model runs rainfed, at full irrigation (the potential) and, for each
stage already finished on the last observed day, rainfed with irrigation in that stage alone
(``run_aquacrop(relief_stage=...)``). The gain of a stage is Y_relief - Y_rainfed, in the
model's t/ha, and as a percentage of the potential yield. Nothing is projected: a stage that
has not finished carries no figure. It is a model estimate; no observed data arbitrates it.

A stage X is finished when the library's growth stage on the last observed day (``as_of``) is
above X: if the last day of X is ``as_of`` it is still in progress (the library irrigates with
the stage of the previous day). In a finished season (the crop reached harvest on observed
weather) every reached stage is finished.

This module has its own process pool and never touches the one of ``aquacrop_engine``.
"""
from __future__ import annotations

import asyncio
import logging
import os
import statistics
from collections.abc import Awaitable, Callable, Mapping
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from datetime import date, timedelta
from multiprocessing import get_context
from typing import Any

from app.services.engines import aquacrop_engine as aq
from app.services.engines.aquacrop_engine import (
    DailyWeather,
    EngineInputError,
    SoilLayer,
)
from app.services.sim_errors import SimulationError

logger = logging.getLogger(__name__)

BASIS_STAGE_BOUNDS = "rainfed_simulation"


class StageBoundsMismatch(SimulationError):
    """Members disagree on the bounds of a finished stage (they only depend on observed weather)."""

    def __init__(self, message: str):
        super().__init__("stage_bounds_mismatch", message, 500)


# ----------------------------------------------------------------------- pool
_pool: ProcessPoolExecutor | None = None


def _get_pool() -> ProcessPoolExecutor:
    """This module's own pool, created on first use (importing never starts processes)."""
    global _pool
    if _pool is None:
        raw = os.environ.get("WATER_LOSS_MAX_WORKERS", "1")
        try:
            workers = int(raw)
        except ValueError:
            raise EngineInputError(f"WATER_LOSS_MAX_WORKERS is not an integer: {raw!r}")
        if workers < 1:
            raise EngineInputError("WATER_LOSS_MAX_WORKERS must be >= 1")
        _pool = ProcessPoolExecutor(max_workers=workers, mp_context=get_context("spawn"))
    return _pool


def shutdown_pool(wait: bool = True) -> None:
    """Close the pool; the lifespan path passes ``wait=False`` so shutdown never blocks."""
    global _pool
    if _pool is not None:
        _pool.shutdown(wait=wait, cancel_futures=not wait)
        _pool = None


# --------------------------------------------------------------------- stages
def _stage_state(stage: Mapping[str, Any], as_of: date) -> str:
    """complete | in_progress | not_reached for one ``stages`` row of the rainfed run."""
    if stage["start"] is None:
        return "not_reached"
    start, end = date.fromisoformat(stage["start"]), date.fromisoformat(stage["end"])
    if end < as_of:
        return "complete"
    if start <= as_of:
        return "in_progress"
    return "not_reached"


def _states(stages: list[dict], as_of: date) -> dict[int, str]:
    return {s["index"]: _stage_state(s, as_of) for s in stages}


def _end_of(stages: list[dict], index: int) -> str | None:
    return next((s["end"] for s in stages if s["index"] == index), None)


def member_runs(
    weather: list[DailyWeather],
    soil: list[SoilLayer],
    crop: str,
    planting_date: date,
    sim_start: date | None,
    crop_params: Mapping[str, float] | None,
    mulch_pct: float | None,
    as_of: date,
) -> dict:
    """Runs of one member (picklable; runs in the pool).

    Rainfed (with stage bounds), potential, then one relief run per stage finished on
    ``as_of``. Returns ``{wl, pot, stages, relief: {index: yield}, relief_end: {index: ISO|None},
    warnings, engine_version}``.
    """
    kw = {"crop_params": crop_params, "mulch_pct": mulch_pct}
    wl = aq.run_aquacrop(weather, soil, crop, planting_date, "rainfed", sim_start,
                         with_stages=True, **kw)
    pot = aq.run_aquacrop(weather, soil, crop, planting_date, "full", sim_start, **kw)
    relief: dict[int, float] = {}
    relief_end: dict[int, str | None] = {}
    for index, state in _states(wl["stages"], as_of).items():
        if state != "complete":
            continue
        r = aq.run_aquacrop(weather, soil, crop, planting_date, "rainfed", sim_start,
                            relief_stage=index, with_stages=True, **kw)
        relief[index] = r["yield_t_ha"]
        relief_end[index] = _end_of(r["stages"], index)
    return {"wl": wl["yield_t_ha"], "pot": pot["yield_t_ha"], "stages": wl["stages"],
            "relief": relief, "relief_end": relief_end, "warnings": list(wl["warnings"]),
            "engine_version": wl["engine_version"]}


# -------------------------------------------------------------------- combine
def _check_bounds(members: list[dict], as_of: date) -> dict[int, str]:
    """States from the first member; every member must agree on finished/started bounds."""
    ref = members[0]["stages"]
    states = _states(ref, as_of)
    for i, m in enumerate(members[1:], start=1):
        other = _states(m["stages"], as_of)
        for s in ref:
            k = s["index"]
            o = next((x for x in m["stages"] if x["index"] == k), None)
            if o is None:
                raise StageBoundsMismatch(f"member {i} has no stage {k} bounds")
            bad = other[k] != states[k]
            if not bad and states[k] == "complete":
                bad = (o["start"], o["end"]) != (s["start"], s["end"])
            elif not bad and states[k] == "in_progress":
                bad = o["start"] != s["start"]
            if bad:
                raise StageBoundsMismatch(
                    f"members 0 and {i} disagree on stage {k} bounds before {as_of.isoformat()}")
    return states


def _gain_pct(gain: float, pot: float) -> float | None:
    return 100.0 * gain / pot if pot > 0 else None


def _relief_end(members: list[dict], index: int, rainfed_end: str | None,
                warns: list[str]) -> str | None:
    ends = [m["relief_end"][index] for m in members]
    if len(set(ends)) == 1:
        end = ends[0]
    else:
        # ASSUMPTION: members can only differ here when the relief run moves a stage end past
        # the last observed day; report the median member's end and say so.
        end = date.fromordinal(statistics.median_low(
            [date.fromisoformat(e).toordinal() for e in ends if e is not None])).isoformat()
        warns.append(f"relief-run end of stage {index} differs between members")
    return end if end != rainfed_end else None


def _combine(members: list[dict], as_of: date, season: str, years: list[int] | None) -> dict:
    states = _check_bounds(members, as_of)
    ref = members[0]["stages"]
    complete = season == "complete"
    warns = list(members[0]["warnings"])
    wl = [m["wl"] for m in members]
    pot = [m["pot"] for m in members]
    wl_ref, pot_ref = statistics.median(wl), statistics.median(pot)  # single member: itself
    gap = 100.0 * (1.0 - wl_ref / pot_ref) if pot_ref > 0 else None
    band = None
    if not complete and all(p > 0 for p in pot):
        band = aq._pct([100.0 * (1.0 - w / p) for w, p in zip(wl, pot)])
    stages = []
    for s in ref:
        k, state = s["index"], states[s["index"]]
        row: dict[str, Any] = {
            "index": k, "key": aq.STAGE_KEYS[k - 1],
            "start": s["start"] if state != "not_reached" else None,
            "end": s["end"] if state == "complete" else None,
            "state": state, "gain_t_ha_model": None, "gain_pct_of_potential": None,
            "end_in_relief_run": None,
        }
        if state == "complete":
            gains = [m["relief"][k] - m["wl"] for m in members]
            pcts = [_gain_pct(g, m["pot"]) for g, m in zip(gains, members)]
            if complete:
                row["gain_t_ha_model"], row["gain_pct_of_potential"] = gains[0], pcts[0]
            else:
                row["gain_t_ha_model"] = aq._pct(gains)
                row["gain_pct_of_potential"] = (
                    aq._pct(pcts) if all(p is not None for p in pcts) else None)
            row["end_in_relief_run"] = _relief_end(members, k, s["end"], warns)
        stages.append(row)
    if any(r["end_in_relief_run"] for r in stages):
        warns.append("in a relief run a stage can end on another day than in the rainfed run; "
                     "both are shown")
    return {
        "season": season,
        "as_of": None,  # filled by compute
        "ensemble": None if complete else {"n_years": len(members), "years": years},
        "total": {
            "water_gap_pct": gap,
            "water_gap_band": band,
            "water_limited_t_ha": wl[0] if complete else aq._pct(wl),
            "potential_t_ha": pot[0] if complete else aq._pct(pot),
        },
        "stages": stages,
        "engine_version": members[0]["engine_version"],
        "warnings": warns,
    }


# -------------------------------------------------------------------- compute
async def compute(
    observed: list[DailyWeather],
    analogs: dict[int, list[DailyWeather]] | Callable[[], Awaitable[dict[int, list[DailyWeather]]]],
    soil: list[SoilLayer],
    crop: str,
    planting_date: date,
    sim_start: date | None,
    crop_params: Mapping[str, float] | None,
    mulch_pct: float | None,
    progress: Callable[[int, int], None] | None = None,
) -> dict:
    """Water loss by stage: one member with observed weather if the season is finished,
    else one per analog year (as ``aquacrop_engine.simulate``). Rainfed only."""
    aq._validate_crop_irrigation(crop, "rainfed")
    aq._validate_crop_params(crop, crop_params)
    aq._field_management(mulch_pct)
    loop = asyncio.get_running_loop()
    pool = _get_pool()
    last = observed[-1].day if observed else None
    args = (soil, crop, planting_date, sim_start, crop_params, mulch_pct)
    # 29 Feb as last observed day: the engine cannot end a run there and says "cannot mature"
    # instead of "weather ends"; below planting + 364 d it is an unfinished season.
    if last is None or last < planting_date:
        if callable(analogs):
            analogs = await analogs()
        if not analogs:
            raise aq.WeatherEndsBeforeHarvestError(
                f"weather ends on {last} before the planting date {planting_date} "
                "and no analog years are available")
    else:
        unfinished = last < planting_date + timedelta(days=364)
        try:
            single = await _run(loop, pool, crop, planting_date, member_runs, observed, *args,
                                date.max)
        except (aq.WeatherEndsBeforeHarvestError, aq.CropCannotMatureError) as e:
            leap_day = isinstance(e, aq.CropCannotMatureError) and (last.month, last.day) == (2, 29)
            if not unfinished or (isinstance(e, aq.CropCannotMatureError) and not leap_day):
                raise
            if callable(analogs):
                analogs = await analogs()
            if not analogs:
                raise
        else:
            if progress:
                progress(1, 1)
            out = _combine([single], date.max, "complete", None)
            return _finish(out, last, crop, planting_date, 1)
    if len(analogs) < aq.MIN_ENSEMBLE_MEMBERS:
        raise EngineInputError(
            f"climatological ensemble needs >= {aq.MIN_ENSEMBLE_MEMBERS} analog years, "
            f"got {len(analogs)}")
    years = sorted(analogs)
    futs = [
        loop.run_in_executor(
            pool, member_runs, aq.project_weather(observed, analogs[y], planting_date, y),
            *args, last)
        for y in years
    ]
    if progress:
        finished = [0]

        def _tick(_fut) -> None:
            finished[0] += 1
            progress(finished[0], len(futs))

        progress(0, len(futs))
        for f in futs:
            f.add_done_callback(_tick)
    try:
        members = list(await asyncio.gather(*futs))
    except BrokenProcessPool:
        _pool_broken(crop, planting_date)
        raise
    out = _combine(members, last, "in_season", years)
    return _finish(out, last, crop, planting_date, len(members))


async def _run(loop, pool, crop: str, planting_date: date, fn, *a):
    try:
        return await loop.run_in_executor(pool, fn, *a)
    except BrokenProcessPool:
        _pool_broken(crop, planting_date)
        raise


def _pool_broken(crop: str, planting_date: date) -> None:
    """A worker died: the cached pool is unusable; drop it so the next call builds a new one."""
    global _pool
    logger.error("water_loss_pool_broken crop=%s planting=%s: resetting the pool", crop,
                 planting_date.isoformat())
    broken, _pool = _pool, None
    if broken is not None:
        broken.shutdown(wait=False, cancel_futures=True)


def _finish(out: dict, as_of: date, crop: str, planting_date: date, n_members: int) -> dict:
    out["as_of"] = as_of.isoformat()
    logger.info(
        "water_loss_by_stage crop=%s planting=%s as_of=%s season=%s members=%d complete_stages=%d",
        crop, planting_date.isoformat(), out["as_of"], out["season"], n_members,
        sum(1 for s in out["stages"] if s["state"] == "complete"))
    return out
