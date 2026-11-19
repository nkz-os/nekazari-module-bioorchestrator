"""Water loss by stage: logic with a fake pool and engine, plus one real-engine integration."""
from __future__ import annotations

from concurrent.futures import Future
from datetime import date, timedelta

import pytest

from app.services import water_loss_by_stage as wl
from app.services.engines import aquacrop_engine as aq

from .test_aquacrop_engine import (
    _by_year,
    _sandyloam_layers,
    _soil,
    _tunis,
    _weather,
    _weather_from_df,
)

PLANTING = date(2027, 10, 1)


class FakePool:
    """Runs submitted calls synchronously (stand-in for the process pool)."""

    def submit(self, fn, *a, **k):
        f: Future = Future()
        try:
            f.set_result(fn(*a, **k))
        except BaseException as e:  # noqa: BLE001 - mirrors a pool future
            f.set_exception(e)
        return f


@pytest.fixture(autouse=True)
def fake_pool(monkeypatch):
    monkeypatch.setattr(wl, "_get_pool", lambda: FakePool())


def _stages(*bounds):
    """bounds: 4 (start, end) ISO pairs or None."""
    return [{"index": i + 1, "start": b[0] if b else None, "end": b[1] if b else None}
            for i, b in enumerate(bounds)]


STAGES = _stages(("2027-10-01", "2027-10-20"), ("2027-10-21", "2028-01-10"),
                 ("2028-01-11", "2028-03-01"), ("2028-03-02", "2028-05-30"))


def _member(wl_y=3.0, pot=6.0, stages=STAGES, gains=None, ends=None):
    """What ``member_runs`` returns: relief only for stages complete at ``as_of``."""
    return {"wl": wl_y, "pot": pot, "stages": stages, "relief": {}, "relief_end": {},
            "warnings": [], "engine_version": "3.1.0", "_gains": gains or {1: 0.5, 2: 1.0, 3: 0.2, 4: 0.1},
            "_ends": ends or {}}


def fake_member_runs(template=None, per_call=None):
    """Replacement for ``member_runs`` honouring as_of like the real one."""
    calls = []

    def run(weather, soil, crop, planting, sim_start, params, mulch, as_of):
        if weather[-1].day < date(2028, 5, 30):  # harvest of STAGES: observed stops before it
            raise aq.WeatherEndsBeforeHarvestError("short")
        calls.append((weather, as_of))
        m = dict((per_call(len(calls) - 1) if per_call else template) or _member())
        m["relief"], m["relief_end"] = {}, {}
        for k, state in wl._states(m["stages"], as_of).items():
            if state == "complete":
                m["relief"][k] = m["wl"] + m["_gains"][k]
                end = m["_ends"].get(k)
                m["relief_end"][k] = end if end is not None else m["stages"][k - 1]["end"]
        return m

    run.calls = calls
    return run


def _obs(last: date, start=date(2027, 9, 1)):
    return _weather(start=start, n=(last - start).days + 1)


async def _compute(observed, analogs=None, **kw):
    return await wl.compute(observed, analogs if analogs is not None else {}, _soil(), "Wheat",
                            PLANTING, None, None, None, **kw)


def _analogs(years=(2018, 2019, 2020)):
    return {y: _weather(start=date(y, 1, 1), n=365 * 3) for y in years}


# ----------------------------------------------------------------- finished
async def test_complete_season_single_member(monkeypatch):
    run = fake_member_runs()
    monkeypatch.setattr(wl, "member_runs", run)
    res = await _compute(_obs(date(2028, 6, 15)))
    assert res["season"] == "complete" and res["ensemble"] is None
    assert res["as_of"] == "2028-06-15"
    assert [s["state"] for s in res["stages"]] == ["complete"] * 4
    assert [s["gain_t_ha_model"] for s in res["stages"]] == pytest.approx([0.5, 1.0, 0.2, 0.1])
    assert res["stages"][1]["gain_pct_of_potential"] == pytest.approx(100 * 1.0 / 6.0)
    assert res["stages"][0]["key"] == "establishment" and res["stages"][3]["key"] == "senescence"
    assert res["total"]["water_gap_band"] is None
    assert res["total"]["water_limited_t_ha"] == 3.0 and res["total"]["potential_t_ha"] == 6.0
    assert len(run.calls) == 1


async def test_in_season_only_completed_stages_have_gain(monkeypatch):
    monkeypatch.setattr(wl, "member_runs", fake_member_runs(per_call=lambda i: _member(
        wl_y=3 + i, pot=6 + i)))
    # as_of inside stage 3: stages 1-2 complete, 3 in progress, 4 not reached
    res = await _compute(_obs(date(2028, 2, 1)), _analogs())
    assert res["season"] == "in_season"
    assert res["ensemble"] == {"n_years": 3, "years": [2018, 2019, 2020]}
    st = res["stages"]
    assert [s["state"] for s in st] == ["complete", "complete", "in_progress", "not_reached"]
    assert st[0]["end"] == "2027-10-20" and st[0]["gain_t_ha_model"].keys() == {"p10", "p50", "p90"}
    assert st[1]["gain_pct_of_potential"]["p50"] == pytest.approx(100 * 1.0 / 7.0)
    for s in st[2:]:
        assert s["gain_t_ha_model"] is None and s["gain_pct_of_potential"] is None
        assert s["end"] is None and s["end_in_relief_run"] is None
    assert st[2]["start"] == "2028-01-11" and st[3]["start"] is None


async def test_stage_last_day_on_as_of_is_in_progress(monkeypatch):
    monkeypatch.setattr(wl, "member_runs", fake_member_runs())
    res = await _compute(_obs(date(2028, 1, 10)), _analogs())  # stage 2 ends 2028-01-10
    assert [s["state"] for s in res["stages"]] == ["complete", "in_progress", "not_reached",
                                                  "not_reached"]


async def test_stage_last_day_before_as_of_is_complete(monkeypatch):
    monkeypatch.setattr(wl, "member_runs", fake_member_runs())
    res = await _compute(_obs(date(2028, 1, 11)), _analogs())
    assert [s["state"] for s in res["stages"]] == ["complete", "complete", "in_progress",
                                                  "not_reached"]


async def test_planting_ahead_has_no_figures(monkeypatch):
    monkeypatch.setattr(wl, "member_runs", fake_member_runs())
    res = await _compute(_obs(date(2027, 9, 20)), _analogs())
    assert {s["state"] for s in res["stages"]} == {"not_reached"}
    assert res["season"] == "in_season"


# -------------------------------------------------------------------- total
async def test_total_matches_in_season_formula(monkeypatch):
    ys = [(3.0, 6.0), (4.0, 6.0), (5.0, 8.0)]
    monkeypatch.setattr(wl, "member_runs", fake_member_runs(per_call=lambda i: _member(*ys[i])))
    res = await _compute(_obs(date(2028, 2, 1)), _analogs())
    assert res["total"]["water_gap_pct"] == pytest.approx(100 * (1 - 4 / 6))
    band = res["total"]["water_gap_band"]
    assert band.keys() == {"p10", "p50", "p90"} and band["p10"] <= band["p50"] <= band["p90"]
    assert res["total"]["water_limited_t_ha"]["p50"] == 4.0


async def test_single_member_total_is_plain_ratio(monkeypatch):
    monkeypatch.setattr(wl, "member_runs", fake_member_runs(template=_member(3.0, 6.0)))
    res = await _compute(_obs(date(2028, 6, 15)))
    assert res["total"]["water_gap_pct"] == pytest.approx(50.0)


async def test_pot_zero_gives_null_pct(monkeypatch):
    monkeypatch.setattr(wl, "member_runs", fake_member_runs(template=_member(0.0, 0.0)))
    res = await _compute(_obs(date(2028, 6, 15)))
    assert res["total"]["water_gap_pct"] is None
    assert all(s["gain_pct_of_potential"] is None for s in res["stages"])
    assert res["stages"][0]["gain_t_ha_model"] == 0.5
    monkeypatch.setattr(wl, "member_runs", fake_member_runs(template=_member(0.0, 0.0)))
    res = await _compute(_obs(date(2028, 2, 1)), _analogs())
    assert res["total"]["water_gap_pct"] is None and res["total"]["water_gap_band"] is None
    assert res["stages"][0]["gain_pct_of_potential"] is None


async def test_negative_gain_returned_raw(monkeypatch):
    monkeypatch.setattr(wl, "member_runs", fake_member_runs(
        template=_member(gains={1: -0.05, 2: 0, 3: 0, 4: 0})))
    res = await _compute(_obs(date(2028, 6, 15)))
    assert res["stages"][0]["gain_t_ha_model"] == pytest.approx(-0.05)


# ------------------------------------------------------------- consistency
async def test_bounds_mismatch_raises(monkeypatch):
    other = _stages(("2027-10-01", "2027-10-21"), ("2027-10-22", "2028-01-10"),
                    ("2028-01-11", "2028-03-01"), ("2028-03-02", "2028-05-30"))
    monkeypatch.setattr(wl, "member_runs", fake_member_runs(
        per_call=lambda i: _member(stages=other if i == 1 else STAGES)))
    with pytest.raises(wl.StageBoundsMismatch) as e:
        await _compute(_obs(date(2028, 2, 1)), _analogs())
    assert e.value.code == "stage_bounds_mismatch"


async def test_state_disagreement_raises(monkeypatch):
    other = _stages(("2027-10-01", "2028-02-10"), None, None, None)
    monkeypatch.setattr(wl, "member_runs", fake_member_runs(
        per_call=lambda i: _member(stages=other if i == 2 else STAGES)))
    with pytest.raises(wl.StageBoundsMismatch):
        await _compute(_obs(date(2028, 2, 1)), _analogs())


async def test_end_in_relief_run_shown_when_it_moves(monkeypatch):
    monkeypatch.setattr(wl, "member_runs", fake_member_runs(
        template=_member(ends={1: "2027-10-23"})))
    res = await _compute(_obs(date(2028, 6, 15)))
    assert res["stages"][0]["end_in_relief_run"] == "2027-10-23"
    assert res["stages"][1]["end_in_relief_run"] is None
    assert any("relief run" in w for w in res["warnings"])


# --------------------------------------------------------------- fallbacks
async def test_leap_day_last_observed_falls_back_to_ensemble(monkeypatch):
    run = fake_member_runs()
    last = date(2028, 2, 29)

    def member(weather, *a):
        if weather[-1].day == last:
            raise aq.CropCannotMatureError("29 Feb")
        return run(weather, *a)

    monkeypatch.setattr(wl, "member_runs", member)
    res = await _compute(_obs(last), _analogs())
    assert res["season"] == "in_season" and res["ensemble"]["n_years"] == 3


async def test_cannot_mature_elsewhere_is_not_swallowed(monkeypatch):
    def member(*a):
        raise aq.CropCannotMatureError("no")

    monkeypatch.setattr(wl, "member_runs", member)
    with pytest.raises(aq.CropCannotMatureError):
        await _compute(_obs(date(2028, 2, 28)), _analogs())
    with pytest.raises(aq.CropCannotMatureError):  # leap day but past planting + 364 d
        await _compute(_obs(date(2028, 2, 29), start=date(2027, 2, 1)), _analogs())


async def test_weather_ends_falls_back_and_awaits_analogs_lazily(monkeypatch):
    run = fake_member_runs()
    loaded = []

    def member(weather, *a):
        if weather[-1].day == date(2028, 2, 1):
            raise aq.WeatherEndsBeforeHarvestError("short")
        return run(weather, *a)

    async def analogs():
        loaded.append(1)
        return _analogs()

    monkeypatch.setattr(wl, "member_runs", member)
    res = await _compute(_obs(date(2028, 2, 1)), analogs)
    assert res["season"] == "in_season" and loaded == [1]
    monkeypatch.setattr(wl, "member_runs", run)
    loaded.clear()
    await _compute(_obs(date(2028, 6, 15)), analogs)
    assert loaded == []  # a finished season never loads climatology


async def test_weather_ends_after_a_year_is_raised(monkeypatch):
    def member(*a):
        raise aq.WeatherEndsBeforeHarvestError("short")

    monkeypatch.setattr(wl, "member_runs", member)
    with pytest.raises(aq.WeatherEndsBeforeHarvestError):
        await _compute(_obs(PLANTING + timedelta(days=364)), _analogs())


async def test_too_few_analogs(monkeypatch):
    monkeypatch.setattr(wl, "member_runs", fake_member_runs())
    with pytest.raises(aq.EngineInputError, match="analog years"):
        await _compute(_obs(date(2028, 2, 1)), _analogs((2018, 2019)))
    with pytest.raises(aq.WeatherEndsBeforeHarvestError):
        await _compute(_obs(date(2027, 9, 20)), {})


async def test_progress(monkeypatch):
    monkeypatch.setattr(wl, "member_runs", fake_member_runs())
    seen = []
    await _compute(_obs(date(2028, 2, 1)), _analogs(), progress=lambda d, t: seen.append((d, t)))
    assert seen[0] == (0, 3) and seen[-1] == (3, 3)
    seen.clear()
    await _compute(_obs(date(2028, 6, 15)), progress=lambda d, t: seen.append((d, t)))
    assert seen == [(1, 1)]


# ------------------------------------------------------------ member_runs
def test_runs_per_member(monkeypatch):
    calls = []

    def fake_run(weather, soil, crop, planting, irrigation="rainfed", sim_start=None, *,
                 relief_stage=None, with_stages=False, **kw):
        calls.append((irrigation, relief_stage, with_stages))
        return {"yield_t_ha": 4.0 + (relief_stage or 0), "stages": STAGES, "warnings": [],
                "engine_version": "x"}

    monkeypatch.setattr(wl.aq, "run_aquacrop", fake_run)
    for as_of, k in ((date(2027, 9, 1), 0), (date(2027, 10, 20), 0), (date(2027, 10, 21), 1),
                     (date(2028, 3, 2), 3), (date.max, 4)):
        calls.clear()
        out = wl.member_runs([], [], "Wheat", PLANTING, None, None, None, as_of)
        assert len(calls) == 2 + k, as_of
        assert calls[0] == ("rainfed", None, True) and calls[1] == ("full", None, False)
        assert sorted(out["relief"]) == list(range(1, k + 1))
        assert [c[1] for c in calls[2:]] == list(range(1, k + 1))
        assert all(c[0] == "rainfed" and c[2] for c in calls[2:])
    assert out["wl"] == 4.0 and out["relief"][2] == 6.0 and out["relief_end"][1] == "2027-10-20"


def test_uses_own_pool(monkeypatch):
    def boom():
        raise AssertionError("shared AquaCrop pool used")

    monkeypatch.setattr(aq, "_get_pool", boom)
    monkeypatch.setattr(wl, "_get_pool", lambda: FakePool())
    import asyncio
    monkeypatch.setattr(wl, "member_runs", fake_member_runs())
    asyncio.run(_compute(_obs(date(2028, 6, 15))))


def test_own_pool_env(monkeypatch):
    monkeypatch.undo()  # real _get_pool
    wl.shutdown_pool()
    monkeypatch.setenv("WATER_LOSS_MAX_WORKERS", "x")
    with pytest.raises(aq.EngineInputError):
        wl._get_pool()
    monkeypatch.setenv("WATER_LOSS_MAX_WORKERS", "0")
    with pytest.raises(aq.EngineInputError):
        wl._get_pool()
    monkeypatch.delenv("WATER_LOSS_MAX_WORKERS")
    try:
        pool = wl._get_pool()
        assert pool._max_workers == 1 and pool is not aq._pool
        assert wl._get_pool() is pool
    finally:
        wl.shutdown_pool()
    assert wl._pool is None


# --------------------------------------------------------- real engine
async def test_wl_and_pot_equal_simulate_real_engine(monkeypatch):
    """Rainfed/potential per member equal ``aq.simulate`` for the same inputs (real engine)."""
    monkeypatch.undo()
    df = _tunis()
    weather, layers = _weather_from_df(df), _sandyloam_layers()
    planting = date(1979, 10, 1)
    start = planting - timedelta(days=60)
    by_year = _by_year(weather)
    analogs = {y: by_year[y] + by_year[y + 1] for y in (1980, 1981, 1982)}
    obs = [w for w in weather if w.day <= date(1980, 2, 1)]
    members = []
    real = wl.member_runs

    def spy(*a):
        out = real(*a)
        members.append(out)
        return out

    monkeypatch.setattr(wl, "member_runs", spy)
    monkeypatch.setattr(wl, "_get_pool", lambda: FakePool())
    res = await wl.compute(obs, analogs, layers, "Wheat", planting, start, None, None)
    ref = await aq.simulate(obs, analogs, layers, "Wheat", planting, "rainfed", start)
    assert res["season"] == "in_season" and res["as_of"] == "1980-02-01"
    assert res["total"]["water_limited_t_ha"] == ref["yield_t_ha"]
    assert res["total"]["potential_t_ha"] == ref["potential_yield_t_ha"]
    assert res["total"]["water_gap_pct"] == pytest.approx(ref["water_gap_pct"])
    assert len(members) == 3
    n_done = sum(1 for s in res["stages"] if s["state"] == "complete")
    assert n_done >= 1  # Wheat sown in October has finished stage 1 by February
    assert all(len(m["relief"]) == n_done for m in members)
    assert {s["state"] for s in res["stages"]} <= {"complete", "in_progress", "not_reached"}
    # finished season: single member equals the direct runs
    done = await wl.compute(weather, analogs, layers, "Wheat", planting, start, None, None)
    direct = aq.run_aquacrop_with_potential(weather, layers, "Wheat", planting, start)
    assert done["season"] == "complete"
    assert done["total"]["water_limited_t_ha"] == direct["water_limited"]["yield_t_ha"]
    assert done["total"]["potential_t_ha"] == direct["potential"]["yield_t_ha"]
    assert [s["state"] for s in done["stages"]] == ["complete"] * 4


async def test_real_pool_round_trip():
    """The module's own spawn pool runs a member end to end (small real job)."""
    df = _tunis()
    weather, layers = _weather_from_df(df), _sandyloam_layers()
    planting = date(1979, 10, 1)
    try:
        res = await wl.compute(weather, {}, layers, "Wheat", planting, planting - timedelta(days=60),
                               None, None)
    finally:
        wl.shutdown_pool()
    assert res["season"] == "complete" and res["total"]["potential_t_ha"] > 0
