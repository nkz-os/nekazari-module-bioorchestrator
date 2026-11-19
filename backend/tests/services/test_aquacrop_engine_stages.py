"""Single-stage relief run and stage bounds of the AquaCrop-OSPy wrapper."""
from __future__ import annotations

import itertools
import json
import warnings
from datetime import date, timedelta
from pathlib import Path

import aquacrop as _aq
import pytest

from app.services.engines import aquacrop_engine as eng
from app.services.engines.aquacrop_engine import EngineInputError, run_aquacrop

from .test_aquacrop_engine import (
    _sandyloam_layers,
    _soil,
    _tunis,
    _weather,
    _weather_from_df,
)

_REAL_IRR = _aq.IrrigationManagement
FIXTURES = Path(__file__).parent / "fixtures"
PLANTING = date(2020, 2, 1)
SIM_START = date(2020, 1, 1)


def _dry(**over):
    """Dry site: WheatGDD gets no rain, so rainfed is far below full irrigation."""
    args = {
        "weather": _weather(start=SIM_START, n=400, tmin_c=5.0, tmax_c=15.0, precip_mm=1.0,
                            et0_mm=3.0),
        "soil": _soil(), "crop": "WheatGDD", "planting_date": PLANTING,
        "irrigation": "rainfed", "sim_start": SIM_START,
    }
    args.update(over)
    return run_aquacrop(**args)


@pytest.fixture(scope="module")
def captured_flux():
    """(flux frame, growth frame) of every ``_assemble`` call, to read per-day irrigation."""
    return []


@pytest.fixture
def capture(monkeypatch, captured_flux):
    captured_flux.clear()
    real = eng._assemble

    def spy(row, growth, flux, *a, **k):
        captured_flux.append((flux, growth))
        return real(row, growth, flux, *a, **k)

    monkeypatch.setattr(eng, "_assemble", spy)
    return captured_flux


def _irrigation_days(flux, start: date) -> list[date]:
    return [start + timedelta(days=i) for i in range(len(flux))
            if float(flux.iloc[i]["IrrDay"]) > 0]


def _bounds(res) -> dict[int, tuple[date, date]]:
    return {s["index"]: (date.fromisoformat(s["start"]), date.fromisoformat(s["end"]))
            for s in res["stages"] if s["start"] is not None}


# ------------------------------------------------------------ unchanged default
def test_stage_keys():
    assert eng.STAGE_KEYS == ("establishment", "canopy_growth", "max_canopy", "senescence")


def test_default_output_unchanged():
    ref = json.loads((FIXTURES / "run_aquacrop_main.json").read_text())
    df = _tunis()
    weather, layers = _weather_from_df(df), _sandyloam_layers()
    for year in (1979, 1982):
        p = date(year, 10, 1)
        got = {
            f"{year}_rainfed": run_aquacrop(weather, layers, "Wheat", p),
            f"{year}_full": run_aquacrop(weather, layers, "Wheat", p, "full"),
            f"{year}_spinup": run_aquacrop(weather, layers, "Wheat", p, "rainfed",
                                           p - timedelta(days=60),
                                           topsoil_until=p + timedelta(days=5)),
        }
        for k, v in got.items():
            assert json.loads(json.dumps(v)) == ref[k], k
            assert "stages" not in v and "relief_stage" not in v


# ------------------------------------------------------------------ validation
def test_relief_requires_rainfed():
    with pytest.raises(EngineInputError, match="rainfed"):
        _dry(irrigation="full", relief_stage=2)


@pytest.mark.parametrize("bad", [0, 5, -1, True, 2.0, "2"])
def test_relief_stage_out_of_range(bad):
    with pytest.raises(EngineInputError, match="relief_stage"):
        _dry(relief_stage=bad)


# ---------------------------------------------------------------------- relief
@pytest.mark.parametrize("x", [1, 2, 3, 4])
def test_relief_irrigates_only_in_stage(x, capture):
    res = _dry(relief_stage=x, with_stages=True)
    assert res["relief_stage"] == x
    flux, _ = capture[-1]
    days = _irrigation_days(flux, date.fromisoformat(res["initial_water"]["start"]))
    assert days, "dry site: relief must irrigate"
    lo, hi = _bounds(res)[x]
    # Irrigation on day d uses the stage of day d-1, so stages 2-4 irrigate in
    # [start+1, end+1]. Stage 1 may irrigate from its first day: the library forces stage 1
    # on DAP 1 (spike: X=1 window is [DAP1, end_1+1]).
    first = lo if x == 1 else lo + timedelta(days=1)
    for d in days:
        assert first <= d <= hi + timedelta(days=1), (x, d, lo, hi)


@pytest.mark.parametrize("x", [1, 2, 3, 4])
def test_off_stages_use_smt_minus_1000(x, monkeypatch):
    """SMT 0 still irrigates when depletion exceeds the TAW under drought: off must be -1000.

    The SMT=0 leak was seen on real dry-year weather in the Task 1 spike (task-1-report.md,
    section (b)) and could not be reproduced on synthetic weather, so the argument is pinned.
    """
    import aquacrop

    seen = []
    real = aquacrop.IrrigationManagement

    def spy(*a, **k):
        seen.append(k)
        return real(*a, **k)

    monkeypatch.setattr(aquacrop, "IrrigationManagement", spy)
    _dry(relief_stage=x)
    assert seen == [{"irrigation_method": 1,
                     "SMT": [100 if i == x - 1 else -1000 for i in range(4)]}]


def test_all_off_equals_rainfed(monkeypatch):
    rainfed = _dry()
    import aquacrop

    monkeypatch.setattr(aquacrop, "IrrigationManagement",
                        lambda **kw: _REAL_IRR(irrigation_method=1, SMT=[-1000] * 4))
    off = _dry()
    assert off["yield_t_ha"] == rainfed["yield_t_ha"]
    assert off["seasonal_irrigation_mm"] == 0


def test_relief_yield_not_below_rainfed_dry_year():
    base = _dry()["yield_t_ha"]
    for x in (1, 2, 3, 4):
        assert _dry(relief_stage=x)["yield_t_ha"] >= base - 0.05, x


def test_relief_adds_irrigation_and_key():
    base = _dry()
    assert "relief_stage" not in base
    assert _dry(relief_stage=2)["seasonal_irrigation_mm"] > 0


# ---------------------------------------------------------------------- stages
def _oracle_stage_trace(**over):
    """Task 1 oracle: step the library by hand and read its growth_stage per day."""
    from aquacrop import (  # noqa: F401
        AquaCropModel,
        FieldMngt,
        InitialWaterContent,
        IrrigationManagement,
    )
    weather = over.get("weather") or _weather(start=SIM_START, n=400, tmin_c=5.0, tmax_c=15.0,
                                              precip_mm=1.0, et0_mm=3.0)
    import pandas as pd
    df = pd.DataFrame({
        "MinTemp": [w.tmin_c for w in weather], "MaxTemp": [w.tmax_c for w in weather],
        "Precipitation": [w.precip_mm for w in weather], "ReferenceET": [w.et0_mm for w in weather],
        "Date": pd.to_datetime([w.day for w in weather]),
    })
    soil_model = eng._build_soil(_soil())
    crop_model = eng._aquacrop_crop("WheatGDD", PLANTING, {})
    sim_end = min(weather[-1].day, PLANTING + timedelta(days=364))
    m = AquaCropModel(SIM_START.strftime("%Y/%m/%d"), sim_end.strftime("%Y/%m/%d"), df,
                      soil_model, crop_model, InitialWaterContent(value=["FC"]),
                      irrigation_management=IrrigationManagement(irrigation_method=0),
                      off_season=True)
    trace, first = [], True
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        while True:
            m.run_model(num_steps=1, initialize_model=first)
            first = False
            trace.append(int(m._init_cond.growth_stage))
            if m._clock_struct.model_is_finished:
                break
    return trace


def test_stage_bounds_match_library():
    res = _dry(with_stages=True)
    trace = _oracle_stage_trace()
    expected: dict[int, list[date]] = {}
    for i, s in enumerate(trace):
        if s >= 1:
            d = SIM_START + timedelta(days=i)
            expected.setdefault(s, []).append(d)
    got = res["stages"]
    assert [s["index"] for s in got] == [1, 2, 3, 4]
    for s in got:
        days = expected.get(s["index"])
        if days is None:
            assert s["start"] is None and s["end"] is None
        else:
            assert (s["start"], s["end"]) == (min(days).isoformat(), max(days).isoformat())
    assert all(s["start"] for s in got), "all four stages reached on this fixture"


def test_stages_contiguous_and_start_on_planting():
    st = _dry(with_stages=True)["stages"]
    assert st[0]["start"] == PLANTING.isoformat()
    for a, b in itertools.pairwise(st):
        assert date.fromisoformat(b["start"]) == date.fromisoformat(a["end"]) + timedelta(days=1)


def test_with_stages_same_result():
    a, b = _dry(), _dry(with_stages=True)
    assert "stages" not in a
    b = {k: v for k, v in b.items() if k != "stages"}
    assert a == b


def test_last_stage_ends_the_day_before_harvest():
    res = _dry(with_stages=True)
    assert date.fromisoformat(res["stages"][3]["end"]) + timedelta(days=1) == \
        date.fromisoformat(res["harvest_date"])


def test_with_stages_error_mapping_cannot_mature():
    weather = _weather(start=date(2020, 1, 1), n=800, tmin_c=6.0, tmax_c=16.0, precip_mm=2.0)
    with pytest.raises(eng.CropCannotMatureError, match="one season"):
        run_aquacrop(weather, _soil(), "MaizeGDD", date(2020, 4, 1), with_stages=True)


def test_with_stages_error_mapping_weather_ends():
    w = _weather(start=SIM_START, n=100, tmin_c=5.0, tmax_c=15.0, precip_mm=1.0, et0_mm=3.0)
    with pytest.raises(eng.WeatherEndsBeforeHarvestError):
        _dry(weather=w, with_stages=True)


def test_calendar_days_crop_steps_and_bounds_match():
    """Built-in calendar-days crop ("Wheat"): stepping gives the same result and sane bounds."""
    df = _tunis()
    weather, layers = _weather_from_df(df), _sandyloam_layers()
    p = date(1979, 10, 1)
    a = run_aquacrop(weather, layers, "Wheat", p)
    b = run_aquacrop(weather, layers, "Wheat", p, with_stages=True)
    st = b.pop("stages")
    assert a == b
    assert st[0]["start"] == p.isoformat()
    reached = [s for s in st if s["start"]]
    assert len(reached) >= 2
    for x, y in itertools.pairwise(reached):
        assert date.fromisoformat(y["start"]) == date.fromisoformat(x["end"]) + timedelta(days=1)
    assert date.fromisoformat(reached[-1]["end"]) + timedelta(days=1) == \
        date.fromisoformat(b["harvest_date"])
