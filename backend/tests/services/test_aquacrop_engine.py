"""Tests for the AquaCrop-OSPy wrapper (app.services.engines.aquacrop_engine)."""
from __future__ import annotations

import itertools
import warnings
from dataclasses import replace
from datetime import date, timedelta

import pytest

from app.services.engines import aquacrop_engine as eng
from app.services.engines.aquacrop_engine import (
    DailyWeather,
    EngineInputError,
    SoilLayer,
    UnsupportedCropError,
    run_aquacrop,
    run_aquacrop_with_potential,
    supported_crops,
)


# ---------------------------------------------------------------- helpers
def _weather(start=date(2020, 1, 1), n=10, **kw):
    base = {"tmin_c": 5.0, "tmax_c": 15.0, "precip_mm": 0.0, "et0_mm": 2.0}
    base.update(kw)
    return [DailyWeather(day=start + timedelta(days=i), **base) for i in range(n)]


def _soil():
    return [SoilLayer(thickness_m=1.2, wp=0.10, fc=0.22, sat=0.41, ksat_mm_day=1200.0)]


def _run(**over):
    args = {"weather": _weather(), "soil": _soil(), "crop": "Wheat", "planting_date": date(2020, 1, 1)}
    args.update(over)
    return run_aquacrop(**args)


# ------------------------------------------------------- weather validation
def test_empty_weather():
    with pytest.raises(EngineInputError, match="weather"):
        _run(weather=[])


def test_gap_names_first_missing_day():
    w = _weather()
    del w[4]  # 2020-01-05 missing
    with pytest.raises(EngineInputError, match="2020-01-05"):
        _run(weather=w)


def test_duplicate_day():
    w = _weather()
    w[3] = w[2]
    with pytest.raises(EngineInputError):
        _run(weather=w)


def test_unsorted():
    w = _weather()
    w[1], w[2] = w[2], w[1]
    with pytest.raises(EngineInputError):
        _run(weather=w)


@pytest.mark.parametrize("field", ["tmin_c", "tmax_c", "precip_mm", "et0_mm"])
@pytest.mark.parametrize("bad", [float("nan"), None])
def test_nan_or_none(field, bad):
    w = _weather()
    w[2] = replace(w[2], **{field: bad})
    with pytest.raises(EngineInputError, match=field):
        _run(weather=w)


def test_tmax_below_tmin():
    w = _weather()
    w[1] = replace(w[1], tmin_c=10.0, tmax_c=9.0)
    with pytest.raises(EngineInputError, match="tmax"):
        _run(weather=w)


def test_negative_precip():
    w = _weather()
    w[1] = replace(w[1], precip_mm=-0.1)
    with pytest.raises(EngineInputError, match="precip"):
        _run(weather=w)


@pytest.mark.parametrize("bad", [-1.0, 0.05])
def test_et0_negative_or_small_positive_is_error(bad):
    w = _weather()
    w[1] = replace(w[1], et0_mm=bad)
    with pytest.raises(EngineInputError, match="et0"):
        _run(weather=w)


def test_et0_zero_is_clipped_and_warned():
    # Pure validation helper: no AquaCrop run needed.
    w = _weather()
    w[1] = replace(w[1], et0_mm=0.0)
    clean, warns = eng._validate_weather(w)
    assert clean[1].et0_mm == 0.1
    assert any("et0" in x.lower() for x in warns)


def test_planting_before_first_weather_day():
    with pytest.raises(EngineInputError, match="planting"):
        _run(planting_date=date(2019, 12, 31))


def test_planting_on_29_february_is_rejected():
    with pytest.raises(EngineInputError, match="29 February"):
        _run(planting_date=date(2020, 2, 29))


def test_season_ending_on_29_february_runs():
    # 2 Mar 2019 + 364 days = 29 Feb 2020: the season end must not be that day.
    weather = _weather(start=date(2019, 1, 1), n=800, tmin_c=10.0, tmax_c=25.0, precip_mm=2.0)
    res = run_aquacrop(weather, _soil(), "MaizeGDD", date(2019, 3, 2))
    assert res["yield_t_ha"] >= 0


def test_season_crossing_the_year_with_weather_ending_first_is_typed():
    # Summer sowing: maturity is reached in December, but AquaCrop's harvest date
    # (maturity + 30 days) falls after 31 Dec, where the weather ends; AquaCrop-OSPy
    # then schedules no season at all (IndexError).
    weather = _weather(start=date(2021, 7, 1), n=549, tmin_c=10.7, tmax_c=24.7, precip_mm=2.0)
    assert weather[-1].day == date(2022, 12, 31)
    with pytest.raises(eng.WeatherEndsBeforeHarvestError):
        run_aquacrop(weather, _soil(), "MaizeGDD", date(2022, 7, 3), sim_start=date(2021, 7, 3))


# --------------------------------------------------------- soil validation
def test_no_soil_layers():
    with pytest.raises(EngineInputError, match="soil"):
        _run(soil=[])


@pytest.mark.parametrize(
    "kw",
    [
        {"wp": 0.0},
        {"wp": 0.25},            # wp >= fc
        {"fc": 0.41},            # fc >= sat
        {"sat": 1.01},
        {"ksat_mm_day": 0.0},
        {"thickness_m": 0.0},
        {"wp": float("nan")},
    ],
)
def test_bad_soil(kw):
    layer = replace(_soil()[0], **kw)
    with pytest.raises(EngineInputError):
        _run(soil=[layer])


def test_sat_equal_one_is_allowed_by_validation():
    layer = replace(_soil()[0], sat=1.0)
    eng._validate_soil([layer])


# -------------------------------------------------------------- crop / irr
def test_unsupported_crop():
    with pytest.raises(UnsupportedCropError):
        _run(crop="Dragonfruit")
    assert issubclass(UnsupportedCropError, EngineInputError)


def test_supported_crops_has_builtins():
    crops = supported_crops()
    assert "Wheat" in crops and "MaizeGDD" in crops


def test_bad_irrigation():
    with pytest.raises(EngineInputError, match="irrigation"):
        _run(irrigation="drip")


def test_weather_ends_before_harvest():
    # 10 days of weather cannot reach wheat harvest.
    with pytest.raises(EngineInputError, match="weather ends before harvest"):
        _run()


# ------------------------------------------------------------ end to end
def _tunis():
    from aquacrop.utils import get_filepath, prepare_weather

    df = prepare_weather(get_filepath("tunis_climate.txt"))
    return df


def _weather_from_df(df):
    return [
        DailyWeather(
            day=r.Date.date(),
            tmin_c=float(r.MinTemp),
            tmax_c=float(r.MaxTemp),
            precip_mm=float(r.Precipitation),
            et0_mm=float(r.ReferenceET),
        )
        for r in df.itertuples()
    ]


def _sandyloam_layers():
    from aquacrop import Soil

    p = Soil("SandyLoam").profile
    out = []
    for _, g in p.groupby("Layer"):
        f = g.iloc[0]
        out.append(
            SoilLayer(
                thickness_m=round(float(g["dz"].sum()), 2),
                wp=float(f["th_wp"]),
                fc=float(f["th_fc"]),
                sat=float(f["th_s"]),
                ksat_mm_day=float(f["Ksat"]),
            )
        )
    return out


def _direct_reference(df, layers, irrigation):
    """Same setup, driven straight through AquaCrop (no wrapper)."""
    from aquacrop import (
        AquaCropModel,
        Crop,
        InitialWaterContent,
        IrrigationManagement,
        Soil,
    )

    s = Soil("custom", dz=[0.1] * 12, adj_rew=0, calc_cn=1)
    for l in layers:
        s.add_layer(l.thickness_m, l.wp, l.fc, l.sat, l.ksat_mm_day, 100)
    irr = (IrrigationManagement(irrigation_method=0) if irrigation == "rainfed"
           else IrrigationManagement(irrigation_method=1, SMT=[100] * 4))
    m = AquaCropModel("1979/10/01", "1980/09/30", df, s,
                      Crop("Wheat", planting_date="10/01"),
                      InitialWaterContent(value=["FC"]), irrigation_management=irr)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        m.run_model(till_termination=True)
    return float(m.get_simulation_results().iloc[0]["Dry yield (tonne/ha)"])


@pytest.fixture(scope="module")
def tunis():
    df = _tunis()
    return df, _weather_from_df(df), _sandyloam_layers()


def test_end_to_end_matches_direct_aquacrop(tunis):
    df, weather, layers = tunis
    assert weather[0].day < date(1979, 10, 1)  # weather starts before planting
    ref = _direct_reference(df, layers, "rainfed")
    res = run_aquacrop(weather, layers, "Wheat", date(1979, 10, 1))
    assert res["yield_t_ha"] == pytest.approx(ref, rel=0.01)
    assert res["engine"] == "AquaCrop-OSPy"
    assert res["engine_version"]
    assert res["crop"] == "Wheat"
    assert res["irrigation"] == "rainfed"
    assert res["planting_date"] == "1979-10-01"
    assert res["harvest_date"] > res["planting_date"]
    assert res["daily"]
    d0 = res["daily"][0]
    assert d0["day"] == "1979-10-01"
    for k in ("canopy_cover", "biomass", "biomass_ns", "root_zone_water_mm", "water_stress"):
        assert k in d0
    for d in res["daily"]:
        assert d["water_stress"] is None or 0.0 <= d["water_stress"] <= 1.0
    assert res["seasonal_irrigation_mm"] == 0


def test_potential_not_below_water_limited(tunis):
    df, weather, layers = tunis
    out = run_aquacrop_with_potential(weather, layers, "Wheat", date(1979, 10, 1))
    wl, pot = out["water_limited"], out["potential"]
    assert pot["irrigation"] == "full" and wl["irrigation"] == "rainfed"
    assert pot["yield_t_ha"] >= wl["yield_t_ha"]
    assert out["water_gap_pct"] == pytest.approx(
        100 * (1 - wl["yield_t_ha"] / pot["yield_t_ha"]))
    ref_pot = _direct_reference(df, layers, "full")
    assert pot["yield_t_ha"] == pytest.approx(ref_pot, rel=0.01)


@pytest.mark.asyncio
async def test_async_runner(tunis):
    _, weather, layers = tunis
    res = await eng.run_aquacrop_async(weather, layers, "Wheat", date(1979, 10, 1))
    direct = run_aquacrop(weather, layers, "Wheat", date(1979, 10, 1))
    assert res["yield_t_ha"] == pytest.approx(direct["yield_t_ha"])
    eng._shutdown_pool()


# ---------------------------------------------------------- soil mapping
def test_two_layer_soil_maps_to_compartments():
    layers = [
        SoilLayer(0.6, 0.15, 0.30, 0.45, 250.0),
        SoilLayer(1.4, 0.17, 0.32, 0.45, 125.0),
    ]
    s = eng._build_soil(layers)
    p = s.profile
    assert len(p) == 20 and p["dz"].sum() == pytest.approx(2.0)
    top, bottom = p[p["Layer"] == 1], p[p["Layer"] == 2]
    assert top["dz"].sum() == pytest.approx(0.6) and bottom["dz"].sum() == pytest.approx(1.4)
    assert top["th_fc"].iloc[0] == pytest.approx(0.30)
    assert bottom["Ksat"].iloc[0] == pytest.approx(125.0)


def test_non_multiple_thickness_is_split_evenly():
    assert eng._compartments([SoilLayer(0.25, 0.1, 0.2, 0.4, 100.0)]) == pytest.approx([0.25 / 3] * 3)


def test_last_daily_day_is_day_before_harvest(tunis):
    # AquaCrop stamps harvest on the step after the last growth row.
    _, weather, layers = tunis
    res = run_aquacrop(weather, layers, "Wheat", date(1979, 10, 1))
    last = date.fromisoformat(res["daily"][-1]["day"])
    assert last == date.fromisoformat(res["harvest_date"]) - timedelta(days=1)


def test_soil_derives_rew_and_cn_from_layers():
    from app.services.engines.aquacrop_engine import _build_soil
    s = _build_soil([SoilLayer(1.2, 0.1, 0.25, 0.45, 300.0)])
    assert s.adj_rew == 0 and s.calc_cn == 1


# --------------------------------------------------------------- spin-up
def test_spinup_start_after_planting_is_error():
    with pytest.raises(EngineInputError, match="sim_start"):
        _run(sim_start=date(2020, 1, 2))


def test_spinup_start_before_first_weather_day_is_error():
    with pytest.raises(EngineInputError, match="sim_start"):
        _run(sim_start=date(2019, 12, 1))


def test_short_spinup_is_assumed_fc(tunis):
    _, weather, layers = tunis
    res = run_aquacrop(weather, layers, "Wheat", date(1979, 10, 1), sim_start=date(1979, 9, 10))
    assert res["initial_water"] == {"method": "assumed_fc", "spinup_days": 0, "start": "1979-10-01"}
    assert any("spin-up" in w for w in res["warnings"])


def test_spinup_reports_initial_water_and_runs(tunis):
    _, weather, layers = tunis
    start = date(1979, 10, 1) - timedelta(days=eng.MIN_SPINUP_DAYS)
    assert weather[0].day <= start
    res = run_aquacrop(weather, layers, "Wheat", date(1979, 10, 1), sim_start=start)
    assert res["initial_water"] == {
        "method": "spinup", "spinup_days": eng.MIN_SPINUP_DAYS, "start": start.isoformat()}
    assert res["daily"][0]["day"] == "1979-10-01"
    assert res["harvest_date"] > res["planting_date"]
    assert res["yield_t_ha"] > 0


def test_no_sim_start_reports_assumed_fc_without_warning(tunis):
    _, weather, layers = tunis
    res = run_aquacrop(weather, layers, "Wheat", date(1979, 10, 1))
    assert res["initial_water"]["method"] == "assumed_fc"
    assert res["initial_water"]["spinup_days"] == 0
    assert not any("spin-up" in w for w in res["warnings"])


# -------------------------------------------------------------- ensemble
def _by_year(weather):
    out: dict[int, list[DailyWeather]] = {}
    for w in weather:
        out.setdefault(w.day.year, []).append(w)
    return out


def test_analog_date_maps_years_and_feb29():
    assert eng.analog_date(date(1979, 10, 1), 1979, 1985) == date(1985, 10, 1)
    assert eng.analog_date(date(1980, 3, 1), 1979, 1985) == date(1986, 3, 1)
    # campaign Feb 29 (leap) -> analog non-leap: Feb 28 duplicated
    assert eng.analog_date(date(1980, 2, 29), 1979, 1985) == date(1986, 2, 28)
    # analog leap year: campaign has no Feb 29, so the analog Feb 29 is never used
    assert eng.analog_date(date(1979, 2, 28), 1979, 1980) == date(1980, 2, 28)
    assert eng.analog_date(date(1979, 3, 1), 1979, 1980) == date(1980, 3, 1)


def test_project_weather_is_continuous_and_reaches_season_end():
    _, weather, _ = _tunis_weather()
    obs = [w for w in weather if w.day <= date(1980, 1, 15)]
    analog = [w for w in weather if date(1984, 1, 1) <= w.day <= date(1986, 12, 31)]
    out = eng.project_weather(obs, analog, date(1979, 10, 1), 1984)
    assert out[: len(obs)] == obs
    assert out[-1].day == date(1979, 10, 1) + timedelta(days=364)
    days = [w.day for w in out]
    assert all(b - a == timedelta(days=1) for a, b in itertools.pairwise(days))
    nxt = out[len(obs)]
    src = next(w for w in analog if w.day == date(1985, 1, 16))
    assert (nxt.day, nxt.precip_mm, nxt.et0_mm) == (date(1980, 1, 16), src.precip_mm, src.et0_mm)


def test_project_weather_missing_analog_day_is_error():
    _, weather, _ = _tunis_weather()
    obs = [w for w in weather if w.day <= date(1980, 1, 15)]
    analog = [w for w in weather if date(1984, 1, 1) <= w.day <= date(1984, 6, 30)]
    with pytest.raises(EngineInputError, match="analog year 1984"):
        eng.project_weather(obs, analog, date(1979, 10, 1), 1984)


def _tunis_weather():
    df = _tunis()
    return df, _weather_from_df(df), _sandyloam_layers()


@pytest.mark.asyncio
async def test_ensemble_in_season_and_complete(tunis):
    _, weather, layers = tunis
    planting = date(1979, 10, 1)
    by_year = _by_year(weather)
    analogs = {y: by_year[y] + by_year[y + 1] for y in range(1980, 1985)}
    start = planting - timedelta(days=60)
    try:
        # observed stops mid-season -> ensemble
        obs = [w for w in weather if w.day <= date(1980, 2, 1)]
        res = await eng.simulate(obs, analogs, layers, "Wheat", planting, "rainfed", start)
        assert res["status"] == "in_season"
        y, p = res["yield_t_ha"], res["potential_yield_t_ha"]
        assert y["p10"] <= y["p50"] <= y["p90"]
        assert p["p10"] <= p["p50"] <= p["p90"]
        assert res["ensemble"] == {"n_years": 5, "method": "climatological_ensemble",
                                   "years": [1980, 1981, 1982, 1983, 1984]}
        assert res["last_weather_day"] == "1980-02-01"
        assert res["initial_water"]["method"] == "spinup"
        assert res["harvest_date"] > "1980-02-01"
        assert res["engine"] == "AquaCrop-OSPy"
        assert res["capabilities"] == {"water_limited": True, "potential": True, "nitrogen": False}
        flags = [d["projected"] for d in res["daily"]]
        assert flags[0] is False and flags[-1] is True
        assert flags == sorted(flags)  # observed rows first
        assert res["daily"][0]["day"] == "1979-10-01"
        # full irrigation: headline yield is the irrigated one
        full = await eng.simulate(obs, analogs, layers, "Wheat", planting, "full", start)
        assert full["yield_t_ha"] == full["potential_yield_t_ha"]
        assert full["water_gap_pct"] == pytest.approx(res["water_gap_pct"])
        # observed reaches harvest -> single run, no ensemble
        done = await eng.simulate(weather, analogs, layers, "Wheat", planting, "rainfed", start)
        assert done["status"] == "complete"
        assert done["ensemble"] is None
        assert isinstance(done["yield_t_ha"], float)
        direct = run_aquacrop(weather, layers, "Wheat", planting, sim_start=start)
        assert done["yield_t_ha"] == pytest.approx(direct["yield_t_ha"])
        assert all(d["projected"] is False for d in done["daily"])
    finally:
        eng._shutdown_pool()


@pytest.mark.asyncio
async def test_ensemble_needs_enough_analog_years(tunis):
    _, weather, layers = tunis
    by_year = _by_year(weather)
    obs = [w for w in weather if w.day <= date(1980, 2, 1)]
    try:
        with pytest.raises(EngineInputError, match="analog years"):
            await eng.simulate(obs, {1980: by_year[1980] + by_year[1981]}, layers, "Wheat",
                               date(1979, 10, 1))
    finally:
        eng._shutdown_pool()


@pytest.mark.asyncio
async def test_no_analogs_and_short_weather_raises_weather_ends(tunis):
    _, weather, layers = tunis
    obs = [w for w in weather if w.day <= date(1980, 2, 1)]
    try:
        with pytest.raises(eng.WeatherEndsBeforeHarvestError):
            await eng.simulate(obs, {}, layers, "Wheat", date(1979, 10, 1))
    finally:
        eng._shutdown_pool()


@pytest.mark.asyncio
async def test_analogs_loader_awaited_only_when_needed(tunis):
    _, weather, layers = tunis
    calls = []

    async def loader():
        calls.append(1)
        return {}

    try:
        await eng.simulate(weather, loader, layers, "Wheat", date(1979, 10, 1))
        assert calls == []
        obs = [w for w in weather if w.day <= date(1980, 2, 1)]
        with pytest.raises(eng.WeatherEndsBeforeHarvestError):
            await eng.simulate(obs, loader, layers, "Wheat", date(1979, 10, 1))
        assert calls == [1]
    finally:
        eng._shutdown_pool()


def test_gdd_crop_with_too_few_degree_days_is_weather_ends_before_harvest(tunis):
    _, weather, layers = tunis
    obs = [w for w in weather if w.day <= date(1980, 2, 1)]
    with pytest.raises(eng.WeatherEndsBeforeHarvestError, match="degree days"):
        run_aquacrop(obs, layers, "WheatGDD", date(1979, 10, 1))


def test_spinup_never_starts_on_previous_planting_anniversary(tunis):
    # AquaCrop plants on the first mm/dd >= start: starting exactly one year
    # earlier would plant a year too early.
    _, weather, layers = tunis
    planting = date(1990, 10, 1)
    res = run_aquacrop(weather, layers, "Wheat", planting, sim_start=planting - timedelta(days=365))
    assert res["initial_water"] == {"method": "spinup", "spinup_days": 364, "start": "1989-10-02"}
    assert res["harvest_date"] > res["planting_date"] == "1990-10-01"
    assert res["daily"][0]["day"] == "1990-10-01"


def test_crop_needing_over_a_year_to_mature_is_typed():
    # Cool site: maize gets ~3 degree days a day, so maturity is more than a year away.
    weather = _weather(start=date(2020, 1, 1), n=800, tmin_c=6.0, tmax_c=16.0, precip_mm=2.0)
    with pytest.raises(eng.CropCannotMatureError, match="one season"):
        run_aquacrop(weather, _soil(), "MaizeGDD", date(2020, 4, 1))


def test_maturity_on_the_last_day_of_the_season_window_is_typed():
    # ~4.66 degree days a day: maize maturity (1700) falls on day 365, which AquaCrop asserts on.
    weather = _weather(start=date(2020, 1, 1), n=800, tmin_c=7.664, tmax_c=17.664, precip_mm=2.0)
    with pytest.raises(eng.CropCannotMatureError, match="one season"):
        run_aquacrop(weather, _soil(), "MaizeGDD", date(2020, 4, 1))


def test_fallow_topsoil_water_covers_the_window_and_stays_physical(tunis):
    _, weather, layers = tunis
    start, end = date(1990, 10, 1), date(1990, 11, 14)
    theta = eng.fallow_topsoil_water(weather, layers, "WheatGDD", start, end,
                                     sim_start=date(1989, 10, 2))
    assert sorted(theta) == [start + timedelta(days=k) for k in range(45)]
    top = layers[0]
    assert all(0.0 < v <= top.sat + 1e-6 for v in theta.values())
    # Rain wets the topsoil: the series is not flat.
    assert max(theta.values()) - min(theta.values()) > 0.01


def test_fallow_window_ending_on_28_february_of_a_leap_year(tunis):
    _, weather, layers = tunis
    start, end = date(1992, 1, 15), date(1992, 2, 28)
    theta = eng.fallow_topsoil_water(weather, layers, "WheatGDD", start, end,
                                     sim_start=date(1991, 3, 1))
    assert max(theta) == end and len(theta) == 45
