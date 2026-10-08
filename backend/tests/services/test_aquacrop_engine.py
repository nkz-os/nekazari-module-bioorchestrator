"""Tests for the AquaCrop-OSPy wrapper (app.services.engines.aquacrop_engine)."""
from __future__ import annotations

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
