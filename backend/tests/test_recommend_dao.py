"""Median reference and evidence listing — query shape and result mapping."""
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.graph import dao as dao_mod
from app.graph.dao import GraphDAO


class _Res:
    def __init__(self, rows):
        self._rows = rows

    async def single(self):
        return self._rows[0] if self._rows else None

    def __aiter__(self):
        self._it = iter(self._rows)
        return self

    async def __anext__(self):
        try:
            return next(self._it)
        except StopIteration:
            raise StopAsyncIteration


def _dao(*results):
    calls = []
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    queue = list(results)

    async def run(query, **params):
        calls.append((query, params))
        return _Res(queue.pop(0))

    session.run = run
    driver = MagicMock()
    driver.session.return_value = session
    return GraphDAO(driver), calls


@pytest.fixture(autouse=True)
def _clear_cache():
    dao_mod._MEDIAN_CACHE.clear()
    dao_mod._RECOMMEND_CACHE.clear()
    yield
    dao_mod._RECOMMEND_CACHE.clear()


async def batch_via_per_crop(self, crops, similar_sites, irrigation_regime=None, top_n=10, **kw):
    """Stand-in for extrapolate_varieties_batch over the (mocked) per-crop method.

    The batch is defined as per-crop extrapolate_varieties; that equivalence is
    proven on real Neo4j in tests/graph/test_extrapolate_batch.py.
    """
    return {c: (await self.extrapolate_varieties(
        crop=c, irrigation_regime=irrigation_regime, top_n=top_n,
        similar_sites_override=similar_sites, **kw))["ranked_varieties"]
        for c in dict.fromkeys(crops)}


@pytest.fixture(autouse=True)
def _batch_via_per_crop():
    from unittest.mock import patch
    with patch.object(GraphDAO, "extrapolate_varieties_batch", batch_via_per_crop):
        yield


@pytest.fixture(autouse=True)
def _shared_sites():
    """recommend_for_conditions looks similar sites up once; keep it off the fake driver."""
    from unittest.mock import patch
    with patch.object(GraphDAO, "get_similar_sites",
                      AsyncMock(return_value=[{"name": "site-a", "distance": None}])), \
         patch.object(GraphDAO, "_crops_with_analog_trials",
                      AsyncMock(side_effect=lambda eppos, names, **kw: set(eppos))):
        yield


async def test_median_with_regime():
    dao, calls = _dao([{"median": 5000.0, "n": 42}])
    out = await dao.get_crop_yield_median("TRZAX", "uri:secano")
    assert out == {"median_kg_ha": 5000.0, "n_trials": 42, "scope": "crop×irrigation"}
    assert calls[0][1]["irrigation_uri"] == "uri:secano"


async def test_median_without_regime_uses_all_trials():
    dao, calls = _dao([{"median": 4800.0, "n": 90}])
    out = await dao.get_crop_yield_median("TRZAX", None)
    assert out["scope"] == "crop"
    assert calls[0][1]["irrigation_uri"] is None


async def test_median_no_trials():
    dao, _ = _dao([{"median": None, "n": 0}])
    assert (await dao.get_crop_yield_median("ZZZZZ", None))["median_kg_ha"] is None


async def test_median_is_cached():
    dao, calls = _dao([{"median": 5000.0, "n": 42}])
    await dao.get_crop_yield_median("TRZAX", None)
    await dao.get_crop_yield_median("TRZAX", None)
    assert len(calls) == 1


async def test_evidence_maps_and_paginates():
    rows = [{"trial_id": "k1", "variety": "V1", "site": "site-a", "year": 2020, "yield_kg_ha": 5000.0,
             "irrigation_regime": "secano", "production_system": "conventional", "source_id": "SRC1",
             "confidence": "high"}]
    dao, calls = _dao([{"total": 7}], rows)
    out = await dao.list_trial_evidence(crop="TRZAX", similar_sites=["site-a"], variety=None,
                                        irrigation_uri=None, page=2, page_size=5)
    assert out["total"] == 7 and out["page"] == 2 and out["page_size"] == 5
    assert out["items"][0]["trial_id"] == "k1"
    assert calls[1][1]["skip"] == 5 and calls[1][1]["limit"] == 5


async def test_evidence_page_past_end():
    dao, _ = _dao([{"total": 3}], [])
    out = await dao.list_trial_evidence(crop="TRZAX", similar_sites=["site-a"], variety=None,
                                        irrigation_uri=None, page=9, page_size=50)
    assert out == {"items": [], "total": 3, "page": 9, "page_size": 50}


async def test_evidence_query_is_total_order_and_page_clamped():
    dao, calls = _dao([{"total": 1}], [])
    out = await dao.list_trial_evidence(crop="TRZAX", similar_sites=["site-a"], variety=None,
                                        irrigation_uri=None, page=0, page_size=5)
    assert out["page"] == 1 and calls[1][1]["skip"] == 0
    q = calls[1][0]
    assert "ORDER BY year DESC, variety, site, trial_id" in q
    assert "ORDER BY ts.name" in q  # ordered before collect


# ── recommend_for_conditions ────────────────────────────────────────────────
from unittest.mock import patch


def _variety(mean=5500.0, n=12):
    return {"variety": "V1", "variety_uri": "urn:x", "mean_yield_kg_ha": mean, "min_yield_kg_ha": 4000.0,
            "max_yield_kg_ha": 7000.0, "stddev_yield_kg_ha": 550.0, "numeric_yield_count": n,
            "trial_count": n, "trial_sites": ["site-a"], "trial_years": [2020], "disease_scores": {},
            "confidence": "high"}


def _conds(**kw):
    base = {"climate_class": "Cfb", "soil_type": None, "soil_ph": None, "soil_texture": None,
            "irrigation_regime": None, "management": "any", "season": "all", "crops": None, "top_n": 10}
    return {**base, **kw}


def _patched(extrap, crops, median=5000.0, heat=None):
    return (
        patch.object(GraphDAO, "get_available_crops",
                     AsyncMock(return_value=[{"eppo_code": c, "scientific_name": c} for c in crops])),
        patch.object(GraphDAO, "extrapolate_varieties", extrap),
        patch.object(GraphDAO, "get_crop_yield_medians", AsyncMock(
            side_effect=lambda crops, irrigation_uri: {
                c: {"median_kg_ha": median, "n_trials": 40, "scope": "crop"} for c in crops})),
        patch.object(GraphDAO, "get_soil_suitability", AsyncMock(return_value=None)),
        patch.object(GraphDAO, "get_heat_tolerance", AsyncMock(return_value=heat)),
    )


async def _run(conds, extrap_by_crop, median=5000.0, heat=None):
    dao, _ = _dao()

    async def extrap(self_, crop, **_):
        return {"ranked_varieties": extrap_by_crop.get(crop, [])}

    p = _patched(extrap, list(extrap_by_crop), median, heat)
    with p[0], p[1], p[2], p[3], p[4]:
        return await dao.recommend_for_conditions(conds)


async def test_ranked_by_relative_yield():
    out = await _run(_conds(), {"TRZAX": [_variety(5500.0)], "HORVX": [_variety(6000.0)]})
    assert [r["crop"]["eppo"] for r in out["recommendations"]] == ["HORVX", "TRZAX"]
    assert out["status"] == "ok"
    assert "climate_detail" not in out["conditions"]


async def test_crop_without_varieties_is_omitted():
    out = await _run(_conds(), {"TRZAX": [_variety()], "ZZZZZ": []})
    assert [r["crop"]["eppo"] for r in out["recommendations"]] == ["TRZAX"]
    assert out["data_quality"] == {"crops_evaluated": 2, "crops_with_trials": 1,
                                   "crops_with_analog_trials": 2}


async def test_organic_scales_yields_and_records_assumption():
    out = await _run(_conds(management="organic"), {"TRZAX": [_variety(5000.0)]})
    rec = out["recommendations"][0]
    assert rec["yield"]["expected_kg_ha"] == pytest.approx(4000.0)
    assert rec["yield"]["interval"] == [pytest.approx(3200.0), pytest.approx(5600.0)]
    assert rec["assumptions"][0]["id"] == "organic_yield_factor"
    assert rec["fit"]["relative_yield_pct"] == pytest.approx(0.0)


async def test_no_climate_marks_unknown_and_gap():
    rec = (await _run(_conds(), {"TRZAX": [_variety()]}))["recommendations"][0]
    assert rec["suitability"]["water"]["level"] == "unknown"
    assert rec["suitability"]["frost"]["level"] == "unknown"
    assert "climate_detail_unavailable" in rec["trust"]["data_gaps"]


async def test_water_from_climate_numbers():
    c = _conds(annual_et0_mm=1000.0, annual_rainfall_mm=300.0)
    rec = (await _run(c, {"TRZAX": [_variety()]}))["recommendations"][0]
    assert rec["suitability"]["water"]["level"] in {"low", "medium", "high"}
    assert rec["suitability"]["water"]["etc_mm"] > 0


async def test_climate_detail_dict_is_accepted():
    c = _conds(climate_detail={"annual_et0_mm": 1000.0, "annual_rainfall_mm": 300.0})
    rec = (await _run(c, {"TRZAX": [_variety()]}))["recommendations"][0]
    assert rec["suitability"]["water"]["etc_mm"] > 0


async def test_frost_risk_uses_margin():
    heat = {"frost_damage_c": -3.0}
    # -7 - 5 = -12 <= -3 -> risk with the default margin
    rec = (await _run(_conds(coldest_month_min_c=-7.0, annual_rainfall_mm=300.0, annual_et0_mm=900.0),
                      {"TRZAX": [_variety()]}, heat=heat))["recommendations"][0]
    assert rec["suitability"]["frost"]["level"] == "risk"
    assert {"id": "frost_margin_c", "value": 5.0,
            "citation": "ASSUMPTION: conservative default, not a published standard; editable"} in rec["assumptions"]
    assert "climate_detail_unavailable" not in rec["trust"]["data_gaps"]


async def test_frost_none_when_margin_small_and_known():
    heat = {"frost_damage_c": -10.0}
    out = await _run(_conds(coldest_month_min_c=2.0, frost_margin_c=0.0), {"TRZAX": [_variety()]}, heat=heat)
    rec = out["recommendations"][0]
    assert rec["suitability"]["frost"]["level"] == "none"
    assert any(a["id"] == "frost_margin_c" and a["value"] == 0.0 for a in rec["assumptions"])


async def test_frost_unknown_without_heat_tolerance():
    rec = (await _run(_conds(coldest_month_min_c=-7.0), {"TRZAX": [_variety()]}))["recommendations"][0]
    assert rec["suitability"]["frost"]["level"] == "unknown"


async def test_season_filter_uses_sowing_type():
    out = await _run(_conds(season="spring"), {"TRZAX": [_variety()], "ZEAMX": [_variety()]})
    assert [r["crop"]["eppo"] for r in out["recommendations"]] == ["ZEAMX"]


async def test_failing_crop_is_skipped():
    dao, _ = _dao()

    async def extrap(self_, crop, **_):
        if crop == "BAD":
            raise RuntimeError("boom")
        return {"ranked_varieties": [_variety()]}

    p = _patched(extrap, ["BAD", "TRZAX"])
    with p[0], p[1], p[2], p[3], p[4]:
        out = await dao.recommend_for_conditions(_conds())
    assert [r["crop"]["eppo"] for r in out["recommendations"]] == ["TRZAX"]


_VEC = {"annual_rainfall_mm": 500.0, "annual_et0_mm": 900.0, "coldest_month_min_c": 1.0, "annual_temp_c": 14.0}


async def _run_hybrid(conds, first, second):
    dao, _ = _dao()
    calls = []

    async def extrap(self_, crop, **kw):
        calls.append(kw)
        return {"ranked_varieties": first if "target_features" not in kw else second}

    p = _patched(extrap, ["TRZAX"])
    with p[0], p[1], p[2], p[3], p[4]:
        out = await dao.recommend_for_conditions(conds)
    return out, calls


async def test_koppen_hit_does_not_retry(hybrid):
    out, calls = await _run_hybrid(_conds(**_VEC), [_variety()], [_variety(1.0)])
    assert len(calls) == 1 and "target_features" not in calls[0]
    assert out["recommendations"][0]["trust"]["similarity"] == "koppen"


async def test_koppen_miss_with_vector_retries_v2(hybrid):
    out, calls = await _run_hybrid(_conds(**_VEC), [], [_variety()])
    assert len(calls) == 2
    assert calls[1]["vector_version"] == "v2"
    assert calls[1]["target_features"] == {"rainfall": 500.0, "et0": 900.0, "coldest_min": 1.0,
                                           "annual_temp": 14.0}
    assert out["recommendations"][0]["trust"]["similarity"] == "vector_v2_fallback"


async def test_koppen_miss_incomplete_vector_no_retry(hybrid):
    c = _conds(annual_rainfall_mm=500.0, annual_et0_mm=900.0)
    out, calls = await _run_hybrid(c, [], [_variety()])
    assert len(calls) == 1
    assert out["recommendations"] == []


async def test_trust_capped_low_without_expected_yield_and_gaps_deduped():
    v = _variety()
    v["mean_yield_kg_ha"] = None
    rec = (await _run(_conds(), {"TRZAX": [v]}))["recommendations"][0]
    gaps = rec["trust"]["data_gaps"]
    assert "no_expected_yield" in gaps
    assert rec["trust"]["level"] == "low"
    assert len(gaps) == len(set(gaps))


# ── fix round 1 ─────────────────────────────────────────────────────────────
@pytest.fixture
def hybrid(monkeypatch):
    monkeypatch.setenv("AGROCLIMATIC_VECTOR", "hybrid")


async def test_heat_tolerance_is_looked_up_by_species_slug():
    dao, _ = _dao()

    async def extrap(self_, crop, **_):
        return {"ranked_varieties": [_variety()]}

    p = _patched(extrap, ["TRZAX"], heat={"frost_damage_c": -3.0})
    with p[0], p[1], p[2], p[3], p[4] as heat_mock:
        out = await dao.recommend_for_conditions(_conds(coldest_month_min_c=-7.0))
    heat_mock.assert_awaited_with("wheat")
    assert out["recommendations"][0]["suitability"]["frost"]["level"] == "risk"


async def test_season_filter_applied_before_cap():
    dao, _ = _dao()
    crops = [f"X{i:04d}" for i in range(35)]
    crops[32] = "ZEAMX"
    seen = []

    async def extrap(self_, crop, **_):
        seen.append(crop)
        return {"ranked_varieties": [_variety()]}

    p = _patched(extrap, crops)
    with p[0], p[1], p[2], p[3], p[4]:
        out = await dao.recommend_for_conditions(_conds(season="spring"))
    assert seen == ["ZEAMX"]
    assert out["data_quality"]["crops_evaluated"] == 1


async def test_kill_switch_v1_never_retries(monkeypatch):
    monkeypatch.setenv("AGROCLIMATIC_VECTOR", "v1")
    out, calls = await _run_hybrid(_conds(**_VEC), [], [_variety()])
    assert len(calls) == 1
    assert out["recommendations"] == []


async def test_invalid_mode_falls_back_to_v1(monkeypatch):
    monkeypatch.setenv("AGROCLIMATIC_VECTOR", "bogus")
    _, calls = await _run_hybrid(_conds(**_VEC), [], [_variety()])
    assert len(calls) == 1


async def test_invalid_mode_logs_critical_once(monkeypatch, caplog):
    import logging

    monkeypatch.setattr(dao_mod, "_INVALID_VECTOR_LOGGED", False)
    monkeypatch.setenv("AGROCLIMATIC_VECTOR", "v9")
    with caplog.at_level(logging.CRITICAL, logger="app.graph.dao"):
        await _run_hybrid(_conds(**_VEC), [_variety()], [])
        dao_mod._RECOMMEND_CACHE.clear()
        await _run_hybrid(_conds(**_VEC), [_variety()], [])
    assert len([r for r in caplog.records if r.levelno == logging.CRITICAL]) == 1


async def test_v2_without_vector_keeps_koppen_path(monkeypatch):
    monkeypatch.setenv("AGROCLIMATIC_VECTOR", "v2")
    c = _conds(annual_rainfall_mm=500.0, annual_et0_mm=900.0)
    _, calls = await _run_hybrid(c, [], [_variety()])
    assert len(calls) == 1 and "target_features" not in calls[0]


async def test_hybrid_retries_v2(hybrid):
    _, calls = await _run_hybrid(_conds(**_VEC), [], [_variety()])
    assert len(calls) == 2


async def test_non_numeric_koppen_and_empty_v2_omits_crop(hybrid):
    nonnum = _variety()
    nonnum["mean_yield_kg_ha"] = None
    out, calls = await _run_hybrid(_conds(**_VEC), [nonnum], [])
    assert len(calls) == 2
    assert out["recommendations"] == []


async def test_zero_rainfall_is_high_deficit_not_unknown():
    rec = (await _run(_conds(annual_rainfall_mm=0.0, annual_et0_mm=1000.0),
                      {"TRZAX": [_variety()]}))["recommendations"][0]
    assert rec["suitability"]["water"]["level"] == "high"


async def test_water_gap_when_rain_missing():
    rec = (await _run(_conds(annual_et0_mm=1000.0, coldest_month_min_c=1.0),
                      {"TRZAX": [_variety()]}))["recommendations"][0]
    assert rec["suitability"]["water"]["level"] == "unknown"
    assert "climate_detail_unavailable" in rec["trust"]["data_gaps"]


async def test_frost_tolerance_gap():
    rec = (await _run(_conds(coldest_month_min_c=1.0, annual_et0_mm=1000.0, annual_rainfall_mm=300.0),
                      {"TRZAX": [_variety()]}, heat={"frost_damage_c": None}))["recommendations"][0]
    assert "frost_tolerance_unavailable" in rec["trust"]["data_gaps"]


async def test_recommendation_id_ignores_presentation_params():
    a = (await _run(_conds(top_n=5), {"TRZAX": [_variety()]}))["recommendations"][0]
    b = (await _run(_conds(top_n=10, crops=["TRZAX"]), {"TRZAX": [_variety()]}))["recommendations"][0]
    assert a["recommendation_id"] == b["recommendation_id"]


async def test_evidence_sources_filled_and_gap_gone():
    v = _variety()
    v["source_ids"] = ["src-b", "src-a"]
    rec = (await _run(_conds(), {"TRZAX": [v]}))["recommendations"][0]
    assert rec["evidence"]["sources"] == ["src-a", "src-b"]
    assert "sources_unavailable" not in rec["trust"]["data_gaps"]


_ROW = {"variety": "V1", "mean_yield": 5000.0, "min_yield": 4000.0, "max_yield": 6000.0,
        "stddev_yield": 400.0, "numeric_yield_count": 5, "trial_count": 5, "derived_count": 0,
        "years": [2020], "sites": ["site-a"], "irrigation_regimes": [], "production_systems": [],
        "disease_scores_list": [], "agronomic_traits_list": [], "confidence_levels": ["high"],
        "source_ids": ["s2", "s1", None]}


async def test_extrapolate_maps_sorted_source_ids():
    dao, calls = _dao([_ROW])
    dao.get_similar_sites = AsyncMock(return_value=[{"name": "site-a", "distance": None}])
    out = await dao.extrapolate_varieties("TRZAX", climate_class="Cfb")
    assert out["ranked_varieties"][0]["source_ids"] == ["s1", "s2"]
    query = calls[0][0]
    assert "collect(DISTINCT vt.source_id) AS source_ids" in query


# ── fix round 2: best variety needs enough trials ───────────────────────────
def _vn(name, mean, n):
    return {**_variety(mean, n), "variety": name}


async def test_best_variety_prefers_enough_trials():
    vs = [_vn("lucky", 9000.0, 1), _vn("solid", 7000.0, 12)]
    rec = (await _run(_conds(), {"TRZAX": vs}))["recommendations"][0]
    assert rec["yield"]["expected_kg_ha"] == 7000.0
    assert rec["trust"]["level"] != "low"
    assert [v["variety"] for v in rec["varieties"]] == ["solid", "lucky"]


async def test_all_below_threshold_keeps_order():
    vs = [_vn("a", 9000.0, 1), _vn("b", 7000.0, 2)]
    rec = (await _run(_conds(), {"TRZAX": vs}))["recommendations"][0]
    assert [v["variety"] for v in rec["varieties"]] == ["a", "b"]
    assert rec["yield"]["expected_kg_ha"] == 9000.0


async def test_reorder_is_stable_within_groups():
    vs = [_vn("s1", 9500.0, 1), _vn("e1", 9000.0, 5), _vn("s2", 8000.0, 2), _vn("e2", 7000.0, 4)]
    rec = (await _run(_conds(), {"TRZAX": vs}))["recommendations"][0]
    assert [v["variety"] for v in rec["varieties"]] == ["e1", "e2", "s1", "s2"]


# ── final fix wave ──────────────────────────────────────────────────────────
async def test_requested_crops_get_catalog_names_in_request_order():
    dao, _ = _dao()
    seen = []

    async def extrap(self_, crop, **_):
        seen.append(crop)
        return {"ranked_varieties": [_variety()]}

    catalog = [{"eppo_code": "HORVX", "scientific_name": "Hordeum vulgare"},
               {"eppo_code": "TRZAX", "scientific_name": "Triticum aestivum"},
               {"eppo_code": "ZEAMX", "scientific_name": "Zea mays"}]
    p = _patched(extrap, [])
    with patch.object(GraphDAO, "get_available_crops", AsyncMock(return_value=catalog)), \
            p[1], p[2], p[3], p[4]:
        out = await dao.recommend_for_conditions(_conds(crops=["TRZAX", "ZZZZZ", "HORVX"]))
    assert seen == ["TRZAX", "ZZZZZ", "HORVX"]
    names = {r["crop"]["eppo"]: r["crop"]["scientific_name"] for r in out["recommendations"]}
    assert names == {"TRZAX": "Triticum aestivum", "ZZZZZ": "ZZZZZ", "HORVX": "Hordeum vulgare"}
    assert out["data_quality"]["crops_evaluated"] == 3


async def _run_capped(crops, prefilter):
    dao, _ = _dao()
    seen = []

    async def extrap(self_, crop, **_):
        seen.append(crop)
        return {"ranked_varieties": [_variety()]}

    p = _patched(extrap, crops)
    with p[0], p[1], p[2], p[3], p[4], patch.object(GraphDAO, "_crops_with_analog_trials", prefilter):
        out = await dao.recommend_for_conditions(_conds())
    return out, seen


async def test_cap_applies_after_prefilter():
    crops = [f"X{i:04d}" for i in range(45)]
    out, seen = await _run_capped(crops, AsyncMock(return_value={"X0040"}))
    assert seen == ["X0040"]
    assert [r["crop"]["eppo"] for r in out["recommendations"]] == ["X0040"]
    assert out["data_quality"]["crops_evaluated"] == 1


async def test_cap_counts_only_prefiltered_crops():
    crops = [f"X{i:04d}" for i in range(80)]
    passed = set(crops[::2])  # 40 pass, the cap keeps the first 30 of them
    out, seen = await _run_capped(crops, AsyncMock(return_value=passed))
    assert sorted(seen) == sorted(crops[::2])[:30]
    assert out["data_quality"]["crops_evaluated"] == 30


async def test_cap_on_catalog_when_prefilter_fails():
    crops = [f"X{i:04d}" for i in range(45)]
    out, seen = await _run_capped(crops, AsyncMock(side_effect=RuntimeError("x")))
    assert sorted(seen) == crops[:30]
    assert out["data_quality"]["crops_evaluated"] == 30


_GSD_DEFAULT = {"id": "growing_season_days_default", "value": 180,
                "citation": "ASSUMPTION: crop cycle unknown; default used for water demand"}


async def test_default_growing_season_is_an_assumption():
    c = _conds(annual_et0_mm=1000.0, annual_rainfall_mm=300.0)
    recs = (await _run(c, {"TRZAX": [_variety()], "ZZZZZ": [_variety()]}))["recommendations"]
    by = {r["crop"]["eppo"]: r for r in recs}
    assert _GSD_DEFAULT in by["ZZZZZ"]["assumptions"]
    assert by["ZZZZZ"]["suitability"]["water"]["etc_mm"] == round(180 / 365 * 1000.0)
    assert all(a["id"] != "growing_season_days_default" for a in by["TRZAX"]["assumptions"])


async def test_no_growing_season_assumption_without_water_estimate():
    rec = (await _run(_conds(), {"ZZZZZ": [_variety()]}))["recommendations"][0]
    assert all(a["id"] != "growing_season_days_default" for a in rec["assumptions"])


async def test_requested_crops_survive_catalog_failure_uncached():
    dao, _ = _dao()

    async def extrap(self_, crop, **_):
        return {"ranked_varieties": [_variety()]}

    p = _patched(extrap, [])
    with patch.object(GraphDAO, "get_available_crops", AsyncMock(side_effect=RuntimeError("down"))), \
            p[1], p[2], p[3], p[4]:
        out = await dao.recommend_for_conditions(_conds(crops=["TRZAX"]))
    assert out["recommendations"][0]["crop"]["scientific_name"] == "TRZAX"
    assert dao_mod._RECOMMEND_CACHE == {}


_ES_ROW = {"eppo": "TRZAX", "sowing_type": "autumn", "koppen": ["Cfb"], "start_month": 10,
           "end_month": 12, "cycle_days": None, "source": "ref-es", "countries": ["ES"]}


@pytest.mark.parametrize("country,source", [("ES", "ref-es"), ("FR", "crop_season_slot"),
                                            (None, "crop_season_slot")])
async def test_sowing_window_scoped_by_country(country, source):
    from app.services import sowing_windows
    with patch.object(sowing_windows, "load_rows", return_value=[_ES_ROW]):
        rec = (await _run(_conds(country=country), {"TRZAX": [_variety()]}))["recommendations"][0]
    assert rec["season"]["source"] == source


async def test_recommend_cache_is_keyed_by_country():
    from app.services import sowing_windows
    with patch.object(sowing_windows, "load_rows", return_value=[_ES_ROW]):
        es = (await _run(_conds(country="ES"), {"TRZAX": [_variety()]}))["recommendations"][0]
        fr = (await _run(_conds(country="FR"), {"TRZAX": [_variety()]}))["recommendations"][0]
    assert es["season"]["source"] == "ref-es"
    assert fr["season"]["source"] == "crop_season_slot"
