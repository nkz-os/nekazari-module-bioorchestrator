"""Latency work for recommend_for_conditions: shared site lists, prefilter, cache."""
from unittest.mock import AsyncMock, patch

import pytest

from app.graph import dao as dao_mod
from app.graph.dao import GraphDAO
from tests.test_recommend_dao import (
    _ROW,
    _conds,
    _dao,
    _variety,
)

_VEC = {"annual_rainfall_mm": 500.0, "annual_et0_mm": 900.0, "coldest_month_min_c": 1.0,
        "annual_temp_c": 14.0}
_SITES = [{"name": "site-a", "distance": None}, {"name": "site-b", "distance": None}]


@pytest.fixture(autouse=True)
def _clear_recommend_cache():
    dao_mod._RECOMMEND_CACHE.clear()
    yield
    dao_mod._RECOMMEND_CACHE.clear()


@pytest.fixture
def hybrid(monkeypatch):
    monkeypatch.setenv("AGROCLIMATIC_VECTOR", "hybrid")


# ── step 1: similar sites computed once ─────────────────────────────────────
async def test_override_equals_internal_computation():
    dao_a, _ = _dao([_ROW])
    dao_a.get_similar_sites = AsyncMock(return_value=_SITES)
    internal = await dao_a.extrapolate_varieties("TRZAX", climate_class="Cfb")

    dao_b, calls_b = _dao([_ROW])
    dao_b.get_similar_sites = AsyncMock(side_effect=AssertionError("must not be called"))
    given = await dao_b.extrapolate_varieties("TRZAX", climate_class="Cfb", similar_sites_override=_SITES)
    assert given == internal
    assert calls_b[0][1]["site_names"] == ["site-a", "site-b"]


async def test_override_must_be_a_list():
    dao, _ = _dao()
    with pytest.raises(TypeError):
        await dao.extrapolate_varieties("TRZAX", similar_sites_override="site-a")


async def test_empty_override_means_no_sites_not_recompute():
    dao, calls = _dao()
    dao.get_similar_sites = AsyncMock(side_effect=AssertionError("must not be called"))
    out = await dao.extrapolate_varieties("TRZAX", similar_sites_override=[])
    assert out["ranked_varieties"] == [] and calls == []


def _medians_mock():
    async def fn(crops, irrigation_uri, purpose="main"):
        return {c: {"median_kg_ha": 5000.0, "n_trials": 40, "scope": "crop"} for c in crops}
    return AsyncMock(side_effect=fn)


def _run_with(conds, crops, extrap, sites_mock):
    dao, _ = _dao()
    patches = (
        patch.object(GraphDAO, "get_available_crops",
                     AsyncMock(return_value=[{"eppo_code": c, "scientific_name": c} for c in crops])),
        patch.object(GraphDAO, "extrapolate_varieties", extrap),
        patch.object(GraphDAO, "get_similar_sites", sites_mock),
        patch.object(GraphDAO, "get_crop_yield_medians", _medians_mock()),
        patch.object(GraphDAO, "get_soil_suitability", AsyncMock(return_value=None)),
        patch.object(GraphDAO, "get_heat_tolerance", AsyncMock(return_value=None)),
    )
    return dao, patches


async def _recommend(conds, crops, extrap, sites_mock):
    dao, p = _run_with(conds, crops, extrap, sites_mock)
    with p[0], p[1], p[2], p[3], p[4], p[5]:
        return await dao.recommend_for_conditions(conds)


async def test_koppen_sites_computed_once_and_passed_through():
    seen = []

    async def extrap(self_, crop, **kw):
        seen.append(kw)
        return {"ranked_varieties": [_variety()]}

    sites = AsyncMock(return_value=_SITES)
    await _recommend(_conds(soil_type="Loam"), ["TRZAX", "HORVX", "ZEAMX"], extrap, sites)
    assert sites.await_count == 1
    kw = sites.await_args.kwargs
    # every matching field site (no alphabetical cut) plus the climate's aggregate sites
    assert kw["climate_class"] == "Cfb" and kw["soil_type"] == "Loam" and kw["limit"] is None
    assert kw["include_aggregate"] is True
    assert kw.get("target_features") is None and kw.get("vector_version", "v1") == "v1"
    assert len(seen) == 3 and all(s["similar_sites_override"] is _SITES or
                                  s["similar_sites_override"] == _SITES for s in seen)


async def test_v2_sites_computed_once_lazily(hybrid):
    seen = []

    async def extrap(self_, crop, **kw):
        seen.append(kw)
        return {"ranked_varieties": [] if "target_features" not in kw else [_variety()]}

    sites = AsyncMock(side_effect=lambda **kw: _SITES if kw.get("vector_version", "v1") == "v1"
                      else [{"name": "site-v2", "distance": 0.1}])
    out = await _recommend(_conds(**_VEC), ["TRZAX", "HORVX", "ZEAMX"], extrap, sites)
    v2_calls = [c for c in sites.await_args_list if c.kwargs.get("vector_version") == "v2"]
    assert len(sites.await_args_list) == 2 and len(v2_calls) == 1
    assert v2_calls[0].kwargs["target_features"] == {
        "rainfall": 500.0, "et0": 900.0, "coldest_min": 1.0, "annual_temp": 14.0}
    v2_extrap = [k for k in seen if "target_features" in k]
    assert len(v2_extrap) == 3
    assert all(k["similar_sites_override"] == [{"name": "site-v2", "distance": 0.1}] for k in v2_extrap)
    assert len(out["recommendations"]) == 3


async def test_v2_sites_not_computed_when_koppen_hits(hybrid):
    async def extrap(self_, crop, **kw):
        return {"ranked_varieties": [_variety()]}

    sites = AsyncMock(return_value=_SITES)
    await _recommend(_conds(**_VEC), ["TRZAX", "HORVX"], extrap, sites)
    assert sites.await_count == 1


async def test_site_lookup_failure_falls_back_to_per_crop_computation():
    seen = []

    async def extrap(self_, crop, **kw):
        seen.append(kw)
        return {"ranked_varieties": [_variety()]}

    sites = AsyncMock(side_effect=RuntimeError("neo4j down"))
    out = await _recommend(_conds(), ["TRZAX"], extrap, sites)
    assert seen[0].get("similar_sites_override") is None
    assert len(out["recommendations"]) == 1


# ── step 2: prefilter crops with trials ─────────────────────────────────────
async def test_prefilter_query_shape_and_result():
    rows = [{"eppo": "TRZAX", "sci": None}, {"eppo": "HORVX", "sci": "Hordeum vulgare"},
            {"eppo": "OTHER", "sci": "Zea mays subsp. mays"}, {"eppo": None, "sci": "Avena sativa"}]
    dao, calls = _dao(rows)
    out = await dao._crops_with_analog_trials(
        ["TRZAX", "HORVX", "ZZZZZ", "Zea mays", "avena SATIVA"], ["site-a", "site-b"])
    # eppo equality, scientific-name CONTAINS (case-sensitive), scientific-name equality (case-insensitive)
    assert out == {"TRZAX", "HORVX", "Zea mays", "avena SATIVA"}
    q, params = calls[0]
    assert params["site_names"] == ["site-a", "site-b"]
    assert "crops" not in params
    assert "UNWIND $crops" not in q
    # site-first anchor so the TrialSite.name index is usable, then a single scan
    assert q.index("MATCH (ts:TrialSite)") < q.index("MATCH (vt:VarietyTrial)-[:TRIAL_AT]->(ts)")
    assert "ts.name IN $site_names" in q
    assert "vt.yieldKgHa IS NOT NULL OR vt.yieldNoteS1 IS NOT NULL" in q
    assert "rankingEligible" in q
    assert "vt.irrigationRegime = $irrigation_uri" in q
    assert "$excluded_sites IS NULL" in q
    assert "DISTINCT" in q
    assert params["irrigation_uri"] is None and params["excluded_sites"] is None


async def test_prefilter_skips_query_without_inputs():
    dao, calls = _dao()
    assert await dao._crops_with_analog_trials([], ["site-a"]) == set()
    assert await dao._crops_with_analog_trials(["TRZAX"], []) == set()
    assert calls == []


def _prefilter(ok_koppen, ok_v2=None):
    async def fn(self_, eppos, site_names, **kw):
        ok = ok_v2 if (ok_v2 is not None and site_names == ["site-v2"]) else ok_koppen
        return {e for e in eppos if e in ok}
    return fn


async def _recommend_pf(conds, crops, extrap, koppen_ok, v2_ok=None):
    sites = AsyncMock(side_effect=lambda **kw: _SITES if kw.get("vector_version", "v1") == "v1"
                      else [{"name": "site-v2", "distance": 0.1}])
    dao, p = _run_with(conds, crops, extrap, sites)
    pf = patch.object(GraphDAO, "_crops_with_analog_trials", _prefilter(koppen_ok, v2_ok))
    with p[0], p[1], p[2], p[3], p[4], p[5], pf:
        return await dao.recommend_for_conditions(conds)


async def test_skipped_crops_never_call_extrapolate():
    seen = []

    async def extrap(self_, crop, **kw):
        seen.append(crop)
        return {"ranked_varieties": [_variety()]}

    out = await _recommend_pf(_conds(), ["TRZAX", "HORVX", "ZEAMX"], extrap, {"TRZAX"})
    assert seen == ["TRZAX"]
    assert [r["crop"]["eppo"] for r in out["recommendations"]] == ["TRZAX"]
    assert out["data_quality"] == {"crops_evaluated": 1, "crops_with_trials": 1,
                                   "crops_with_analog_trials": 1}


async def test_prefiltered_equals_unfiltered_when_skipped_return_nothing():
    async def extrap(self_, crop, **kw):
        return {"ranked_varieties": [_variety()] if crop in {"TRZAX", "HORVX"} else []}

    crops = ["TRZAX", "HORVX", "ZEAMX", "AVESA"]
    filtered = await _recommend_pf(_conds(), crops, extrap, {"TRZAX", "HORVX"})
    unfiltered = await _recommend_pf(_conds(), crops, extrap, set(crops))
    assert filtered["recommendations"] == unfiltered["recommendations"]
    assert filtered["conditions"] == unfiltered["conditions"]


async def test_v2_retry_only_for_crops_with_v2_analog_trials(hybrid):
    seen = []

    async def extrap(self_, crop, **kw):
        seen.append((crop, kw.get("vector_version", "v1")))
        return {"ranked_varieties": [_variety()] if kw.get("vector_version") == "v2" else []}

    out = await _recommend_pf(_conds(**_VEC), ["TRZAX", "HORVX", "ZEAMX"], extrap,
                              koppen_ok=set(), v2_ok={"HORVX"})
    assert seen == [("HORVX", "v2")]
    rec = out["recommendations"][0]
    assert rec["crop"]["eppo"] == "HORVX" and rec["trust"]["similarity"] == "vector_v2_fallback"
    assert out["data_quality"]["crops_with_analog_trials"] == 1


async def test_crop_in_neither_list_makes_no_neo4j_calls(hybrid):
    async def extrap(self_, crop, **kw):
        raise AssertionError("extrapolate must not run")

    out = await _recommend_pf(_conds(**_VEC), ["TRZAX"], extrap, koppen_ok=set(), v2_ok=set())
    assert out["recommendations"] == []
    assert out["data_quality"]["crops_with_analog_trials"] == 0


async def test_prefilter_failure_falls_back_to_evaluating_every_crop():
    seen = []

    async def extrap(self_, crop, **kw):
        seen.append(crop)
        return {"ranked_varieties": [_variety()]}

    sites = AsyncMock(return_value=_SITES)
    dao, p = _run_with(_conds(), ["TRZAX", "HORVX"], extrap, sites)
    pf = patch.object(GraphDAO, "_crops_with_analog_trials", AsyncMock(side_effect=RuntimeError("x")))
    with p[0], p[1], p[2], p[3], p[4], p[5], pf:
        out = await dao.recommend_for_conditions(_conds())
    assert sorted(seen) == ["HORVX", "TRZAX"]
    assert out["data_quality"]["crops_with_analog_trials"] is None


# ── step 3: whole-response cache ────────────────────────────────────────────
async def _ok_extrap(self_, crop, **kw):
    return {"ranked_varieties": [_variety()]}


async def _cached_run(conds, crops=("TRZAX",), extrap=_ok_extrap):
    """Runs recommend with fresh mocks; returns (output, sites_mock, extrapolate_calls)."""
    seen = []

    async def spy(self_, crop, **kw):
        seen.append(crop)
        return await extrap(self_, crop, **kw)

    sites = AsyncMock(return_value=_SITES)
    dao, p = _run_with(conds, list(crops), spy, sites)
    pf = patch.object(GraphDAO, "_crops_with_analog_trials",
                      AsyncMock(side_effect=lambda eppos, names, **kw: set(eppos)))
    with p[0], p[1], p[2], p[3], p[4], p[5], pf:
        out = await dao.recommend_for_conditions(conds)
    return out, sites, seen


async def test_cache_hit_makes_no_neo4j_calls():
    first, _, _ = await _cached_run(_conds())
    second, sites, seen = await _cached_run(_conds())
    assert second == first
    assert sites.await_count == 0 and seen == []


async def test_cache_returns_independent_copies():
    first, _, _ = await _cached_run(_conds())
    first["recommendations"].clear()
    second, _, _ = await _cached_run(_conds())
    assert second["recommendations"]
    second["recommendations"][0]["crop"]["eppo"] = "MUTATED"
    third, _, _ = await _cached_run(_conds())
    assert third["recommendations"][0]["crop"]["eppo"] == "TRZAX"


@pytest.mark.parametrize("change", [
    {"top_n": 3}, {"crops": ["TRZAX"]}, {"season": "autumn"}, {"management": "organic"},
    {"frost_margin_c": 2.0}, {"climate_class": "Csa"}, {"soil_type": "Loam"}, {"soil_ph": 6.5},
    {"irrigation_regime": "secano"}, {"annual_rainfall_mm": 400.0},
])
async def test_different_conditions_miss(change):
    await _cached_run(_conds())
    _, sites, seen = await _cached_run(_conds(**change))
    assert sites.await_count == 1 and seen == ["TRZAX"]


async def test_climate_detail_and_top_level_share_an_entry():
    await _cached_run(_conds(annual_rainfall_mm=400.0))
    _, sites, _ = await _cached_run(_conds(climate_detail={"annual_rainfall_mm": 400.0}))
    assert sites.await_count == 0


async def test_default_margin_and_explicit_default_share_an_entry():
    await _cached_run(_conds())
    _, sites, _ = await _cached_run(_conds(frost_margin_c=5.0))
    assert sites.await_count == 0


async def test_cache_expires_after_ttl():
    await _cached_run(_conds())
    assert len(dao_mod._RECOMMEND_CACHE) == 1
    key = next(iter(dao_mod._RECOMMEND_CACHE))
    stored_at, value = dao_mod._RECOMMEND_CACHE[key]
    dao_mod._RECOMMEND_CACHE[key] = (stored_at - dao_mod._RECOMMEND_TTL - 1, value)
    _, sites, _ = await _cached_run(_conds())
    assert sites.await_count == 1


async def test_vector_mode_change_misses(monkeypatch):
    monkeypatch.setenv("AGROCLIMATIC_VECTOR", "v1")
    await _cached_run(_conds(**_VEC))
    monkeypatch.setenv("AGROCLIMATIC_VECTOR", "hybrid")
    _, sites, _ = await _cached_run(_conds(**_VEC))
    assert sites.await_count == 1


async def test_cache_is_bounded_and_evicts_oldest(monkeypatch):
    monkeypatch.setattr(dao_mod, "_RECOMMEND_CACHE_MAX", 3)
    for i in range(5):
        await _cached_run(_conds(annual_rainfall_mm=100.0 + i))
    assert len(dao_mod._RECOMMEND_CACHE) == 3
    _, sites_old, _ = await _cached_run(_conds(annual_rainfall_mm=100.0))   # evicted
    assert sites_old.await_count == 1
    _, sites_new, _ = await _cached_run(_conds(annual_rainfall_mm=104.0))   # kept
    assert sites_new.await_count == 0


async def test_degraded_result_is_not_cached():
    async def boom(self_, crop, **kw):
        raise RuntimeError("transient")

    out, _, _ = await _cached_run(_conds(), extrap=boom)
    assert out["recommendations"] == []
    assert len(dao_mod._RECOMMEND_CACHE) == 0
    _, sites, seen = await _cached_run(_conds())
    assert sites.await_count == 1 and seen == ["TRZAX"]


async def test_prefilter_passes_regime_and_exclusions():
    dao, calls = _dao([])
    await dao._crops_with_analog_trials(["TRZAX"], ["site-a", "Site-X"], irrigation_uri="uri:secano",
                                        exclude_sites=["site-x"])
    params = calls[0][1]
    assert params["irrigation_uri"] == "uri:secano"
    assert params["excluded_sites"] == ["site-x"]
    assert params["site_names"] == ["site-a"]


async def test_recommend_prefilter_receives_irrigation_uri():
    seen = []

    async def pf(self_, eppos, names, **kw):
        seen.append(kw)
        return set(eppos)

    sites = AsyncMock(return_value=_SITES)
    dao, p = _run_with(_conds(irrigation_regime="secano"), ["TRZAX"], _ok_extrap, sites)
    with p[0], p[1], p[2], p[3], p[4], p[5], patch.object(GraphDAO, "_crops_with_analog_trials", pf):
        await dao.recommend_for_conditions(_conds(irrigation_regime="secano"))
    assert seen[0]["irrigation_uri"] == "http://aims.fao.org/aos/agrovoc/c_6436"



async def test_recommend_concurrency_constant():
    assert dao_mod.RECOMMEND_CONCURRENCY == 4


async def test_recommend_never_exceeds_concurrency_limit():
    import asyncio
    state = {"now": 0, "peak": 0}

    async def extrap(self_, crop, **kw):
        state["now"] += 1
        state["peak"] = max(state["peak"], state["now"])
        await asyncio.sleep(0.01)
        state["now"] -= 1
        return {"ranked_varieties": [_variety()]}

    # per-crop fan-out (batch unavailable) stays bounded by the semaphore
    with patch.object(GraphDAO, "extrapolate_varieties_batch",
                      AsyncMock(side_effect=RuntimeError("batch down"))):
        await _cached_run(_conds(), crops=[f"C{i:04d}" for i in range(12)], extrap=extrap)
    assert state["peak"] == dao_mod.RECOMMEND_CONCURRENCY


# ── batched Köppen extrapolation ────────────────────────────────────────────
async def _batched_run(conds, crops, batch, koppen_ok=None, sites=None):
    async def no_v1(self_, crop, **kw):
        assert kw.get("vector_version") == "v2", "Köppen path must not call per-crop extrapolate"
        return {"ranked_varieties": [_variety()]}

    dao, p = _run_with(conds, list(crops), no_v1, sites or AsyncMock(return_value=_SITES))
    ok = set(crops) if koppen_ok is None else koppen_ok
    pf = patch.object(GraphDAO, "_crops_with_analog_trials",
                      AsyncMock(side_effect=lambda eppos, names, **kw: ok & set(eppos)))
    pb = patch.object(GraphDAO, "extrapolate_varieties_batch", batch)
    with p[0], p[1], p[2], p[3], p[4], p[5], pf, pb:
        return await dao.recommend_for_conditions(conds)


async def test_koppen_path_is_one_batch_for_prefiltered_crops():
    batch = AsyncMock(side_effect=lambda crops, sites, **kw: {c: [_variety()] for c in crops})
    out = await _batched_run(_conds(irrigation_regime="secano"), ["TRZAX", "HORVX", "ZEAMX"], batch,
                             koppen_ok={"TRZAX", "ZEAMX"})
    assert batch.await_count == 1
    args, kw = batch.await_args
    assert args == (["TRZAX", "ZEAMX"], _SITES)
    assert kw == {"irrigation_regime": "secano", "top_n": 5, "purpose": "main"}
    assert sorted(r["crop"]["eppo"] for r in out["recommendations"]) == ["TRZAX", "ZEAMX"]


async def test_batch_failure_falls_back_to_per_crop_with_same_answer():
    async def extrap(self_, crop, **kw):
        return {"ranked_varieties": [_variety(mean=5000.0 + len(crop))]}

    with patch.object(GraphDAO, "extrapolate_varieties_batch",
                      AsyncMock(side_effect=RuntimeError("batch down"))):
        failed, _, seen = await _cached_run(_conds(), crops=["TRZAX", "HORVX"], extrap=extrap)
    dao_mod._RECOMMEND_CACHE.clear()
    ok, _, _ = await _cached_run(_conds(), crops=["TRZAX", "HORVX"], extrap=extrap)
    assert failed == ok and sorted(seen) == ["HORVX", "TRZAX"]


async def test_no_batch_when_shared_site_lookup_failed():
    batch = AsyncMock(side_effect=AssertionError("must not be called"))
    seen = []

    async def extrap(self_, crop, **kw):
        seen.append(kw.get("similar_sites_override"))
        return {"ranked_varieties": [_variety()]}

    dao, p = _run_with(_conds(), ["TRZAX"], extrap, AsyncMock(side_effect=RuntimeError("down")))
    with p[0], p[1], p[2], p[3], p[4], p[5], patch.object(GraphDAO, "extrapolate_varieties_batch", batch):
        out = await dao.recommend_for_conditions(_conds())
    assert seen == [None] and len(out["recommendations"]) == 1


async def test_batch_result_without_numeric_yield_still_triggers_v2(hybrid):
    note_only = {**_variety(), "mean_yield_kg_ha": None}
    batch = AsyncMock(side_effect=lambda crops, sites, **kw: {c: [note_only] for c in crops})
    out = await _batched_run(_conds(**_VEC), ["TRZAX"], batch)
    assert out["recommendations"][0]["trust"]["similarity"] == "vector_v2_fallback"


# ── round 3: batched medians ────────────────────────────────────────────────
@pytest.fixture(autouse=True)
def _clear_median_cache():
    dao_mod._MEDIAN_CACHE.clear()
    yield
    dao_mod._MEDIAN_CACHE.clear()


async def test_batch_is_one_call_for_many_misses_and_fills_defaults():
    dao, calls = _dao([{"crop": "TRZAX", "median": 5000.0, "n": 42}])
    out = await dao.get_crop_yield_medians(["TRZAX", "HORVX", "ZZZZZ"], None)
    assert len(calls) == 1
    assert out["TRZAX"] == {"median_kg_ha": 5000.0, "n_trials": 42, "scope": "crop"}
    assert out["HORVX"] == {"median_kg_ha": None, "n_trials": 0, "scope": "crop"}
    assert set(out) == {"TRZAX", "HORVX", "ZZZZZ"}
    assert calls[0][1]["crops"] == ["TRZAX", "HORVX", "ZZZZZ"]


async def test_batch_zero_calls_when_all_cached():
    dao, calls = _dao([{"crop": "TRZAX", "median": 5000.0, "n": 42}])
    await dao.get_crop_yield_medians(["TRZAX", "HORVX"], "uri:secano")
    out = await dao.get_crop_yield_medians(["HORVX", "TRZAX"], "uri:secano")
    assert len(calls) == 1 and out["TRZAX"]["scope"] == "crop×irrigation"


async def test_batch_and_single_share_cache():
    dao, calls = _dao([{"crop": "TRZAX", "median": 5000.0, "n": 42}], [{"crop": "HORVX", "median": 4000.0, "n": 9}])
    await dao.get_crop_yield_medians(["TRZAX"], None)
    assert (await dao.get_crop_yield_median("TRZAX", None))["median_kg_ha"] == 5000.0
    assert len(calls) == 1
    out = await dao.get_crop_yield_medians(["TRZAX", "HORVX"], None)   # only HORVX is a miss
    assert calls[1][1]["crops"] == ["HORVX"] and out["HORVX"]["median_kg_ha"] == 4000.0


async def test_batch_empty_input_makes_no_call():
    dao, calls = _dao()
    assert await dao.get_crop_yield_medians([], None) == {}
    assert calls == []


async def test_recommend_calls_batch_once_for_prefiltered_crops():
    medians = _medians_mock()
    sites = AsyncMock(return_value=_SITES)
    dao, p = _run_with(_conds(), ["TRZAX", "HORVX", "ZEAMX"], _ok_extrap, sites)
    pf = patch.object(GraphDAO, "_crops_with_analog_trials",
                      AsyncMock(return_value={"TRZAX", "ZEAMX"}))
    with p[0], p[1], p[2], p[3], p[4], p[5], pf, patch.object(GraphDAO, "get_crop_yield_medians", medians):
        out = await dao.recommend_for_conditions(_conds())
    assert medians.await_count == 1
    assert sorted(medians.await_args.args[0]) == ["TRZAX", "ZEAMX"]
    assert len(out["recommendations"]) == 2


async def test_recommend_medians_failure_falls_back_to_per_crop():
    sites = AsyncMock(return_value=_SITES)
    dao, p = _run_with(_conds(), ["TRZAX"], _ok_extrap, sites)
    single = AsyncMock(return_value={"median_kg_ha": 5000.0, "n_trials": 40, "scope": "crop"})
    with p[0], p[1], p[2], p[3], p[4], p[5], \
            patch.object(GraphDAO, "get_crop_yield_medians", AsyncMock(side_effect=RuntimeError("x"))), \
            patch.object(GraphDAO, "get_crop_yield_median", single):
        out = await dao.recommend_for_conditions(_conds())
    assert single.await_count == 1 and len(out["recommendations"]) == 1


# ── final fix wave: process-wide cold-computation guard ─────────────────────
def _all_pass():
    return patch.object(GraphDAO, "_crops_with_analog_trials",
                        AsyncMock(side_effect=lambda eppos, names, **kw: set(eppos)))


async def test_identical_concurrent_cold_calls_compute_once():
    import asyncio
    seen = []

    async def extrap(self_, crop, **kw):
        seen.append(crop)
        await asyncio.sleep(0.02)
        return {"ranked_varieties": [_variety()]}

    sites = AsyncMock(return_value=_SITES)
    dao, p = _run_with(_conds(), ["TRZAX"], extrap, sites)
    with p[0], p[1], p[2], p[3], p[4], p[5], _all_pass():
        a, b = await asyncio.gather(dao.recommend_for_conditions(_conds()),
                                    dao.recommend_for_conditions(_conds()))
    assert seen == ["TRZAX"] and sites.await_count == 1
    assert a == b and a is not b
    a["recommendations"].clear()
    assert b["recommendations"]


async def test_at_most_two_cold_computations_run_concurrently():
    import asyncio
    state = {"now": 0, "peak": 0}

    async def extrap(self_, crop, **kw):
        state["now"] += 1
        state["peak"] = max(state["peak"], state["now"])
        await asyncio.sleep(0.02)
        state["now"] -= 1
        return {"ranked_varieties": [_variety()]}

    sites = AsyncMock(return_value=_SITES)
    dao, p = _run_with(_conds(), ["TRZAX"], extrap, sites)
    with p[0], p[1], p[2], p[3], p[4], p[5], _all_pass():
        outs = await asyncio.gather(*(dao.recommend_for_conditions(_conds(annual_rainfall_mm=100.0 + i))
                                      for i in range(3)))
    assert state["peak"] == 2
    assert dao_mod.RECOMMEND_MAX_CONCURRENT_REQUESTS == 2
    assert all(o["recommendations"] for o in outs)


async def test_cache_hit_bypasses_cold_guard():
    import asyncio
    first, _, _ = await _cached_run(_conds())
    guard = dao_mod._cold_guard()
    for _ in range(dao_mod.RECOMMEND_MAX_CONCURRENT_REQUESTS):
        await guard.sem.acquire()
    try:
        second, _, _ = await asyncio.wait_for(_cached_run(_conds()), timeout=1)
    finally:
        for _ in range(dao_mod.RECOMMEND_MAX_CONCURRENT_REQUESTS):
            guard.sem.release()
    assert second == first


async def test_inflight_entry_removed_after_completion_and_failure():
    async def boom(self_, crop, **kw):
        raise RuntimeError("transient")

    await _cached_run(_conds(), extrap=boom)
    await _cached_run(_conds(annual_rainfall_mm=1.0))
    assert dao_mod._cold_guard().inflight == {}


# ── evidence tiers and purpose through recommend_for_conditions ──────────────
_FIELD_SITE = {"name": "site-a", "distance": None, "site_kind": "field"}
_AGG_SITE = {"name": "UK national list", "distance": None, "site_kind": "aggregate"}


def _tier_prefilter(field_ok, regional_ok):
    async def fn(self_, eppos, site_names, **kw):
        ok = regional_ok if kw.get("tier") == "regional" else field_ok
        return {e for e in eppos if e in ok}
    return fn


async def _recommend_tiers(conds, crops, extrap, field_ok, regional_ok, sites=None):
    sites = sites or AsyncMock(return_value=[_FIELD_SITE, _AGG_SITE])
    dao, p = _run_with(conds, crops, extrap, sites)
    pf = patch.object(GraphDAO, "_crops_with_analog_trials", _tier_prefilter(field_ok, regional_ok))
    with p[0], p[1], p[2], p[3], p[4], p[5], pf:
        return await dao.recommend_for_conditions(conds), sites


def _by_tier_extrap(field, regional, seen=None):
    async def extrap(self_, crop, **kw):
        if seen is not None:
            seen.append((crop, kw.get("tier", "field"), kw.get("purpose"),
                         [s["name"] for s in kw.get("similar_sites_override") or []]))
        rows = regional if kw.get("tier") == "regional" else field
        return {"ranked_varieties": rows.get(crop, [])}
    return extrap


async def test_regional_tier_backs_crops_without_numeric_field_evidence():
    seen = []
    nonnum = {**_variety(), "mean_yield_kg_ha": None, "numeric_yield_count": 0}
    extrap = _by_tier_extrap(
        field={"TRZAX": [_variety(5500.0, 12)], "HORVX": [nonnum]},
        regional={"TRZAX": [_variety(7000.0, 9)], "HORVX": [_variety(4000.0, 6)], "LYPES": [_variety(50000.0, 5)]},
        seen=seen)
    out, sites = await _recommend_tiers(_conds(), ["TRZAX", "HORVX", "LYPES"], extrap,
                                        {"TRZAX", "HORVX"}, {"TRZAX", "HORVX", "LYPES"})
    recs = {r["crop"]["eppo"]: r for r in out["recommendations"]}
    assert sites.await_count == 1  # one site scan serves both tiers
    assert recs["TRZAX"]["evidence"]["tier"] == "field" and recs["TRZAX"]["yield"]["expected_kg_ha"] == 5500.0
    assert recs["TRZAX"]["evidence"]["regional_trial_count"] == 9  # supplementary, in no number
    for eppo, kg in (("HORVX", 4000.0), ("LYPES", 50000.0)):
        assert recs[eppo]["evidence"]["tier"] == "regional" and recs[eppo]["yield"]["expected_kg_ha"] == kg
        assert recs[eppo]["trust"]["level"] == "low" and recs[eppo]["fit"]["relative_yield_pct"] is None
    assert out["recommendations"][0]["evidence"]["tier"] == "field"
    assert out["data_quality"]["crops_with_analog_trials"] == 3
    # field extrapolation sees the field sites only, regional the aggregate ones
    assert {(c, t, tuple(n)) for c, t, _, n in seen if t == "regional"} == {
        ("TRZAX", "regional", ("UK national list",)), ("HORVX", "regional", ("UK national list",)),
        ("LYPES", "regional", ("UK national list",))}
    assert all(n == ["site-a"] for _, t, _, n in seen if t == "field")


async def test_regional_trial_count_is_the_crop_total_not_the_listed_rows():
    # the query reports the crop total on every row; the listed rows are only the top_n cut
    capped = [{**_variety(7000.0, 9), "crop_numeric_trial_count": 740}, _variety(6000.0, 4)]
    extrap = _by_tier_extrap(field={"TRZAX": [_variety(5500.0, 12)]}, regional={"TRZAX": capped})
    out, _ = await _recommend_tiers(_conds(), ["TRZAX"], extrap, {"TRZAX"}, {"TRZAX"})
    assert out["recommendations"][0]["evidence"]["regional_trial_count"] == 740
    # rows without the crop-level field (older shape) fall back to the listed rows
    dao_mod._RECOMMEND_CACHE.clear()
    extrap = _by_tier_extrap(field={"TRZAX": [_variety(5500.0, 12)]},
                             regional={"TRZAX": [_variety(7000.0, 9), _variety(6000.0, 4)]})
    out, _ = await _recommend_tiers(_conds(), ["TRZAX"], extrap, {"TRZAX"}, {"TRZAX"})
    assert out["recommendations"][0]["evidence"]["regional_trial_count"] == 13


def _unknown_basis_rows(n=3):
    return [{**_variety(), "mean_yield_kg_ha": None, "min_yield_kg_ha": None, "max_yield_kg_ha": None,
             "stddev_yield_kg_ha": None, "numeric_yield_count": 0, "trial_count": n,
             "unknown_basis_trial_count": n}]


async def test_forage_field_crop_with_only_unknown_basis_keeps_its_field_rec():
    extrap = _by_tier_extrap(field={"SETIT": _unknown_basis_rows()},
                             regional={"SETIT": [_variety(5000.0, 4)]})
    out, _ = await _recommend_tiers(_conds(purpose="forage"), ["SETIT"], extrap, {"SETIT"}, {"SETIT"})
    rec = out["recommendations"][0]
    assert rec["evidence"]["tier"] == "field" and rec["evidence"]["regional_trial_count"] == 4
    assert rec["yield"]["expected_kg_ha"] is None and rec["yield"]["n_trials"] == 3
    assert "forage_basis_unknown" in rec["trust"]["data_gaps"]
    assert "regional_evidence_only" not in rec["trust"]["data_gaps"]


async def test_main_mode_field_crop_without_a_number_still_falls_back_to_regional():
    # the unknown-basis rule is forage only: a main-mode crop with presence-only field rows is
    # backed by numeric regional evidence as before
    extrap = _by_tier_extrap(field={"TRZAX": _unknown_basis_rows()},
                             regional={"TRZAX": [_variety(5000.0, 4)]})
    out, _ = await _recommend_tiers(_conds(), ["TRZAX"], extrap, {"TRZAX"}, {"TRZAX"})
    assert out["recommendations"][0]["evidence"]["tier"] == "regional"


async def test_forage_unknown_basis_field_rows_survive_the_v2_fallback(hybrid):
    async def extrap(self_, crop, **kw):
        if kw.get("tier") == "regional":
            return {"ranked_varieties": [_variety(5000.0, 4)]}
        if kw.get("vector_version") == "v2":
            return {"ranked_varieties": []}  # nothing at the vector-similar sites either
        return {"ranked_varieties": _unknown_basis_rows()}

    conds = _conds(purpose="forage", **_VEC)
    out, _ = await _recommend_tiers(conds, ["SETIT"], extrap, {"SETIT"}, {"SETIT"})
    rec = out["recommendations"][0]
    assert rec["evidence"]["tier"] == "field" and rec["trust"]["similarity"] == "koppen"
    assert "forage_basis_unknown" in rec["trust"]["data_gaps"]


def _presence_info(n=40):
    return {"trial_count": n, "years": [2016, 2020], "sites": ["BSL container"], "sources": ["BSL"]}


async def _recommend_presence(conds, crops, extrap, field_ok, regional_ok, presence):
    """``_recommend_tiers`` with the presence scan stubbed; returns the answer and the scan's calls."""
    calls = []

    async def scan(self_, crop_list, site_names, **kw):
        calls.append((list(crop_list), list(site_names), kw))
        if isinstance(presence, Exception):
            raise presence
        return {c: v for c, v in presence.items() if c in crop_list}

    sites = AsyncMock(return_value=[_FIELD_SITE, _AGG_SITE])
    dao, p = _run_with(conds, crops, extrap, sites)
    pf = patch.object(GraphDAO, "_crops_with_analog_trials", _tier_prefilter(field_ok, regional_ok))
    with p[0], p[1], p[2], p[3], p[4], p[5], pf, patch.object(GraphDAO, "regional_presence_trials", scan):
        return await dao.recommend_for_conditions(conds), calls


async def test_presence_only_crops_are_regional_recs_without_a_number():
    extrap = _by_tier_extrap(field={"TRZAX": [_variety(5500.0, 12)]}, regional={"LYPES": [_variety(50000.0, 5)]})
    out, calls = await _recommend_presence(
        _conds(), ["SECCE", "TRZAX", "LYPES"], extrap, {"TRZAX"}, {"LYPES"}, {"SECCE": _presence_info()})
    # one scan, only for the crops with no evidence at either tier, over the aggregate sites
    assert len(calls) == 1 and calls[0][0] == ["SECCE"] and calls[0][1] == ["UK national list"]
    assert calls[0][2] == {"irrigation_uri": None, "purpose": "main"}
    recs = {r["crop"]["eppo"]: r for r in out["recommendations"]}
    sec = recs["SECCE"]
    assert sec["evidence"]["tier"] == "regional" and sec["yield"]["expected_kg_ha"] is None
    assert sec["yield"]["n_trials"] == 40 and sec["varieties"] == [] and sec["trust"]["level"] == "low"
    assert "no_measured_yield" in sec["trust"]["data_gaps"]
    assert [r["crop"]["eppo"] for r in out["recommendations"]] == ["TRZAX", "LYPES", "SECCE"]
    assert out["data_quality"]["crops_with_analog_trials"] == 3


async def test_presence_scan_is_main_mode_only_and_failures_degrade():
    extrap = _by_tier_extrap(field={}, regional={})
    out, calls = await _recommend_presence(
        _conds(purpose="forage"), ["SECCE"], extrap, set(), set(), {"SECCE": _presence_info()})
    assert calls == [] and out["recommendations"] == []
    dao_mod._RECOMMEND_CACHE.clear()
    out, calls = await _recommend_presence(
        _conds(), ["SECCE", "TRZAX"], _by_tier_extrap(field={"TRZAX": [_variety()]}, regional={}),
        {"TRZAX"}, set(), RuntimeError("neo4j hiccup"))
    assert len(calls) == 1 and [r["crop"]["eppo"] for r in out["recommendations"]] == ["TRZAX"]
    assert not dao_mod._RECOMMEND_CACHE  # a degraded answer is never pinned


async def test_presence_only_crops_come_last_under_the_cap(monkeypatch):
    monkeypatch.setattr(dao_mod, "_RECOMMEND_MAX_CROPS", 2)
    extrap = _by_tier_extrap(field={"FLD1": [_variety()]}, regional={"REG1": [_variety()]})
    out, _ = await _recommend_presence(
        _conds(), ["PRES1", "REG1", "FLD1"], extrap, {"FLD1"}, {"REG1"}, {"PRES1": _presence_info()})
    assert sorted(r["crop"]["eppo"] for r in out["recommendations"]) == ["FLD1", "REG1"]


async def test_regional_failure_keeps_field_answer_and_is_not_cached():
    calls = []

    async def extrap(self_, crop, **kw):
        calls.append(kw.get("tier", "field"))
        if kw.get("tier") == "regional":
            raise RuntimeError("neo4j hiccup")
        return {"ranked_varieties": [_variety()]}

    out, _ = await _recommend_tiers(_conds(), ["TRZAX"], extrap, {"TRZAX"}, {"TRZAX"})
    assert [r["crop"]["eppo"] for r in out["recommendations"]] == ["TRZAX"]
    assert out["recommendations"][0]["evidence"]["regional_trial_count"] is None
    assert not dao_mod._RECOMMEND_CACHE  # a degraded answer is never pinned


async def test_regional_only_crops_come_after_field_capable_ones_under_the_cap(monkeypatch):
    monkeypatch.setattr(dao_mod, "_RECOMMEND_MAX_CROPS", 2)
    crops = ["REG1", "REG2", "FLD1", "FLD2"]  # catalog order puts the regional-only crops first
    extrap = _by_tier_extrap(field={"FLD1": [_variety()], "FLD2": [_variety()]},
                             regional={"REG1": [_variety()], "REG2": [_variety()]})
    out, _ = await _recommend_tiers(_conds(), crops, extrap, {"FLD1", "FLD2"}, {"REG1", "REG2"})
    assert sorted(r["crop"]["eppo"] for r in out["recommendations"]) == ["FLD1", "FLD2"]


async def test_no_aggregate_sites_means_no_regional_pass():
    seen = []
    extrap = _by_tier_extrap(field={"TRZAX": [_variety()]}, regional={}, seen=seen)
    sites = AsyncMock(return_value=[_FIELD_SITE])
    out, _ = await _recommend_tiers(_conds(), ["TRZAX"], extrap, {"TRZAX"}, {"TRZAX"}, sites)
    assert all(t == "field" for _, t, _, _ in seen)
    assert out["recommendations"][0]["evidence"]["regional_trial_count"] is None


async def test_purpose_reaches_every_stage_and_the_cache_key():
    seen, med = [], []

    async def medians(crops, irrigation_uri, purpose="main"):
        med.append(purpose)
        return {c: {"median_kg_ha": 5000.0, "n_trials": 40, "scope": "crop"} for c in crops}

    async def prefilter(self_, eppos, site_names, **kw):
        seen.append(("prefilter", kw.get("purpose")))
        return set(eppos)

    extrap = _by_tier_extrap(field={"ZEAMX": [_variety()]}, regional={}, seen=seen)
    out = {}
    for purpose in ("main", "forage"):
        dao, p = _run_with(_conds(purpose=purpose), ["ZEAMX"], extrap,
                           AsyncMock(return_value=[_FIELD_SITE]))
        with p[0], p[1], p[2], p[4], p[5], patch.object(GraphDAO, "get_crop_yield_medians",
                                                         AsyncMock(side_effect=medians)), \
                patch.object(GraphDAO, "_crops_with_analog_trials", prefilter):
            out[purpose] = await dao.recommend_for_conditions(_conds(purpose=purpose))
    assert med == ["main", "forage"]  # distinct cache entries: both computed
    assert ("prefilter", "forage") in seen and any(s[0] == "ZEAMX" and s[2] == "forage" for s in seen if len(s) == 4)
    assert out["main"]["conditions"]["purpose"] == "main" and out["forage"]["conditions"]["purpose"] == "forage"
    main_id = out["main"]["recommendations"][0]["recommendation_id"]
    assert main_id != out["forage"]["recommendations"][0]["recommendation_id"]
    # the default purpose keeps the id it had before the parameter existed
    from app.graph.recommend import recommendation_id
    agro = {k: v for k, v in _conds().items() if k not in ("top_n", "crops")}
    sow = out["main"]["recommendations"][0]["crop"]["sowing_type"]
    assert main_id == recommendation_id(agro, "ZEAMX", sow)


async def test_unknown_purpose_is_a_value_error():
    dao, _ = _dao()
    with pytest.raises(ValueError):
        await dao.recommend_for_conditions(_conds(purpose="grain"))
