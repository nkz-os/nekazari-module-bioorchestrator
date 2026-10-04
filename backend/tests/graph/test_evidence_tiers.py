"""Evidence policy applied to extrapolation, medians, evidence listing and recommend, on real Neo4j:
duplicates collapse, BSL kg is ignored, forage is excluded from the main answer (counted apart)
and averaged in kg dry matter/ha in forage mode only when the basis is known, aggregate sites
back a ``regional`` tier that never mixes with field numbers, and the Köppen analog set is every
matching field site (no alphabetical cut)."""
from __future__ import annotations

import asyncio
import json
import shutil

import pytest
from testcontainers.neo4j import Neo4jContainer

from app.graph import dao as dao_mod
from app.graph import evidence_policy as ep
from app.graph.dao import GraphDAO
from neo4j import AsyncGraphDatabase

pytestmark = [
    pytest.mark.real_batch,
    pytest.mark.skipif(shutil.which("docker") is None, reason="docker unavailable for testcontainers"),
]

_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
_PW = "testpassword"
_FORAGE_QP = json.dumps({"ndf_pct": 40.1, "dry_matter_pct": 33.0})


def _run(coro):
    return _loop.run_until_complete(coro)


# 60 field sites of the climate (more than the old 50-site cut), two aggregate sites of it,
# one aggregate site with no climate class, and a field site of another climate.
_FIELD = [f"f{i:02d}" for i in range(1, 61)]
_AGG = ["UK national list", "BSL Deutschland Cfb"]
_SITES = (
    [{"name": n, "climateClass": "Cfb", "soilType": "Loam"} for n in _FIELD]
    + [{"name": n, "climateClass": "Cfb"} for n in _AGG]
    + [{"name": "Media 5 Località"}, {"name": "g01", "climateClass": "Csa", "soilType": "Loam"},
       {"name": "Poland (national average)", "climateClass": "Dfb"}]
)


def _t(crop, variety, sites, kg=None, key=None, **props):
    base = {"cropEppo": crop, "varietyNormalized": variety, "year": 2020, "aggregationScope": "site",
            "source_id": "SRC", "mergeKey": key or f"{crop}|{variety}|{'+'.join(sites)}|{kg}",
            "yieldKgHa": kg, **props}
    return {"props": {k: v for k, v in base.items() if v is not None}, "sites": sites}


_TRIALS = [
    # TRZAX field: V1 = 6000 and 8000 (f58 is past the old alphabetical cut), V2 = 7000 twice
    # (a re-ingest twin: one observation), V3 note only; BSL kg at the aggregate container.
    _t("TRZAX", "V1", ["f01"], 6000.0), _t("TRZAX", "V1", ["f58"], 8000.0),
    _t("TRZAX", "V2", ["f59"], 7000.0, key="twin-a"), _t("TRZAX", "V2", ["f59"], 7000.0, key="twin-b"),
    _t("TRZAX", "V3", ["f60"], None, yieldNoteS1="5"),
    _t("TRZAX", "V9", ["BSL Deutschland Cfb"], 9500.0, source_id="BSL", aggregationScope="regional"),
    # ZEAMX: grain (unknown purpose), forage with a cited dry-matter basis (twin on f04),
    # forage with an unknown basis.
    _t("ZEAMX", "G1", ["f01"], 12000.0), _t("ZEAMX", "G1", ["f02"], 14000.0),
    _t("ZEAMX", "F1", ["f03"], 20000.0, source_id="NAVARRA-AGRARIA", year=2019, qualityParams=_FORAGE_QP),
    _t("ZEAMX", "F1", ["f04"], 22000.0, source_id="NAVARRA-AGRARIA", year=2019, qualityParams=_FORAGE_QP,
       key="f1-a"),
    _t("ZEAMX", "F1", ["f04"], 22000.0, source_id="NAVARRA-AGRARIA", year=2019, qualityParams=_FORAGE_QP,
       key="f1-b"),
    _t("ZEAMX", "F2", ["f05"], 18000.0, year=2015, qualityParams=_FORAGE_QP),
    # SETIT: forage only, basis unknown.
    _t("SETIT", "S1", ["f06"], 4000.0, year=2015, qualityParams=_FORAGE_QP),
    # LYPES: numeric evidence only at an aggregate site; AVESA: regional scope at a field-named
    # site; SECCE: BSL kg at the aggregate container; HORVX: note-only field presence.
    _t("LYPES", "L1", ["UK national list"], 50000.0, source_id="AHDB"),
    _t("AVESA", "A1", ["f07"], 4000.0, source_id="LFL-BAYERN", aggregationScope="regional"),
    _t("SECCE", "R1", ["BSL Deutschland Cfb"], 3000.0, source_id="BSL", aggregationScope="regional"),
    _t("HORVX", "H1", ["f08"], None, yieldNoteS1="6"),
    # SETIT also has a numeric forage trial (dry-matter basis on the record) at an aggregate site:
    # in forage mode the field rows (unknown basis) must still win over it.
    _t("SETIT", "S2", ["UK national list"], 5000.0, source_id="AHDB", yieldBasis="dry_matter",
       qualityParams=_FORAGE_QP),
    # CPSAN: eight varieties with one trial each at an aggregate site of another climate.
    *[_t("CPSAN", f"C{i}", ["Poland (national average)"], 40000.0 + i, source_id="NATIONAL")
      for i in range(8)],
]


@pytest.fixture(scope="module")
def dao():
    with Neo4jContainer("neo4j:5.26-community", password=_PW) as n:
        driver = AsyncGraphDatabase.driver(n.get_connection_url(), auth=(n.username, n.password))
        d = GraphDAO(driver)
        _run(_seed(d))
        yield d
        _run(driver.close())


async def _seed(dao):
    async with dao._driver.session() as s:
        await s.run("CREATE INDEX trial_site_name IF NOT EXISTS FOR (ts:TrialSite) ON (ts.name)")
        await s.run("UNWIND $sites AS s CREATE (t:TrialSite) SET t = s", sites=_SITES)
        await s.run(
            """
            UNWIND $trials AS t
            CREATE (vt:VarietyTrial) SET vt = t.props
            WITH vt, t
            UNWIND t.sites AS n
            MATCH (ts:TrialSite {name: n})
            CREATE (vt)-[:TRIAL_AT]->(ts)
            """,
            trials=_TRIALS,
        )


@pytest.fixture(autouse=True)
def _clear_caches():
    dao_mod._MEDIAN_CACHE.clear()
    dao_mod._RECOMMEND_CACHE.clear()
    yield
    dao_mod._MEDIAN_CACHE.clear()
    dao_mod._RECOMMEND_CACHE.clear()


_FIELD_SITES = [{"name": n, "distance": None} for n in _FIELD]
_AGG_SITES = [{"name": n, "distance": None} for n in _AGG]
_CROPS = ["TRZAX", "ZEAMX", "SETIT", "LYPES", "AVESA", "SECCE", "HORVX", "CPSAN"]


def _by_variety(ranked):
    return {v["variety"]: v for v in ranked}


# ── Köppen analog set ────────────────────────────────────────────────────────

def test_koppen_set_is_every_field_site_without_a_cut(dao):
    sites = _run(dao.get_similar_sites(climate_class="Cfb", limit=None))
    assert [s["name"] for s in sites] == _FIELD  # 60 > 50, deterministic order, no aggregate
    assert {s["site_kind"] for s in sites} == {"field"}
    assert len(_run(dao.get_similar_sites(climate_class="Cfb", limit=50))) == 50  # the old cut, opt-in


def test_aggregate_sites_are_opt_in_and_exempt_from_soil_filters(dao):
    sites = _run(dao.get_similar_sites(climate_class="Cfb", soil_type="Loam", limit=10,
                                       include_aggregate=True))
    kinds = {s["name"]: s["site_kind"] for s in sites}
    assert [n for n, k in kinds.items() if k == "aggregate"] == sorted(_AGG)  # no soil data, still kept
    assert sum(k == "field" for k in kinds.values()) == 10  # the cap counts field sites only
    assert "Media 5 Località" not in kinds  # no climate class: not an analog of any climate


def test_extrapolate_without_override_finds_trials_past_the_old_cut(dao):
    res = _run(dao.extrapolate_varieties("TRZAX", climate_class="Cfb", top_n=10))
    v = _by_variety(res["ranked_varieties"])
    assert v["V1"]["mean_yield_kg_ha"] == 7000.0 and "f58" in v["V1"]["trial_sites"]
    assert res["evidence_tier"] == "field" and res["purpose"] == "main"
    assert not set(res["similar_sites"]) & set(_AGG)


# ── field tier, main purpose ─────────────────────────────────────────────────

def test_field_main_dedups_ignores_bsl_and_counts_distinct_trials(dao):
    out = _run(dao.extrapolate_varieties_batch(_CROPS, _FIELD_SITES, top_n=10))
    v = _by_variety(out["TRZAX"])
    assert set(v) == {"V1", "V2", "V3"}  # the BSL variety is not at a field site
    assert (v["V1"]["mean_yield_kg_ha"], v["V1"]["numeric_yield_count"], v["V1"]["trial_count"]) == (7000.0, 2, 2)
    assert (v["V2"]["mean_yield_kg_ha"], v["V2"]["numeric_yield_count"], v["V2"]["trial_count"]) == (7000.0, 1, 1)
    assert v["V3"]["mean_yield_kg_ha"] is None and v["V3"]["trial_count"] == 1
    assert out["HORVX"][0]["numeric_yield_count"] == 0 and out["HORVX"][0]["trial_count"] == 1
    assert out["SECCE"] == [] and out["LYPES"] == []  # no field evidence


def test_scope_regional_trial_at_a_field_named_site_is_not_field_evidence(dao):
    assert _run(dao.extrapolate_varieties_batch(["AVESA"], _FIELD_SITES))["AVESA"] == []


def test_main_mode_leaves_forage_out_and_counts_it_once(dao):
    out = _run(dao.extrapolate_varieties_batch(["ZEAMX"], _FIELD_SITES))["ZEAMX"]
    assert [v["variety"] for v in out] == ["G1"]
    assert out[0]["mean_yield_kg_ha"] == 13000.0
    assert out[0]["crop_other_purpose_trials"] == 3  # F1@f03, F1@f04 (twin once), F2@f05
    assert _run(dao.extrapolate_varieties_batch(["SETIT"], _FIELD_SITES))["SETIT"] == []


# ── forage mode ──────────────────────────────────────────────────────────────

def test_forage_mode_averages_known_basis_in_dry_matter_only(dao):
    out = _by_variety(_run(dao.extrapolate_varieties_batch(["ZEAMX"], _FIELD_SITES, purpose="forage"))["ZEAMX"])
    assert set(out) == {"F1", "F2"}  # grain is not forage evidence
    f1, f2 = out["F1"], out["F2"]
    assert (f1["mean_yield_kg_ha"], f1["numeric_yield_count"], f1["trial_count"]) == (21000.0, 2, 2)
    assert f2["mean_yield_kg_ha"] is None and f2["numeric_yield_count"] == 0
    assert f2["trial_count"] == 1 and f2["unknown_basis_trial_count"] == 1
    assert f1["crop_other_purpose_trials"] == 0


def test_forage_mode_with_only_unknown_basis_has_no_number(dao):
    out = _run(dao.extrapolate_varieties_batch(["SETIT"], _FIELD_SITES, purpose="forage"))["SETIT"]
    assert out[0]["mean_yield_kg_ha"] is None and out[0]["unknown_basis_trial_count"] == 1


# ── regional tier ────────────────────────────────────────────────────────────

def test_regional_tier_carries_numeric_aggregate_evidence_only(dao):
    out = _run(dao.extrapolate_varieties_batch(
        _CROPS, _AGG_SITES + [{"name": "f07", "distance": None}], tier="regional"))
    assert out["LYPES"][0]["mean_yield_kg_ha"] == 50000.0
    assert out["AVESA"][0]["mean_yield_kg_ha"] == 4000.0  # scope regional at f07
    assert out["SECCE"] == [] and out["TRZAX"] == []  # BSL kg never numeric evidence
    assert out["ZEAMX"] == [] and out["HORVX"] == []


def test_regional_numeric_trial_count_is_exact_under_a_top_n_cut(dao):
    sites = [{"name": "Poland (national average)", "distance": None}]
    capped = _run(dao.extrapolate_varieties_batch(["CPSAN"], sites, top_n=3, tier="regional"))["CPSAN"]
    assert len(capped) == 3  # the list is cut ...
    assert {v["crop_numeric_trial_count"] for v in capped} == {8}  # ... the crop total is not
    full = _run(dao.extrapolate_varieties_batch(["CPSAN"], sites, top_n=50, tier="regional"))["CPSAN"]
    assert len(full) == 8 and sum(v["numeric_yield_count"] for v in full) == 8
    assert [v["variety"] for v in capped] == [v["variety"] for v in full][:3]  # same order, cut only
    single = _run(dao.extrapolate_varieties("CPSAN", similar_sites_override=sites, top_n=3, tier="regional"))
    assert single["ranked_varieties"] == capped


# ── per-crop == batch, and the prefilter agrees with both ────────────────────

@pytest.mark.parametrize("purpose,tier,sites", [
    ("main", "field", _FIELD_SITES), ("forage", "field", _FIELD_SITES),
    ("main", "regional", _AGG_SITES), ("forage", "regional", _AGG_SITES),
])
def test_batch_equals_per_crop_and_prefilter(dao, purpose, tier, sites):
    kw = {"purpose": purpose, "tier": tier, "top_n": 10}
    batch = _run(dao.extrapolate_varieties_batch(_CROPS, sites, **kw))
    for crop in _CROPS:
        single = _run(dao.extrapolate_varieties(crop, similar_sites_override=sites, **kw))
        assert single["ranked_varieties"] == batch[crop]
    names = [s["name"] for s in sites]
    members = _run(dao._crops_with_analog_trials(_CROPS, names, purpose=purpose, tier=tier))
    assert members == {c for c in _CROPS if batch[c]}


# ── medians ──────────────────────────────────────────────────────────────────

def test_medians_use_distinct_eligible_field_trials_of_the_purpose(dao):
    m = _run(dao.get_crop_yield_medians(["TRZAX", "ZEAMX", "LYPES", "SECCE"], None))
    assert (m["TRZAX"]["median_kg_ha"], m["TRZAX"]["n_trials"]) == (7000.0, 3)  # no BSL, twin once
    assert (m["ZEAMX"]["median_kg_ha"], m["ZEAMX"]["n_trials"]) == (13000.0, 2)  # forage left out
    assert m["LYPES"] == {"median_kg_ha": None, "n_trials": 0, "scope": "crop"}  # aggregate only
    assert m["SECCE"]["median_kg_ha"] is None
    f = _run(dao.get_crop_yield_medians(["ZEAMX", "SETIT"], None, "forage"))
    assert (f["ZEAMX"]["median_kg_ha"], f["ZEAMX"]["n_trials"], f["ZEAMX"]["scope"]) == (21000.0, 2, "crop:forage")
    assert f["SETIT"]["median_kg_ha"] is None and f["SETIT"]["n_trials"] == 0


def test_single_median_equals_batch(dao):
    for purpose in ("main", "forage"):
        for crop in _CROPS:
            dao_mod._MEDIAN_CACHE.clear()
            single = _run(dao.get_crop_yield_median(crop, None, purpose))
            dao_mod._MEDIAN_CACHE.clear()
            assert single == _run(dao.get_crop_yield_medians([crop], None, purpose))[crop]


# ── evidence listing ─────────────────────────────────────────────────────────

def _evidence(dao, crop, sites, **kw):
    return _run(dao.list_trial_evidence(crop=crop, similar_sites=sites, variety=None, irrigation_uri=None,
                                        page=1, page_size=50, **kw))


def test_evidence_lists_distinct_trials_with_their_tier(dao):
    page = _evidence(dao, "TRZAX", _FIELD)
    assert page["total"] == 3 and len(page["items"]) == 3  # twin once, BSL absent
    assert {i["tier"] for i in page["items"]} == {"field"}
    assert sorted(i["yield_kg_ha"] for i in page["items"]) == [6000.0, 7000.0, 8000.0]
    reg = _evidence(dao, "LYPES", _AGG, tier="regional")
    assert reg["total"] == 1 and reg["items"][0]["tier"] == "regional"
    assert _evidence(dao, "LYPES", _AGG)["total"] == 0  # not field evidence
    assert _evidence(dao, "SECCE", _AGG, tier="regional")["total"] == 0  # BSL kg


def test_evidence_forage_mode_lists_dry_matter_yields(dao):
    page = _evidence(dao, "ZEAMX", _FIELD, purpose="forage")
    assert page["total"] == 2 and page["purpose"] == "forage"
    assert sorted(i["yield_kg_ha"] for i in page["items"]) == [20000.0, 22000.0]
    assert {i["basis"] for i in page["items"]} == {"dry_matter"}
    assert _evidence(dao, "ZEAMX", _FIELD)["total"] == 2  # main: the grain trials only


# ── recommend_for_conditions end to end ──────────────────────────────────────

def _recommend(dao, **kw):
    dao_mod._RECOMMEND_CACHE.clear()
    cond = {"climate_class": "Cfb", "soil_type": None, "irrigation_regime": None, "management": "any",
            "season": "all", "top_n": 30, **kw}
    return _run(dao.recommend_for_conditions(cond))


def test_recommend_main_field_before_regional_with_honest_numbers(dao):
    out = _recommend(dao)
    recs = {r["crop"]["eppo"]: r for r in out["recommendations"]}
    assert out["evidence_policy"] == ep.POLICY_VERSION and out["conditions"]["purpose"] == "main"
    # SECCE: BSL only; SETIT: forage only; AVESA: regional scope at a field-named site, which
    # no production trial does, is outside both site sets.
    assert set(recs) == {"TRZAX", "ZEAMX", "HORVX", "LYPES"}
    trz = recs["TRZAX"]
    assert trz["evidence"]["tier"] == "field" and trz["yield"]["expected_kg_ha"] == 7000.0
    assert trz["evidence"]["regional_trial_count"] == 0
    assert recs["ZEAMX"]["yield"]["expected_kg_ha"] == 13000.0
    assert recs["ZEAMX"]["evidence"]["other_purpose_trials"] == {"forage": 3}
    lyp = recs["LYPES"]
    assert lyp["evidence"]["tier"] == "regional" and lyp["yield"]["expected_kg_ha"] == 50000.0
    assert lyp["trust"]["level"] == "low" and lyp["fit"]["relative_yield_pct"] is None
    assert {"regional_evidence_only", "regional_not_comparable"} <= set(lyp["trust"]["data_gaps"])
    assert recs["HORVX"]["yield"]["expected_kg_ha"] is None  # presence only: null, never 0
    tiers = [r["evidence"]["tier"] for r in out["recommendations"]]
    assert tiers == sorted(tiers, key=lambda t: t == "regional")  # field recs first


def test_recommend_forage_mode(dao):
    out = _recommend(dao, purpose="forage")
    recs = {r["crop"]["eppo"]: r for r in out["recommendations"]}
    assert set(recs) == {"ZEAMX", "SETIT"}
    z = recs["ZEAMX"]
    assert z["yield"]["expected_kg_ha"] == 21000.0 and z["yield"]["basis"] == "dry_matter"
    assert z["evidence"]["purpose"] == "forage" and z["evidence"]["other_purpose_trials"] == {}
    s = recs["SETIT"]
    # the field forage trial has an unknown basis; the numeric regional one must not replace it
    assert s["evidence"]["tier"] == "field" and s["evidence"]["regional_trial_count"] == 1
    assert s["yield"]["expected_kg_ha"] is None and s["yield"]["basis"] is None
    assert s["yield"]["n_trials"] == 1 and "forage_basis_unknown" in s["trust"]["data_gaps"]
    assert "regional_evidence_only" not in s["trust"]["data_gaps"]
    assert s["evidence"]["unknown_basis_trials"] == 1 and s["trust"]["level"] == "low"
    assert out["conditions"]["purpose"] == "forage"
