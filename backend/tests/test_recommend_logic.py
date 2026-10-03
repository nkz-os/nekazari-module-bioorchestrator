"""Pure ranking and assembly rules of the recommendation contract."""
import pytest

from app.graph import recommend as r


def test_relative_yield_basic():
    assert r.relative_yield(5500.0, 5000.0, 40) == (pytest.approx(10.0), None)


@pytest.mark.parametrize("median,n,gap", [(0.0, 40, "reference_zero"), (5000.0, 2, "reference_too_small"),
                                          (None, 40, "reference_too_small")])
def test_relative_yield_undefined(median, n, gap):
    assert r.relative_yield(5000.0, median, n) == (None, gap)


def test_relative_yield_missing_expected():
    assert r.relative_yield(None, 5000.0, 40) == (None, "no_expected_yield")


def test_cv():
    assert r.stability_cv(5000.0, 500.0, 10) == (pytest.approx(0.1), None)


@pytest.mark.parametrize("mean,sd,n", [(0.0, 10.0, 5), (5000.0, 500.0, 1), (5000.0, None, 5), (None, 1.0, 5)])
def test_cv_undefined(mean, sd, n):
    assert r.stability_cv(mean, sd, n) == (None, "cv_undefined")


def test_disease_summary_counts_resistant_at_or_above_0_7():
    scores = {"rust": {"value": 0.8}, "mildew": {"value": 0.4}, "septoria": {"value": 0.7}, "x": {"value": None}}
    assert r.disease_summary(scores) == {"resistant": 2, "total": 3}


def test_sowing_info_falls_back_to_season_slot():
    info = r.sowing_info("TRZAX", "Cfb", [])
    assert info == {"sowing_type": "autumn", "sowing_window": None, "cycle_days": None,
                    "source": "crop_season_slot"}


def test_sowing_info_both_slots_is_null_type():
    assert r.sowing_info("ZZZZZ", "Cfb", [])["sowing_type"] is None


def test_sowing_info_table_row_wins():
    row = {"eppo": "CIEAR", "sowing_type": "spring", "koppen": ["Cfb"], "start_month": 2,
           "end_month": 3, "cycle_days": 120, "source": "ref-1"}
    info = r.sowing_info("CIEAR", "Cfb", [row])
    assert info == {"sowing_type": "spring", "sowing_window": {"start_month": 2, "end_month": 3},
                    "cycle_days": 120, "source": "ref-1"}


_ES_ROW = {"eppo": "TRZAX", "sowing_type": "autumn", "koppen": ["Csa"], "start_month": 10,
           "end_month": 12, "cycle_days": None, "source": "ref-es", "countries": ["ES"]}
_ANY_ROW = {**{k: v for k, v in _ES_ROW.items() if k != "countries"}, "source": "ref-any"}


def test_sowing_info_country_scoped_row_matches_its_country():
    assert r.sowing_info("TRZAX", "Csa", [_ES_ROW], country="ES")["source"] == "ref-es"


def test_sowing_info_country_mismatch_falls_back_to_season_slot():
    info = r.sowing_info("TRZAX", "Csa", [_ES_ROW], country="FR")
    assert info["source"] == "crop_season_slot" and info["sowing_window"] is None


def test_sowing_info_unknown_country_does_not_match_scoped_row():
    assert r.sowing_info("TRZAX", "Csa", [_ES_ROW])["source"] == "crop_season_slot"
    assert r.sowing_info("TRZAX", "Csa", [_ES_ROW], country=None)["source"] == "crop_season_slot"


@pytest.mark.parametrize("country", [None, "ES", "FR"])
def test_sowing_info_unscoped_row_matches_any_country(country):
    assert r.sowing_info("TRZAX", "Csa", [_ANY_ROW], country=country)["source"] == "ref-any"


def test_sowing_info_first_match_wins_after_country_filter():
    rows = [_ES_ROW, _ANY_ROW]
    assert r.sowing_info("TRZAX", "Csa", rows, country="ES")["source"] == "ref-es"
    assert r.sowing_info("TRZAX", "Csa", rows, country="FR")["source"] == "ref-any"


def _rec(eppo, rel, n, soil="suitable", frost="none"):
    return {"crop": {"eppo": eppo}, "fit": {"relative_yield_pct": rel},
            "yield": {"n_trials": n},
            "suitability": {"soil": {"level": soil}, "frost": {"level": frost}}}


def test_rank_blockers_first_then_relative_then_trials_then_eppo():
    recs = [_rec("AAA", 30.0, 5, soil="unsuitable"), _rec("BBB", 5.0, 9), _rec("CCC", 5.0, 20),
            _rec("DDD", None, 50), _rec("EEE", 10.0, 3, frost="risk"), _rec("FFF", 5.0, 20)]
    assert [x["crop"]["eppo"] for x in r.rank_recommendations(recs)] == \
        ["CCC", "FFF", "BBB", "DDD", "AAA", "EEE"]


def test_recommendation_id_stable_and_condition_sensitive():
    c = {"climate_class": "Cfb", "irrigation_regime": "secano"}
    a = r.recommendation_id(c, "TRZAX", "autumn")
    assert a == r.recommendation_id(dict(c), "TRZAX", "autumn")
    assert a != r.recommendation_id({**c, "climate_class": "Csa"}, "TRZAX", "autumn")


def test_build_recommendation_none_without_varieties():
    assert r.build_recommendation(
        eppo="TRZAX", scientific_name="Triticum aestivum", conditions={}, varieties=[],
        reference={"median_kg_ha": 5000.0, "n_trials": 40, "scope": "crop"},
        soil_verdict={"verdict": "unknown", "reason": ""}, water=None, frost_level="unknown",
        sowing=r.sowing_info("TRZAX", None, []), data_gaps_extra=[], assumptions=[]) is None


def test_build_recommendation_shape():
    v = {"variety": "V1", "variety_uri": "urn:x", "mean_yield_kg_ha": 5500.0, "min_yield_kg_ha": 4000.0,
         "max_yield_kg_ha": 7000.0, "stddev_yield_kg_ha": 550.0, "numeric_yield_count": 12,
         "trial_count": 12, "trial_sites": ["site-a", "site-b"], "trial_years": [2015, 2020],
         "disease_scores": {"rust": {"value": 0.9}}, "confidence": "high", "source_ids": ["SRC1"]}
    rec = r.build_recommendation(
        eppo="TRZAX", scientific_name="Triticum aestivum", conditions={"climate_class": "Cfb"},
        varieties=[v], reference={"median_kg_ha": 5000.0, "n_trials": 40, "scope": "crop×irrigation"},
        soil_verdict={"verdict": "marginal", "reason": "pH high"},
        water={"level": "low", "etc_mm": 300.0}, frost_level="none",
        sowing=r.sowing_info("TRZAX", "Cfb", []), data_gaps_extra=[], assumptions=[])
    assert rec["crop"] == {"eppo": "TRZAX", "scientific_name": "Triticum aestivum", "sowing_type": "autumn"}
    assert rec["fit"]["relative_yield_pct"] == pytest.approx(10.0)
    assert rec["fit"]["stability_cv"] == pytest.approx(0.1)
    assert rec["yield"] == {"expected_kg_ha": 5500.0, "interval": [4000.0, 7000.0],
                            "interval_method": "observed_range", "sd": 550.0, "n_trials": 12, "n_sites": 2}
    assert rec["suitability"]["soil"] == {"level": "marginal", "warnings": ["pH high"]}
    assert rec["suitability"]["water"] == {"level": "low", "etc_mm": 300.0}
    assert rec["suitability"]["frost"] == {"level": "none"}
    assert rec["varieties"][0]["disease_summary"] == {"resistant": 1, "total": 1}
    assert rec["evidence"] == {"trial_count": 12, "sources": ["SRC1"], "sites": ["site-a", "site-b"],
                               "years": [2015, 2020]}
    assert rec["trust"]["level"] == "high"
    assert rec["season"]["source"] == "crop_season_slot"
    assert "recommendation_id" in rec


def test_build_recommendation_low_trust_and_gaps():
    v = {"variety": "V1", "variety_uri": "urn:x", "mean_yield_kg_ha": 5000.0, "min_yield_kg_ha": 5000.0,
         "max_yield_kg_ha": 5000.0, "stddev_yield_kg_ha": None, "numeric_yield_count": 1,
         "trial_count": 1, "trial_sites": ["site-a"], "trial_years": [2019], "disease_scores": {},
         "confidence": None}
    rec = r.build_recommendation(
        eppo="TRZAX", scientific_name="T", conditions={}, varieties=[v],
        reference={"median_kg_ha": 0.0, "n_trials": 40, "scope": "crop"},
        soil_verdict={"verdict": "unknown", "reason": "x"}, water=None, frost_level="unknown",
        sowing=r.sowing_info("TRZAX", None, []), data_gaps_extra=["climate_detail_unavailable"], assumptions=[])
    assert rec["trust"]["level"] == "low"
    assert set(rec["trust"]["data_gaps"]) >= {"reference_zero", "cv_undefined", "low_trial_count",
                                              "climate_detail_unavailable"}
    assert rec["suitability"]["water"] == {"level": "unknown", "etc_mm": None}


# ── final fix wave ──────────────────────────────────────────────────────────
def _nonnumeric_variety(**kw):
    v = {"variety": "V1", "variety_uri": "urn:x", "mean_yield_kg_ha": None, "min_yield_kg_ha": None,
         "max_yield_kg_ha": None, "stddev_yield_kg_ha": None, "numeric_yield_count": None,
         "trial_count": 12, "trial_sites": ["site-a"], "trial_years": [], "disease_scores": {},
         "confidence": "high"}
    return {**v, **kw}


def _build(v):
    return r.build_recommendation(
        eppo="TRZAX", scientific_name="T", conditions={}, varieties=[v],
        reference={"median_kg_ha": 5000.0, "n_trials": 40, "scope": "crop"},
        soil_verdict={"verdict": "unknown", "reason": ""}, water=None, frost_level="unknown",
        sowing=r.sowing_info("TRZAX", None, []), data_gaps_extra=[], assumptions=[])


def test_n_trials_counts_numeric_trials_only():
    rec = _build(_nonnumeric_variety())
    assert rec["yield"]["n_trials"] == 0
    assert rec["varieties"][0]["n_trials"] == 0
    assert rec["trust"]["level"] == "low"
    assert "low_trial_count" in rec["trust"]["data_gaps"]
    assert rec["evidence"]["trial_count"] == 12


def test_rank_uses_numeric_n_trials():
    a = _build(_nonnumeric_variety())
    b = _build(_nonnumeric_variety(numeric_yield_count=3, trial_count=3))
    a["crop"]["eppo"], b["crop"]["eppo"] = "AAA", "BBB"
    assert [x["crop"]["eppo"] for x in r.rank_recommendations([a, b])] == ["BBB", "AAA"]


def test_evidence_years_null_when_unknown():
    assert _build(_nonnumeric_variety())["evidence"]["years"] is None
    assert _build(_nonnumeric_variety(trial_years=[2021, 2018]))["evidence"]["years"] == [2018, 2021]
