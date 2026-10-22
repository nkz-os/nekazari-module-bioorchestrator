"""phenology-params emits the additive `agronomic` envelope (P1 contract)."""
from app.api.v1 import graph as graph_mod


def _build_agronomic(params: dict) -> dict:
    return graph_mod._phenology_agronomic(params)


def test_exact_match_real_value_is_high():
    params = {
        "kc": 1.15, "d1": 0.3, "d2": 0.7, "mds_ref": 0.18, "ky": None,
        "is_default": False, "match_level": "exact",
        "provenance": {"short": "FAO-56", "doi": "10.x", "institution": "FAO"},
    }
    ag = _build_agronomic(params)
    assert ag["kc"]["value"] == 1.15
    assert ag["kc"]["confidence"] == "high"
    assert ag["kc"]["source"]["short"] == "FAO-56"
    # missing value still wrapped, low + note
    assert ag["ky"]["value"] is None
    assert ag["ky"]["confidence"] == "low"
    assert ag["ky"]["notes"]


def test_default_value_is_low_regardless_of_match():
    params = {
        "kc": 0.85, "d1": None, "d2": None, "mds_ref": None, "ky": None,
        "is_default": True, "match_level": "exact",
        "provenance": {"short": None},
    }
    ag = _build_agronomic(params)
    assert ag["kc"]["confidence"] == "low"
    assert ag["kc"]["source"]["short"] == "default"  # empty short → "default"


def test_generic_match_is_medium():
    params = {
        "kc": 1.0, "d1": None, "d2": None, "mds_ref": None, "ky": 1.05,
        "is_default": False, "match_level": "generic",
        "provenance": {"short": "AquaCrop"},
    }
    ag = _build_agronomic(params)
    assert ag["kc"]["confidence"] == "medium"
    assert ag["ky"]["value"] == 1.05
    assert ag["ky"]["confidence"] == "medium"
