import pytest
from pydantic import ValidationError

from app.planning import models as m


def test_fit_response_round_trip():
    payload = {
        "parcel_id": "urn:ngsi-ld:AgriParcel:t:p1", "campaign": 2027,
        "facts": {"climate_class": "Csa", "climate_source": "chelsa_v2.1", "soil_texture": None,
                  "soil_ph": None, "soil_source": "unavailable", "regime": None,
                  "regime_source": "unknown", "history_available": False},
        "species": [{"species": "wheat", "eppo": "TRZAX", "names": {"es": "trigo"}, "verdict": "caveats",
                     "reasons": [{"code": "regime_missing", "gate": "data", "params": {}}],
                     "sowing": {"sowing_type": "autumn", "start_month": 10, "end_month": 12},
                     "has_variety_trials": True, "estimate_providers": ["trial_evidence"]}],
        "graphs": ["global"],
    }
    out = m.FitResponse.model_validate(payload)
    assert out.contract_version == m.CONTRACT_VERSION == "2.0"


def test_plan_rejects_unknown_regime_and_empty_cycles():
    with pytest.raises(ValidationError):
        m.PlanIn.model_validate({"campaign": 2027, "regime": "drip", "management": "organic", "cycles": []})


def test_estimate_1_1_is_additive_and_range_only_is_valid():
    base = {"provider_id": "x", "provider_version": "1", "basis": "trial_evidence",
            "percentiles": {"p10": 1, "p50": 2, "p90": 3}, "n": 3, "n_kind": "trials",
            "validation": {"status": "not_applicable"}}
    old = m.Estimate.model_validate(base)  # a 1.0 payload still parses
    assert old.range is None and old.coverage is None and old.moisture.basis == "as_reported"
    only = m.Estimate.model_validate({**base, "basis": "gap_corrected", "percentiles": None,
                                      "range": {"low": 2, "high": 5}, "coverage": 0.78,
                                      "moisture": {"basis": "commercial", "pct": 13.0}})
    assert only.percentiles is None and only.range.high == 5
    assert m.Unavailable(provider_id="aquacrop", reason="engine_error", code="soil_unavailable").code


def test_the_spanish_regime_words_of_earlier_clients_are_read_as_the_new_ones():
    # Owner decision (2026-10-13): rainfed | irrigated; the Spanish words are accepted on input only.
    plan = m.PlanIn.model_validate({"campaign": 2027, "regime": "secano", "management": "conventional",
                                    "cycles": [{"role": "main_crop", "crop": "wheat", "sowing_start": "2026-10-20"}]})
    assert plan.regime == "rainfed"
    assert m.PlanIn.model_validate({**plan.model_dump(mode="json"), "regime": "regadío"}).regime == "irrigated"
