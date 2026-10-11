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
    assert out.contract_version == m.CONTRACT_VERSION == "1.0"


def test_plan_rejects_unknown_regime_and_empty_cycles():
    with pytest.raises(ValidationError):
        m.PlanIn.model_validate({"campaign": 2027, "regime": "drip", "management": "organic", "cycles": []})
