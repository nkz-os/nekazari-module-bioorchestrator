from app.api.v1 import graph as graph_mod


def test_water_budget_envelope_uses_dao_confidence():
    result = {
        "irrigation_required_mm": 18.0, "etc_weekly_mm": 22.5, "kc": 1.1,
        "confidence": "low",
        "confidence_notes": "Using default AWC 120mm (Soil module unavailable)",
    }
    ag = graph_mod._water_budget_agronomic(result)
    assert ag["irrigation_required_mm"]["value"] == 18.0
    assert ag["irrigation_required_mm"]["confidence"] == "low"
    # the human reason rides in notes (never blocks the suggestion)
    assert any("AWC" in n for n in ag["irrigation_required_mm"]["notes"])
    assert ag["kc"]["value"] == 1.1
    assert ag["etc_weekly_mm"]["confidence"] == "low"


def test_water_budget_envelope_high_when_all_sources():
    result = {
        "irrigation_required_mm": 12.0, "etc_weekly_mm": 20.0, "kc": 1.0,
        "confidence": "high", "confidence_notes": "All data sources available",
    }
    ag = graph_mod._water_budget_agronomic(result)
    assert ag["irrigation_required_mm"]["confidence"] == "high"
    assert ag["irrigation_required_mm"]["notes"] == []  # boilerplate note suppressed
