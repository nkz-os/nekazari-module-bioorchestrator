import json
from pathlib import Path

BUNDLE = Path(__file__).parent / "fixtures" / "vision_2024_fungi.jsonld"
AREAL = {"yield_kg_ha", "grain_yield_kg_ha", "dry_yield_kg_ha",
         "fresh_yield_kg_ha", "truffle_yield_kg_ha"}


def test_no_fabricated_yield_in_bundle():
    graph = json.loads(BUNDLE.read_text())["@graph"]
    vts = [n for n in graph if n.get("@type") == "VarietyTrial"]
    assert vts, "bundle must contain VarietyTrials"
    for vt in vts:
        ydt = (vt.get("yield_data_type") or "").lower()
        if ydt not in AREAL:
            assert vt.get("yield_kg_ha") in (None,), (
                f"{vt.get('id')}: non-areal yield_data_type {ydt!r} "
                f"must have null yield_kg_ha, got {vt.get('yield_kg_ha')!r}")
            assert vt.get("ranking_eligible") is False, (
                f"{vt.get('id')}: non-areal trial must be ranking_eligible=false")
