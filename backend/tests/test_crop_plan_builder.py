from app.graph.crop_plan import build_segment_entity, segment_urn


def _seg():
    return {"crop": "Vicia sativa", "variety": "comun", "role": "cover_crop",
            "sowing_window": ["2025-11-01", "2025-11-20"],
            "termination_method": "roller_crimper", "expected_termination": "2026-03-15"}


def test_id_scheme_includes_seq():
    assert segment_urn("montiko", "urn:ngsi-ld:AgriParcel:p-1", "2026", 0) == \
        "urn:ngsi-ld:AgriCrop:montiko:p-1:2026:0"


def test_builds_planned_segment_with_windows_no_actual_dates():
    e = build_segment_entity("montiko", "urn:ngsi-ld:AgriParcel:p-1", "2026", 0, _seg())
    assert e["id"] == "urn:ngsi-ld:AgriCrop:montiko:p-1:2026:0"
    assert e["type"] == "AgriCrop"
    assert e["hasAgriParcel"]["object"] == "urn:ngsi-ld:AgriParcel:p-1"
    assert e["role"]["value"] == "cover_crop"
    assert e["cropSeason"]["value"] == "2026"
    assert e["seq"]["value"] == 0
    assert e["sowingWindowStart"]["value"]["@value"] == "2025-11-01"
    assert e["sowingWindowEnd"]["value"]["@value"] == "2025-11-20"
    assert e["expectedTerminationDate"]["value"]["@value"] == "2026-03-15"
    assert e["terminationMethod"]["value"] == "roller_crimper"
    assert e["status"]["value"] == "planned"
    # actual dates absent until advance
    assert "plantingDate" not in e
    assert "terminationDate" not in e
