from app.graph.crop_plan import build_segment_entity, sanity_warnings, segment_urn


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


def test_builds_segment_omits_species_when_crop_missing():
    seg = _seg()
    seg["crop"] = None
    e = build_segment_entity("montiko", "urn:ngsi-ld:AgriParcel:p-1", "2026", 0, seg)
    assert "species" not in e


def test_sanity_warnings_invalid_role_and_method():
    segs = [
        {"crop": "Vicia sativa", "role": "not_a_role", "termination_method": "not_a_method"},
        {"crop": "Zea mays", "role": "main_crop", "termination_method": "harvest"},
    ]
    out = sanity_warnings(segs)
    assert {"type": "invalid_role", "seq": 0, "value": "not_a_role"} in out
    assert {"type": "invalid_termination_method", "seq": 0, "value": "not_a_method"} in out
    # seg 1 is valid -> no warnings about it
    assert not any(w["seq"] == 1 for w in out if w["type"] in ("invalid_role", "invalid_termination_method"))


def test_sanity_warnings_window_overlap():
    segs = [
        {"crop": "Vicia sativa", "role": "cover_crop", "termination_method": "roller_crimper",
         "sowing_window": ["2025-11-01", "2025-12-20"]},
        {"crop": "Zea mays", "role": "main_crop", "termination_method": "harvest",
         "sowing_window": ["2025-12-01", "2026-05-01"]},
    ]
    out = sanity_warnings(segs)
    assert {"type": "window_overlap", "seq": [0, 1]} in out


def test_sanity_warnings_no_overlap_and_missing_windows_are_silent():
    segs = [
        {"crop": "Vicia sativa", "role": "cover_crop", "termination_method": "roller_crimper",
         "sowing_window": ["2025-11-01", "2025-11-20"]},
        {"crop": "Zea mays", "role": "main_crop", "termination_method": "harvest",
         "sowing_window": ["2026-04-10", "2026-05-01"]},
        {"crop": "Brassica", "role": "catch_crop", "termination_method": "grazing"},  # no window
    ]
    out = sanity_warnings(segs)
    assert out == []
