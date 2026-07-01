from datetime import date
from app.graph.rule_engine import evaluate_conditions, flatten_context, build_advisory

BASE = {"crop.role": "cover_crop", "crop.status": "active",
        "crop.termination_method": "roller_crimper", "phenology.current_stage": "flowering"}

def test_all_clauses_match():
    tree = {"all": [
        {"field": "crop.role", "op": "eq", "value": "cover_crop"},
        {"field": "phenology.current_stage", "op": "eq", "value": "flowering"},
    ]}
    assert evaluate_conditions(tree, BASE) is True

def test_one_clause_fails():
    tree = {"all": [{"field": "phenology.current_stage", "op": "eq", "value": "maturity"}]}
    assert evaluate_conditions(tree, BASE) is False

def test_missing_field_is_false_not_error():
    tree = {"all": [{"field": "water_deficit_mm", "op": "gt", "value": 30}]}
    assert evaluate_conditions(tree, BASE) is False

def test_numeric_and_membership_ops():
    ctx = {"n": 5, "stage": "tillering"}
    assert evaluate_conditions({"all": [{"field": "n", "op": "lte", "value": 7}]}, ctx) is True
    assert evaluate_conditions({"all": [{"field": "n", "op": "gte", "value": 5}]}, ctx) is True
    assert evaluate_conditions({"all": [{"field": "n", "op": "lt", "value": 7}]}, ctx) is True
    assert evaluate_conditions({"all": [{"field": "n", "op": "lt", "value": 5}]}, ctx) is False
    assert evaluate_conditions({"all": [{"field": "n", "op": "gt", "value": 7}]}, ctx) is False
    assert evaluate_conditions({"all": [{"field": "stage", "op": "in", "value": ["tillering", "vegetative"]}]}, ctx) is True
    assert evaluate_conditions({"all": [{"field": "stage", "op": "nin", "value": ["maturity"]}]}, ctx) is True

def test_empty_tree_is_true():
    assert evaluate_conditions({}, BASE) is True
    assert evaluate_conditions({"all": []}, BASE) is True


def test_flatten_maps_crop_and_phenology():
    crop = {"role": "cover_crop", "status": "active", "species": "VICSA",
            "terminationMethod": "roller_crimper", "sowingWindowStart": "2026-10-20"}
    ctx = flatten_context(crop, {"phenology.current_stage": "flowering", "water_deficit_mm": 12},
                          today=date(2026, 10, 15))
    assert ctx["crop.role"] == "cover_crop"
    assert ctx["crop.termination_method"] == "roller_crimper"
    assert ctx["crop.days_until_sowing_window"] == 5
    assert ctx["phenology.current_stage"] == "flowering"
    assert ctx["water_deficit_mm"] == 12

def test_flatten_tolerates_missing_sowing_window():
    ctx = flatten_context({"role": "main_crop", "status": "active"}, {}, today=date(2026, 1, 1))
    assert "crop.days_until_sowing_window" not in ctx
    assert ctx["crop.role"] == "main_crop"

def test_build_advisory_shape_and_deterministic_id():
    rule = {"id": "cover_crop_termination_flowering",
            "action": {"operation_type": "tillage", "urgency": "high", "window_days": 7,
                       "description_template": "Tumbar la cubierta de {crop.species} con roller crimper"},
            "source_doi": "10.x", "source_short": "INTIA"}
    ctx = {"crop.species": "VICSA", "phenology.current_stage": "flowering"}
    adv = build_advisory(rule, ctx, "montiko", "urn:ngsi-ld:AgriParcel:montiko:p1",
                         "urn:ngsi-ld:AgriCrop:montiko:p1:2026", "flowering", now="2026-06-30T10:00:00Z")
    assert adv["id"] == "urn:ngsi-ld:CropAdvisory:montiko:p1:cover_crop_termination_flowering:flowering"
    assert adv["type"] == "CropAdvisory"
    assert adv["hasAgriParcel"]["object"] == "urn:ngsi-ld:AgriParcel:montiko:p1"
    assert adv["hasAgriCrop"]["object"] == "urn:ngsi-ld:AgriCrop:montiko:p1:2026"
    assert adv["operationType"]["value"] == "tillage"
    assert adv["description"]["value"] == "Tumbar la cubierta de VICSA con roller crimper"
    assert adv["urgency"]["value"] == "high"
    assert adv["phenologyStage"]["value"] == "flowering"
    assert adv["status"]["value"] == "open"

def test_build_advisory_omits_missing_action_fields_no_null_props():
    # A malformed rule missing operation_type/urgency must NOT yield value:null
    # properties (Orion-LD rejects null → whole advisory silently dropped).
    rule = {"id": "r1", "action": {"description_template": "do something"}}
    adv = build_advisory(rule, {}, "montiko", "urn:ngsi-ld:AgriParcel:montiko:p1",
                         "urn:ngsi-ld:AgriCrop:montiko:p1:2026", "flowering", now="2026-06-30T10:00:00Z")
    assert "operationType" not in adv
    assert "urgency" not in adv
    assert "cropSpecies" not in adv
    assert adv["id"].endswith("r1:flowering")
    assert adv["type"] == "CropAdvisory"
    assert adv["status"]["value"] == "open"
    assert all(not (isinstance(v, dict) and v.get("type") == "Property" and v.get("value") is None)
               for v in adv.values())
