from app.graph.rule_engine import evaluate_conditions

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
    assert evaluate_conditions({"all": [{"field": "n", "op": "gt", "value": 7}]}, ctx) is False
    assert evaluate_conditions({"all": [{"field": "stage", "op": "in", "value": ["tillering", "vegetative"]}]}, ctx) is True
    assert evaluate_conditions({"all": [{"field": "stage", "op": "nin", "value": ["maturity"]}]}, ctx) is True

def test_empty_tree_is_true():
    assert evaluate_conditions({}, BASE) is True
    assert evaluate_conditions({"all": []}, BASE) is True
