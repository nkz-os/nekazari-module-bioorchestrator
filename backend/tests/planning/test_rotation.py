from app.planning.rotation import Constraint, PastCrop, rotation_reasons

WHEAT_WHEAT = Constraint("wheat", "wheat", "restriction", 2, "take-all", "FAO")
CHICKPEA_WHEAT = Constraint("chickpea", "wheat", "benefit", 1, "N fixation", "FAO")


def test_restriction_inside_interval_names_the_first_allowed_campaign():
    out = rotation_reasons([PastCrop("wheat", 2026)], "wheat", 2027, [WHEAT_WHEAT])
    assert out == [{"code": "rotation_restriction", "gate": "rotation", "params": {
        "previous": "wheat", "previous_campaign": 2026, "interval_years": 2,
        "allowed_from": 2029, "source": "FAO"}}]


def test_restriction_ends_after_the_interval():
    assert rotation_reasons([PastCrop("wheat", 2024)], "wheat", 2027, [WHEAT_WHEAT]) == []


def test_latest_previous_crop_wins_and_same_or_later_campaigns_are_ignored():
    hist = [PastCrop("wheat", 2025), PastCrop("wheat", 2026), PastCrop("wheat", 2027)]
    out = rotation_reasons(hist, "wheat", 2027, [WHEAT_WHEAT])
    assert len(out) == 1 and out[0]["params"]["previous_campaign"] == 2026


def test_benefit_only_right_after_the_previous_crop():
    assert rotation_reasons([PastCrop("chickpea", 2026)], "wheat", 2027, [CHICKPEA_WHEAT])[0]["code"] == "rotation_benefit"
    assert rotation_reasons([PastCrop("chickpea", 2025)], "wheat", 2027, [CHICKPEA_WHEAT]) == []
