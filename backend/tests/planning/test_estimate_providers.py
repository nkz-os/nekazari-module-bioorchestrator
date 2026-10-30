import pytest

from app.planning.estimates import estimate_providers


@pytest.mark.parametrize(("eppo", "trials", "regime", "country", "expected"), [
    ("TRZAX", True, "rainfed", "ES", ["trial_evidence", "aquacrop"]),  # the validated scope
    ("TRZAX", True, "irrigated", "ES", ["trial_evidence"]),
    ("TRZAX", True, "rainfed", "FR", ["trial_evidence"]),
    ("TRZAX", True, None, "ES", ["trial_evidence"]),  # regime unknown: the engine is not offered
    ("TRZAX", False, "rainfed", "ES", ["aquacrop"]),
    ("HORVX", True, "rainfed", "ES", ["trial_evidence"]),  # barley is not validated
    (None, False, "rainfed", "ES", []),
])
def test_estimate_providers_list_only_the_providers_that_can_answer(eppo, trials, regime, country, expected):
    assert estimate_providers(eppo, has_trials=trials, regime=regime, country=country) == expected
