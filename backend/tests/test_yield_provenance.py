from app.graph.dao import _yield_provenance


def test_all_measured():
    assert _yield_provenance(0, 5) == "measured"


def test_all_derived():
    assert _yield_provenance(5, 5) == "derived"


def test_partial():
    assert _yield_provenance(2, 5) == "partial"


def test_no_trials_is_measured():
    assert _yield_provenance(0, 0) == "measured"


def test_none_inputs_are_safe():
    assert _yield_provenance(None, None) == "measured"
