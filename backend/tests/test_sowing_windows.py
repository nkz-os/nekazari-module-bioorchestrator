import json

import pytest

from app.services import sowing_windows as sw


def _write(tmp_path, rows):
    p = tmp_path / "t.json"
    p.write_text(json.dumps({"version": 1, "rows": rows}))
    return p


def test_default_table_loads_and_is_list():
    assert isinstance(sw.load_rows(), list)


def test_default_path_is_backend_data():
    assert sw.DEFAULT_PATH.parts[-2:] == ("data", "sowing_windows.json")
    assert sw.DEFAULT_PATH.exists()


def test_valid_row(tmp_path):
    row = {"eppo": "CIEAR", "sowing_type": "spring", "koppen": ["Cfb"], "start_month": 2,
           "end_month": 3, "cycle_days": 120, "source": "ref-1"}
    assert sw.load_rows(_write(tmp_path, [row])) == [row]


@pytest.mark.parametrize("field", ["eppo", "sowing_type", "koppen", "start_month", "end_month", "source"])
def test_missing_required_field_rejected(tmp_path, field):
    row = {"eppo": "CIEAR", "sowing_type": "spring", "koppen": ["Cfb"], "start_month": 2,
           "end_month": 3, "source": "ref-1"}
    del row[field]
    with pytest.raises(ValueError, match=field):
        sw.load_rows(_write(tmp_path, [row]))


@pytest.mark.parametrize("patch", [{"start_month": 0}, {"end_month": 13}, {"sowing_type": "winter"},
                                   {"source": "  "}, {"koppen": []}])
def test_bad_values_rejected(tmp_path, patch):
    row = {"eppo": "CIEAR", "sowing_type": "spring", "koppen": ["Cfb"], "start_month": 2,
           "end_month": 3, "source": "ref-1", **patch}
    with pytest.raises(ValueError):
        sw.load_rows(_write(tmp_path, [row]))
