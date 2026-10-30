"""Generator of data/cover_crop_reference.yaml from the INRAE means table."""
from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from scripts import build_cover_crop_reference as gen

DATA = Path(__file__).resolve().parent.parent / "data" / "cover_crop_reference.yaml"
RAW = Path(os.getenv("COVER_CROP_RAW", "")) if os.getenv("COVER_CROP_RAW") else None


def _row(common, latin, part="Aerial DM", stage="Immature", dm=(None, None, None, None), n=(None,) * 4, cn=(None,) * 4):
    r = [None] * 66
    r[0], r[1], r[3], r[4] = common, latin, part, stage
    r[5], r[6], r[7], r[8] = dm
    r[57], r[58], r[59], r[60] = n
    r[62], r[63], r[64], r[65] = cn
    return tuple(r)


HEADER_OK = {4: {5: "(ton ha-1)", 57: "(% DM)"}, 3: {0: "Common name", 1: "Latin name", 3: "Plant part", 4: "Harvest stage", 57: "Total N", 62: "C/N"},
             5: {5: "Median", 6: "Mean", 7: "SE", 8: "n", 57: "Median", 58: "Mean", 59: "SE", 60: "n",
                 62: "Median", 63: "Mean", 64: "SE", 65: "n"}}


def test_season_from_common_name_suffix():
    assert gen.season_of("Hairy vetch (AutD)") == "autumn"
    assert gen.season_of("Hairy vetch (SpD)") == "spring"
    assert gen.season_of("Phacelia") == "unspecified"


def test_rows_are_filtered_and_rounded():
    rows = [
        _row("Hairy vetch (AutD)", "Vicia villosa", dm=(4.67, 4.992, 0.4449068, 15), n=(4, 3.9486670, 0.2133, 20), cn=(5.4, 9.5625624, 0.4785, 12)),
        _row("Hairy vetch (AutD)", "Vicia villosa", part="Roots", dm=(None, 0.79, 0.17, 10)),
        _row("Hairy vetch (SpD)", "Vicia villosa", stage="Mature", dm=(1, 1, 1, 1)),
        _row("Unknown crop", "Nothing real", dm=(1, 1, 1, 1)),
    ]
    out = gen.build_species(rows)
    assert list(out) == ["hairy_vetch"]
    assert list(out["hairy_vetch"]) == ["autumn"]
    row = out["hairy_vetch"]["autumn"]
    assert row["dm_t_ha"] == {"mean": 4.99, "se": 0.44, "n": 15}
    assert row["n_pct"] == {"mean": 3.95, "se": 0.21, "n": 20}
    assert row["c_n"] == {"mean": 9.56, "se": 0.48, "n": 12}


def test_missing_metric_is_null_and_empty_rows_dropped():
    rows = [_row("Phacelia", "Phacelia tanacetifolia", dm=(None, 3.1, 0.2, 4)),
            _row("Italian ryegrass", "Lolium multiflorum")]
    out = gen.build_species(rows)
    assert out["phacelia"]["unspecified"]["n_pct"] is None
    assert out["phacelia"]["unspecified"]["dm_t_ha"]["n"] == 4
    assert "italian_ryegrass" not in out


def test_subspecies_is_not_merged_into_the_species():
    rows = [_row("Oil radish", "Raphanus sativus oleiformis", dm=(1, 2, 0.1, 3))]
    assert gen.build_species(rows) == {}


def test_duplicate_season_row_is_an_error():
    r = _row("Phacelia", "Phacelia tanacetifolia", dm=(None, 3.1, 0.2, 4))
    with pytest.raises(ValueError, match="duplicate"):
        gen.build_species([r, r])


def test_header_mismatch_is_an_error():
    bad = {**HEADER_OK, 5: {**HEADER_OK[5], 6: "Median"}}
    with pytest.raises(ValueError, match="header"):
        gen.check_header(bad)
    gen.check_header(HEADER_OK)


def test_committed_file_has_provenance_header_and_vicia_villosa_example():
    doc = yaml.safe_load(DATA.read_text(encoding="utf-8"))
    src = doc["source"]
    assert src["doi"] == "10.15454/LBI3U7" and src["version"] == "v3"
    assert src["licence"] == "etalab-2.0" and src["download_date"] == "2026-10-30"
    row = doc["species"]["hairy_vetch"]["autumn"]
    assert row["dm_t_ha"] == {"mean": 4.99, "se": 0.44, "n": 15}
    assert row["n_pct"] == {"mean": 3.95, "se": 0.21, "n": 20}
    assert row["c_n"] == {"mean": 9.56, "se": 0.48, "n": 12}


@pytest.mark.skipif(RAW is None or not RAW.exists(), reason="raw table not available (COVER_CROP_RAW)")
def test_generator_output_matches_the_committed_file():
    assert gen.generate(RAW)["species"] == yaml.safe_load(DATA.read_text(encoding="utf-8"))["species"]
