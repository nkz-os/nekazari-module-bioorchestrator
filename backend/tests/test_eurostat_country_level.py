"""Country-level (geoLevel=country) Eurostat yields: parsing, node shape, idempotent seeding."""
import gzip
import json
import sys
from unittest.mock import MagicMock, patch

import pytest

from app.services.eurostat_yields import (
    AREA,
    HUMIDITY,
    PRODUCTION,
    YIELD_METHOD,
    build_series,
    nuts_level,
    parse_jsonstat,
    request_url,
    to_node_props,
)
from scripts import seed_regional_yields as seed


def _country_doc():
    # dims: freq(1) x crops(1) x strucpro(3) x geo(2) x time(1); flat = strucpro*2 + geo
    return {
        "version": "2.0", "class": "dataset", "updated": "2026-09-08T23:00:00+0200",
        "id": ["freq", "crops", "strucpro", "geo", "time"],
        "size": [1, 1, 3, 2, 1],
        "dimension": {
            "freq": {"category": {"index": {"A": 0}}},
            "crops": {"category": {"index": {"C1110": 0}}},
            "strucpro": {"category": {"index": {AREA: 0, HUMIDITY: 1, PRODUCTION: 2}}},
            "geo": {"category": {"index": {"ES": 0, "FR": 1}}},
            "time": {"category": {"index": {"2024": 0}}},
        },
        "value": {"0": 1500.0, "1": 4000.0, "2": 14.0, "3": 14.0, "4": 6000.0, "5": 28000.0},
        "status": {},
    }


def _nodes():
    rows = build_series(parse_jsonstat(_country_doc()), {"C1110": "TRZAX"})
    return [to_node_props(r, dataset_updated="u", extracted_at="t", nuts_version="v") for r in rows]


def test_country_nodes_have_level_zero_and_ratio_yield():
    nodes = {n["nuts"]: n for n in _nodes()}
    es = nodes["ES"]
    assert es["nutsLevel"] == 0
    assert es["key"] == "apro_cpshr|ES|C1110|2024"
    assert es["yieldMethod"] == YIELD_METHOD
    assert es["yieldTHa"] == pytest.approx(6000.0 / 1500.0)
    assert nodes["FR"]["yieldTHa"] == pytest.approx(7.0)


def test_nuts2_node_keeps_level_two():
    doc = _country_doc()
    doc["dimension"]["geo"]["category"]["index"] = {"ES22": 0, "ES41": 1}
    rows = build_series(parse_jsonstat(doc), {"C1110": "TRZAX"})
    assert {to_node_props(r, dataset_updated=None, extracted_at="t", nuts_version="v")["nutsLevel"]
            for r in rows} == {2}


def test_request_url_country_level():
    assert "geoLevel=country" in request_url(["C1110"], "country")
    assert "geoLevel=nuts2" in request_url(["C1110"])
    with pytest.raises(ValueError):
        request_url(["C1110"], "nuts3")


def _data_dir(tmp_path):
    d = tmp_path / "eurostat"
    d.mkdir()
    (d / "crop_codes.yaml").write_text("crops:\n  C1110: TRZAX\n")
    snap = {"jsonstat": _country_doc(), "dataset_updated": "u", "retrieved_at": "t",
            "nuts_version": "v"}
    for level in ("nuts2", "country"):
        with gzip.open(d / f"apro_cpshr_{level}_2026-11-03.json.gz", "wt") as f:
            json.dump(snap, f)
    return tmp_path


def test_seed_merges_country_rows_by_key_and_is_repeatable(tmp_path):
    data = _data_dir(tmp_path)
    batches = []
    for _ in range(2):
        driver = MagicMock()
        session = driver.session.return_value.__enter__.return_value
        argv = ["seed", "--data-dir", str(data), "--execute"]
        with patch.object(sys, "argv", argv), \
                patch.object(seed.GraphDatabase, "driver", return_value=driver):
            seed.main()
        merges = [c for c in session.run.call_args_list if c.args[0] == seed.MERGE]
        batches.append([r["key"] for c in merges for r in c.kwargs["rows"]])
    assert "MERGE (n:RegionalYieldStat {key: row.key})" in seed.MERGE
    assert batches[0] == batches[1]
    assert "apro_cpshr|ES|C1110|2024" in batches[0]


def test_country_mode_drops_aggregates(caplog):
    doc = _country_doc()
    doc["size"] = [1, 1, 3, 3, 1]
    doc["dimension"]["geo"]["category"]["index"] = {"ES": 0, "FR": 1, "EU27_2020": 2}
    doc["value"] = {str(i): v for i, v in enumerate(
        [1500.0, 4000.0, 99.0, 14.0, 14.0, 14.0, 6000.0, 28000.0, 999.0])}
    snap = {"jsonstat": doc, "dataset_updated": "u", "retrieved_at": "t", "nuts_version": "v"}
    with caplog.at_level("INFO", logger="seed_regional_yields"):
        rows = seed.build_rows(snap, {"C1110": "TRZAX"}, "country")
    assert sorted(r["nuts"] for r in rows) == ["ES", "FR"]
    assert {r["nutsLevel"] for r in rows} == {0}
    assert "EU27_2020" in caplog.text


def test_nuts_level_rejects_non_region_shapes():
    assert (nuts_level("ES"), nuts_level("ES22"), nuts_level("DE1A")) == (0, 2, 2)
    for bad in ("EU27_2020", "ESP", "es22", "E"):
        with pytest.raises(ValueError):
            nuts_level(bad)
