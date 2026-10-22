"""The committed Eurostat snapshot: provenance present, codes resolved, known values reproduced."""
import gzip
import json
from pathlib import Path

import pytest
import yaml

from app.services.eurostat_yields import build_series, parse_jsonstat
from app.species_registry import resolve_species

DATA = Path(__file__).resolve().parents[1] / "data"


def _snapshot() -> dict:
    path = max((DATA / "eurostat").glob("apro_cpshr_nuts2_*.json.gz"))
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return json.load(f)


def _crop_map() -> dict[str, str]:
    return yaml.safe_load((DATA / "eurostat" / "crop_codes.yaml").read_text())["crops"]


def test_snapshot_provenance():
    s = _snapshot()
    assert s["dataset"] == "apro_cpshr"
    assert s["request_url"].startswith("https://ec.europa.eu/eurostat/api/")
    assert s["retrieved_at"] and s["dataset_updated"] and s["nuts_version"]


def test_every_mapped_crop_is_in_the_snapshot():
    index = _snapshot()["jsonstat"]["dimension"]["crops"]["category"]["index"]
    assert set(_crop_map()) <= set(index)


def test_mapped_eppo_codes_are_registered_species():
    # resolve_species honours eppo_aliases (ZEAMX resolves to maize).
    unresolved = sorted(c for c in set(_crop_map().values()) if resolve_species(c) is None)
    assert unresolved == []


def test_crop_codes_map_to_distinct_eppo():
    values = list(_crop_map().values())
    assert len(values) == len(set(values))


def test_navarra_common_and_durum_wheat_2024_are_separate_and_reproduced():
    rows = build_series(parse_jsonstat(_snapshot()["jsonstat"]), _crop_map())
    by = {(r.nuts, r.eppo, r.year): r for r in rows}
    common, durum = by[("ES22", "TRZAX", 2024)], by[("ES22", "TRZDU", 2024)]
    assert common.yield_t_ha == pytest.approx(431.91 / 76.6, rel=1e-6)
    assert common.humidity_pct == 14.0
    assert durum.yield_t_ha != pytest.approx(common.yield_t_ha)
