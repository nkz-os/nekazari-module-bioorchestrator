"""Spanish municipality -> NUTS 2 through the INE province code (pure)."""
import gzip
import json
from pathlib import Path

import pytest

from app.services.nuts_regions import load_province_table, nuts2_for_municipality

DATA = Path(__file__).resolve().parents[1] / "data"


@pytest.fixture(scope="module")
def table():
    return load_province_table()


def test_all_52_provinces_two_digit_keys(table):
    assert sorted(table) == [f"{i:02d}" for i in range(1, 53)]


def test_table_is_nuts_2024_for_spain():
    import yaml
    doc = yaml.safe_load((DATA / "eurostat" / "nuts2_es_provinces.yaml").read_text(encoding="utf-8"))
    assert (doc["nuts_version"], doc["country"]) == (2024, "ES")


def test_every_region_is_a_spanish_nuts2_in_the_eurostat_snapshot(table):
    path = max((DATA / "eurostat").glob("apro_cpshr_nuts2_*.json.gz"))
    with gzip.open(path, "rt", encoding="utf-8") as f:
        geo = json.load(f)["jsonstat"]["dimension"]["geo"]["category"]["index"]
    spanish = {c for c in geo if c.startswith("ES") and len(c) == 4 and c != "ESZZ"}
    assert set(table.values()) == spanish   # every region covered, none invented


@pytest.mark.parametrize("ine,nuts", [
    ("31201", "ES22"),   # Pamplona/Iruña
    ("01059", "ES21"),   # Vitoria-Gasteiz: leading zero kept
    ("07040", "ES53"),   # Palma: island NUTS 3, region ES53
    ("35016", "ES70"),   # Las Palmas de Gran Canaria
    ("38038", "ES70"),   # Santa Cruz de Tenerife
    ("51001", "ES63"),   # Ceuta
    ("52001", "ES64"),   # Melilla
])
def test_known_municipalities(table, ine, nuts):
    assert nuts2_for_municipality(ine, table) == nuts


@pytest.mark.parametrize("bad", ["", "3120", "312011", "99001", "00001", "ab201", " 31201", "31201\n"])
def test_malformed_or_unknown_codes_give_none(table, bad):
    assert nuts2_for_municipality(bad, table) is None


from app.services.nuts_regions import is_superseded, load_region_names, region_names


def test_region_names_keep_only_nuts2_and_drop_extra_regio():
    doc = {"dimension": {"geo": {"category": {
        "index": {"ES": 0, "ES2": 1, "ES22": 2, "ESZZ": 3, "FRK1": 4},
        "label": {"ES": "Spain", "ES2": "Noreste", "ES22": "Comunidad Foral de Navarra",
                  "ESZZ": "Extra-Regio NUTS 2", "FRK1": "Auvergne"}}}}}
    assert region_names(doc) == {"ES22": "Comunidad Foral de Navarra", "FRK1": "Auvergne"}


def test_snapshot_names_include_navarra():
    names = load_region_names()
    assert names["ES22"] == "Comunidad Foral de Navarra"
    assert not any(code.endswith("ZZ") for code in names)


@pytest.mark.parametrize("code,expected", [
    ("ITD5", True),    # (NUTS 2006)
    ("EL11", True),    # (NUTS 2010)
    ("FR21", True),    # (NUTS 2013)
    ("NL31", True),    # (NUTS 2021)
    ("HR01", True),    # (former statistical region)
    ("NO01", True),    # (statistical region 2016)
    ("ITH5", False),
    ("ES22", False),
])
def test_is_superseded_on_real_snapshot_labels(code, expected):
    assert is_superseded(load_region_names()[code]) is expected


def test_is_superseded_plain_labels():
    assert is_superseded("Utrecht") is False
    assert is_superseded("Emilia-Romagna (NUTS 2006)") is True
    assert is_superseded("X (former statistical region)") is True
