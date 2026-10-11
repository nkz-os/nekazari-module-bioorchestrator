from datetime import date

import pytest

from app.planning.farm import (
    campaign_columns,
    default_target_campaign,
    farm_payload,
    parcel_regime,
)
from app.planning.models import FarmResponse
from app.planning.rotation import Constraint

RAPE = Constraint("rapeseed", "rapeseed", "restriction", 3, "clubroot", "FAO")
P1 = {"id": "urn:ngsi-ld:AgriParcel:t:p1", "name": "Soto", "area": 12.4, "irrigationRegime": "secano"}
P2 = {"id": "urn:ngsi-ld:AgriParcel:t:p2", "name": "Vega", "area": "6.1"}
CYCLES = {
    P1["id"]: {"cycles": [
        {"crop_id": "c1", "role": "main_crop", "species": "BRSNN", "status": "harvested", "campaign_label": 2026,
         "start": {"date": "2025-09-20", "provenance": "actual"}, "end": {"date": "2026-06-20", "provenance": "actual"}},
        {"crop_id": "c2", "role": "main_crop", "species": "BRSNN", "status": "planned", "campaign_label": 2027,
         "start": {"date": "2026-09-20", "provenance": "planned"}, "end": {"date": "2027-06-20", "provenance": "planned"}},
    ]},
    P2["id"]: None,
}
UNKNOWN_SPECIES_CYCLE = {
    "crop_id": "c9", "role": "main_crop", "species": "ZZZZZ", "status": "harvested", "campaign_label": 2025,
    "start": {"date": "2024-09-20", "provenance": "actual"}, "end": {"date": "2025-06-20", "provenance": "actual"},
}
ROTATION_UNKNOWN = {"code": "rotation_unknown", "campaign": 2027, "params": {}}


def test_open_perennial_fills_its_campaigns_and_is_not_undecided():
    alfalfa = {"crop_id": "a1", "role": "main_crop", "species": "MEDSA", "status": "active", "lifecycle": "perennial",
               "campaign_label": 2024, "start": {"date": "2024-09-15", "provenance": "actual"},
               "end": {"date": None, "provenance": None}}
    p = {"id": "urn:ngsi-ld:AgriParcel:t:p3", "name": "Alfalfa", "area": 3, "irrigationRegime": "regadío"}
    out = farm_payload([p], [], {}, {p["id"]: {"cycles": [alfalfa]}}, 2027, [])
    row = out["parcels"][0]
    assert [len(row["cells"][str(c)]) for c in (2024, 2025, 2026, 2027)] == [0, 1, 1, 1]
    assert out["summary"]["undecided"] == 0 and row["cells"]["2027"][0]["first_campaign"] == 2025
    assert row["warnings"] == [ROTATION_UNKNOWN]


def test_target_campaign_rolls_over_in_july():
    assert default_target_campaign(date(2026, 10, 11)) == 2027
    assert default_target_campaign(date(2027, 3, 1)) == 2027
    assert campaign_columns(2027) == [2024, 2025, 2026, 2027]


def test_regime_prefers_the_target_campaign_crop():
    assert parcel_regime({"irrigationRegime": "secano"}, [{"irrigationRegime": "regadío"}]) == ("regadío", "crop")
    assert parcel_regime({"irrigationSystemType": "drip"}, []) == ("regadío", "parcel:irrigationSystemType")
    assert parcel_regime({}, []) == (None, "unknown")


def test_matrix_rows_warnings_and_summary():
    out = farm_payload([P1, P2], [], {"c2": {"id": "c2", "variety": "Atlas"}}, CYCLES, 2027, [RAPE])
    FarmResponse.model_validate(out)
    r1, r2 = out["parcels"]
    assert [c["provenance"] for c in r1["cells"]["2027"]] == ["planned"] and r1["cells"]["2027"][0]["variety"] == "Atlas"
    assert {w["code"] for w in r1["warnings"]} == {"rotation_restriction"}
    assert r2["cycles_available"] is False and {w["code"] for w in r2["warnings"]} == {"cycles_unavailable", "regime_missing"}
    assert out["summary"]["undecided"] == 1 and out["summary"]["rotation_warnings"] == 1
    assert out["summary"]["area_by_crop"][0]["area_ha"] == 12.4
    assert out["farms"] == [{"farm_id": None, "name": None}]


def test_incomplete_history_warns_rotation_unknown_instead_of_scanning():
    cycles = {P1["id"]: {"cycles": [UNKNOWN_SPECIES_CYCLE, *CYCLES[P1["id"]]["cycles"]]}}
    out = farm_payload([P1], [], {}, cycles, 2027, [RAPE])
    FarmResponse.model_validate(out)
    row = out["parcels"][0]
    assert row["cycles_available"] is True
    assert row["warnings"] == [ROTATION_UNKNOWN]
    assert out["summary"]["rotation_warnings"] == 0


def test_empty_constraints_warn_rotation_unknown_instead_of_reading_as_ok():
    out = farm_payload([P1], [], {}, CYCLES, 2027, [])
    FarmResponse.model_validate(out)
    row = out["parcels"][0]
    assert row["warnings"] == [ROTATION_UNKNOWN]
    assert out["summary"]["rotation_warnings"] == 0


def test_missing_cycles_keep_only_cycles_unavailable():
    out = farm_payload([P2], [], {}, CYCLES, 2027, [RAPE])
    codes = [w["code"] for w in out["parcels"][0]["warnings"]]
    assert "cycles_unavailable" in codes and "rotation_unknown" not in codes


def _with_area(area):
    return farm_payload([{"id": "urn:ngsi-ld:AgriParcel:t:pa", "name": "A", "area": area, "irrigationRegime": "secano"}],
                        [], {}, {}, 2027, [])["parcels"][0]["area_ha"]


@pytest.mark.parametrize("bad", [True, False, float("nan"), float("inf"), float("-inf"), -1, "-3.5", "nan", "inf",
                                 "abc", None, [1], {"value": True}])
def test_unusable_areas_become_none(bad):
    assert _with_area(bad) is None


@pytest.mark.parametrize("good, expected", [(12.4, 12.4), ("6.1", 6.1), (3, 3.0), (0, 0.0), ({"value": 2.5}, 2.5)])
def test_usable_areas_are_kept(good, expected):
    assert _with_area(good) == expected


def test_farm_groups_follow_the_parcel_links_and_name_the_farms():
    farm_a, farm_b, farm_c = ("urn:ngsi-ld:AgriFarm:t:a", "urn:ngsi-ld:AgriFarm:t:b", "urn:ngsi-ld:AgriFarm:t:c")
    parcels = [
        {"id": "urn:ngsi-ld:AgriParcel:t:p1", "hasAgriFarm": farm_b, "irrigationRegime": "secano"},
        {"id": "urn:ngsi-ld:AgriParcel:t:p2", "refAgriFarm": farm_a, "irrigationRegime": "secano"},
        {"id": "urn:ngsi-ld:AgriParcel:t:p3", "hasAgriFarm": {"type": "Relationship", "object": farm_c},
         "irrigationRegime": "secano"},
        {"id": "urn:ngsi-ld:AgriParcel:t:p4", "hasAgriFarm": farm_a, "irrigationRegime": "secano"},
        {"id": "urn:ngsi-ld:AgriParcel:t:p5", "irrigationRegime": "secano"},
    ]
    farms = [{"id": farm_a, "name": "Finca Norte"}, {"id": farm_b, "name": {"type": "Property", "value": "Finca Sur"}},
             {"id": "urn:ngsi-ld:AgriFarm:t:unlinked", "name": "Sin parcelas"}]
    out = farm_payload(parcels, farms, {}, {}, 2027, [])
    FarmResponse.model_validate(out)
    assert out["farms"] == [{"farm_id": farm_a, "name": "Finca Norte"}, {"farm_id": farm_b, "name": "Finca Sur"},
                            {"farm_id": farm_c, "name": None}]
    assert [r["farm_id"] for r in out["parcels"]] == [farm_b, farm_a, farm_c, farm_a, None]
