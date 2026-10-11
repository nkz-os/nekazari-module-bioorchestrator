from datetime import date
from unittest.mock import AsyncMock

import pytest

from app.graph.crop_plan import build_segment_entity
from app.planning import plan_write
from app.planning.models import PlanIn

TODAY = date(2026, 10, 11)
URN = "urn:ngsi-ld:AgriParcel:t:p1"


def _plan(**over):
    base = {"campaign": 2027, "regime": "secano", "management": "conventional", "cycles": [
        {"role": "cover_crop", "crop": "vetch", "sowing_start": "2026-10-01", "expected_termination": "2027-04-20",
         "termination_method": "roller_crimper"},
        {"role": "main_crop", "crop": "maize", "variety": "P1524", "sowing_start": "2027-04-20",
         "expected_termination": "2027-10-01"}]}
    return PlanIn.model_validate({**base, **over})


def test_roller_crimper_day_equals_sowing_day_is_not_an_overlap():
    plan_write.validate_plan(_plan(), TODAY)


@pytest.mark.parametrize("over, code", [
    ({"campaign": 2025}, "campaign_past"),
    ({"cycles": [{"role": "main_crop", "crop": "wheat", "sowing_start": "2026-11-01", "expected_termination": "2028-07-01"}]},
     "termination_outside_campaign"),
    ({"cycles": [{"role": "main_crop", "crop": "wheat", "sowing_start": "2027-08-01", "expected_termination": "2027-07-01"}]},
     "dates_inconsistent"),
    ({"cycles": [{"role": "main_crop", "crop": "wheat", "sowing_start": "2026-11-01", "expected_termination": "2027-07-01"},
                 {"role": "main_crop", "crop": "maize", "sowing_start": "2027-05-01", "expected_termination": "2027-10-01"}]},
     "cycles_overlap"),
])
def test_invalid_plans(over, code):
    with pytest.raises(plan_write.PlanError) as e:
        plan_write.validate_plan(_plan(**over), TODAY)
    assert e.value.code == code


def test_segments_are_chronological_and_carry_regime_and_management():
    segs = plan_write.segments_from_plan(_plan())
    assert [s["role"] for s in segs] == ["cover_crop", "main_crop"]
    assert segs[1]["crop"] == "ZEAMX" and segs[1]["sowing_window"] == ["2027-04-20", "2027-04-20"]
    ent = build_segment_entity("t", URN, "2027", 1, segs[1])
    assert ent["irrigationRegime"]["value"] == "secano" and ent["management"]["value"] == "conventional"
    assert ent["expectedTerminationDate"]["value"]["@value"] == "2027-10-01" and "hasAgriCrop" not in ent


def test_segment_entity_without_regime_and_management_is_unchanged():
    ent = build_segment_entity("t", URN, "2027", 0, {"crop": "ZEAMX", "role": "main_crop"})
    assert "irrigationRegime" not in ent and "management" not in ent


def test_unknown_crop_is_a_plan_error():
    bad = {"cycles": [{"role": "main_crop", "crop": "zzz", "sowing_start": "2026-11-01", "expected_termination": "2027-07-01"}]}
    with pytest.raises(plan_write.PlanError) as e:
        plan_write.segments_from_plan(_plan(**bad))
    assert e.value.code == "unknown_crop"


def test_alfalfa_spans_campaigns_but_must_start_in_this_one():
    ok = {"cycles": [{"role": "main_crop", "crop": "alfalfa", "sowing_start": "2026-09-15", "expected_termination": "2030-06-30"}]}
    plan_write.validate_plan(_plan(**ok), TODAY)
    late = {"cycles": [{"role": "main_crop", "crop": "alfalfa", "sowing_start": "2027-09-15", "expected_termination": "2030-06-30"}]}
    with pytest.raises(plan_write.PlanError) as e:
        plan_write.validate_plan(_plan(**late), TODAY)
    assert e.value.code == "perennial_start_outside_campaign"
    too_long = {"cycles": [{"role": "main_crop", "crop": "alfalfa", "sowing_start": "2026-09-15", "expected_termination": "2040-06-30"}]}
    with pytest.raises(plan_write.PlanError) as e2:
        plan_write.validate_plan(_plan(**too_long), TODAY)
    assert e2.value.code == "perennial_too_long"


async def test_perennial_from_another_campaign_blocks_the_plan(monkeypatch):
    client = AsyncMock()
    alfalfa = {"id": "urn:ngsi-ld:AgriCrop:t:p1:2026:0", "status": "active", "cropSeason": "2026",
               "cropLifecycle": "perennial", "plantingDate": {"@type": "Date", "@value": "2025-09-20"}}
    client.query_entities.side_effect = [[], [alfalfa]]
    monkeypatch.setattr(plan_write, "OrionClient", lambda tenant_id: client)
    with pytest.raises(plan_write.PlanError) as e:
        await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=_plan(), today=TODAY)
    assert (e.value.code, e.value.status) == ("perennial_occupies", 409)
    client.create_entity.assert_not_awaited()


async def test_rewrite_replaces_planned_and_removes_surplus(monkeypatch):
    client = AsyncMock()
    existing = [{"id": f"urn:ngsi-ld:AgriCrop:t:p1:2027:{i}", "status": "planned"} for i in range(3)]
    client.query_entities.side_effect = [existing, []]
    monkeypatch.setattr(plan_write, "OrionClient", lambda tenant_id: client)
    out = await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=_plan(), today=TODAY)
    assert out["written"] == [existing[0]["id"], existing[1]["id"]] and out["removed"] == [existing[2]["id"]]
    assert client.delete_entity.await_count == 3 and client.create_entity.await_count == 2


async def test_started_campaign_is_refused(monkeypatch):
    client = AsyncMock()
    client.query_entities.return_value = [{"id": "urn:ngsi-ld:AgriCrop:t:p1:2027:0", "status": "active"}]
    monkeypatch.setattr(plan_write, "OrionClient", lambda tenant_id: client)
    with pytest.raises(plan_write.PlanError) as e:
        await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=_plan(), today=TODAY)
    assert (e.value.code, e.value.status) == ("campaign_started", 409)
    client.create_entity.assert_not_awaited()
