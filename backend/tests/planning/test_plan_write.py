from datetime import date
from unittest.mock import AsyncMock

import httpx
import pytest

from app.graph.crop_plan import build_segment_entity
from app.planning import plan_write
from app.planning.models import PlanIn
from app.planning.service import InvalidParcelId

TODAY = date(2026, 10, 11)
URN = "urn:ngsi-ld:AgriParcel:t:p1"


def _plan(**over):
    base = {"campaign": 2027, "regime": "secano", "management": "conventional", "cycles": [
        {"role": "cover_crop", "crop": "vetch", "sowing_start": "2026-10-01", "expected_termination": "2027-04-20",
         "termination_method": "roller_crimper"},
        {"role": "main_crop", "crop": "maize", "variety": "P1524", "sowing_start": "2027-04-20",
         "expected_termination": "2027-10-01"}]}
    return PlanIn.model_validate({**base, **over})


ALFALFA = {"cycles": [{"role": "main_crop", "crop": "alfalfa", "sowing_start": "2026-09-15",
                       "expected_termination": "2030-06-30"}]}


def _orion(monkeypatch, crops, parcel=True):
    """An OrionClient stand-in: one parcel read, then one AgriCrop listing."""
    client = AsyncMock()
    if not parcel:
        request = httpx.Request("GET", "http://orion.invalid/entities")
        client.get_entity.side_effect = httpx.HTTPStatusError(
            "orion", request=request, response=httpx.Response(404, request=request))
    client.query_entities.return_value = crops
    monkeypatch.setattr(plan_write, "OrionClient", lambda tenant_id: client)
    return client


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
    plan_write.validate_plan(_plan(**ALFALFA), TODAY)
    late = {"cycles": [{"role": "main_crop", "crop": "alfalfa", "sowing_start": "2027-09-15", "expected_termination": "2030-06-30"}]}
    with pytest.raises(plan_write.PlanError) as e:
        plan_write.validate_plan(_plan(**late), TODAY)
    assert e.value.code == "perennial_start_outside_campaign"
    too_long = {"cycles": [{"role": "main_crop", "crop": "alfalfa", "sowing_start": "2026-09-15", "expected_termination": "2040-06-30"}]}
    with pytest.raises(plan_write.PlanError) as e2:
        plan_write.validate_plan(_plan(**too_long), TODAY)
    assert e2.value.code == "perennial_too_long"


def test_perennial_ending_before_the_campaign_is_inconsistent_not_too_long():
    early = {"cycles": [{"role": "main_crop", "crop": "alfalfa", "sowing_start": "2026-09-15",
                         "expected_termination": "2026-12-31"}]}
    with pytest.raises(plan_write.PlanError) as e:
        plan_write.validate_plan(_plan(**early), TODAY)
    assert e.value.code == "dates_inconsistent"


async def test_perennial_from_another_campaign_blocks_the_plan(monkeypatch):
    alfalfa = {"id": "urn:ngsi-ld:AgriCrop:t:p1:2026:0", "status": "active", "cropSeason": "2026",
               "cropLifecycle": "perennial", "plantingDate": {"@type": "Date", "@value": "2025-09-20"}}
    client = _orion(monkeypatch, [alfalfa])
    with pytest.raises(plan_write.PlanError) as e:
        await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=_plan(), today=TODAY)
    assert (e.value.code, e.value.status) == ("perennial_occupies", 409)
    client.create_entity.assert_not_awaited()


async def test_rewrite_replaces_planned_and_removes_surplus(monkeypatch):
    existing = [{"id": f"urn:ngsi-ld:AgriCrop:t:p1:2027:{i}", "status": "planned", "cropSeason": "2027"}
                for i in range(3)]
    client = _orion(monkeypatch, existing)
    out = await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=_plan(), today=TODAY)
    assert out["written"] == [existing[0]["id"], existing[1]["id"]] and out["removed"] == [existing[2]["id"]]
    assert client.delete_entity.await_count == 3 and client.create_entity.await_count == 2


async def test_started_campaign_is_refused(monkeypatch):
    client = _orion(monkeypatch, [{"id": "urn:ngsi-ld:AgriCrop:t:p1:2027:0", "status": "active", "cropSeason": "2027"}])
    with pytest.raises(plan_write.PlanError) as e:
        await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=_plan(), today=TODAY)
    assert (e.value.code, e.value.status) == ("campaign_started", 409)
    client.create_entity.assert_not_awaited()


async def test_parcel_cycles_are_listed_once_for_every_season(monkeypatch):
    client = _orion(monkeypatch, [])
    await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=_plan(), today=TODAY)
    client.get_entity.assert_awaited_once_with(URN, options="keyValues")
    client.query_entities.assert_awaited_once()
    assert client.query_entities.await_args.kwargs["q"] == f'hasAgriParcel=="{URN}"'
    assert client.query_entities.await_args.kwargs["limit"] == 200


async def test_listing_follows_pages_until_the_last_short_one(monkeypatch):
    page = [{"id": f"urn:ngsi-ld:AgriCrop:t:p1:2020:{i}", "status": "harvested", "cropSeason": "2020",
             "plantingDate": "2020-03-01", "harvestDate": "2020-09-01"} for i in range(200)]
    started = {"id": "urn:ngsi-ld:AgriCrop:t:p1:2027:0", "status": "active", "cropSeason": "2027"}
    client = _orion(monkeypatch, [])
    client.query_entities.side_effect = [page, [started]]
    with pytest.raises(plan_write.PlanError) as e:
        await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=_plan(), today=TODAY)
    assert e.value.code == "campaign_started" and client.query_entities.await_count == 2
    assert client.query_entities.await_args.kwargs["offset"] == 200


async def test_missing_parcel_is_refused_before_any_write(monkeypatch):
    client = _orion(monkeypatch, [], parcel=False)
    with pytest.raises(plan_write.PlanError) as e:
        await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=_plan(), today=TODAY)
    assert (e.value.code, e.value.status) == ("parcel_not_found", 404)
    client.query_entities.assert_not_awaited()
    client.create_entity.assert_not_awaited()


async def test_other_orion_errors_are_not_reported_as_a_missing_parcel(monkeypatch):
    client = _orion(monkeypatch, [])
    request = httpx.Request("GET", "http://orion.invalid/entities")
    client.get_entity.side_effect = httpx.HTTPStatusError(
        "orion", request=request, response=httpx.Response(500, request=request))
    with pytest.raises(httpx.HTTPStatusError):
        await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=_plan(), today=TODAY)


@pytest.mark.parametrize("bad", ['urn:ngsi-ld:AgriParcel:t:p1"', "p1", "urn:ngsi-ld:AgriParcel:t:p1\n",
                                 "urn:ngsi-ld:AgriParcel:a;b"])
async def test_write_plan_refuses_a_malformed_parcel_urn_before_any_io(monkeypatch, bad):
    client = _orion(monkeypatch, [])
    with pytest.raises(InvalidParcelId):
        await plan_write.write_plan(tenant_id="t", parcel_urn=bad, plan=_plan(), today=TODAY)
    client.get_entity.assert_not_awaited()


async def test_legacy_crop_without_season_started_in_the_campaign_is_refused(monkeypatch):
    legacy = {"id": "urn:ngsi-ld:AgriCrop:legacy1", "status": "active",
              "plantingDate": {"@type": "Date", "@value": "2027-03-01"}, "harvestDate": "2027-09-01"}
    client = _orion(monkeypatch, [legacy])
    with pytest.raises(plan_write.PlanError) as e:
        await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=_plan(), today=TODAY)
    assert (e.value.code, e.value.status) == ("campaign_started", 409)
    client.create_entity.assert_not_awaited()


async def test_started_crop_of_another_season_outside_the_campaign_does_not_block(monkeypatch):
    previous = {"id": "urn:ngsi-ld:AgriCrop:t:p1:2026:0", "status": "harvested", "cropSeason": "2026",
                "plantingDate": "2026-03-01", "harvestDate": "2026-09-01"}
    client = _orion(monkeypatch, [previous])
    await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=_plan(), today=TODAY)
    assert client.create_entity.await_count == 2


async def test_perennial_harvested_before_the_campaign_does_not_occupy_it(monkeypatch):
    old = {"id": "urn:ngsi-ld:AgriCrop:t:p1:2024:0", "status": "harvested", "cropSeason": "2024",
           "cropLifecycle": "perennial", "plantingDate": "2024-09-20", "harvestDate": "2026-06-01"}
    client = _orion(monkeypatch, [old])
    await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=_plan(), today=TODAY)
    assert client.create_entity.await_count == 2


async def test_cancelled_perennial_is_ignored(monkeypatch):
    dropped = {"id": "urn:ngsi-ld:AgriCrop:t:p1:2026:0", "status": "cancelled", "cropSeason": "2026",
               "cropLifecycle": "perennial", "plantingDate": "2025-09-20"}
    client = _orion(monkeypatch, [dropped])
    await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=_plan(), today=TODAY)
    assert client.create_entity.await_count == 2


async def test_perennial_plan_over_an_annual_planned_in_a_later_campaign_is_refused(monkeypatch):
    later = {"id": "urn:ngsi-ld:AgriCrop:t:p1:2029:0", "status": "planned", "cropSeason": "2029",
             "sowingWindowStart": {"@type": "Date", "@value": "2028-11-01"},
             "expectedTerminationDate": {"@type": "Date", "@value": "2029-07-01"}}
    client = _orion(monkeypatch, [later])
    with pytest.raises(plan_write.PlanError) as e:
        await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=_plan(**ALFALFA), today=TODAY)
    assert (e.value.code, e.value.status) == ("perennial_overlaps_plans", 409)
    client.create_entity.assert_not_awaited()


async def test_annual_plan_does_not_care_about_later_campaigns(monkeypatch):
    later = {"id": "urn:ngsi-ld:AgriCrop:t:p1:2029:0", "status": "planned", "cropSeason": "2029",
             "sowingWindowStart": "2028-11-01", "expectedTerminationDate": "2029-07-01"}
    client = _orion(monkeypatch, [later])
    await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=_plan(), today=TODAY)
    assert client.create_entity.await_count == 2


async def test_written_perennial_cycle_carries_its_lifecycle(monkeypatch):
    client = _orion(monkeypatch, [])
    await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=_plan(**ALFALFA), today=TODAY)
    entity = client.create_entity.await_args.args[0]
    assert entity["cropLifecycle"]["value"] == "perennial" and entity["species"]["value"] == "MEDSA"
