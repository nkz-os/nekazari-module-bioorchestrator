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
    base = {"campaign": 2027, "regime": "rainfed", "management": "conventional", "cycles": [
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
    assert ent["irrigationRegime"]["value"] == "rainfed" and ent["management"]["value"] == "conventional"
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


# --- final-review fixes: conflicts by date overlap, raw cover species, woody and early annuals ---------------

def _wheat_plan(campaign=2027, start="2026-11-10", end="2027-07-01", **over):
    return PlanIn.model_validate({"campaign": campaign, "regime": "rainfed", "management": "conventional",
                                  "cycles": [{"role": "main_crop", "crop": "wheat", "sowing_start": start,
                                              "expected_termination": end}], **over})


def _maize_plan(campaign, start, end):
    return PlanIn.model_validate({"campaign": campaign, "regime": "irrigated", "management": "conventional",
                                  "cycles": [{"role": "main_crop", "crop": "maize", "sowing_start": start,
                                              "expected_termination": end}]})


def _crop(season, status, start=None, end=None, *, lifecycle=None, seq=0, **extra):
    entity = {"id": f"urn:ngsi-ld:AgriCrop:t:p1:{season or 'legacy'}:{seq}", "status": status, **extra}
    if season:
        entity["cropSeason"] = season
    if lifecycle:
        entity["cropLifecycle"] = lifecycle
    if start:
        entity["plantingDate" if status != "planned" else "sowingWindowStart"] = {"@type": "Date", "@value": start}
    if end:
        entity["harvestDate" if status != "planned" else "expectedTerminationDate"] = end
    return entity


async def test_catch_crop_that_ended_before_the_plan_starts_does_not_block(monkeypatch):
    catch = _crop("2026", "terminated", "2026-08-10", None, role="catch_crop", terminationDate="2026-11-05")
    client = _orion(monkeypatch, [catch])
    await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=_wheat_plan(), today=TODAY)
    assert client.create_entity.await_count == 1


async def test_finishing_the_day_the_next_cycle_starts_is_not_an_overlap(monkeypatch):
    previous = _crop("2026", "harvested", "2026-03-01", "2026-11-10")
    client = _orion(monkeypatch, [previous])
    await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=_wheat_plan(), today=TODAY)
    assert client.create_entity.await_count == 1


ALFALFA_2029 = _crop("2027", "active", "2026-09-15", "2029-03-15", lifecycle="perennial")


async def test_annual_after_a_perennial_that_ends_before_it_is_accepted(monkeypatch):
    client = _orion(monkeypatch, [ALFALFA_2029])
    await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=_maize_plan(2029, "2029-04-01", "2029-10-01"),
                                today=TODAY)
    assert client.create_entity.await_count == 1


async def test_annual_sown_while_the_perennial_still_stands_is_refused(monkeypatch):
    client = _orion(monkeypatch, [ALFALFA_2029])
    with pytest.raises(plan_write.PlanError) as e:
        await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=_maize_plan(2029, "2029-03-01", "2029-10-01"),
                                    today=TODAY)
    assert (e.value.code, e.value.status) == ("perennial_occupies", 409)
    client.create_entity.assert_not_awaited()


async def test_new_perennial_over_an_annual_planned_in_a_later_campaign(monkeypatch):
    later = _crop("2028", "planned", "2027-11-01", "2028-07-01")
    client = _orion(monkeypatch, [later])
    alfalfa = {"campaign": 2027, "cycles": [{"role": "main_crop", "crop": "alfalfa", "sowing_start": "2026-10-01",
                                              "expected_termination": "2030-06-30"}]}
    with pytest.raises(plan_write.PlanError) as e:
        await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=_plan(**alfalfa), today=TODAY)
    assert (e.value.code, e.value.status) == ("perennial_overlaps_plans", 409)
    client.create_entity.assert_not_awaited()


async def test_legacy_active_crop_without_season_or_end_blocks_overlapping_plans(monkeypatch):
    legacy = _crop(None, "active", "2026-11-01", None)
    client = _orion(monkeypatch, [legacy])
    with pytest.raises(plan_write.PlanError) as e:
        await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=_wheat_plan(), today=TODAY)
    assert (e.value.code, e.value.status) == ("campaign_started", 409)
    client.create_entity.assert_not_awaited()


async def test_planned_cycle_of_another_season_that_overlaps_is_a_cycles_overlap(monkeypatch):
    other = _crop("2026", "planned", "2026-12-01", "2026-12-31")
    client = _orion(monkeypatch, [other])
    with pytest.raises(plan_write.PlanError) as e:
        await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=_wheat_plan(), today=TODAY)
    assert (e.value.code, e.value.status) == ("cycles_overlap", 409)
    client.create_entity.assert_not_awaited()


async def test_entities_without_a_start_are_ignored(monkeypatch):
    client = _orion(monkeypatch, [{"id": "urn:ngsi-ld:AgriCrop:t:p1:2026:0", "status": "active", "cropSeason": "2026"}])
    await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=_wheat_plan(), today=TODAY)
    assert client.create_entity.await_count == 1


async def test_a_cycle_of_this_season_that_started_still_refuses_the_plan(monkeypatch):
    client = _orion(monkeypatch, [_crop("2027", "active", "2026-11-10", None)])
    with pytest.raises(plan_write.PlanError) as e:
        await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=_wheat_plan(), today=TODAY)
    assert (e.value.code, e.value.status) == ("campaign_started", 409)
    client.create_entity.assert_not_awaited()


def _cover_plan(crop):
    return PlanIn.model_validate({"campaign": 2027, "regime": "rainfed", "management": "conventional", "cycles": [
        {"role": "cover_crop", "crop": crop, "sowing_start": "2026-10-01", "expected_termination": "2027-04-20"},
        {"role": "main_crop", "crop": "maize", "sowing_start": "2027-04-20", "expected_termination": "2027-10-01"}]})


@pytest.mark.parametrize("role", ["cover_crop", "catch_crop"])
def test_cover_or_catch_crop_outside_the_registry_is_kept_as_written(role):
    plan = PlanIn.model_validate({"campaign": 2027, "regime": "rainfed", "management": "conventional", "cycles": [
        {"role": role, "crop": "Phacelia tanacetifolia", "sowing_start": "2026-10-01",
         "expected_termination": "2027-04-20"}]})
    segments = plan_write.segments_from_plan(plan)
    assert segments[0]["crop"] == "Phacelia tanacetifolia"
    entity = build_segment_entity("t", URN, "2027", 0, segments[0])
    assert entity["species"]["value"] == "Phacelia tanacetifolia" and "cropLifecycle" not in entity


def test_cover_crop_in_the_registry_still_resolves_to_its_code():
    assert plan_write.segments_from_plan(_cover_plan("vetch"))[0]["crop"] == "VICSA"


@pytest.mark.parametrize("bad", ["<script>", "Phacelia;DROP", "1phacelia", "_phacelia", "Phacelia\n", 'Pha"celia'])
def test_raw_cover_crop_names_outside_the_alphabet_are_refused(bad):
    with pytest.raises(plan_write.PlanError) as e:
        plan_write.segments_from_plan(_cover_plan(bad))
    assert e.value.code == "unknown_crop"


def test_one_letter_raw_species_is_refused_by_the_name_rule():
    with pytest.raises(plan_write.PlanError) as e:
        plan_write.raw_species("x")
    assert e.value.code == "unknown_crop"
    assert plan_write.raw_species("Phacélia (mix)") == "Phacélia (mix)"


def test_unknown_main_crop_is_still_refused():
    plan = _cover_plan("vetch").model_copy(deep=True)
    plan.cycles[1].crop = "Phacelia tanacetifolia"
    with pytest.raises(plan_write.PlanError) as e:
        plan_write.segments_from_plan(plan)
    assert e.value.code == "unknown_crop"


@pytest.mark.parametrize("woody", ["olive", "apple", "almond", "grapevine"])
def test_woody_species_are_not_campaign_crops(woody):
    plan = _wheat_plan().model_copy(deep=True)
    plan.cycles[0].crop = woody
    with pytest.raises(plan_write.PlanError) as e:
        plan_write.validate_plan(plan, TODAY)
    assert (e.value.code, e.value.status) == ("woody_not_campaign_crop", 422)


def test_annual_sown_long_before_its_campaign_is_inconsistent():
    with pytest.raises(plan_write.PlanError) as e:
        plan_write.validate_plan(_wheat_plan(start="2025-06-01"), TODAY)
    assert e.value.code == "dates_inconsistent"
    plan_write.validate_plan(_wheat_plan(start="2025-08-01"), TODAY)  # opens the previous campaign: allowed


def test_a_cover_can_end_by_mowing():
    cover = {"role": "cover_crop", "crop": "vetch", "sowing_start": "2026-10-01",
             "expected_termination": "2027-04-20", "termination_method": "mowing"}
    segs = plan_write.segments_from_plan(_plan(cycles=[cover]))
    ent = build_segment_entity("t", URN, "2027", 0, segs[0])
    assert ent["terminationMethod"]["value"] == "mowing"


# --- no harvest date asked (owner decision 2026-10-12) ---------------------------------------------------

def _undated(cycles, campaign=2027):
    return PlanIn.model_validate({"campaign": campaign, "regime": "rainfed", "management": "conventional", "cycles": cycles})


def test_campaign_close_is_the_day_before_the_autumn_sowings():
    assert plan_write.campaign_close(2027) == date(2027, 7, 31)


async def test_an_annual_main_crop_without_end_stores_none(monkeypatch):
    client = _orion(monkeypatch, [])
    plan = _undated([{"role": "main_crop", "crop": "wheat", "sowing_start": "2026-10-20"}])
    await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=plan, today=TODAY)
    entity = client.create_entity.await_args.args[0]
    assert "expectedTerminationDate" not in entity and entity["sowingWindowStart"]["value"]["@value"] == "2026-10-20"


def test_a_cover_without_end_ends_when_the_main_crop_is_sown():
    plan = _undated([{"role": "cover_crop", "crop": "vetch", "sowing_start": "2026-10-01", "termination_method": "roller_crimper"},
                     {"role": "main_crop", "crop": "maize", "sowing_start": "2027-04-20"}])
    plan_write.validate_plan(plan, TODAY)
    cover, main = plan_write.segments_from_plan(plan)
    assert cover["expected_termination"] == "2027-04-20" and main["expected_termination"] is None


def test_a_cover_sown_after_the_main_crop_is_inconsistent():
    plan = _undated([{"role": "cover_crop", "crop": "vetch", "sowing_start": "2027-05-01"},
                     {"role": "main_crop", "crop": "maize", "sowing_start": "2027-04-20"}])
    with pytest.raises(plan_write.PlanError, match="dates_inconsistent"):
        plan_write.validate_plan(plan, TODAY)


def test_a_perennial_stands_for_its_years():
    plan = _undated([{"role": "main_crop", "crop": "alfalfa", "sowing_start": "2026-09-15", "perennial_years": 4}])
    plan_write.validate_plan(plan, TODAY)
    (segment,) = plan_write.segments_from_plan(plan)
    assert segment["expected_termination"] == "2030-07-31"  # campaigns 2027, 2028, 2029 and 2030


def test_a_perennial_without_years_or_end_is_missing_its_duration():
    with pytest.raises(plan_write.PlanError, match="missing_duration"):
        plan_write.validate_plan(_undated([{"role": "main_crop", "crop": "alfalfa", "sowing_start": "2026-09-15"}]), TODAY)


def test_perennial_years_are_bounded_by_the_contract():
    with pytest.raises(ValueError):
        _undated([{"role": "main_crop", "crop": "alfalfa", "sowing_start": "2026-09-15", "perennial_years": 11}])


@pytest.mark.parametrize("start", ["2027-08-15", "2026-03-01"])
def test_an_undated_main_crop_must_be_sown_in_its_campaign(start):
    with pytest.raises(plan_write.PlanError, match="sowing_outside_campaign"):
        plan_write.validate_plan(_undated([{"role": "main_crop", "crop": "wheat", "sowing_start": start}]), TODAY)


def test_a_catch_crop_still_needs_its_date():
    plan = _undated([{"role": "main_crop", "crop": "wheat", "sowing_start": "2026-10-20"},
                     {"role": "catch_crop", "crop": "phacelia", "sowing_start": "2027-07-10"}])
    with pytest.raises(plan_write.PlanError, match="missing_dates"):
        plan_write.validate_plan(plan, TODAY)


async def test_an_undated_wheat_does_not_block_the_next_campaigns_autumn_sowing(monkeypatch):
    # The 2027 wheat was planned with no end; planning 2028 sown in October 2027 must not see an overlap.
    wheat = _crop("2027", "planned", "2026-10-20")
    _orion(monkeypatch, [wheat])
    plan = _undated([{"role": "main_crop", "crop": "barley", "sowing_start": "2027-10-25"}], campaign=2028)
    out = await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=plan, today=TODAY)
    assert out["written"]


async def test_an_undated_wheat_still_occupies_its_own_campaign(monkeypatch):
    # Bounded at the campaign close (Jul 31): a 2028 cover sown in July 2027 overlaps the 2027 wheat.
    _orion(monkeypatch, [_crop("2027", "planned", "2026-10-20")])
    plan = _undated([{"role": "cover_crop", "crop": "vetch", "sowing_start": "2027-07-10"},
                     {"role": "main_crop", "crop": "barley", "sowing_start": "2027-10-25"}], campaign=2028)
    with pytest.raises(plan_write.PlanError, match="cycles_overlap"):
        await plan_write.write_plan(tenant_id="t", parcel_urn=URN, plan=plan, today=TODAY)
