"""crop-context thermal limits: species limits found by the registry slug, and sourced frost limits per stage."""
import asyncio
from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.graph.dao import GraphDAO
from app.planning.late_frost import STAGE_THRESHOLDS, frost_by_stage
from app.services.crop_calendar import effective_calendar, stage_table

PARCEL_ID = "urn:ngsi-ld:AgriParcel:test-parcel-001"
WHEAT = [{"stage": "flowering", "threshold_c": -1.0, "exposure_h": 2.0, "source": "kstate_c646"},
         {"stage": "grain_filling", "threshold_c": -2.0, "exposure_h": 2.0, "source": "kstate_c646"}]
BARLEY = [{"stage": "flowering", "threshold_c": -1.0, "exposure_h": None, "source": "fao_2005_t45"}]
SPECIES_LIMITS = {"heat_damage_c": 35.0, "frost_damage_c": -5.0, "heat_accum_hours": 48}
NO_LIMITS = {"heat_damage_c": None, "frost_damage_c": None, "heat_accum_hours": None}


@pytest.mark.parametrize("slug", sorted(STAGE_THRESHOLDS))
def test_every_stage_with_a_limit_is_a_stage_of_that_crop(slug):
    names = {row["stage"] for row in stage_table(effective_calendar(slug))}
    assert {t.stage for t in STAGE_THRESHOLDS[slug]} <= names


@pytest.mark.parametrize("slug, expected", [
    ("wheat", WHEAT), ("barley", BARLEY),
    # The wheat source is common wheat; pea and the others have a limit but no stage model.
    ("durum_wheat", []), ("pea", []), ("oat", []), ("vetch", []), ("not-a-crop", []), ("", []), (None, [])])
def test_frost_limits_per_stage_come_only_from_a_source(slug, expected):
    assert frost_by_stage(slug) == expected


def _context(crop: dict, heat=None):
    crop_uri = "urn:ngsi-ld:AgriCrop:tenant-a:p1:2027:0"
    parcel = {"id": PARCEL_ID, "hasAgriCrop": {"type": "Relationship", "object": crop_uri}}

    class Orion:
        def __init__(self, tenant_id):
            pass

        async def get_entity(self, entity_id):
            return parcel if entity_id == PARCEL_ID else {"id": crop_uri, **crop}

        async def query_entities(self, **_):
            return []

        async def close(self):
            pass

    found = AsyncMock(return_value=heat)
    with ExitStack() as stack:
        stack.enter_context(patch("app.graph.dao.OrionClient", Orion))
        stack.enter_context(patch.object(GraphDAO, "get_phenology_params", AsyncMock(return_value=None)))
        stack.enter_context(patch.object(GraphDAO, "get_heat_tolerance", found))
        stack.enter_context(patch.object(GraphDAO, "get_soil_suitability", AsyncMock(return_value=None)))
        stack.enter_context(patch("app.graph.dao.fetch_crop_cycles", AsyncMock(return_value=None)))
        stack.enter_context(patch("app.services.soil_client.get_parcel_soil_properties",
                                  AsyncMock(return_value={"data_available": False})))
        result = asyncio.run(GraphDAO(MagicMock()).get_crop_context(PARCEL_ID, tenant_id="tenant-a"))
    return result, found


def prop(value):
    return {"type": "Property", "value": value}


@pytest.mark.parametrize("crop, slug", [
    ({"species": prop("TRZAX")}, "wheat"), ({"species": prop("TRZDU")}, "durum_wheat"),
    ({"species": prop("wheat")}, "wheat"), ({"name": prop("Triticum aestivum")}, "wheat"),
    ({"name": prop("cebada"), "species": prop("HORVX")}, "barley"),
    ({"scientificName": prop("Pisum sativum")}, "pea"), ({"species": prop("unknown plant")}, "unknown plant")])
def test_species_limits_are_looked_up_by_the_registry_slug(crop, slug):
    _, found = _context(crop)
    found.assert_awaited_once_with(slug)


def test_wheat_carries_its_species_and_stage_limits():
    result, _ = _context({"species": prop("TRZAX")}, SPECIES_LIMITS)
    assert result["thermal_limits"] == {**SPECIES_LIMITS, "frost_by_stage": WHEAT}


def test_a_crop_without_a_species_node_keeps_its_stage_limits():
    result, _ = _context({"species": prop("HORVX")})
    assert result["thermal_limits"] == {**NO_LIMITS, "frost_by_stage": BARLEY}


def test_a_crop_without_any_limit_still_answers_the_object():
    result, _ = _context({"species": prop("unknown plant")})
    assert result["thermal_limits"] == {**NO_LIMITS, "frost_by_stage": []}
