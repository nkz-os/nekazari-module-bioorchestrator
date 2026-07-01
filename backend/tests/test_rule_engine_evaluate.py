# tests/test_rule_engine_evaluate.py
import pytest
from unittest.mock import AsyncMock
from app.graph.rule_engine import evaluate

COVER_RULE = {
    "id": "cover_crop_termination_flowering",
    "conditions": {"all": [
        {"field": "crop.role", "op": "eq", "value": "cover_crop"},
        {"field": "crop.status", "op": "eq", "value": "active"},
        {"field": "crop.termination_method", "op": "eq", "value": "roller_crimper"},
        {"field": "phenology.current_stage", "op": "eq", "value": "flowering"},
    ]},
    "action": {"operation_type": "tillage", "urgency": "high", "window_days": 7,
               "description_template": "Tumbar {crop.species}"},
    "source_short": "INTIA",
}

def _orion(parcel, crop):
    o = AsyncMock()
    async def _get(eid, options=None):
        return parcel if eid.startswith("urn:ngsi-ld:AgriParcel") else crop
    o.get_entity.side_effect = _get
    o.upsert_entities_batch.return_value = {"upserted": 1, "errors": [], "entity_ids": []}
    return o

@pytest.mark.asyncio
async def test_flowering_cover_crop_produces_advisory():
    parcel = {"id": "urn:ngsi-ld:AgriParcel:montiko:p1", "hasAgriCrop": "urn:ngsi-ld:AgriCrop:montiko:p1:2026"}
    crop = {"role": "cover_crop", "status": "active", "species": "VICSA", "terminationMethod": "roller_crimper"}
    dao = AsyncMock(); dao.get_action_rules.return_value = [COVER_RULE]
    orion = _orion(parcel, crop)
    out = await evaluate(dao, orion, "montiko", parcel["id"], {"phenology.current_stage": "flowering"})
    assert len(out) == 1
    assert out[0]["id"].endswith("cover_crop_termination_flowering:flowering")
    orion.upsert_entities_batch.assert_awaited_once()

@pytest.mark.asyncio
async def test_refagricrop_fallback_resolves_crop():
    # Legacy relationship name: parcel has refAgriCrop, not hasAgriCrop.
    parcel = {"id": "urn:ngsi-ld:AgriParcel:montiko:p1", "refAgriCrop": "urn:ngsi-ld:AgriCrop:montiko:p1:2026"}
    crop = {"role": "cover_crop", "status": "active", "species": "VICSA", "terminationMethod": "roller_crimper"}
    dao = AsyncMock(); dao.get_action_rules.return_value = [COVER_RULE]
    orion = _orion(parcel, crop)
    out = await evaluate(dao, orion, "montiko", parcel["id"], {"phenology.current_stage": "flowering"})
    assert len(out) == 1
    orion.upsert_entities_batch.assert_awaited_once()

@pytest.mark.asyncio
async def test_no_crop_assigned_returns_empty():
    parcel = {"id": "urn:ngsi-ld:AgriParcel:montiko:p1"}  # no hasAgriCrop
    dao = AsyncMock(); dao.get_action_rules.return_value = [COVER_RULE]
    orion = _orion(parcel, {})
    out = await evaluate(dao, orion, "montiko", parcel["id"], {"phenology.current_stage": "flowering"})
    assert out == []
    orion.upsert_entities_batch.assert_not_awaited()

@pytest.mark.asyncio
async def test_wrong_stage_no_advisory():
    parcel = {"id": "urn:ngsi-ld:AgriParcel:montiko:p1", "hasAgriCrop": "urn:ngsi-ld:AgriCrop:montiko:p1:2026"}
    crop = {"role": "cover_crop", "status": "active", "species": "VICSA", "terminationMethod": "roller_crimper"}
    dao = AsyncMock(); dao.get_action_rules.return_value = [COVER_RULE]
    orion = _orion(parcel, crop)
    out = await evaluate(dao, orion, "montiko", parcel["id"], {"phenology.current_stage": "tillering"})
    assert out == []
