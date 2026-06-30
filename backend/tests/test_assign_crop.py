"""Tests for assign_crop_to_parcel method."""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from app.graph.dao import GraphDAO


@pytest.mark.asyncio
async def test_assign_crop_creates_entity():
    """assign_crop_to_parcel should create an AgriCrop entity."""
    mock_driver = AsyncMock()
    dao = GraphDAO(mock_driver)

    # Mock the OrionClient
    with patch("app.graph.dao.OrionClient") as MockClient:
        instance = MockClient.return_value
        instance.create_entity = AsyncMock(return_value={"id": "new-id", "status": "created"})
        instance.get_entity = AsyncMock(side_effect=Exception("not found"))
        instance.update_entity_attrs = AsyncMock()
        instance.append_entity_attrs = AsyncMock()
        instance.close = AsyncMock()

        result = await dao.assign_crop_to_parcel(
            parcel_id="urn:ngsi-ld:AgriParcel:test-parcel",
            crop_uri="urn:ngsi-ld:AgriCrop:TRZAX",
            variety_uri="urn:ngsi-ld:AgriCropVariety:LG_AURUS",
            management="conventional",
            season_start="2026-03-01",
            season_end="2026-06-30",
            tenant_id="test-tenant",
        )

        assert result["status"] == "assigned"
        assert result["crop"] == "TRZAX"
        assert result["entity_id"] == "urn:ngsi-ld:AgriCrop:test-tenant:test-parcel:2026"
        MockClient.assert_called_once_with(tenant_id="test-tenant")
        instance.create_entity.assert_called_once()
        # Should patch both the parcel (hasAgriCrop) and NOT mark old crop (no old crop)
        # parcel commitment must be APPENDED (POST /attrs), not PATCHed, so a
        # first-time hasAgriCrop actually persists instead of landing in notUpdated.
        parcel_patch_call = [c for c in instance.append_entity_attrs.call_args_list
                            if c[0][0] == "urn:ngsi-ld:AgriParcel:test-parcel"]
        assert len(parcel_patch_call) == 1
        patch_body = parcel_patch_call[0][0][1]
        assert patch_body["hasAgriCrop"]["object"] == result["entity_id"]


@pytest.mark.asyncio
async def test_assign_crop_marks_old_harvested():
    """If parcel already has a crop, mark it harvested before assigning new one."""
    mock_driver = AsyncMock()
    dao = GraphDAO(mock_driver)

    old_crop_id = "urn:ngsi-ld:AgriCrop:test-tenant:test-parcel:2025"

    with patch("app.graph.dao.OrionClient") as MockClient:
        instance = MockClient.return_value
        instance.create_entity = AsyncMock(return_value={"id": "new-id", "status": "created"})
        instance.get_entity = AsyncMock(return_value={
            "hasAgriCrop": {"type": "Relationship", "object": old_crop_id},
        })
        instance.update_entity_attrs = AsyncMock()
        instance.append_entity_attrs = AsyncMock()
        instance.close = AsyncMock()

        await dao.assign_crop_to_parcel(
            parcel_id="urn:ngsi-ld:AgriParcel:test-parcel",
            crop_uri="urn:ngsi-ld:AgriCrop:TRZAX",
            variety_uri="urn:ngsi-ld:AgriCropVariety:LG_AURUS",
            management="conventional",
            season_start="2026-03-01",
            season_end="2026-06-30",
            tenant_id="test-tenant",
        )

        MockClient.assert_called_once_with(tenant_id="test-tenant")
        # Old crop should have been patched with status=harvested
        old_crop_calls = [c for c in instance.update_entity_attrs.call_args_list
                         if c[0][0] == old_crop_id]
        assert len(old_crop_calls) == 1
        assert old_crop_calls[0][0][1]["status"]["value"] == "harvested"


@pytest.mark.asyncio
async def test_assign_crop_409_upserts():
    """If AgriCrop entity already exists (409), do PATCH instead of failing."""
    mock_driver = AsyncMock()
    dao = GraphDAO(mock_driver)

    import httpx

    with patch("app.graph.dao.OrionClient") as MockClient:
        instance = MockClient.return_value
        # Simulate 409 on create
        error_response = MagicMock(spec=httpx.Response)
        error_response.status_code = 409
        error_response.text = "Already exists"
        instance.create_entity = AsyncMock(side_effect=httpx.HTTPStatusError(
            "Conflict", request=MagicMock(), response=error_response
        ))
        instance.get_entity = AsyncMock(side_effect=Exception("not found"))
        instance.update_entity_attrs = AsyncMock()
        instance.append_entity_attrs = AsyncMock()
        instance.close = AsyncMock()

        result = await dao.assign_crop_to_parcel(
            parcel_id="urn:ngsi-ld:AgriParcel:test-parcel",
            crop_uri="urn:ngsi-ld:AgriCrop:TRZAX",
            variety_uri="urn:ngsi-ld:AgriCropVariety:LG_AURUS",
            management="conventional",
            season_start="2026-03-01",
            season_end="2026-06-30",
            tenant_id="test-tenant",
        )

        assert result["status"] == "assigned"
        # Should have called update_entity_attrs for the existing entity
        existing_crop_calls = [c for c in instance.update_entity_attrs.call_args_list
                              if c[0][0] == result["entity_id"]]
        assert len(existing_crop_calls) == 1
        patch_body = existing_crop_calls[0][0][1]
        assert "id" not in patch_body
        assert "type" not in patch_body
        assert "dateCreated" not in patch_body
        assert patch_body["species"]["value"] == "TRZAX"
        MockClient.assert_called_once_with(tenant_id="test-tenant")


# ── clear_crop_assignment ──────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_clear_crop_assignment_uses_sdk_and_tenant():
    """clear_crop_assignment must use OrionClient with the correct tenant and patch body."""
    mock_driver = AsyncMock()
    dao = GraphDAO(mock_driver)

    captured_entity_id = None
    captured_attrs = None
    close_called = False

    class FakeOrion:
        def __init__(self, tenant_id):
            self.tenant_id = tenant_id

        async def update_entity_attrs(self, entity_id, attrs):
            nonlocal captured_entity_id, captured_attrs
            captured_entity_id = entity_id
            captured_attrs = attrs

        async def close(self):
            nonlocal close_called
            close_called = True

    with patch("app.graph.dao.OrionClient", side_effect=lambda t: FakeOrion(t)) as MockClient:
        result = await dao.clear_crop_assignment(
            parcel_id="urn:ngsi-ld:AgriParcel:test-parcel",
            tenant_id="acme",
        )

    MockClient.assert_called_once_with("acme")
    assert captured_entity_id == "urn:ngsi-ld:AgriParcel:test-parcel"
    assert set(captured_attrs.keys()) == {
        "hasAgriCrop", "hasAgriCropVariety", "management", "cropSeasonStart", "cropSeasonEnd"
    }
    assert result == {"status": "cleared", "parcel_id": "urn:ngsi-ld:AgriParcel:test-parcel"}
    assert close_called, "close() must be called (finally block)"


@pytest.mark.asyncio
async def test_clear_crop_assignment_raises_on_orion_error():
    """clear_crop_assignment must propagate Orion failures (not swallow them)."""
    mock_driver = AsyncMock()
    dao = GraphDAO(mock_driver)

    close_called = False

    class FakeOrionFailing:
        def __init__(self, tenant_id):
            pass

        async def update_entity_attrs(self, entity_id, attrs):
            raise RuntimeError("orion 400")

        async def close(self):
            nonlocal close_called
            close_called = True

    with patch("app.graph.dao.OrionClient", side_effect=lambda tenant_id: FakeOrionFailing(tenant_id)):
        with pytest.raises(RuntimeError, match="orion 400"):
            await dao.clear_crop_assignment(
                parcel_id="urn:ngsi-ld:AgriParcel:test-parcel",
                tenant_id="acme",
            )

    assert close_called, "close() must still be called even when update_entity_attrs raises"
