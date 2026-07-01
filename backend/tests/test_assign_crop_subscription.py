# tests/test_assign_crop_subscription.py
import pytest
from unittest.mock import AsyncMock, patch
from app.graph.dao import GraphDAO

@pytest.mark.asyncio
async def test_ensure_phenology_subscription_uses_watched_attrs():
    dao = GraphDAO(AsyncMock())
    with patch("app.graph.dao.SubscriptionRegistrar") as MockReg:
        MockReg.return_value.ensure_all = AsyncMock(return_value={"created": 1, "skipped": 0, "errors": []})
        await dao._ensure_phenology_subscription("montiko")
        _, kwargs = MockReg.call_args
        subs = kwargs["subscriptions"]
        assert subs[0].type == "CropHealthAssessment"
        assert subs[0].watched_attributes == ["phenologyStage"]
        assert subs[0].condition == {"attrs": ["phenologyStage"]}
        assert "bioorchestrator-api-service:8420" in kwargs["notification_url"]
        assert "phenology-update" in kwargs["notification_url"]
        MockReg.return_value.ensure_all.assert_awaited_once_with(["montiko"])

@pytest.mark.asyncio
async def test_ensure_phenology_subscription_never_raises():
    dao = GraphDAO(AsyncMock())
    with patch("app.graph.dao.SubscriptionRegistrar", side_effect=RuntimeError("orion down")):
        await dao._ensure_phenology_subscription("montiko")  # must not raise
