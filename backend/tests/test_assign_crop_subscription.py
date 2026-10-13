# tests/test_assign_crop_subscription.py
from unittest.mock import AsyncMock, patch

import pytest

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
        # NGSI-LD has no top-level `condition` field (Orion 400s on it) — watchedAttributes alone.
        assert subs[0].condition is None
        assert "bioorchestrator-api-service:8420" in kwargs["notification_url"]
        assert "phenology-update" in kwargs["notification_url"]
        MockReg.return_value.ensure_all.assert_awaited_once_with(["montiko"])

@pytest.mark.asyncio
async def test_ensure_phenology_subscription_never_raises():
    dao = GraphDAO(AsyncMock())
    with patch("app.graph.dao.SubscriptionRegistrar", side_effect=RuntimeError("orion down")):
        await dao._ensure_phenology_subscription("montiko")  # must not raise


@pytest.mark.asyncio
async def test_phenology_registrar_owns_receiver_auth_header(monkeypatch):
    monkeypatch.setenv("INTERNAL_SERVICE_SECRET", "s3cr3t")
    monkeypatch.delenv("NKZ_NOTIFY_DERIVED_TOKENS", raising=False)
    dao = GraphDAO(AsyncMock())
    with patch("app.graph.dao.SubscriptionRegistrar") as MockReg:
        MockReg.return_value.ensure_all = AsyncMock(return_value={"created": 0, "skipped": 1, "errors": []})
        await dao._ensure_phenology_subscription("tenant-a")
    kwargs = MockReg.call_args.kwargs
    assert kwargs["internal_secret"] == "s3cr3t"
    assert kwargs["derived_tokens"] is False
    assert "notification_headers" not in kwargs
