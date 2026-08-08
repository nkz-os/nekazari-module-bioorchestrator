from unittest.mock import AsyncMock, patch

import pytest

from app.main import _ensure_catalog_subscription


@pytest.mark.asyncio
async def test_ensure_subscription_registers_agricrop_in_catalog_tenant():
    fake_registrar = AsyncMock()
    with patch("app.main.SubscriptionRegistrar", return_value=fake_registrar) as ctor:
        await _ensure_catalog_subscription()
    _, kwargs = ctor.call_args
    assert kwargs["notification_url"].endswith("/api/ngsi-ld/notify")
    assert kwargs["module_name"] == "bioorchestrator"
    assert kwargs["subscriptions"] == [{"type": "AgriCrop"}]
    fake_registrar.ensure_all.assert_awaited_once_with(["default"])
