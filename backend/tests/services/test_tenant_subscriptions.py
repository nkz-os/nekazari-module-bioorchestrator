"""Per-tenant subscriptions: registered only where the module is installed, with the receiver auth of the SDK."""
import asyncio
from unittest.mock import AsyncMock, patch

from app.services import tenant_subscriptions as ts


def test_the_phenology_subscription_watches_the_stage_and_owns_the_receiver_auth(monkeypatch):
    monkeypatch.setenv("INTERNAL_SERVICE_SECRET", "s3cr3t")
    monkeypatch.delenv("NKZ_NOTIFY_DERIVED_TOKENS", raising=False)
    with patch.object(ts, "SubscriptionRegistrar") as reg:
        ts.build_registrar()
    kwargs = reg.call_args.kwargs
    (sub,) = kwargs["subscriptions"]
    assert (sub.type, sub.watched_attributes) == ("CropHealthAssessment", ["phenologyStage"])
    # NGSI-LD has no top-level `condition` field (Orion 400s on it): watchedAttributes alone.
    assert sub.condition is None
    assert sub.throttling == 30  # what the existing subscriptions carry: no churn on convergence
    assert kwargs["notification_url"].endswith("/api/graph/internal/phenology-update")
    assert kwargs["internal_secret"] == "s3cr3t" and kwargs["derived_tokens"] is False
    assert "notification_headers" not in kwargs


def test_no_secret_means_no_subscriptions(monkeypatch):
    monkeypatch.delenv("INTERNAL_SERVICE_SECRET", raising=False)
    assert ts.build_registrar() is None


async def test_the_reconciler_asks_entity_manager_then_heals_hourly(monkeypatch):
    monkeypatch.setenv("INTERNAL_SERVICE_SECRET", "s")
    with patch.object(ts, "SubscriptionRegistrar") as reg:
        reg.return_value.reconcile_installations = AsyncMock(return_value={"created": 0})
        reg.return_value.periodic_reconcile = AsyncMock()
        await asyncio.wait_for(ts.run_reconciler(), timeout=5)
    reg.return_value.reconcile_installations.assert_awaited_once_with("bioorchestrator")
    reg.return_value.periodic_reconcile.assert_awaited_once_with("bioorchestrator", ts.RECONCILE_INTERVAL_MINUTES)


async def test_a_failed_first_pass_still_starts_the_heal(monkeypatch):
    monkeypatch.setenv("INTERNAL_SERVICE_SECRET", "s")
    with patch.object(ts, "SubscriptionRegistrar") as reg:
        reg.return_value.reconcile_installations = AsyncMock(side_effect=RuntimeError("entity-manager down"))
        reg.return_value.periodic_reconcile = AsyncMock()
        await asyncio.wait_for(ts.run_reconciler(), timeout=5)
    reg.return_value.periodic_reconcile.assert_awaited_once()


async def test_without_secret_nothing_runs(monkeypatch):
    monkeypatch.delenv("INTERNAL_SERVICE_SECRET", raising=False)
    with patch.object(ts, "SubscriptionRegistrar") as reg:
        await ts.run_reconciler()
    reg.assert_not_called()
