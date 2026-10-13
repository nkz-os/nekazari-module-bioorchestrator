"""Per-tenant subscriptions: registered only where the module is installed, with the receiver auth of the SDK."""
import asyncio
from unittest.mock import AsyncMock, patch

import pytest

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
    assert kwargs["notification_url"].endswith("/api/graph/internal/phenology-update")
    assert kwargs["internal_secret"] == "s3cr3t" and kwargs["derived_tokens"] is False
    assert "notification_headers" not in kwargs


def test_no_secret_means_no_subscriptions(monkeypatch):
    monkeypatch.delenv("INTERNAL_SERVICE_SECRET", raising=False)
    assert ts.build_registrar() is None


def test_installed_tenants_needs_a_database(monkeypatch):
    monkeypatch.delenv("POSTGRES_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(RuntimeError):
        ts.installed_tenants()


async def test_only_the_installed_tenants_are_reconciled(monkeypatch):
    monkeypatch.setattr(ts, "installed_tenants", lambda: ["a", "b"])
    registrar = AsyncMock()
    registrar.ensure_all.return_value = {"created": 1, "skipped": 1, "converged": 0, "errors": []}
    assert (await ts.reconcile_once(registrar))["created"] == 1
    registrar.ensure_all.assert_awaited_once_with(["a", "b"])


async def test_an_unreadable_tenant_list_registers_nothing_and_never_raises(monkeypatch):
    def boom():
        raise RuntimeError("db down")
    monkeypatch.setattr(ts, "installed_tenants", boom)
    registrar = AsyncMock()
    assert await ts.reconcile_once(registrar) is None
    registrar.ensure_all.assert_not_awaited()


async def test_the_reconciler_heals_hourly_after_a_first_pass(monkeypatch):
    monkeypatch.setenv("INTERNAL_SERVICE_SECRET", "s")
    monkeypatch.setattr(ts, "installed_tenants", lambda: ["a"])
    with patch.object(ts, "SubscriptionRegistrar") as reg:
        reg.return_value.ensure_all = AsyncMock(return_value={"created": 0, "skipped": 1, "errors": []})
        reg.return_value.periodic_heal = AsyncMock()
        await asyncio.wait_for(ts.run_reconciler(), timeout=5)
    reg.return_value.ensure_all.assert_awaited_once_with(["a"])
    assert reg.return_value.periodic_heal.await_args.kwargs["interval_minutes"] == ts.RECONCILE_INTERVAL_MINUTES
