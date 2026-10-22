"""Per-tenant subscriptions: registered only where the module is installed, with the receiver auth of the SDK."""
import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from app.services import tenant_subscriptions as ts


@pytest.fixture(autouse=True)
def _fresh_registrar(monkeypatch):
    # The registrar is cached per process; every test builds its own.
    monkeypatch.setattr(ts, "_REGISTRAR", None)


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


def _lifecycle_client(monkeypatch, calls):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    class _Registrar:
        async def reconcile_installations(self, module_id):
            calls.append(module_id)
            return {}
    monkeypatch.setattr(ts, "get_registrar", lambda: _Registrar())
    app = FastAPI()
    app.include_router(ts.lifecycle_router())
    return TestClient(app)


def test_a_signed_lifecycle_hint_reconciles_the_module_at_once(monkeypatch):
    from nkz_platform_sdk.notify_auth import (
        notification_token,
        receiver_host_from_headers,
    )
    monkeypatch.setenv("INTERNAL_SERVICE_SECRET", "s3cr3t")
    calls: list[str] = []
    client = _lifecycle_client(monkeypatch, calls)
    host = "bioorchestrator-api-service:8420"
    token = notification_token("s3cr3t", "acmefarm", receiver_host_from_headers({"Host": host}))
    r = client.post(ts.LIFECYCLE_PATH, json={"event": "module.enabled", "tenant_id": "acmefarm"},
                    headers={"X-Notification-Token": token, "NGSILD-Tenant": "acmefarm", "Host": host})
    assert r.status_code == 202
    assert calls and calls[0] == "bioorchestrator"


def test_an_unsigned_lifecycle_hint_is_refused(monkeypatch):
    monkeypatch.setenv("INTERNAL_SERVICE_SECRET", "s3cr3t")
    calls: list[str] = []
    client = _lifecycle_client(monkeypatch, calls)
    r = client.post(ts.LIFECYCLE_PATH, json={"event": "module.enabled"}, headers={"NGSILD-Tenant": "acmefarm"})
    assert r.status_code in (401, 403) and calls == []


def test_the_registrar_is_built_once_per_process(monkeypatch):
    monkeypatch.setenv("INTERNAL_SERVICE_SECRET", "s")
    monkeypatch.setattr(ts, "_REGISTRAR", None)
    assert ts.get_registrar() is ts.get_registrar()


def test_the_app_mounts_the_lifecycle_receiver(monkeypatch):
    # Mounted and past the user-auth middleware: an unsigned hint is the SDK's 401, never a 404.
    from fastapi.testclient import TestClient

    from app.main import app
    monkeypatch.setenv("INTERNAL_SERVICE_SECRET", "s3cr3t")
    assert TestClient(app).post(ts.LIFECYCLE_PATH, json={}).status_code == 401
