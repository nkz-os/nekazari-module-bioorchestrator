"""Per-tenant Orion subscriptions of bioorchestrator, kept where the module is installed.

Platform rule: a module's subscriptions register themselves where the module is installed (never
through a manual step or a user action) and never in a tenant without it. The SDK asks
entity-manager which tenants have the module (no database credential here) and ensures the
subscriptions where it is enabled and removes them where it is not; when entity-manager does not
answer it touches nothing. Done at startup and every hour.

One subscription today: crop-health's ``CropHealthAssessment.phenologyStage`` → phenology-update,
which drives the action rules. Only the legacy assign-crop flow used to create it. The catalog
subscription (AgriCrop in the shared tenant) is a separate registrar and is not touched here.
"""
from __future__ import annotations

import asyncio
import logging
import os

from nkz_platform_sdk.notify_auth import derived_tokens_from_env
from nkz_platform_sdk.subscriptions import SubscriptionDef, SubscriptionRegistrar

from app.core.config import settings

logger = logging.getLogger(__name__)

MODULE_ID = "bioorchestrator"
RECONCILE_INTERVAL_MINUTES = 60
PHENOLOGY_RECEIVER = "http://bioorchestrator-api-service:8420/api/graph/internal/phenology-update"


def build_registrar() -> SubscriptionRegistrar | None:
    """The module's tenant subscriptions, or None when they could not authenticate.

    Without the secret Orion would deliver notifications the receiver rejects (and pause the
    subscriptions), and entity-manager would not say where the module is installed.
    """
    secret = os.getenv("INTERNAL_SERVICE_SECRET", "")
    if not secret:
        logger.error("INTERNAL_SERVICE_SECRET not set: tenant subscriptions not registered")
        return None
    return SubscriptionRegistrar(
        orion_url=settings.orion_ld_url,
        notification_url=PHENOLOGY_RECEIVER,
        # The SDK default throttling (30 s) is what the existing subscriptions already carry.
        subscriptions=[SubscriptionDef(type="CropHealthAssessment", watched_attributes=["phenologyStage"])],
        module_name=MODULE_ID,
        context_url=settings.context_url,
        # The registrar owns the receiver auth header: master secret (legacy) or, with
        # NKZ_NOTIFY_DERIVED_TOKENS on, a token bound to the tenant and this receiver.
        internal_secret=secret,
        derived_tokens=derived_tokens_from_env(),
    )


async def run_reconciler() -> None:
    """Reconcile now, then every hour. Never raises (cancellation at shutdown still propagates)."""
    registrar = build_registrar()
    if registrar is None:
        return
    try:
        result = await registrar.reconcile_installations(MODULE_ID)
        logger.info("tenant subscriptions reconciled: %s", result)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.warning("initial tenant subscription reconcile failed: %s", exc)
    # Same registrar: its lock serialises the startup pass and the heal.
    await registrar.periodic_reconcile(MODULE_ID, RECONCILE_INTERVAL_MINUTES)
