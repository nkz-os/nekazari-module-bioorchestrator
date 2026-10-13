"""Per-tenant Orion subscriptions of bioorchestrator, registered wherever the module is installed.

Platform rule: a module's subscriptions register themselves where the module is installed (never
through a manual step or a user action) and never in a tenant without it. A reconciler reads
``tenant_installed_modules`` at startup and every hour, the same pattern as the other modules.

One subscription today: crop-health's ``CropHealthAssessment.phenologyStage`` → phenology-update,
which drives the action rules. Only the legacy assign-crop flow used to create it.
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


def installed_tenants() -> list[str]:
    """Tenants with the module installed and enabled. Raises when the database cannot be read."""
    url = os.getenv("POSTGRES_URL") or os.getenv("DATABASE_URL") or ""
    if not url:
        raise RuntimeError("POSTGRES_URL is not set")
    import psycopg2

    conn = psycopg2.connect(url, connect_timeout=10)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT tenant_id FROM tenant_installed_modules "
                "WHERE module_id = %s AND is_enabled AND tenant_id IS NOT NULL AND tenant_id <> ''",
                (MODULE_ID,),
            )
            return sorted(row[0] for row in cur.fetchall())
    finally:
        conn.close()


def build_registrar() -> SubscriptionRegistrar | None:
    """The module's tenant subscriptions, or None when they could not authenticate.

    Without the secret Orion would deliver notifications the receiver rejects, and pause the
    subscriptions after three failures: better none, loudly.
    """
    secret = os.getenv("INTERNAL_SERVICE_SECRET", "")
    if not secret:
        logger.error("INTERNAL_SERVICE_SECRET not set: tenant subscriptions not registered")
        return None
    return SubscriptionRegistrar(
        orion_url=settings.orion_ld_url,
        notification_url=PHENOLOGY_RECEIVER,
        subscriptions=[SubscriptionDef(type="CropHealthAssessment", watched_attributes=["phenologyStage"])],
        module_name=MODULE_ID,
        context_url=settings.context_url,
        # The registrar owns the receiver auth header: master secret (legacy) or, with
        # NKZ_NOTIFY_DERIVED_TOKENS on, a token bound to the tenant and this receiver.
        internal_secret=secret,
        derived_tokens=derived_tokens_from_env(),
    )


async def _tenants() -> list[str]:
    return await asyncio.to_thread(installed_tenants)


async def reconcile_once(registrar: SubscriptionRegistrar) -> dict | None:
    """One pass over the installed tenants. Never raises; None when the tenants could not be read."""
    try:
        tenants = await _tenants()
    except Exception as exc:  # noqa: BLE001
        logger.error("tenant subscriptions skipped, installed tenants unreadable: %s", exc)
        return None
    result = await registrar.ensure_all(tenants)
    logger.info("tenant subscriptions reconciled tenants=%s created=%d converged=%d skipped=%d errors=%d",
                tenants, result["created"], result.get("converged", 0), result.get("skipped", 0),
                len(result["errors"]))
    for error in result["errors"]:
        logger.warning("tenant subscription error: %s", error)
    return result


async def run_reconciler() -> None:
    """Reconcile now, then every hour. Never raises (cancellation at shutdown still propagates)."""
    registrar = build_registrar()
    if registrar is None:
        return
    try:
        await reconcile_once(registrar)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.warning("initial tenant subscription reconcile failed: %s", exc)
    await registrar.periodic_heal(_tenants, interval_minutes=RECONCILE_INTERVAL_MINUTES)
