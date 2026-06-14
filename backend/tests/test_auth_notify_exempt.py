"""Orion subscription notifications carry no JWT — the notify endpoint must be auth-exempt."""
from app.auth import SKIP_AUTH_PREFIXES


def test_orion_notify_endpoint_is_auth_exempt():
    # Orion-LD POSTs notifications directly to the in-cluster service (no api-gateway,
    # no Bearer token). Without an exemption the production JWT branch returns 401 and
    # the Orion->Neo4j catalog sync is silently dead.
    path = "/api/ngsi-ld/notify"
    assert any(path.startswith(p) for p in SKIP_AUTH_PREFIXES), (
        "/api/ngsi-ld/notify must match a SKIP_AUTH_PREFIXES entry"
    )


def test_context_endpoint_still_exempt():
    # The bare /ngsi-ld/ context endpoint must remain exempt (regression guard).
    path = "/ngsi-ld/bioorchestrator-context.jsonld"
    assert any(path.startswith(p) for p in SKIP_AUTH_PREFIXES)
