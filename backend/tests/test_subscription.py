"""TDD: _create_orion_subscription sends application/json + Link header (not ld+json).

Bug: previous code used Content-Type: application/ld+json without @context in body
→ Orion-LD 400. Fix: send as application/json + Link header carrying @context URI.
"""
from __future__ import annotations

import asyncio
from unittest.mock import MagicMock


# ── Fake httpx infrastructure ──────────────────────────────────────────────────

class _FakeResponse:
    def __init__(self, status_code: int, data):
        self.status_code = status_code
        self._data = data

    def json(self):
        return self._data


class _FakeClient:
    """Minimal async context-manager stub replacing httpx.AsyncClient."""

    def __init__(self, get_data, *, timeout=None):
        self._get_data = get_data
        self.get_calls: list[dict] = []
        self.post_calls: list[dict] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass

    async def get(self, url, *, params=None, headers=None):
        self.get_calls.append({"url": url, "params": params, "headers": headers})
        return _FakeResponse(200, self._get_data)

    async def post(self, url, *, json=None, headers=None):
        self.post_calls.append({"url": url, "json": json, "headers": headers})
        return _FakeResponse(201, {})


# ── Tests ──────────────────────────────────────────────────────────────────────

def test_subscription_posted_as_json_with_link(monkeypatch):
    """When no matching subscription exists, POST with application/json + Link header."""
    fake_client = _FakeClient(get_data=[])  # GET returns empty list → no existing sub

    def _fake_async_client(**kwargs):
        return fake_client

    import httpx
    monkeypatch.setattr(httpx, "AsyncClient", _fake_async_client)

    from app.main import _create_orion_subscription
    asyncio.run(_create_orion_subscription())

    # A POST must have happened
    assert len(fake_client.post_calls) == 1, "Expected exactly one POST"
    call = fake_client.post_calls[0]

    # Content-Type must be application/json (NOT ld+json)
    assert call["headers"].get("Content-Type") == "application/json", (
        f"Expected application/json, got {call['headers'].get('Content-Type')}"
    )

    # Link header must reference the json-ld context relation
    link = call["headers"].get("Link", "")
    assert "json-ld#context" in link, f"Link header missing json-ld#context: {link!r}"

    # Body must be a valid Subscription
    body = call["json"]
    assert body["type"] == "Subscription"
    assert body["entities"] == [{"type": "AgriCrop"}]
    assert "kcIni" in body["watchedAttributes"]
    assert body["notification"]["endpoint"]["uri"] == (
        "http://bioorchestrator-service:8420/api/ngsi-ld/notify"
    )


def test_subscription_idempotent_when_ours_exists(monkeypatch):
    """When our subscription already exists, skip the POST entirely."""
    existing = [
        {
            "notification": {
                "endpoint": {
                    "uri": "http://bioorchestrator-service:8420/api/ngsi-ld/notify"
                }
            }
        }
    ]
    fake_client = _FakeClient(get_data=existing)

    def _fake_async_client(**kwargs):
        return fake_client

    import httpx
    monkeypatch.setattr(httpx, "AsyncClient", _fake_async_client)

    from app.main import _create_orion_subscription
    asyncio.run(_create_orion_subscription())

    assert len(fake_client.post_calls) == 0, (
        "POST should be skipped when our subscription already exists"
    )


def test_subscription_does_not_raise_on_error(monkeypatch):
    """A broken Orion (exception in GET) must not propagate — pod must not crash."""
    class _BrokenClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            pass

        async def get(self, *args, **kwargs):
            raise ConnectionError("Orion is down")

    import httpx
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: _BrokenClient())

    from app.main import _create_orion_subscription
    # Must not raise
    asyncio.run(_create_orion_subscription())
