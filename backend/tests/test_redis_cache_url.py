"""Redis access for the best-effort stores uses the authenticated URL.

Pipeline history, the pipeline progress stream and crop event alerts used a
hardcoded password-less URL; against a Redis that requires a password every
call failed and the errors were swallowed.
"""

from __future__ import annotations

import re
from pathlib import Path

from app.core.config import Settings

APP_DIR = Path(__file__).resolve().parents[1] / "app"


def test_url_carries_password_when_set():
    s = Settings(redis_host="redis-test:6379", redis_password="s3cret")
    assert s.redis_cache_url == "redis://:s3cret@redis-test:6379/0"


def test_password_is_url_quoted():
    s = Settings(redis_host="redis-test:6379", redis_password="a/b@c:d")
    assert s.redis_cache_url == "redis://:a%2Fb%40c%3Ad@redis-test:6379/0"


def test_no_password_keeps_plain_url():
    s = Settings(redis_host="redis-test:6379", redis_password="")
    assert s.redis_cache_url == "redis://redis-test:6379/0"


def test_env_variables_feed_the_url(monkeypatch):
    monkeypatch.setenv("REDIS_HOST", "redis-env:6380")
    monkeypatch.setenv("REDIS_PASSWORD", "fromenv")
    assert Settings().redis_cache_url == "redis://:fromenv@redis-env:6380/0"


def test_no_hardcoded_redis_urls_in_app():
    offenders = [
        f"{p.relative_to(APP_DIR)}:{n}"
        for p in APP_DIR.rglob("*.py")
        for n, line in enumerate(p.read_text().splitlines(), 1)
        if re.search(r"""from_url\(\s*["']redis://""", line)
    ]
    assert offenders == [], f"hardcoded Redis URL at {offenders}"
