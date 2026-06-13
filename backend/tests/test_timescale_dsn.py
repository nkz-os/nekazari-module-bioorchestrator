"""TIMESCALE_DSN must be mandatory — fail fast, never silently fall back to localhost."""
import pytest

from app.core.config import settings
from app.services import timescale


def test_connect_raises_when_dsn_unset(monkeypatch):
    monkeypatch.setattr(settings, "timescale_dsn", "", raising=False)
    with pytest.raises(RuntimeError, match="TIMESCALE_DSN is required"):
        timescale._connect()
