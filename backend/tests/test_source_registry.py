"""Tests for source_registry.py."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

# Path relative to backend/
REGISTRY_PATH = Path(__file__).resolve().parent.parent / "data" / "sources_registry.json"


@pytest.fixture(autouse=True)
def _set_expected_env():
    """Ensure env points to the real registry for tests."""
    os.environ.setdefault("SOURCES_REGISTRY_PATH", str(REGISTRY_PATH))
    yield


def test_get_source_found():
    """get_source returns correct data for a known source_id."""
    from app.common.source_registry import get_source

    src = get_source("NAVARRA-AGRARIA")
    assert src["source_id"] == "NAVARRA-AGRARIA"
    assert src["institution_short"] == "INTIA"
    assert src["license_class"] == "public-sector-psi"
    assert src["use_type"] == "variety-trial-network"
    assert src["official_status"] is False


def test_get_source_not_found():
    """get_source raises KeyError for unknown source_id."""
    from app.common.source_registry import get_source

    with pytest.raises(KeyError, match="UNKNOWN-SOURCE"):
        get_source("UNKNOWN-SOURCE")


def test_get_source_lowercase():
    """get_source is case-sensitive: lowercase should raise KeyError."""
    from app.common.source_registry import get_source

    with pytest.raises(KeyError):
        get_source("navarra-agraria")


def test_all_sources_returns_all():
    """all_sources returns all 10 entries."""
    from app.common.source_registry import all_sources

    sources = all_sources()
    assert len(sources) == 10
    ids = [s["source_id"] for s in sources]
    assert "NAVARRA-AGRARIA" in ids
    assert "GENVCE" in ids
    assert "CTIFL" in ids
    assert "LFL-BAYERN" in ids
    assert "NEBIH" in ids
    assert "INIAV-LVR" in ids
    assert "ITACYL" in ids
    assert "IFAPA" in ids
    assert "INTIA-EXP" in ids
    assert "CREA" in ids


def test_all_source_ids():
    """all_source_ids returns 10 strings."""
    from app.common.source_registry import all_source_ids

    ids = all_source_ids()
    assert len(ids) == 10
    assert all(isinstance(s, str) for s in ids)


def test_get_attribution_default_locale():
    """get_attribution returns English by default."""
    from app.common.source_registry import get_attribution

    text = get_attribution("NAVARRA-AGRARIA")
    assert "INTIA" in text
    assert "Navarre" in text


def test_get_attribution_spanish():
    """get_attribution returns Spanish when requested."""
    from app.common.source_registry import get_attribution

    text = get_attribution("NAVARRA-AGRARIA", locale="es")
    assert "INTIA" in text
    assert "Navarra" in text


def test_get_attribution_fallback():
    """get_attribution falls back to English for unsupported locale."""
    from app.common.source_registry import get_attribution

    text = get_attribution("GENVCE", locale="de")
    assert "GENVCE" in text  # English text


def test_get_disclaimer():
    """get_disclaimer returns a non-empty string."""
    from app.common.source_registry import get_disclaimer

    text = get_disclaimer("NEBIH", locale="en")
    assert "NÉBIH" in text or "NEBIH" in text
    assert len(text) > 20


def test_sources_by_license():
    """sources_by_license returns correct count for public-sector-psi."""
    from app.common.source_registry import sources_by_license

    public = sources_by_license("public-sector-psi")
    assert len(public) >= 7  # most sources are public
    assert all(s["license_class"] == "public-sector-psi" for s in public)


def test_get_combined_attribution():
    """get_combined_attribution joins multiple sources."""
    from app.common.source_registry import get_combined_attribution

    text = get_combined_attribution(["NAVARRA-AGRARIA", "GENVCE"])
    assert "INTIA" in text
    assert "GENVCE" in text


def test_get_combined_disclaimer():
    """get_combined_disclaimer joins disclaimers."""
    from app.common.source_registry import get_combined_disclaimer

    text = get_combined_disclaimer(["NAVARRA-AGRARIA", "LFL-BAYERN"])
    assert "INTIA" in text
    assert "LfL" in text


def test_validation_all_have_required_fields():
    """Every source entry has all required fields."""
    from app.common.source_registry import all_sources

    required = {"source_id", "name", "institution", "institution_short", "country",
                "license_class", "use_type", "official_status", "attribution",
                "disclaimer"}
    for src in all_sources():
        missing = required - set(src.keys())
        assert not missing, f"{src['source_id']} missing: {missing}"
        # attribution must have at least "en"
        assert "en" in src["attribution"], f"{src['source_id']} missing 'en' attribution"
        assert "en" in src["disclaimer"], f"{src['source_id']} missing 'en' disclaimer"
