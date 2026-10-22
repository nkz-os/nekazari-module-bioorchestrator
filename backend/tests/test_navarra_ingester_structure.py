"""Structural + behavior guards for NavarraIngester multi-source provenance.

Regression guard: a module-level `_resolve_source` must not be defined inside
the class body — doing so silently ends the class and swallows the trailing
`@staticmethod` helpers (e.g. `_load_jsonld`), which `ingest()` depends on.
"""
from app.ingestion.navarra_ingester import NavarraIngester, _resolve_source


def test_load_jsonld_is_a_class_staticmethod():
    # If _resolve_source is misplaced inside the class, _load_jsonld is lost
    # and ingest() raises AttributeError at runtime.
    assert hasattr(NavarraIngester, "_load_jsonld")


def test_resolve_source_direct_field():
    assert _resolve_source({"source": "GENVCE"}) == "genvce"


def test_resolve_source_nested_metadata():
    assert _resolve_source({"metadata": {"source": "NÉBIH"}}) == "nébih"


def test_resolve_source_from_id_prefix():
    assert _resolve_source({"@id": "urn:nkz:nebih:trial:42"}) == "nebih"


def test_resolve_source_fallback_unknown():
    assert _resolve_source({}) == "unknown"
