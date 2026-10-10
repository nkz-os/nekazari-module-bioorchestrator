"""EPPO taxonomy lookup: the API returns the lineage as a JSON array, one object per rank."""
from __future__ import annotations

import json
import logging
from pathlib import Path

import httpx
import pytest
import respx

from app.services import crop_reference

FIXTURES = Path(__file__).parent / "fixtures"


def _load(code: str) -> list[dict]:
    return json.loads((FIXTURES / f"eppo_taxonomy_{code}.json").read_text(encoding="utf-8"))


@pytest.fixture(autouse=True)
def _eppo(monkeypatch):
    monkeypatch.setattr(crop_reference, "EPPO_API_KEY", "test-key")
    crop_reference._eppo_taxonomy_cache.clear()
    yield
    crop_reference._eppo_taxonomy_cache.clear()


@pytest.mark.parametrize(
    ("code", "family", "scientific_name"),
    [
        ("TRZAX", "Poaceae", "Triticum aestivum subsp. aestivum"),
        ("OLVEU", "Oleaceae", "Olea europaea"),
    ],
)
@respx.mock
async def test_taxonomy_list_shape_yields_family_and_scientific_name(code, family, scientific_name):
    route = respx.get(f"{crop_reference.EPPO_BASE}/taxons/taxon/{code}/taxonomy").mock(
        return_value=httpx.Response(200, json=_load(code))
    )

    tax = await crop_reference._fetch_eppo_taxonomy(code)

    assert tax is not None
    assert tax["family"] == family
    assert tax["scientific_name"] == scientific_name
    assert route.calls[0].request.headers["x-api-key"] == "test-key"


@respx.mock
async def test_taxonomy_unexpected_shape_is_unavailable_and_logged(caplog, monkeypatch):
    # Importing pcse runs a dictConfig that disables already-created loggers (import-order
    # dependent), so re-enable this one for caplog.
    monkeypatch.setattr(logging.getLogger("app.services.crop_reference"), "disabled", False)
    respx.get(f"{crop_reference.EPPO_BASE}/taxons/taxon/TRZAX/taxonomy").mock(
        return_value=httpx.Response(200, json={"family": "Poaceae"})
    )

    with caplog.at_level(logging.WARNING, logger="app.services.crop_reference"):
        tax = await crop_reference._fetch_eppo_taxonomy("TRZAX")

    assert tax is None
    assert any("unexpected" in r.getMessage() and "TRZAX" in r.getMessage() for r in caplog.records)
