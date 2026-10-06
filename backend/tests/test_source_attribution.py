"""Mandatory source attribution: registry fields and the response helper.

GENVCE (Ley 37/2007 reuse notice) prescribes an exact credit line with the download date;
CREA (CC BY 3.0 IT) needs the credit, the source documents, the licence and a note that the
data were changed. Responses list only the attributions of the sources actually present.
"""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from urllib.parse import urlparse

import pytest

from app.common import source_registry as sr

_REGISTRY = Path(__file__).resolve().parent.parent / "data" / "sources_registry.json"

GENVCE_TEXT = (
    "Fuente: Datos Abiertos GENVCE. Url: https://genvce.org/mapa-de-resultados/ "
    "(Descarga: 01/06/2026.)"
)
_ITEM_KEYS = {"source_id", "text", "url", "licence_id", "licence_url", "processing_note"}


@pytest.fixture(autouse=True)
def _reset_warned():
    sr._WARNED_NO_ATTRIBUTION.clear()
    yield
    sr._WARNED_NO_ATTRIBUTION.clear()


# ── registry ────────────────────────────────────────────────────────────────


def test_genvce_attribution_is_the_exact_required_string():
    src = sr.get_source("GENVCE")
    assert src["attribution_text"] == GENVCE_TEXT
    assert src["attribution_url"] == "https://genvce.org/mapa-de-resultados/"
    assert src["licence_id"] and src["licence_url"]


def test_genvce_download_date_matches_the_credit_line():
    src = sr.get_source("GENVCE")
    year, month, day = src["download_date"].split("-")
    assert f"(Descarga: {day}/{month}/{year}.)" in src["attribution_text"]


def test_genvce_processing_note_says_figures_are_nekazari_calculations():
    note = sr.get_source("GENVCE")["processing_note"]
    assert {"en", "es"} <= set(note)
    assert "Nekazari" in note["en"] and "calculations" in note["en"]
    assert "Nekazari" in note["es"] and "cálculos" in note["es"]


def test_crea_attribution_has_credit_documents_licence_and_change_notice():
    src = sr.get_source("CREA")
    text = src["attribution_text"]
    assert "CREA" in text and "Consiglio per la ricerca in agricoltura" in text
    assert "CC BY 3.0" in text
    assert re.search(r"modificati|elaborati", text)
    assert src["licence_id"] == "CC-BY-3.0-IT"
    assert src["licence_url"].startswith("https://creativecommons.org/licenses/by/3.0/it")
    docs = src["source_documents"]
    assert [d["year"] for d in docs] == [2021, 2022, 2023, 2024, 2025]
    assert all(d["title"] and d["url"].endswith(f"Mais+{d['year']}.pdf") for d in docs)
    assert {"en", "es"} <= set(src["processing_note"])


def test_attribution_urls_are_public_third_party_hosts_only():
    allowed = {"genvce.org", "www.crea.gov.it", "creativecommons.org"}
    for sid in ("GENVCE", "CREA"):
        src = sr.get_source(sid)
        urls = [src["attribution_url"], src["licence_url"], *(d["url"] for d in src.get("source_documents", []))]
        assert {urlparse(u).hostname for u in urls} <= allowed


def test_existing_registry_fields_are_kept():
    for sid in ("GENVCE", "CREA"):
        src = sr.get_source(sid)
        assert src["attribution"]["en"] and src["disclaimer"]["en"]
        assert src["license_class"] == "public-sector-psi"


# ── helper ──────────────────────────────────────────────────────────────────


def test_get_attributions_lists_only_the_sources_present():
    out = sr.get_attributions(["GENVCE"])
    assert [a["source_id"] for a in out] == ["GENVCE"]
    assert set(out[0]) == _ITEM_KEYS
    assert out[0]["text"] == GENVCE_TEXT
    assert out[0]["url"] == "https://genvce.org/mapa-de-resultados/"


def test_get_attributions_dedupes_sorts_and_ignores_empty_values():
    out = sr.get_attributions(["GENVCE", None, "", "CREA", "GENVCE"])
    assert [a["source_id"] for a in out] == ["CREA", "GENVCE"]
    assert sr.get_attributions([]) == []
    assert sr.get_attributions([None]) == []


def test_get_attributions_accepts_a_generator():
    assert [a["source_id"] for a in sr.get_attributions(s for s in ("CREA", "CREA"))] == ["CREA"]


def test_unknown_source_is_left_out_and_logged_once(caplog):
    with caplog.at_level(logging.WARNING, logger=sr.logger.name):
        assert sr.get_attributions(["NO-SUCH-SOURCE"]) == []
        assert sr.get_attributions(["NO-SUCH-SOURCE"]) == []
    msgs = [r.getMessage() for r in caplog.records if "source_attribution_missing" in r.getMessage()]
    assert len(msgs) == 1
    assert "source_id=NO-SUCH-SOURCE" in msgs[0] and "not_in_registry" in msgs[0]


def test_registered_source_without_structured_attribution_is_logged_not_listed(caplog):
    with caplog.at_level(logging.WARNING, logger=sr.logger.name):
        assert sr.get_attributions(["NAVARRA-AGRARIA"]) == []
    assert any("no_structured_attribution" in r.getMessage() for r in caplog.records)


def test_items_carry_the_processing_note_for_non_ui_clients():
    for item in sr.get_attributions(["GENVCE", "CREA"]):
        note = item["processing_note"]
        assert {"en", "es"} <= set(note) and all(note[k] for k in ("en", "es"))
        assert "Nekazari" in note["en"] and "not data published" in note["en"]
        assert "Nekazari" in note["es"] and "no son datos publicados" in note["es"]
    for item in sr.all_attributions():
        assert item["processing_note"] == sr.get_source(item["source_id"])["processing_note"]


def test_returned_items_are_copies_of_the_registry():
    first = sr.get_attributions(["GENVCE"])
    first[0]["text"] = "tampered"
    first[0]["processing_note"]["en"] = "tampered"
    again = sr.get_attributions(["GENVCE"])[0]
    assert again["text"] == GENVCE_TEXT and again["processing_note"]["en"] != "tampered"


def test_all_attributions_lists_every_structured_source_with_extras():
    out = sr.all_attributions()
    by_id = {a["source_id"]: a for a in out}
    assert {"GENVCE", "CREA"} <= set(by_id)
    assert all(_ITEM_KEYS <= set(a) for a in out)
    assert [a["source_id"] for a in out] == sorted(by_id)
    assert by_id["GENVCE"]["download_date"] == "2026-06-01"
    assert len(by_id["CREA"]["source_documents"]) == 5
    assert "processing_note" in by_id["GENVCE"]
    # the registry file is the single source: no id lists a text the file does not hold
    raw = {s["source_id"]: s for s in json.loads(_REGISTRY.read_text(encoding="utf-8"))}
    assert all(a["text"] == raw[a["source_id"]]["attribution_text"] for a in out)
