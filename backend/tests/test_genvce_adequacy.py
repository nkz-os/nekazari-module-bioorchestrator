"""GENVCE adequate bundle gate test."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.ingestion.validate_ingest_bundle import validate_bundle

BUNDLE = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "nkz-genvce-scraper",
        "data",
        "jsonld",
        "all_trials_adequate.jsonld",
    )
)


@pytest.mark.skipif(not os.path.isfile(BUNDLE), reason="adequate bundle missing")
def test_genvce_adequate_gate_pass():
    report = validate_bundle(BUNDLE)
    assert report.ok, [e.message for e in report.errors()[:5]]
    assert report.stats["orphan_trials"] == 0
    assert report.stats["ranking_ready"] > 3000


@pytest.mark.skipif(not os.path.isfile(BUNDLE), reason="adequate bundle missing")
def test_genvce_trials_have_canonical_mergekeys():
    graph = json.loads(Path(BUNDLE).read_text(encoding="utf-8"))["@graph"]
    for node in graph:
        if node.get("@type") != "VarietyTrial":
            continue
        assert node.get("source_id") == "GENVCE"
        assert "eppo:" in node["mergeKey"]
