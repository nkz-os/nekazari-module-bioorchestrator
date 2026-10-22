"""Tests for regen-protein adequacy script."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.ingestion.validate_ingest_bundle import validate_bundle
from scripts.adequate_regen_protein import adequate_curated

IFAPA_CURATED = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "nkz-ifapa-scraper",
        "data",
        "jsonld",
        "curated_regen_protein.jsonld",
    )
)


@pytest.mark.skipif(not os.path.isfile(IFAPA_CURATED), reason="curated bundle missing")
def test_ifapa_regen_protein_adequacy_gate_pass():
    doc = adequate_curated(Path(IFAPA_CURATED), canonical_source="IFAPA")
    vt = sum(1 for n in doc["@graph"] if n.get("@type") == "VarietyTrial")
    assert vt == 116
    for node in doc["@graph"]:
        if node.get("@type") == "VarietyTrial":
            assert "eppo:" in node["mergeKey"]
            assert node["source_id"] == "IFAPA"

    tmp = Path(__file__).parent / "_tmp_ifapa_adequate.jsonld"
    tmp.write_text(json.dumps(doc), encoding="utf-8")
    try:
        report = validate_bundle(str(tmp))
        assert report.ok, [e.message for e in report.errors()[:5]]
    finally:
        tmp.unlink(missing_ok=True)
