"""The cover-crop reference dataset is a registered, attributable source (Etalab 2.0)."""
from __future__ import annotations

from app.common import source_registry as sr
from app.common.attribution import attach_attributions

SID = "thiebeau_inrae_2021"


def test_source_has_structured_attribution():
    src = sr.get_source(SID)
    assert src["licence_id"] == "etalab-2.0"
    assert src["licence_url"] == "https://www.etalab.gouv.fr/licence-ouverte-open-licence/"
    assert src["attribution_url"] == "https://doi.org/10.15454/LBI3U7"
    assert src["download_date"] == "2026-10-30"
    for needle in ("Thiébeau", "Data INRAE", "v3", "10.15454/LBI3U7", "Nekazari"):
        assert needle in src["attribution_text"], needle
    assert {"en", "es"} <= set(src["processing_note"])


def test_attributions_listed_in_a_response():
    out = attach_attributions({}, [SID])["attributions"]
    assert [a["source_id"] for a in out] == [SID]


def test_yaml_registry_allows_commercial_use():
    from app.kg.registries import DEFAULT_REGISTRIES_PATH, load_registries
    src = load_registries(DEFAULT_REGISTRIES_PATH).source(SID)
    assert src.licence.commercial_use == "allowed"
    assert src.licence.licence_id == "etalab-2.0"
