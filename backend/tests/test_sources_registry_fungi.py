import pytest

FUNGI_IDS = [
    "VISION2024", "REDALYC-PLEUROTUS-2017", "WAGENINGEN-FUNGAL-SUBSTRATES-2021",
    "NATURE-CORDYCEPS-2026", "HUNGARY-KING-OYSTER-2016", "EXCALIBUR-H2020",
]
REQUIRED = {"source_id", "name", "institution", "institution_short", "country",
            "country_name", "license_class", "use_type", "official_status",
            "data_format", "confidence_default", "attribution", "disclaimer"}


@pytest.mark.parametrize("sid", FUNGI_IDS)
def test_fungi_source_registered(sid):
    from app.common.source_registry import get_source
    src = get_source(sid)
    assert REQUIRED.issubset(src.keys()), f"{sid} missing {REQUIRED - set(src)}"
    assert isinstance(src["country_name"], dict) and "en" in src["country_name"]
    assert isinstance(src["attribution"], dict) and "en" in src["attribution"]
