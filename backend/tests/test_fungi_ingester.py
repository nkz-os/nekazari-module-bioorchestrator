import json
import pytest

BUNDLE = "/home/g/Documents/nekazari/nkz-setas-scraper/data/jsonld/vision_2024_fungi.jsonld"


@pytest.fixture
async def nodes():
    from app.ingestion.fungi_ingester import FungiIngester
    return await FungiIngester(driver=None).transform(BUNDLE)


@pytest.mark.asyncio
async def test_partition_counts(nodes):
    assert len(nodes["variety_trials"]) == 13
    assert len(nodes["management_trials"]) == 2
    assert len(nodes["trial_sites"]) == 4
    assert len(nodes["article_sources"]) == 5


@pytest.mark.asyncio
async def test_real_source_id_preserved(nodes):
    sids = {vt["source_id"] for vt in nodes["variety_trials"]}
    assert "REDALYC-PLEUROTUS-2017" in sids
    assert "WAGENINGEN-FUNGAL-SUBSTRATES-2021" in sids
    assert all(vt["source_id"] != "VISION2024" for vt in nodes["variety_trials"])


@pytest.mark.asyncio
async def test_quality_params_serialized_with_yield_type(nodes):
    for vt in nodes["variety_trials"]:
        assert isinstance(vt["qualityParams"], str)
        parsed = json.loads(vt["qualityParams"])
        assert "yieldDataType" in parsed


@pytest.mark.asyncio
async def test_defensive_yield_off_for_non_areal(nodes):
    # every VT in this bundle is BE/colonization/bioactive → never areal
    for vt in nodes["variety_trials"]:
        assert vt["yieldKgHa"] is None
        assert vt["rankingEligible"] is False


@pytest.mark.asyncio
async def test_management_trial_shape(nodes):
    mt = nodes["management_trials"][0]
    assert mt["source_id"] == "EXCALIBUR-H2020"
    assert mt["experimentType"] == "biological_inoculation"
    assert "Trichoderma" in mt["treatment"] or "AMF" in mt["treatment"] or "mycorrhiz" in mt["treatment"].lower()
    assert mt["cropScientific"] == "Solanum lycopersicum"
    assert isinstance(mt["qualityParams"], str)
