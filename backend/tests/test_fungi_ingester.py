import json
import os

import pytest

BUNDLE = os.path.join(os.path.dirname(__file__), "fixtures", "vision_2024_fungi.jsonld")


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


@pytest.mark.asyncio
async def test_article_provenance_per_source(nodes):
    by_id = {a["source_id"]: a for a in nodes["article_sources"]}
    assert "REDALYC-PLEUROTUS-2017" in by_id
    redalyc = by_id["REDALYC-PLEUROTUS-2017"]
    assert "Cuyo" in redalyc["institution"]
    assert redalyc["license_class"] == "open-access-cc"
    # umbrella must NOT leak onto real articles
    assert all(a["source_id"] != "VISION2024" for a in nodes["article_sources"])


class _FakeResult:
    async def single(self):
        return {"c": 0}


class _FakeSession:
    def __init__(self, sink):
        self._sink = sink

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def run(self, query, **params):
        self._sink.append((query, params))
        return _FakeResult()


class _FakeDriver:
    def __init__(self):
        self.calls = []

    def session(self):
        return _FakeSession(self.calls)


@pytest.mark.asyncio
async def test_mt_merge_persists_quality_and_host():
    from app.ingestion.fungi_ingester import FungiIngester
    ing = FungiIngester(driver=None)
    driver = _FakeDriver()
    trials = [{
        "source_id": "EXCALIBUR-H2020",
        "cropScientific": "Solanum lycopersicum",
        "treatment": "Inoculation: Trichoderma sp.",
        "experimentType": "biological_inoculation",
        "resultMetric": "soil_fungal_population_dominance",
        "resultValue": 100, "resultUnit": "pct_colonization",
        "qualityParams": '{"applicationMethod": "soil"}',
        "mergeKey": "EXCALIBUR-H2020|mt|trichoderma|2021",
    }]
    count = await ing._merge_management_trials(driver, trials)
    assert count == 1
    query, params = driver.calls[0]
    assert "mt.qualityParams" in query
    assert "mt.cropScientific" in query
    assert "mt.unitQudtUri" in query
    assert params["quality"] == '{"applicationMethod": "soil"}'
    assert params["host"] == "Solanum lycopersicum"
    assert "qudt_uri" in params


@pytest.mark.asyncio
async def test_management_fallback_key_disambiguates_by_metric():
    # Two MTs with identical treatment+year but different result_metric and NO
    # explicit mergeKey must NOT collapse into one node (silent overwrite).
    from app.ingestion.fungi_ingester import FungiIngester
    ing = FungiIngester(driver=None)
    a = ing._convert_management({
        "source_id": "EXCALIBUR-H2020", "treatment": "Trichoderma sp.",
        "result_metric": "soil_fungal_population_dominance", "result_value": 100, "year": 2021,
    })
    b = ing._convert_management({
        "source_id": "EXCALIBUR-H2020", "treatment": "Trichoderma sp.",
        "result_metric": "yield_increase_pct", "result_value": 15, "year": 2021,
    })
    assert a["mergeKey"] != b["mergeKey"]


@pytest.mark.asyncio
async def test_management_explicit_mergekey_wins():
    # An explicit mergeKey from the bundle is preserved verbatim (fallback unused).
    from app.ingestion.fungi_ingester import FungiIngester
    ing = FungiIngester(driver=None)
    mt = ing._convert_management({
        "source_id": "EXCALIBUR-H2020", "treatment": "Trichoderma sp.",
        "result_metric": "yield_increase_pct", "result_value": 15, "year": 2021,
        "mergeKey": "explicit_key_2021",
    })
    assert mt["mergeKey"] == "explicit_key_2021"


@pytest.mark.asyncio
async def test_relationships_source_set_aware():
    from app.ingestion.fungi_ingester import FungiIngester
    ing = FungiIngester(driver=None)
    driver = _FakeDriver()
    vts = [
        {"source_id": "REDALYC-PLEUROTUS-2017", "trialLocation": "Guaranda", "trialLocationKey": "guaranda"},
        {"source_id": "WAGENINGEN-FUNGAL-SUBSTRATES-2021", "trialLocation": "Wageningen", "trialLocationKey": "wageningen"},
    ]
    await ing._merge_relationships(driver, vts, [])
    query, params = driver.calls[0]
    assert "n.source_id IN $sids" in query
    assert set(params["sids"]) == {"REDALYC-PLEUROTUS-2017", "WAGENINGEN-FUNGAL-SUBSTRATES-2021"}
    assert "VISION2024" not in params["sids"]
