import os

import pytest

BUNDLE = os.path.join(os.path.dirname(__file__), "fixtures", "vision_2024_fungi.jsonld")


@pytest.mark.asyncio
async def test_dry_run_passes_gate_and_transforms(caplog):
    from scripts.ingest_fungi import run
    caplog.set_level("INFO")
    code = await run(BUNDLE, execute=False)
    assert code == 0
    assert "Gate: PASS" in caplog.text
    assert "DRY RUN" in caplog.text


class _FakeResult:
    def __init__(self, c):
        self._c = c

    async def single(self):
        return {"c": self._c}


class _FakeSession:
    def __init__(self, c):
        self._c = c
        self.queries = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def run(self, query, **params):
        self.queries.append((query, params))
        return _FakeResult(self._c)


class _FakeDriver:
    def __init__(self, c):
        self._c = c
        self.session_obj = _FakeSession(c)

    def session(self):
        return self.session_obj


@pytest.mark.asyncio
async def test_unlinked_mt_with_location_counts():
    # Visibility helper: counts ManagementTrials that carry a location but did
    # not link. Scoped to the batch source_ids; filters on trialLocation IS NOT NULL.
    from scripts.ingest_fungi import _unlinked_mt_with_location
    driver = _FakeDriver(2)
    n = await _unlinked_mt_with_location(driver, ["EXCALIBUR-H2020"])
    assert n == 2
    query, params = driver.session_obj.queries[0]
    assert "ManagementTrial" in query
    assert "m.trialLocation IS NOT NULL" in query
    assert "n.source_id IN $sids" not in query  # uses m., not n.
    assert params["sids"] == ["EXCALIBUR-H2020"]
