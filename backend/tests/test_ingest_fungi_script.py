import pytest

BUNDLE = "/home/g/Documents/nekazari/nkz-setas-scraper/data/jsonld/vision_2024_fungi.jsonld"


@pytest.mark.asyncio
async def test_dry_run_passes_gate_and_transforms(caplog):
    from scripts.ingest_fungi import run
    caplog.set_level("INFO")
    code = await run(BUNDLE, execute=False)
    assert code == 0
    assert "Gate: PASS" in caplog.text
    assert "DRY RUN" in caplog.text
