import asyncio
import os
from app.ingestion.almond_ifapa_ingester import AlmondIfapaIngester

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "ifapa_almond_lastorres.jsonld")


def test_transform_las_torres():
    ing = AlmondIfapaIngester()
    # asyncio.run() (not get_event_loop()) — avoids a closed/stale loop left
    # behind by prior pytest-asyncio async tests in the same session, which
    # made this test order-dependent (RuntimeError: Event loop is closed).
    nodes = asyncio.run(ing.transform(FIX))
    assert len(nodes["variety_trials"]) == 3
    assert len(nodes["trial_sites"]) == 1
    # source_id alignment (prevents orphan TRIAL_AT)
    assert nodes["trial_sites"][0]["source_id"] == "IFAPA_ALMOND"
    vt = [t for t in nodes["variety_trials"] if t["variety"] == "Guara" and t["rootstock"] == "Garnem"][0]
    assert vt["cropEppo"] == "PRNDU"
    assert vt["yieldKgHa"] == 2100
    assert vt["plantingYear"] == 2013
    # note-only record carries the relative note, NOT a fabricated kg/ha
    note = [t for t in nodes["variety_trials"] if t["variety"] == "Lauranne"][0]
    assert note.get("yieldKgHa") is None
    assert note.get("yieldNoteS1") == 7
