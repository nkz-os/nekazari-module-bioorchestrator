"""Parcel crop cycles come from the broker reader of the platform SDK, same payload as before."""
from unittest.mock import AsyncMock

from app.services import crop_cycles_client as cc

URN = "urn:ngsi-ld:AgriParcel:t:p1"


async def test_cycles_come_from_the_broker_reader(monkeypatch):
    timeline = {"parcel_id": URN, "current": None, "previous": None, "cycles": []}
    reader = AsyncMock(return_value=timeline)
    monkeypatch.setattr(cc, "fetch_crop_timeline", reader)
    assert await cc.fetch_crop_cycles(URN, "t") == timeline
    reader.assert_awaited_once_with("t", URN)


async def test_a_broker_failure_is_no_answer(monkeypatch):
    monkeypatch.setattr(cc, "fetch_crop_timeline", AsyncMock(return_value=None))
    assert await cc.fetch_crop_cycles(URN, "t") is None


async def test_an_unexpected_reader_error_is_no_answer_and_is_logged(monkeypatch, caplog):
    monkeypatch.setattr(cc, "fetch_crop_timeline", AsyncMock(side_effect=ValueError("bad urn")))
    assert await cc.fetch_crop_cycles(URN, "t") is None
    assert "crop timeline failed" in caplog.text
