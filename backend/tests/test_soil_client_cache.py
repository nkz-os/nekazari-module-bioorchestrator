"""The 24 h soil cache: a fresh read skips it, but only a success may replace an entry."""
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from app.services import soil_client

SUMMARY = {"horizons": [{"usdaTextureClass": "loam", "ph": 6.5, "depthFrom": 0, "depthTo": 30}],
           "dataSource": {"value": "soilgrids"}}


def _client(status: int, body: dict | None = None):
    resp = MagicMock(status_code=status)
    resp.json.return_value = body or {}
    http = MagicMock()
    http.get = AsyncMock(return_value=resp)
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=http)
    cm.__aexit__ = AsyncMock(return_value=False)
    return cm, http


@pytest.fixture(autouse=True)
def _empty_cache():
    soil_client._soil_cache.clear()
    yield
    soil_client._soil_cache.clear()


async def test_cached_read_does_not_call_the_soil_module_again():
    cm, http = _client(200, SUMMARY)
    with patch.object(httpx, "AsyncClient", return_value=cm):
        first = await soil_client.get_parcel_soil_properties("p1", "tenant-a")
        again = await soil_client.get_parcel_soil_properties("p1", "tenant-a")
    assert first["texture"] == again["texture"] == "loam" and http.get.await_count == 1


async def test_fresh_read_skips_the_cache_and_stores_the_success():
    cm, http = _client(200, SUMMARY)
    with patch.object(httpx, "AsyncClient", return_value=cm):
        await soil_client.get_parcel_soil_properties("p1", "tenant-a")
        await soil_client.get_parcel_soil_properties("p1", "tenant-a", fresh=True)
    assert http.get.await_count == 2


@pytest.mark.parametrize("status", [404, 500])
async def test_fresh_failure_keeps_the_cached_entry(status):
    good, _ = _client(200, SUMMARY)
    with patch.object(httpx, "AsyncClient", return_value=good):
        await soil_client.get_parcel_soil_properties("p1", "tenant-a")
    bad, _ = _client(status)
    with patch.object(httpx, "AsyncClient", return_value=bad):
        failed = await soil_client.get_parcel_soil_properties("p1", "tenant-a", fresh=True)
    assert failed["data_available"] is False
    assert (await soil_client.get_parcel_soil_properties("p1", "tenant-a"))["texture"] == "loam"


async def test_texture_falls_back_to_the_summary_topsoil_class():
    no_class = {"horizons": [{"ph": 6.5, "depthFrom": 0, "depthTo": 30}],
                "topsoilTexture": {"usdaTextureClass": "clay_loam", "source": "entered"},
                "dataSource": {"value": "soilgrids"}}
    cm, _ = _client(200, no_class)
    with patch.object(httpx, "AsyncClient", return_value=cm):
        got = await soil_client.get_parcel_soil_properties("p1", "tenant-a")
    assert got["texture"] == "clay_loam"


async def test_a_horizon_class_wins_over_the_summary_topsoil_class():
    cm, _ = _client(200, {**SUMMARY, "topsoilTexture": {"usdaTextureClass": "clay", "source": "entered"}})
    with patch.object(httpx, "AsyncClient", return_value=cm):
        got = await soil_client.get_parcel_soil_properties("p1", "tenant-a")
    assert got["texture"] == "loam"
