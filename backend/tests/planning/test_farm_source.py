from unittest.mock import AsyncMock

import pytest

from app.planning import farm_source
from app.planning.farm import farm_payload


def _patch_orion(monkeypatch, data, calls=None):
    client = AsyncMock()

    async def query(type=None, q=None, limit=100, offset=0, attrs=None, options=None):
        if calls is not None:
            calls.append({"type": type, "q": q, "limit": limit, "offset": offset, "options": options})
        return data[type] if offset == 0 else []
    client.query_entities.side_effect = query
    monkeypatch.setattr(farm_source, "OrionClient", lambda tenant_id: client)
    return client


async def test_load_farm_pages_orion_and_fetches_cycles_per_parcel(monkeypatch):
    client = _patch_orion(monkeypatch, {"AgriParcel": [{"id": "p1"}, {"id": "p2"}], "AgriFarm": [],
                                        "AgriCrop": [{"id": "c1"}]})
    cycles = AsyncMock(side_effect=lambda urn, tenant: {"cycles": []} if urn == "p1" else None)
    monkeypatch.setattr(farm_source, "fetch_crop_cycles", cycles)
    parcels, _farms, crops, by_parcel = await farm_source.load_farm("t")
    assert [p["id"] for p in parcels] == ["p1", "p2"] and crops == {"c1": {"id": "c1"}}
    assert by_parcel == {"p1": {"cycles": []}, "p2": None}
    client.close.assert_awaited()


async def test_load_farm_for_one_parcel_reads_it_and_only_its_crops(monkeypatch):
    calls: list[dict] = []
    client = _patch_orion(monkeypatch, {"AgriFarm": [], "AgriCrop": [{"id": "c1"}]}, calls)
    client.get_entity.return_value = {"id": "urn:ngsi-ld:AgriParcel:t:p1"}
    monkeypatch.setattr(farm_source, "fetch_crop_cycles", AsyncMock(return_value=None))
    parcels, _farms, crops, by_parcel = await farm_source.load_farm("t", "urn:ngsi-ld:AgriParcel:t:p1")
    client.get_entity.assert_awaited_once_with("urn:ngsi-ld:AgriParcel:t:p1", options="keyValues")
    assert [p["id"] for p in parcels] == ["urn:ngsi-ld:AgriParcel:t:p1"] and list(crops) == ["c1"]
    assert by_parcel == {"urn:ngsi-ld:AgriParcel:t:p1": None}
    crop_call = next(c for c in calls if c["type"] == "AgriCrop")
    assert crop_call["q"] == 'hasAgriParcel=="urn:ngsi-ld:AgriParcel:t:p1"'
    assert all(c["type"] != "AgriParcel" for c in calls)


async def test_load_farm_closes_the_client_when_orion_fails(monkeypatch):
    client = AsyncMock()
    client.get_entity.side_effect = RuntimeError("orion down")
    monkeypatch.setattr(farm_source, "OrionClient", lambda tenant_id: client)
    with pytest.raises(RuntimeError, match="orion down"):
        await farm_source.load_farm("t", "urn:ngsi-ld:AgriParcel:t:p1")
    client.close.assert_awaited()


@pytest.mark.parametrize("bad_answer", ["raises", "list", "string"])
async def test_one_bad_cycles_answer_only_takes_that_parcel_out(monkeypatch, caplog, bad_answer):
    _patch_orion(monkeypatch, {"AgriParcel": [{"id": "p1", "irrigationRegime": "secano"}, {"id": "p2"}],
                               "AgriFarm": [], "AgriCrop": []})

    async def fetch(urn, tenant):
        if urn == "p2":
            if bad_answer == "raises":
                raise ValueError("malformed")
            return [] if bad_answer == "list" else "oops"
        return {"cycles": []}
    monkeypatch.setattr(farm_source, "fetch_crop_cycles", fetch)
    with caplog.at_level("WARNING", logger=farm_source.logger.name):
        parcels, farms, crops, by_parcel = await farm_source.load_farm("t")
    assert by_parcel == {"p1": {"cycles": []}, "p2": None}
    assert any("tenant=t" in r.getMessage() and "parcel=p2" in r.getMessage() for r in caplog.records)
    rows = farm_payload(parcels, farms, crops, by_parcel, 2027, [])["parcels"]
    assert [r["cycles_available"] for r in rows] == [True, False]


async def test_listing_reads_every_page(monkeypatch):
    monkeypatch.setattr(farm_source, "PAGE", 2)
    pages = {0: [{"id": "a"}, {"id": "b"}], 2: [{"id": "c"}]}

    async def query(type=None, q=None, limit=100, offset=0, attrs=None, options=None):
        return pages.get(offset, [])
    client = AsyncMock()
    client.query_entities.side_effect = query
    assert [e["id"] for e in await farm_source._all(client, "AgriParcel")] == ["a", "b", "c"]
