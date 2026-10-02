from unittest.mock import AsyncMock, MagicMock, patch

from scripts import backfill_climate_cells as bf

CELL = {"koppen": "Cfb"}


def _parcel(lon, lat):
    return {"id": f"urn:ngsi-ld:AgriParcel:{lon}{lat}",
            "location": {"type": "Polygon",
                         "coordinates": [[[lon, lat], [lon + 1e-4, lat], [lon + 1e-4, lat + 1e-4], [lon, lat]]]}}


# 3 parcels: two share a cell (A), one in another cell (B, already cached).
PARCELS = [_parcel(-1.6458, 42.8125), _parcel(-1.6459, 42.8126), _parcel(-1.50, 41.20)]


def _orion():
    client = MagicMock()
    client.query_entities = AsyncMock(return_value=PARCELS)
    client.close = AsyncMock()
    return client


def _dao(cached_key):
    dao = MagicMock()
    dao.get_climate_cell = AsyncMock(side_effect=lambda k: CELL if k == cached_key else None)
    dao.parcel_climate = AsyncMock(return_value=CELL)
    return dao


def _keys():
    return [bf.cell_key(*bf.parcel_centroid(p)) for p in PARCELS]


async def test_computes_only_uncached_cells_with_wait(capsys):
    keys = _keys()
    assert keys[0] == keys[1] != keys[2]
    dao = _dao(cached_key=keys[2])
    with patch.object(bf, "OrionClient", return_value=_orion()):
        counts = await bf.backfill(dao, ["t1"], dry_run=False)
    assert dao.parcel_climate.await_count == 1
    lat, lon = dao.parcel_climate.await_args.args
    assert bf.cell_key(lat, lon) == keys[0]
    assert dao.parcel_climate.await_args.kwargs == {"wait": True, "timeout_s": bf.CELL_TIMEOUT_S}
    assert counts == {"parcels": 3, "cells": 2, "cached": 1, "to_compute": 1, "computed": 1, "failed": 0}


async def test_dry_run_computes_nothing_and_prints_counts(capsys):
    dao = _dao(cached_key=_keys()[2])
    with patch.object(bf, "OrionClient", return_value=_orion()):
        await bf.backfill(dao, ["t1"], dry_run=True)
    dao.parcel_climate.assert_not_awaited()
    assert "parcels=3 cells=2 cached=1 to_compute=1" in capsys.readouterr().out


async def test_orion_pagination_and_close():
    full = [_parcel(-2.0 + i * 0.1, 42.0) for i in range(bf.PAGE_SIZE)]
    client = MagicMock()
    client.query_entities = AsyncMock(side_effect=[full, [_parcel(-1.0, 41.0)]])
    client.close = AsyncMock()
    with patch.object(bf, "OrionClient", return_value=client):
        parcels, _ = await bf.collect_cells(["t1"])
    assert parcels == bf.PAGE_SIZE + 1
    assert [c.kwargs["offset"] for c in client.query_entities.await_args_list] == [0, bf.PAGE_SIZE]
    client.close.assert_awaited_once()


def test_parcel_centroid_normalized_and_missing():
    normalized = {"location": {"type": "GeoProperty", "value": PARCELS[0]["location"]}}
    assert bf.parcel_centroid(normalized) is not None
    assert bf.parcel_centroid({}) is None


async def test_collect_cells_does_not_log_tenant_ids(caplog):
    import logging

    with caplog.at_level(logging.INFO), patch.object(bf, "OrionClient", return_value=_orion()):
        await bf.collect_cells(["secret-tenant-a", "secret-tenant-b"])
    assert "secret-tenant" not in caplog.text
    assert "tenant_index=1" in caplog.text


async def test_dry_run_saves_nothing():
    dao = _dao(cached_key=_keys()[2])
    dao.save_climate_cell = AsyncMock()
    with patch.object(bf, "OrionClient", return_value=_orion()):
        await bf.backfill(dao, ["t1"], dry_run=True)
    dao.save_climate_cell.assert_not_awaited()
    dao.parcel_climate.assert_not_awaited()
