"""CHELSA unit conversions, cell grid and reader orchestration (no network)."""
import asyncio

import pytest

from app.services import chelsa_climate as cc


def test_cell_key_and_center_stable_within_cell():
    a, b = cc.cell_key(42.8125, -1.6458), cc.cell_key(42.8126, -1.6457)
    assert a == b
    lat, lon = cc.cell_center(42.8125, -1.6458)
    assert cc.cell_key(lat, lon) == a


def test_cell_key_negative_coords_floor():
    assert cc.cell_key(-0.001, -0.001) == "-1:-1"


@pytest.mark.parametrize("name,raw,expected", [
    ("tas_01", 2781, 4.95), ("pr_04", 920, 92.0), ("bio06", 2740, 0.85), ("petmean", 7960, 955.2),
])
def test_convert(name, raw, expected):
    assert cc.convert(name, raw) == pytest.approx(expected, abs=0.01)


@pytest.mark.parametrize("raw", [None, cc.NODATA])
def test_convert_missing(raw):
    assert cc.convert("tas_01", raw) is None


def test_layer_paths_complete():
    paths = cc.layer_paths()
    assert len(paths) == 26
    assert paths["tas_07"] == "climatologies/tas/1981-2010/CHELSA_tas_07_1981-2010_V.2.1.tif"
    assert paths["petmean"] == "bioclim/petmean/1981-2010/CHELSA_petmean_1981-2010_V.2.1.tif"


def _fake_sampler(table):
    def sampler(path, lon, lat):
        return table(path)
    return sampler


OCEANIC_TAS_RAW = [2781, 2788, 2816, 2835, 2872, 2911, 2936, 2936, 2906, 2868, 2818, 2790]
OCEANIC_PR_RAW = [770, 650, 660, 920, 810, 580, 390, 330, 480, 810, 930, 940]


def _oceanic(path):
    name = path.split("CHELSA_")[1].split("_1981")[0]
    if name.startswith("tas_"):
        return OCEANIC_TAS_RAW[int(name[4:]) - 1]
    if name.startswith("pr_"):
        return OCEANIC_PR_RAW[int(name[3:]) - 1]
    return {"bio06": 2740, "petmean": 7960}[name]


async def test_read_cell_summarizes():
    out = await cc.read_cell(42.81, -1.65, sampler=_fake_sampler(_oceanic))
    assert out["koppen"] == "Cfb"
    assert out["annual_rainfall_mm"] == pytest.approx(828.0, abs=1)
    assert out["annual_temp_c"] == pytest.approx(12.3, abs=0.1)
    assert out["annual_et0_mm"] == pytest.approx(955.2, abs=0.1)
    assert out["coldest_month_min_c"] == pytest.approx(0.85, abs=0.01)
    assert len(out["monthly_tas_c"]) == 12 and out["source"] == "CHELSA v2.1 1981-2010"


async def test_nodata_cell_is_none():
    assert await cc.read_cell(0.0, -30.0, sampler=_fake_sampler(lambda p: None)) is None


async def test_timeout_returns_none():
    import time

    def slow(path, lon, lat):
        time.sleep(0.5)
        return 1
    assert await cc.read_cell(42.6, -2.0, sampler=slow, timeout_s=0.1) is None


async def test_sampler_error_returns_none():
    def boom(path, lon, lat):
        raise OSError("http 503")
    assert await cc.read_cell(42.6, -2.0, sampler=boom) is None


async def test_event_loop_not_blocked():
    import time

    def slow(path, lon, lat):
        time.sleep(0.2)
    ticks = 0

    async def ticker():
        nonlocal ticks
        for _ in range(10):
            await asyncio.sleep(0.01)
            ticks += 1
    await asyncio.gather(cc.read_cell(1.0, 1.0, sampler=slow, timeout_s=2), ticker())
    assert ticks == 10


@pytest.mark.network
async def test_live_reference_cell():
    out = await cc.read_cell(42.8125, -1.6458)
    assert out["koppen"] == "Cfb"
    assert sum(out["monthly_pr_mm"]) == pytest.approx(out["annual_rainfall_mm"])
    assert out["annual_rainfall_mm"] == pytest.approx(859, rel=0.02)


class _FakeBounds:
    left, bottom, right, top = -10.0, 35.0, 5.0, 45.0


class _FakeDs:
    bounds = _FakeBounds()

    def sample(self, pts):
        yield [0]  # rasterio fill value outside the raster


def test_sampler_returns_none_outside_bounds():
    assert cc._sample_dataset(_FakeDs(), 100.0, 10.0) is None


def test_sampler_reads_inside_bounds():
    class Ds(_FakeDs):
        def sample(self, pts):
            yield [2800]

    assert cc._sample_dataset(Ds(), -2.0, 42.0) == 2800
