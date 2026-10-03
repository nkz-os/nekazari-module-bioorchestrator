"""Tests for GET /agriculture/parcel-environment endpoint."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.graph.dao import GraphDAO
from app.services.chelsa_climate import cell_key
from neo4j import AsyncDriver


class _MockResult:
    """Minimal mock result with async iterator and single() support."""

    def __init__(self, records: list[dict] | None = None):
        self._records = records or []

    def __aiter__(self):
        self._idx = 0
        return self

    async def __anext__(self):
        if self._idx >= len(self._records):
            raise StopAsyncIteration
        rec = self._records[self._idx]
        self._idx += 1
        return self._make_record(rec)

    def _make_record(self, rec: dict):
        m = MagicMock()
        m.__getitem__ = lambda s, k: rec.get(k)
        m.get = rec.get
        m.keys = lambda: rec.keys()
        return m

    async def single(self):
        if not self._records:
            return None
        return self._make_record(self._records[0])

    async def data(self):
        return [self._make_record(r) for r in self._records]


def _make_driver(records: list[dict] | None = None):
    """Build a mock AsyncDriver whose session.run() yields _MockResult records."""
    driver = MagicMock(spec=AsyncDriver)
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    session.run = AsyncMock(return_value=_MockResult(records))
    driver.session.return_value = session
    return driver


@pytest.fixture(autouse=True)
def _no_chelsa_network():
    """Keep tests off the CHELSA network path; tests that need a cell patch it themselves."""
    with patch.object(GraphDAO, "parcel_climate", AsyncMock(return_value=None)):
        yield


class TestParcelEnvironment:
    """Verify the DAO method resolves parcel profile without assigned crop."""

    @pytest.mark.asyncio
    async def test_returns_profile_when_no_crop_assigned(self):
        """Core spec requirement: must NOT require hasAgriCrop."""
        with patch("app.graph.dao.OrionClient") as mock_orion_cls, \
             patch("app.services.soil_client.get_parcel_soil_properties") as mock_soil, \
             patch.object(GraphDAO, "parcel_climate", AsyncMock(return_value=None)):

            mock_orion = AsyncMock()
            mock_orion_cls.return_value = mock_orion
            mock_orion.get_entity.return_value = {
                "id": "urn:ngsi-ld:AgriParcel:test-42",
                "type": "AgriParcel",
                "location": {
                    "type": "GeoProperty",
                    "value": {
                        "type": "Point",
                        "coordinates": [-1.8, 42.1],
                    },
                },
                "area": {"type": "Property", "value": 12.4},
            }
            mock_orion.close = AsyncMock()

            mock_soil.return_value = {
                "ph": 7.2,
                "texture": "loam",
                "awc_mm": 120,
                "data_available": True,
                "source": "soilgrids",
            }

            driver = _make_driver([])  # no trial sites nearby
            dao = GraphDAO(driver)
            result = await dao.get_parcel_environment("urn:ngsi-ld:AgriParcel:test-42")

            assert result["parcel_id"] == "urn:ngsi-ld:AgriParcel:test-42"
            assert result["area_ha"] == 12.4
            assert result["centroid"] == {"lat": 42.1, "lon": -1.8}
            assert result["campaign"]["assigned"] is False
            assert result["soil"]["data_available"] is True
            assert result["soil"]["texture"] == "loam"
            assert result["inputs_used"]["soil"] == "soil_module"
            assert isinstance(result["irrigation"], dict)
            assert result["irrigation"]["overridable"] is True

    @pytest.mark.asyncio
    async def test_handles_missing_parcel(self):
        """Should return error dict when parcel not found in Orion."""
        import httpx

        from app.graph.dao import GraphDAO

        with patch("app.graph.dao.OrionClient") as mock_orion_cls:
            mock_orion = AsyncMock()
            mock_orion_cls.return_value = mock_orion
            mock_orion.get_entity.side_effect = httpx.HTTPStatusError(
                "404",
                request=MagicMock(),
                response=MagicMock(status_code=404),
            )
            mock_orion.close = AsyncMock()

            driver = _make_driver()
            dao = GraphDAO(driver)
            result = await dao.get_parcel_environment("urn:ngsi-ld:AgriParcel:missing")

            assert "error" in result
            assert "not found" in result["error"].lower() or "missing" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_irrigation_from_system_type(self):
        """irrigationSystemType should map to secano/regadío."""
        with patch("app.graph.dao.OrionClient") as mock_orion_cls, \
             patch("app.services.soil_client.get_parcel_soil_properties") as mock_soil:

            mock_orion = AsyncMock()
            mock_orion_cls.return_value = mock_orion
            mock_orion.get_entity.return_value = {
                "id": "urn:ngsi-ld:AgriParcel:irr-1",
                "type": "AgriParcel",
                "location": {
                    "type": "GeoProperty",
                    "value": {"type": "Point", "coordinates": [-1.8, 42.1]},
                },
                "irrigationSystemType": {"type": "Property", "value": "drip_irrigation"},
            }
            mock_orion.close = AsyncMock()
            mock_soil.return_value = {"data_available": False, "source": "unavailable"}

            driver = _make_driver([])
            dao = GraphDAO(driver)
            result = await dao.get_parcel_environment("urn:ngsi-ld:AgriParcel:irr-1")

            assert result["irrigation"]["inferred"] == "regadío"
            assert result["irrigation"]["source"] == "irrigationSystemType"

    @pytest.mark.asyncio
    async def test_climate_from_nearest_trial_site(self):
        """Should resolve Köppen class from nearest TrialSite."""
        with patch("app.graph.dao.OrionClient") as mock_orion_cls, \
             patch("app.services.soil_client.get_parcel_soil_properties") as mock_soil, \
             patch.object(GraphDAO, "parcel_climate", AsyncMock(return_value=None)):

            mock_orion = AsyncMock()
            mock_orion_cls.return_value = mock_orion
            mock_orion.get_entity.return_value = {
                "id": "urn:ngsi-ld:AgriParcel:clim-1",
                "type": "AgriParcel",
                "location": {
                    "type": "GeoProperty",
                    "value": {"type": "Point", "coordinates": [-1.8, 42.1]},
                },
            }
            mock_orion.close = AsyncMock()
            mock_soil.return_value = {"data_available": False, "source": "unavailable"}

            # Nearby site within 50km: return Csa
            driver = _make_driver([
                {"cc": "Csa", "tlat": 42.2, "tlon": -1.9},
            ])
            dao = GraphDAO(driver)
            result = await dao.get_parcel_environment("urn:ngsi-ld:AgriParcel:clim-1")

            assert result["climate_class"] == "Csa"
            assert result["inputs_used"]["climate"] == "trial_proxy"


def test_polygon_parcel_terminates_and_yields_centroid():
    """A Polygon parcel used to spin forever in the centroid loop, freezing the server."""
    import asyncio
    import threading

    parcel = {
        "id": "urn:ngsi-ld:AgriParcel:poly-1",
        "type": "AgriParcel",
        "location": {
            "type": "GeoProperty",
            "value": {
                "type": "Polygon",
                "coordinates": [[[-2.0, 42.6], [-1.9, 42.6], [-1.9, 42.7], [-2.0, 42.7], [-2.0, 42.6]]],
            },
        },
    }
    outcome: dict = {}

    def run():
        with patch("app.graph.dao.OrionClient") as mock_orion_cls, \
             patch("app.services.soil_client.get_parcel_soil_properties",
                   AsyncMock(return_value={"data_available": False})):
            mock_orion = AsyncMock()
            mock_orion.get_entity.return_value = parcel
            mock_orion_cls.return_value = mock_orion
            dao = GraphDAO(_make_driver([]))
            outcome["result"] = asyncio.run(dao.get_parcel_environment("urn:ngsi-ld:AgriParcel:poly-1"))

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    worker.join(timeout=5)
    assert not worker.is_alive(), "get_parcel_environment did not terminate for a Polygon parcel"
    centroid = outcome["result"]["centroid"]
    assert centroid["lon"] == pytest.approx(-1.95)
    assert centroid["lat"] == pytest.approx(42.65)


# ── CHELSA climate wiring ─────────────────────────────────────────────────────

_POINT_PARCEL = {
    "id": "urn:ngsi-ld:AgriParcel:p1", "type": "AgriParcel",
    "location": {"type": "GeoProperty", "value": {"type": "Point", "coordinates": [-1.6458, 42.8125]}},
}


async def _env_with(cell, records, *, side_effect=None, flag="1", driver=None, mock=None):
    if mock is None:
        mock = AsyncMock(return_value=cell) if side_effect is None else AsyncMock(side_effect=side_effect)
    env = {"CHELSA_PARCEL_CLIMATE_ENABLED": flag} if flag is not None else {}
    with patch.dict("os.environ", env), \
         patch("app.graph.dao.OrionClient") as orion_cls, \
         patch("app.services.soil_client.get_parcel_soil_properties", AsyncMock(return_value={"data_available": False})), \
         patch.object(GraphDAO, "parcel_climate", mock):
        orion = AsyncMock()
        orion.get_entity.return_value = _POINT_PARCEL
        orion_cls.return_value = orion
        dao = GraphDAO(driver or _make_driver(records))
        return await dao.get_parcel_environment("urn:ngsi-ld:AgriParcel:p1", "tenant-a")


@pytest.mark.asyncio
async def test_chelsa_climate_preferred():
    cell = {"koppen": "Cfb", "annual_temp_c": 12.3, "annual_rainfall_mm": 828.0, "annual_et0_mm": 955.2,
            "coldest_month_min_c": 0.85, "monthly_tas_c": [5.0] * 12, "monthly_pr_mm": [69.0] * 12,
            "source": "CHELSA v2.1 1981-2010"}
    env = await _env_with(cell, [])
    assert env["climate_class"] == "Cfb"
    assert env["inputs_used"]["climate"] == "chelsa_v2.1"
    assert env["climate_detail"]["annual_et0_mm"] == 955.2
    assert env["climate_detail"]["coldest_month_min_c"] == 0.85
    assert env["climate_detail"]["source"] == "chelsa_v2.1"
    assert env["climate_detail"]["frost_days_per_year"] is None
    assert env["climate_detail"]["cell"] == cell_key(42.8125, -1.6458)


@pytest.mark.asyncio
async def test_chelsa_timeout_falls_back():
    site = {"cc": "Cfb", "tlat": 42.815, "tlon": -1.65, "name": "site-a",
            "rain": 650.0, "et0": 900.0, "frost": 30}
    env = await _env_with(None, [site])
    assert env["climate_class"] == "Cfb"
    assert env["inputs_used"]["climate"] == "trial_proxy"
    detail = env["climate_detail"]
    assert detail["source"] == "trial_proxy" and detail["site"] == "site-a"
    assert detail["annual_rainfall_mm"] == 650.0 and detail["annual_et0_mm"] == 900.0
    assert detail["frost_days_per_year"] == 30 and detail["distance_km"] < 5


@pytest.mark.asyncio
async def test_chelsa_exception_falls_back():
    site = {"cc": "Cfb", "tlat": 42.815, "tlon": -1.65, "name": "site-a",
            "rain": 650.0, "et0": 900.0, "frost": 30}
    env = await _env_with(None, [site], side_effect=RuntimeError("boom"))
    assert env["inputs_used"]["climate"] == "trial_proxy"


@pytest.mark.asyncio
async def test_no_climate_anywhere():
    env = await _env_with(None, [])
    assert env["climate_class"] is None
    assert env["climate_detail"] is None
    assert env["inputs_used"]["climate"] == "unavailable"


def test_era5_no_longer_called():
    import inspect

    from app.graph import dao
    assert "ERA5ClimateConnector" not in inspect.getsource(dao.GraphDAO.get_parcel_environment)


# ── Feature flag (CHELSA_PARCEL_CLIMATE_ENABLED) ──────────────────────────────

@pytest.mark.asyncio
@pytest.mark.parametrize("flag", [None, "", "0", "false", "no", "off", "2"])
async def test_flag_off_skips_parcel_climate(flag):
    mock = AsyncMock(return_value={"koppen": "Cfb"})
    env = await _env_with(None, [], flag=flag, mock=mock)
    mock.assert_not_awaited()
    assert env["inputs_used"]["climate"] == "unavailable"


@pytest.mark.asyncio
@pytest.mark.parametrize("flag", ["1", "true", "TRUE", "Yes"])
async def test_flag_on_awaits_parcel_climate(flag):
    mock = AsyncMock(return_value=None)
    await _env_with(None, [], flag=flag, mock=mock)
    mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_flag_off_still_uses_trial_site_fallback():
    site = {"cc": "Cfb", "tlat": 42.815, "tlon": -1.65, "name": "site-a",
            "rain": 650.0, "et0": 900.0, "frost": 30}
    env = await _env_with(None, [site], flag=None, mock=AsyncMock())
    assert env["inputs_used"]["climate"] == "trial_proxy"


# ── Trial-site fallback prefers CHELSA site climate ───────────────────────────

@pytest.mark.asyncio
async def test_fallback_query_coalesces_chelsa_properties():
    driver = _make_driver([])
    await _env_with(None, [], flag=None, driver=driver, mock=AsyncMock())
    queries = [c.args[0] for c in driver.session.return_value.run.await_args_list]
    q = next(x for x in queries if "MATCH (ts:TrialSite)" in x and "tlat" in x)
    assert "coalesce(ts.climateClassChelsa, ts.climateClass) IS NOT NULL" in q
    assert "coalesce(ts.climateClassChelsa, ts.climateClass) AS cc" in q
    assert "coalesce(ts.annualRainfallMmChelsa, ts.annualRainfallMm) AS rain" in q
    assert "coalesce(ts.annualET0MmChelsa, ts.annualET0Mm) AS et0" in q
    assert "ts.frostDaysPerYear AS frost" in q


# ── Country from the centroid ─────────────────────────────────────────────────

async def _env_for_parcel(parcel):
    with patch("app.graph.dao.OrionClient") as orion_cls, \
         patch("app.services.soil_client.get_parcel_soil_properties", AsyncMock(return_value={"data_available": False})):
        orion = AsyncMock()
        orion.get_entity.return_value = parcel
        orion_cls.return_value = orion
        return await GraphDAO(_make_driver([])).get_parcel_environment("urn:ngsi-ld:AgriParcel:c1", "tenant-a")


@pytest.mark.asyncio
@pytest.mark.parametrize("lon,lat,iso2", [(-3.7038, 40.4168, "ES"), (2.3522, 48.8566, "FR"),
                                          (-5.0, 45.5, None)])  # Madrid, Paris, Bay of Biscay
async def test_country_from_centroid(lon, lat, iso2):
    parcel = {"id": "urn:ngsi-ld:AgriParcel:c1", "type": "AgriParcel",
              "location": {"type": "GeoProperty", "value": {"type": "Point", "coordinates": [lon, lat]}}}
    env = await _env_for_parcel(parcel)
    assert env["country"] == iso2


@pytest.mark.asyncio
async def test_country_none_without_location():
    env = await _env_for_parcel({"id": "urn:ngsi-ld:AgriParcel:c1", "type": "AgriParcel"})
    assert env["country"] is None
