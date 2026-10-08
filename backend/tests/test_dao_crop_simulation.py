"""run_crop_simulation: real inputs only, typed errors, AquaCrop end to end (tunis weather)."""
from __future__ import annotations

import asyncio
import json
from datetime import date, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.graph.dao import GraphDAO
from app.services.engines import aquacrop_engine as eng
from app.services.sim_errors import SimulationError

PARCEL_ID = "urn:ngsi-ld:AgriParcel:p1"
TENANT = "t1"
POLYGON = {"type": "Polygon", "coordinates": [[[-1.6, 42.5], [-1.5, 42.5], [-1.5, 42.6],
                                              [-1.6, 42.6], [-1.6, 42.5]]]}


def _parcel(crop="urn:ngsi-ld:AgriCrop:TRZAX", location=POLYGON):
    p = {"id": PARCEL_ID, "type": "AgriParcel"}
    if crop:
        p["hasAgriCrop"] = {"type": "Relationship", "object": crop}
    if location:
        p["location"] = {"type": "GeoProperty", "value": location}
    return p


def _sowing(day="1990-10-01", status="completed"):
    return {"id": "urn:ngsi-ld:AgriParcelOperation:s", "type": "AgriParcelOperation",
            "operationType": {"type": "Property", "value": "sowing"},
            "status": {"type": "Property", "value": status},
            "plannedDate": {"type": "Property", "value": {"@type": "DateTime", "@value": f"{day}T00:00:00Z"}}}


SUMMARY = {"horizons": [
    {"depthFrom": 0, "depthTo": 30, "fieldCapacity": 0.30, "wiltingPoint": 0.15,
     "saturation": 0.45, "ksatSaturated": 10.0},
    {"depthFrom": 30, "depthTo": 120, "fieldCapacity": 0.27, "wiltingPoint": 0.14,
     "saturation": 0.43, "ksatSaturated": 8.0}]}


class _Orion:
    parcel = None
    ops: list = []  # noqa: RUF012
    queries: list = []  # noqa: RUF012

    def __init__(self, tenant_id):
        pass

    async def get_entity(self, entity_id):
        if _Orion.parcel is None:
            import httpx
            resp = MagicMock(status_code=404)
            raise httpx.HTTPStatusError("nf", request=MagicMock(), response=resp)
        return _Orion.parcel

    async def query_entities(self, **kw):
        _Orion.queries.append(kw)
        return _Orion.ops

    async def close(self):
        pass


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("WEATHER_API_URL", "http://weather.test")
    monkeypatch.setenv("OPENMETEO_ARCHIVE_URL", "http://archive.test")
    monkeypatch.setenv("CLIMATOLOGY_YEARS", "5")
    _Orion.parcel, _Orion.ops, _Orion.queries = _parcel(), [_sowing()], []
    yield
    eng._shutdown_pool()


@pytest.fixture(scope="module")
def tunis():
    from aquacrop.utils import get_filepath, prepare_weather
    df = prepare_weather(get_filepath("tunis_climate.txt"))
    return {r.Date.date(): (float(r.MinTemp), float(r.MaxTemp), float(r.Precipitation),
                            float(r.ReferenceET)) for r in df.itertuples()}


class _Net:
    """httpx.AsyncClient stand-in serving tunis weather as weather-api / archive."""

    def __init__(self, tunis, parcel_from=date(1990, 6, 1), parcel_status=200, archive_status=200):
        self.tunis, self.parcel_from = tunis, parcel_from
        self.parcel_status, self.archive_status = parcel_status, archive_status
        self.urls: list[str] = []

    def __call__(self, *a, **k):
        net = self

        async def get(url, params=None, headers=None):
            net.urls.append(url)
            resp = MagicMock()
            if "/api/weather/parcel/" in url:
                resp.status_code = net.parcel_status
                s, e = date.fromisoformat(params["start"]), date.fromisoformat(params["end"])
                days = []
                while s <= e:
                    if s >= net.parcel_from and s in net.tunis:
                        v = net.tunis[s]
                        days.append({"date": s.isoformat(), "tmin_c": v[0], "tmax_c": v[1],
                                     "precip_mm": v[2], "et0_mm": v[3]})
                    s += timedelta(days=1)
                resp.json.return_value = {"days": days}
            else:
                resp.status_code = net.archive_status
                s, e = date.fromisoformat(params["start_date"]), date.fromisoformat(params["end_date"])
                cols = {k: [] for k in ("time", "temperature_2m_min", "temperature_2m_max",
                                        "precipitation_sum", "et0_fao_evapotranspiration")}
                while s <= e:
                    v = net.tunis.get(s)
                    cols["time"].append(s.isoformat())
                    for i, col in enumerate(list(cols)[1:]):
                        cols[col].append(None if v is None else v[i])
                    s += timedelta(days=1)
                resp.json.return_value = {"daily": cols}
            return resp

        client = MagicMock()
        client.get = get
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=client)
        cm.__aexit__ = AsyncMock(return_value=False)
        return cm


def _run(dao, net=None, summary=SUMMARY, **kw):
    kw.setdefault("today", date(1991, 2, 1))
    with patch("app.graph.dao.OrionClient", _Orion), \
         patch("httpx.AsyncClient", net or MagicMock(side_effect=AssertionError("unexpected HTTP"))), \
         patch("app.services.crop_simulation.get_parcel_soil_summary", AsyncMock(return_value=summary)):
        return asyncio.run(dao.run_crop_simulation(PARCEL_ID, tenant_id=TENANT, **kw))


def _err(dao, **kw):
    with pytest.raises(SimulationError) as ei:
        _run(dao, **kw)
    return ei.value


# ------------------------------------------------------------ input errors
def test_no_crop_assigned_and_none_given(mock_driver):
    _Orion.parcel = _parcel(crop=None)
    e = _err(GraphDAO(mock_driver))
    assert (e.code, e.status_code) == ("crop_missing", 422)


def test_unsupported_crop_from_parcel(mock_driver):
    _Orion.parcel = _parcel(crop="urn:ngsi-ld:AgriCrop:PRNDU")  # almond
    e = _err(GraphDAO(mock_driver))
    assert e.code == "unsupported_crop"


def test_unsupported_crop_param(mock_driver):
    e = _err(GraphDAO(mock_driver), crop_slug="alfalfa")
    assert e.code == "unsupported_crop"


def test_parcel_not_found_is_404(mock_driver):
    _Orion.parcel = None
    e = _err(GraphDAO(mock_driver))
    assert (e.code, e.status_code) == ("parcel_not_found", 404)


def test_parcel_without_location(mock_driver):
    _Orion.parcel = _parcel(location=None)
    assert _err(GraphDAO(mock_driver)).code == "parcel_location_missing"


def test_future_sowing_date(mock_driver):
    e = _err(GraphDAO(mock_driver), sowing_date_str="1991-02-02")
    assert (e.code, e.status_code) == ("sowing_date_future", 422)


def test_invalid_sowing_date(mock_driver):
    assert _err(GraphDAO(mock_driver), sowing_date_str="soon").code == "invalid_sowing_date"


def test_no_sowing_operation(mock_driver):
    _Orion.ops = []
    assert _err(GraphDAO(mock_driver)).code == "sowing_date_missing"


def test_invalid_irrigation_and_engine(mock_driver):
    assert _err(GraphDAO(mock_driver), irrigation="drip").code == "invalid_irrigation"
    assert _err(GraphDAO(mock_driver), engine="wofost81").code == "unsupported_engine"


def test_soil_without_saturation_is_soil_incomplete(mock_driver):
    bad = {"horizons": [{"depthFrom": 0, "depthTo": 30, "fieldCapacity": 0.3,
                         "wiltingPoint": 0.15, "ksatSaturated": 10.0}]}
    e = _err(GraphDAO(mock_driver), summary=bad)
    assert e.code == "soil_incomplete" and e.status_code == 422


def test_soil_module_down_is_503(mock_driver):
    from app.services.soil_client import SoilSummaryError
    with patch("app.graph.dao.OrionClient", _Orion), \
         patch("app.services.crop_simulation.get_parcel_soil_summary",
               AsyncMock(side_effect=SoilSummaryError("down"))), \
         pytest.raises(SimulationError) as ei:
        asyncio.run(GraphDAO(mock_driver).run_crop_simulation(
            PARCEL_ID, tenant_id=TENANT, today=date(1991, 2, 1)))
    assert (ei.value.code, ei.value.status_code) == ("soil_unavailable", 503)


def test_weather_service_down_is_503(mock_driver, tunis):
    e = _err(GraphDAO(mock_driver), net=_Net(tunis, parcel_status=503))
    assert (e.code, e.status_code) == ("weather_unavailable", 503)


def test_archive_not_configured_is_503(mock_driver, tunis, monkeypatch):
    monkeypatch.setenv("OPENMETEO_ARCHIVE_URL", "")
    e = _err(GraphDAO(mock_driver), net=_Net(tunis))
    assert (e.code, e.status_code) == ("archive_not_configured", 503)


# -------------------------------------------------------------- end to end
def test_in_season_response_shape(mock_driver, tunis):
    net = _Net(tunis)
    res = _run(GraphDAO(mock_driver), net=net)
    assert _Orion.queries[-1]["limit"] == 200
    assert res["engine"] == "aquacrop" and res["engine_version"]
    assert res["capabilities"] == {"water_limited": True, "potential": True, "nitrogen": False}
    assert (res["crop_slug"], res["aquacrop_crop"]) == ("wheat", "WheatGDD")
    assert "crop_parameters_note" not in res
    assert res["parcel_id"] == PARCEL_ID and res["sowing_date"] == "1990-10-01"
    assert res["irrigation"] == "rainfed"
    assert res["initial_water"] == {"method": "spinup", "spinup_days": 364, "start": "1989-10-02"}
    assert res["status"] == "in_season"
    y, p = res["yield_t_ha"], res["potential_yield_t_ha"]
    assert y["p10"] <= y["p50"] <= y["p90"] and p["p10"] <= p["p50"] <= p["p90"]
    assert res["ensemble"] == {"n_years": 5, "method": "climatological_ensemble",
                               "years": [1985, 1986, 1987, 1988, 1989]}
    assert res["last_weather_day"] == "1991-01-31"
    assert res["harvest_date"] > "1991-01-31"
    d0 = res["daily"][0]
    assert d0["day"] == "1990-10-01" and d0["projected"] is False
    assert res["daily"][-1]["projected"] is True
    assert set(d0) >= {"day", "canopy_cover", "biomass", "root_zone_water_mm", "water_stress"}
    w = res["inputs"]["weather"]
    assert w["source"] == "archive+parcel_daily"
    assert (w["start"], w["end"], w["days"]) == ("1989-10-01", "1991-01-31", 488)
    assert [s["source"] for s in w["segments"]] == ["archive", "parcel_daily"]
    assert w["segments"][0]["end"] == "1990-05-31"
    assert len(res["inputs"]["soil"]["layers"]) == 2
    assert res["inputs"]["sowing"] == {"source": "field_operations"}
    json.dumps(res)  # response must be serialisable
    print(json.dumps({k: v for k, v in res.items() if k != "daily"}, indent=1))


def test_complete_season_has_no_ensemble_and_never_loads_climatology(mock_driver, tunis):
    net = _Net(tunis, parcel_from=date(1989, 10, 1))
    res = _run(GraphDAO(mock_driver), net=net, today=date(1992, 1, 1), sowing_date_str="1990-10-01",
               crop_slug="wheat")
    assert res["status"] == "complete" and res["ensemble"] is None
    assert isinstance(res["yield_t_ha"], float) and isinstance(res["potential_yield_t_ha"], float)
    assert res["yield_t_ha"] <= res["potential_yield_t_ha"] + 1e-9
    assert res["inputs"]["sowing"] == {"source": "request"}
    assert res["inputs"]["weather"]["source"] == "parcel_daily"
    assert not any("archive.test" in u for u in net.urls)


def test_full_irrigation_headline_is_potential(mock_driver, tunis):
    res = _run(GraphDAO(mock_driver), net=_Net(tunis), irrigation="full")
    assert res["irrigation"] == "full"
    assert res["yield_t_ha"] == res["potential_yield_t_ha"]


def test_durum_wheat_carries_parameters_note(mock_driver, tunis):
    res = _run(GraphDAO(mock_driver), net=_Net(tunis), crop_slug="durum_wheat")
    assert res["aquacrop_crop"] == "WheatGDD"
    assert "bread wheat" in res["crop_parameters_note"]
