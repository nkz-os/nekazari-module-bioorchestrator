# backend/tests/planning/test_calendar_service.py
from datetime import date
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

import app.api.v1.planning as planning_api
from app.main import app
from app.planning import service
from app.planning.calendar import PreviousCrop
from neo4j import AsyncDriver

ENV = {"parcel_id": "urn:ngsi-ld:AgriParcel:t:p1", "climate_class": "Csa", "country": "ES",
       "centroid": {"lat": 42.6, "lon": -1.6}, "irrigation": {"inferred": "rainfed", "source": "irrigationRegime"}}
URL = f"/api/graph/planning/parcels/{ENV['parcel_id']}/calendar"
BODY = {"parcel_id": ENV["parcel_id"], "campaign": 2027, "crop": "wheat", "cover_crop": None, "fortnights": [],
        "main": {"window": None, "window_source": None, "default": None, "selected": None, "options": [],
                 "perennial_years_default": None},
        "previous": {"crop": None, "end": None, "end_source": "campaign_start"}, "cover": None}


def _cycle(species, start, end, end_prov, label=None):
    return {"crop_id": "c", "role": "main_crop", "species": species, "status": "harvested", "campaign_label": label,
            "start": {"date": start, "provenance": "actual"}, "end": {"date": end, "provenance": end_prov}}


def test_previous_crop_prefers_a_recorded_harvest():
    cycles = {"cycles": [_cycle("TRZAX", "2025-10-20", "2026-07-05", "actual", 2026)]}
    assert service.previous_crop(cycles, 2027, sowing_for=lambda eppo: {}) == \
        PreviousCrop("wheat", date(2026, 7, 5), "harvest_recorded")


def test_previous_crop_uses_a_planned_end_then_typical_maturity():
    planned = {"cycles": [_cycle("TRZAX", "2025-10-20", "2026-07-01", "planned", 2026)]}
    assert service.previous_crop(planned, 2027, sowing_for=lambda e: {}).end_source == "planned_end"
    open_end = {"cycles": [_cycle("ZEAMX", "2026-04-20", None, None)]}  # no label: campaign from the sowing date
    prev = service.previous_crop(open_end, 2027, sowing_for=lambda e: {"cycle_days": 150})
    assert prev == PreviousCrop("maize", date(2026, 9, 17), "typical_maturity")


def test_the_campaign_being_planned_and_later_ones_are_not_previous():
    cycles = {"cycles": [_cycle("TRZAX", "2026-10-20", None, None, 2027), _cycle("ZEAMX", "2027-04-20", None, None)]}
    assert service.previous_crop(cycles, 2027, sowing_for=lambda e: {}) is None


def test_no_previous_crop_is_none():
    assert service.previous_crop({"cycles": []}, 2027, sowing_for=lambda e: {}) is None
    assert service.previous_crop(None, 2027, sowing_for=lambda e: {}) is None


async def test_calendar_service_wires_sowing_cycles_and_cover(monkeypatch):
    dao = AsyncMock()
    dao.get_parcel_environment.return_value = ENV
    monkeypatch.setattr(service, "fetch_crop_cycles", AsyncMock(return_value={"cycles": []}))
    monkeypatch.setattr(service, "sowing_info", lambda *a, **k: {"sowing_window": {"start_month": 10, "end_month": 11},
                                                                 "typical_sowing_doy": None, "source": "table"})
    out = await service.parcel_calendar(dao=dao, parcel_id=ENV["parcel_id"], tenant_id="t", campaign=2027,
                                        crop="wheat", cover="vetch", main_sowing=None)
    assert out["crop"] == "wheat" and out["cover_crop"] == "vetch"
    assert out["cover"]["fits"] is True and out["main"]["default"]["start"] == "2026-10-16"


async def test_a_main_sowing_outside_the_campaign_is_refused(monkeypatch):
    dao = AsyncMock()
    dao.get_parcel_environment.return_value = ENV
    monkeypatch.setattr(service, "fetch_crop_cycles", AsyncMock(return_value={"cycles": []}))
    with pytest.raises(service.CalendarInputError, match="main_sowing_outside_campaign"):
        await service.parcel_calendar(dao=dao, parcel_id=ENV["parcel_id"], tenant_id="t", campaign=2027,
                                      crop="wheat", cover=None, main_sowing=date(2027, 9, 1))


async def test_a_main_sowing_mid_fortnight_snaps_to_its_fortnight(monkeypatch):
    dao = AsyncMock()
    dao.get_parcel_environment.return_value = ENV
    monkeypatch.setattr(service, "fetch_crop_cycles", AsyncMock(return_value=None))  # core down: no history
    monkeypatch.setattr(service, "sowing_info", lambda *a, **k: {"sowing_window": None, "typical_sowing_doy": None,
                                                                 "source": None})
    out = await service.parcel_calendar(dao=dao, parcel_id=ENV["parcel_id"], tenant_id="t", campaign=2027,
                                        crop="wheat", cover=None, main_sowing=date(2026, 11, 9))
    assert out["main"]["selected"] == {"start": "2026-11-01", "end": "2026-11-15"}
    assert out["previous"]["end_source"] == "campaign_start"


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr("app.core.dependencies.get_driver", lambda: MagicMock(spec=AsyncDriver))
    monkeypatch.setattr("app.core.knowledge.get_driver", lambda: MagicMock(spec=AsyncDriver))
    return TestClient(app)


def test_calendar_route_validates_the_contract(client, monkeypatch):
    monkeypatch.setattr(planning_api, "parcel_calendar", AsyncMock(return_value=BODY))
    r = client.get(URL, params={"campaign": 2027, "crop": "wheat"})
    assert r.status_code == 200 and r.json()["contract_version"] == "2.2"


def test_calendar_route_requires_a_verified_tenant(client, monkeypatch):
    # The conftest default disables auth; this test runs the real production branch.
    monkeypatch.setenv("AUTH_DISABLED", "false")
    called = AsyncMock(return_value=BODY)
    monkeypatch.setattr(planning_api, "parcel_calendar", called)
    assert client.get(URL, params={"campaign": 2027, "crop": "wheat"}).status_code == 401
    called.assert_not_called()


@pytest.mark.parametrize("error, status", [(service.ParcelNotFound("x"), 404), (service.UnknownCrop("x"), 422)])
def test_calendar_route_maps_errors(client, monkeypatch, error, status):
    monkeypatch.setattr(planning_api, "parcel_calendar", AsyncMock(side_effect=error))
    assert client.get(URL, params={"campaign": 2027, "crop": "wheat"}).status_code == status


def test_calendar_route_names_a_refused_main_sowing(client, monkeypatch):
    monkeypatch.setattr(planning_api, "parcel_calendar",
                        AsyncMock(side_effect=service.CalendarInputError("main_sowing_outside_campaign")))
    r = client.get(URL, params={"campaign": 2027, "crop": "wheat", "main_sowing": "2027-09-01"})
    assert r.status_code == 422 and r.json()["detail"]["code"] == "main_sowing_outside_campaign"


def test_calendar_route_requires_a_crop(client):
    assert client.get(URL, params={"campaign": 2027}).status_code == 422
