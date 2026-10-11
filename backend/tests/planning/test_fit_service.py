from unittest.mock import AsyncMock

from app.core.knowledge import KnowledgeSource
from app.planning import service
from app.planning.models import FitResponse
from app.planning.reference import Reference

EMPTY_REF = Reference(tolerances={}, constraints=(), trial_eppo=frozenset({"TRZAX"}))
BARE_ENV = {"parcel_id": "urn:ngsi-ld:AgriParcel:t:p1", "climate_class": None, "climate_detail": None,
            "soil": {"data_available": False, "source": "unavailable"}, "irrigation": {"inferred": None, "source": "unknown"},
            "centroid": {"lat": None, "lon": None}, "country": None}


async def test_bare_parcel_never_fits_and_history_down_is_explicit(monkeypatch):
    # No soil, no climate, no regime, crop-cycles unavailable.
    dao = AsyncMock()
    dao.get_parcel_environment.return_value = BARE_ENV
    monkeypatch.setattr(service, "fetch_crop_cycles", AsyncMock(return_value=None))
    monkeypatch.setattr(service, "load_reference", AsyncMock(return_value=EMPTY_REF))
    out = await service.parcel_fit(dao=dao, source=KnowledgeSource("global", None), parcel_id="p1",
                                   tenant_id="t", campaign=2027, regime=None)
    FitResponse.model_validate(out)
    assert out["species"] and all(s["verdict"] != "fits" for s in out["species"])
    wheat = next(s for s in out["species"] if s["species"] == "wheat")
    codes = {r["code"] for r in wheat["reasons"]}
    assert {"regime_missing", "rotation_unknown", "soil_unknown"} <= codes
    assert wheat["has_variety_trials"] is True
    assert out["facts"]["history_available"] is False
    assert all(s["species"] not in ("olive", "apple", "grapevine") for s in out["species"])  # woody out
    alfalfa = next(s for s in out["species"] if s["species"] == "alfalfa")
    assert alfalfa["lifecycle"] == "perennial" and "multi_year" in {r["code"] for r in alfalfa["reasons"]}


async def test_unknown_parcel_raises(monkeypatch):
    dao = AsyncMock()
    dao.get_parcel_environment.return_value = {"error": "Parcel not found: x"}
    try:
        await service.parcel_fit(dao=dao, source=KnowledgeSource("global", None), parcel_id="x",
                                 tenant_id="t", campaign=2027, regime=None)
    except service.ParcelNotFound:
        return
    raise AssertionError("expected ParcelNotFound")


def _fit_args(dao):
    return {"dao": dao, "source": KnowledgeSource("global", None), "parcel_id": "p1", "tenant_id": "t",
            "campaign": 2027, "regime": None}


async def test_empty_rotation_rules_are_unknown_even_with_history(monkeypatch):
    # No rotation rules in the graph is missing data, not "no restrictions".
    dao = AsyncMock()
    dao.get_parcel_environment.return_value = BARE_ENV
    monkeypatch.setattr(service, "fetch_crop_cycles", AsyncMock(return_value={"cycles": []}))
    monkeypatch.setattr(service, "load_reference", AsyncMock(return_value=EMPTY_REF))
    out = await service.parcel_fit(**_fit_args(dao))
    assert out["facts"]["history_available"] is True
    assert all("rotation_unknown" in {r["code"] for r in s["reasons"]} for s in out["species"])


async def test_rotation_rules_with_history_are_applied(monkeypatch):
    from app.planning.rotation import Constraint

    ref = Reference(tolerances={}, trial_eppo=frozenset({"TRZAX"}), constraints=(
        Constraint("wheat", "wheat", "restriction", 1, "disease carry-over", "src"),))
    history = {"cycles": [{"crop_id": "c1", "species": "TRZAX", "status": "harvested", "campaign_label": 2026,
                           "start": {"date": "2025-11-02", "provenance": "actual"}, "end": {"date": "2026-07-01"}}]}
    dao = AsyncMock()
    dao.get_parcel_environment.return_value = BARE_ENV
    monkeypatch.setattr(service, "fetch_crop_cycles", AsyncMock(return_value=history))
    monkeypatch.setattr(service, "load_reference", AsyncMock(return_value=ref))
    out = await service.parcel_fit(**_fit_args(dao))
    wheat = next(s for s in out["species"] if s["species"] == "wheat")
    barley = next(s for s in out["species"] if s["species"] == "barley")
    assert "rotation_restriction" in {r["code"] for r in wheat["reasons"]}
    assert not {"rotation_unknown", "rotation_restriction"} & {r["code"] for r in barley["reasons"]}
