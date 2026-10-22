"""Background warm-up of the first crops' evidence when a parcel's fit list is read."""
import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock

from app.planning import prefetch
from app.planning.evidence import load_crop_evidence

ENV = {"parcel_id": "urn:ngsi-ld:AgriParcel:t:p1", "climate_class": "Csa", "country": "ES",
       "centroid": {"lat": None, "lon": None}, "soil": {"data_available": False}, "irrigation": {}, "climate_detail": {}}
P = ENV["parcel_id"]


def _sp(eppo, verdict="fits", trials=True):
    return {"eppo": eppo, "verdict": verdict, "has_variety_trials": trials}


def _dao():
    dao = AsyncMock()
    dao.get_parcel_environment.return_value = ENV
    dao.recommend_for_conditions.return_value = {"status": "ok", "recommendations": []}
    return dao


def test_the_first_three_crops_that_fit_and_have_trials_are_chosen_in_list_order():
    species = [_sp("A"), _sp("B", verdict="no_fit"), _sp("C", trials=False), _sp("D", verdict="caveats"), _sp("E"), _sp("F")]
    assert prefetch.prefetch_crops(species) == ["A", "D", "E"]


async def test_nothing_is_scheduled_without_a_regime_or_a_crop():
    dao = _dao()
    assert prefetch.schedule_evidence_prefetch(dao, parcel_id=P, tenant_id="t", regime=None, species=[_sp("A")]) is None
    assert prefetch.schedule_evidence_prefetch(dao, parcel_id=P, tenant_id="t", regime="rainfed", species=[]) is None
    dao.get_parcel_environment.assert_not_called()


async def test_the_prefetch_asks_the_recommender_exactly_what_the_evidence_will():
    # Same conditions = same recommender cache key: picking the crop then answers from the cache.
    dao = _dao()
    await prefetch.schedule_evidence_prefetch(dao, parcel_id=P, tenant_id="t", regime="rainfed", species=[_sp("TRZAX")])
    warmed = dao.recommend_for_conditions.await_args.args[0]
    later = _dao()
    await load_crop_evidence(later, ENV, crop_eppo="TRZAX", regime="rainfed", management="conventional")
    assert warmed == later.recommend_for_conditions.await_args.args[0]


async def test_crops_are_warmed_one_at_a_time_and_prefetches_never_overlap():
    dao, running, peak, gate = _dao(), 0, 0, asyncio.Event()

    async def slow(_conditions):
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await gate.wait()
        running -= 1
        return {"status": "ok", "recommendations": []}

    dao.recommend_for_conditions.side_effect = slow
    a = prefetch.schedule_evidence_prefetch(dao, parcel_id=P, tenant_id="t", regime="rainfed", species=[_sp("A"), _sp("B")])
    b = prefetch.schedule_evidence_prefetch(dao, parcel_id=P, tenant_id="u", regime="rainfed", species=[_sp("C")])
    await asyncio.sleep(0.05)
    gate.set()
    await asyncio.gather(a, b)
    assert peak == 1 and dao.recommend_for_conditions.await_count == 3


async def test_a_failing_prefetch_is_logged_and_never_raises(caplog):
    dao = _dao()
    dao.recommend_for_conditions.side_effect = RuntimeError("neo4j down")
    with caplog.at_level(logging.WARNING, logger="app.planning.prefetch"):
        await prefetch.schedule_evidence_prefetch(dao, parcel_id=P, tenant_id="t", regime="rainfed", species=[_sp("A")])
    assert "planning prefetch failed" in caplog.text


async def test_an_unknown_parcel_or_climate_stops_without_the_recommender():
    dao = _dao()
    dao.get_parcel_environment.return_value = {"error": "nope"}
    await prefetch.schedule_evidence_prefetch(dao, parcel_id=P, tenant_id="t", regime="rainfed", species=[_sp("A")])
    dao.get_parcel_environment.return_value = {**ENV, "climate_class": None}
    await prefetch.schedule_evidence_prefetch(dao, parcel_id=P, tenant_id="t", regime="rainfed", species=[_sp("A")])
    dao.recommend_for_conditions.assert_not_called()


def test_the_fit_route_schedules_the_prefetch_with_the_resolved_regime(monkeypatch):
    from fastapi.testclient import TestClient

    import app.api.v1.planning as planning_api
    from app.main import app
    from neo4j import AsyncDriver
    monkeypatch.setattr("app.core.dependencies.get_driver", lambda: MagicMock(spec=AsyncDriver))
    monkeypatch.setattr("app.core.knowledge.get_driver", lambda: MagicMock(spec=AsyncDriver))
    fit = {"parcel_id": P, "campaign": 2027, "graphs": ["global"], "species": [],
           "facts": {"climate_class": "Csa", "climate_source": "chelsa", "soil_texture": None, "soil_ph": None,
                     "soil_source": "unavailable", "regime": "rainfed", "regime_source": "declared", "history_available": True}}
    monkeypatch.setattr(planning_api, "parcel_fit", AsyncMock(return_value=fit))
    scheduled = MagicMock()
    monkeypatch.setattr(planning_api, "schedule_evidence_prefetch", scheduled)
    r = TestClient(app).get(f"/api/graph/planning/parcels/{P}/fit", params={"campaign": 2027})
    assert r.status_code == 200
    kw = scheduled.call_args.kwargs
    assert (kw["parcel_id"], kw["regime"], kw["species"]) == (P, "rainfed", [])
