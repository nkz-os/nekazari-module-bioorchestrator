"""parcel_fit: regional yield orders crops inside a verdict and never changes the verdict."""
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.core.knowledge import KnowledgeSource
from app.graph import balance_dao
from app.planning import service
from app.planning.models import FitResponse
from app.planning.reference import Reference

REF = Reference(tolerances={}, constraints=(), trial_eppo=frozenset({"TRZAX"}))
ES_POINT = {"lat": 41.65, "lon": -0.88}   # inside Spain
FR_POINT = {"lat": 48.85, "lon": 2.35}    # outside Spain
NUTS = "ES24"


def _env(centroid):
    return {"parcel_id": "urn:ngsi-ld:AgriParcel:t:p1", "climate_class": None, "climate_detail": None,
            "soil": {"data_available": False, "source": "unavailable"},
            "irrigation": {"inferred": None, "source": "unknown"}, "centroid": centroid, "country": None}


def _series(eppo, nuts, base):
    return [{"eppo": eppo, "nuts": nuts, "year": y, "yieldTHa": base + 0.01 * i, "humidityPct": 14.0}
            for i, y in enumerate(range(2014, 2024))]


# Barley is the best region, wheat middling, maize short in the region; country is 4.0 for all.
def _rows():
    rows = []
    for eppo, region in (("HORVX", 6.0), ("TRZAX", 5.0), ("ZEAMX", None)):
        rows += _series(eppo, "ES", 4.0)
        if region:
            rows += _series(eppo, NUTS, region)
    return rows


@pytest.fixture
def run(monkeypatch):
    calls = []

    async def batch(self, eppos, nuts):
        calls.append((list(eppos), list(nuts)))
        return _rows()

    monkeypatch.setattr(balance_dao.BalanceDao, "get_regional_yields_batch", batch)
    monkeypatch.setattr(service, "fetch_crop_cycles", AsyncMock(return_value=None))
    monkeypatch.setattr(service, "load_reference", AsyncMock(return_value=REF))

    async def go(centroid):
        dao = AsyncMock()
        dao.get_parcel_environment.return_value = _env(centroid)
        out = await service.parcel_fit(dao=dao, source=KnowledgeSource("global", MagicMock()),
                                       parcel_id="p1", tenant_id="t", campaign=2027, regime=None)
        FitResponse.model_validate(out)
        return out, calls

    return go


def _by(out, slug):
    return next(s for s in out["species"] if s["species"] == slug)


async def test_one_graph_query_and_crops_ordered_by_index(run, monkeypatch):
    monkeypatch.setattr(service, "parcel_nuts2", lambda lat, lon: NUTS)
    out, calls = await run(ES_POINT)
    assert len(calls) == 1 and calls[0][1] == [NUTS, "ES"]
    barley, wheat = _by(out, "barley"), _by(out, "wheat")
    assert barley["regional_yield"]["relative_index"] > wheat["regional_yield"]["relative_index"] > 1
    assert barley["regional_yield"]["attributions"]
    verdicts = {s["verdict"] for s in out["species"]}
    assert verdicts == {"caveats"}  # bare parcel: everything is caveats
    order = [s["species"] for s in out["species"]]
    assert order.index("barley") < order.index("wheat")
    # crops with an index first, then the rest by slug
    with_idx = [s for s in out["species"] if (s["regional_yield"] or {}).get("relative_index") is not None]
    rest = [s["species"] for s in out["species"] if s not in with_idx]
    assert out["species"][:len(with_idx)] == with_idx and rest == sorted(rest)


async def test_verdict_does_not_depend_on_yield(run, monkeypatch):
    monkeypatch.setattr(service, "parcel_nuts2", lambda lat, lon: NUTS)
    with_y, _ = await run(ES_POINT)
    monkeypatch.setattr(service, "parcel_nuts2", lambda lat, lon: None)
    without, _ = await run(ES_POINT)
    key = lambda o: {s["species"]: (s["verdict"], s["reasons"]) for s in o["species"]}  # noqa: E731
    assert key(with_y) == key(without)


async def test_crop_outside_statistics(run, monkeypatch):
    monkeypatch.setattr(service, "parcel_nuts2", lambda lat, lon: NUTS)
    out, _ = await run(ES_POINT)
    rice = _by(out, "rice") if any(s["species"] == "rice" for s in out["species"]) else None
    other = next(s for s in out["species"] if s["eppo"] not in ("TRZAX", "HORVX", "ZEAMX")
                 and service.catalog_code(s["eppo"] or "") not in service._statistics_crops())
    assert other["regional_yield"] is None and other["regional_yield_unavailable"] == "crop_not_in_statistics"
    assert rice is None or rice["regional_yield"] is None


async def test_short_series_reason_is_not_a_fit_reason(run, monkeypatch):
    monkeypatch.setattr(service, "parcel_nuts2", lambda lat, lon: NUTS)
    out, _ = await run(ES_POINT)
    maize = _by(out, "maize")
    assert maize["regional_yield"] is None and maize["regional_yield_unavailable"] == "region_series_short"
    assert "region_series_short" not in {r["code"] for r in maize["reasons"]}


async def test_parcel_in_france_has_no_region(run):
    out, calls = await run(FR_POINT)
    assert calls == []  # no region, no graph read
    wheat = _by(out, "wheat")
    assert wheat["regional_yield"] is None and wheat["regional_yield_unavailable"] == "region_lookup_unavailable"


async def test_graph_failure_degrades_without_changing_verdicts(run, monkeypatch):
    monkeypatch.setattr(service, "parcel_nuts2", lambda lat, lon: NUTS)

    async def boom(self, eppos, nuts):
        raise RuntimeError("graph down")

    monkeypatch.setattr(balance_dao.BalanceDao, "get_regional_yields_batch", boom)
    out, _ = await run(ES_POINT)
    assert _by(out, "wheat")["regional_yield_unavailable"] == "region_stats_unavailable"


async def test_real_point_resolves_a_nuts2(run):
    out, calls = await run(ES_POINT)
    assert len(calls) == 1 and calls[0][1][0].startswith("ES") and len(calls[0][1][0]) == 4
