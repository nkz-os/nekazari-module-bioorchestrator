from datetime import date, timedelta
from unittest.mock import AsyncMock, MagicMock

from app.core.knowledge import KnowledgeSource
from app.planning import service
from app.planning.fit import BLOCKERS, INFORMATIVE
from app.planning.late_frost import FrostHistory, stage_windows
from app.planning.reference import Reference
from app.services.crop_calendar import effective_calendar

REF = Reference(tolerances={}, constraints=(), trial_eppo=frozenset({"TRZAX"}))
ENV = {"parcel_id": "urn:ngsi-ld:AgriParcel:tenant-a:p1", "climate_class": None, "climate_detail": None,
       "soil": {"data_available": False, "source": "unavailable"}, "irrigation": {"inferred": None, "source": "unknown"},
       "centroid": {"lat": None, "lon": None}, "country": None}
SOWING_DOY = 300


def _history(frost_years: int) -> FrostHistory:
    days = {}
    d = date(1991, 1, 1)
    while d <= date(1998, 1, 1):
        days[d] = (5.0, 15.0)
        d += timedelta(days=1)
    cal = effective_calendar("wheat", irrigation="rainfed")
    windows = stage_windows(cal, SOWING_DOY, days, "flowering")
    for year in sorted(windows)[:frost_years]:
        first, last = windows[year]  # mid-window: the frost day itself changes the thermal time
        days[first + (last - first) // 2] = (-3.0, 15.0)
    return FrostHistory(days=days, tmin_caveat="warm_bias")


async def _fit(monkeypatch, status, history, schedule=None):
    dao = AsyncMock()
    dao.get_parcel_environment.return_value = ENV
    monkeypatch.setattr(service, "fetch_crop_cycles", AsyncMock(return_value=None))
    monkeypatch.setattr(service, "load_reference", AsyncMock(return_value=REF))
    monkeypatch.setattr(service, "_season_start_doy", lambda *a, **k: SOWING_DOY)
    schedule = schedule or MagicMock(return_value=None)
    monkeypatch.setattr(service, "schedule_frost_history", schedule)
    monkeypatch.setattr(service, "get_frost_history", lambda t, p: history)
    monkeypatch.setattr(service, "frost_history_status", lambda t, p: status)
    out = await service.parcel_fit(dao=dao, source=KnowledgeSource("global", None), parcel_id="p1",
                                   tenant_id="tenant-a", campaign=2027, regime="rainfed")
    return out, schedule


def _late(s):
    return [r for r in s["reasons"] if r["code"].startswith("frost_late_")]


async def test_fit_adds_one_late_frost_reason_per_species(monkeypatch):
    out, _ = await _fit(monkeypatch, "ready", _history(2))
    assert all(len(_late(s)) == 1 for s in out["species"])
    wheat = next(s for s in out["species"] if s["species"] == "wheat")
    assert _late(wheat)[0]["code"] == "frost_late_risk"
    assert wheat["verdict"] == "caveats"
    assert _late(wheat)[0]["params"]["years_hit"] == 2
    sunflower = next(s for s in out["species"] if s["species"] == "sunflower")
    assert _late(sunflower)[0] == {"code": "frost_late_unknown", "gate": "frost",
                                   "params": {"why": "no_stage_model"}}
    olive_like = next(s for s in out["species"] if s["species"] not in
                      ("wheat", "barley", "oat", "pea", "lentil", "sunflower", "maize"))
    assert _late(olive_like)[0]["params"] == {"why": "no_threshold"}
    # unknown/info never change a verdict by themselves
    assert {"frost_late_info", "frost_late_unknown"} <= INFORMATIVE


async def test_fit_does_not_wait_for_history(monkeypatch):
    out, schedule = await _fit(monkeypatch, "absent", None)
    schedule.assert_called_once_with("tenant-a", ENV["parcel_id"])
    wheat = next(s for s in out["species"] if s["species"] == "wheat")
    assert _late(wheat)[0]["params"] == {"why": "history_pending"}


async def test_late_frost_never_blocks():
    assert not {c for c in BLOCKERS if c.startswith("frost_late_")}
    assert "frost_late_risk" not in BLOCKERS


async def test_late_frost_failure_keeps_one_reason_and_the_page(monkeypatch):
    def boom(slug, **kw):
        if kw.get("cal") is not None:
            raise RuntimeError("boom")
        return real(slug, **kw)

    real = service.late_frost_reason
    monkeypatch.setattr(service, "late_frost_reason", boom)
    out, _ = await _fit(monkeypatch, "ready", _history(2))
    assert all(len(_late(s)) == 1 for s in out["species"])
    wheat = next(s for s in out["species"] if s["species"] == "wheat")
    assert _late(wheat)[0]["params"] == {"why": "history_unavailable"}
    sunflower = next(s for s in out["species"] if s["species"] == "sunflower")
    assert _late(sunflower)[0]["params"] == {"why": "no_stage_model"}


async def test_late_frost_reason_is_last(monkeypatch):
    out, _ = await _fit(monkeypatch, "ready", _history(2))
    for s in out["species"]:
        assert s["reasons"][-1]["code"].startswith("frost_late_")
    assert any(len(s["reasons"]) > 1 for s in out["species"])
    # no rotation history, no climate, a perennial: all of these precede the frost reason
    perennial = [s for s in out["species"] if any(r["code"] == "multi_year" for r in s["reasons"])]
    assert perennial
    for s in perennial:
        codes = [r["code"] for r in s["reasons"]]
        assert {"rotation_unknown", "climate_unknown"} <= set(codes)
        assert codes[-1].startswith("frost_late_") and codes.index("multi_year") < len(codes) - 1


async def test_scheduling_failure_is_unavailable_not_a_500(monkeypatch):
    out, _ = await _fit(monkeypatch, "ready", None, schedule=MagicMock(side_effect=RuntimeError("no loop")))
    wheat = next(s for s in out["species"] if s["species"] == "wheat")
    assert _late(wheat)[0]["params"] == {"why": "history_unavailable"}


async def test_sowing_window_backs_up_a_missing_sowing_day(monkeypatch):
    seen = []
    real = service.late_frost_reason

    def spy(slug, **kw):
        seen.append((slug, kw["sowing_doy"]))
        return real(slug, **kw)

    monkeypatch.setattr(service, "late_frost_reason", spy)
    sowing = {"sowing_window": {"start_month": 10, "end_month": 1}, "typical_sowing_doy": None}
    monkeypatch.setattr(service, "_season_start_doy", lambda *a, **k: None)
    reason = await service._late_frost_reason("barley", "HORVX", sowing, {"lat": 42.5, "lon": -1.6}, "rainfed",
                                              _history(1), "ready", "tenant-a", "p1")
    assert seen == [("barley", 335)]
    assert reason["code"] in {"frost_late_risk", "frost_late_info"}


async def test_no_sowing_day_and_no_window_is_no_stage_model(monkeypatch):
    monkeypatch.setattr(service, "_season_start_doy", lambda *a, **k: None)
    reason = await service._late_frost_reason("barley", "HORVX", {"sowing_window": None}, {"lat": 42.5, "lon": -1.6},
                                              "rainfed", _history(1), "ready", "tenant-a", "p1")
    assert reason == {"code": "frost_late_unknown", "gate": "frost", "params": {"why": "no_stage_model"}}
