from array import array
from unittest.mock import AsyncMock

import pytest

from app.core.knowledge import KnowledgeSource
from app.planning import irrigation as irr
from app.planning import service
from app.planning.climatology import DAYS, Climatology
from app.planning.fit import BLOCKERS, INFORMATIVE, allotment_reasons, verdict_of
from app.planning.models import CONTRACT_VERSION, FitResponse
from app.planning.reference import Reference

REF = Reference(tolerances={}, constraints=(), trial_eppo=frozenset({"TRZAX"}))
ENV = {"parcel_id": "urn:ngsi-ld:AgriParcel:t:p1", "climate_class": None, "climate_detail": None,
       "soil": {"data_available": False, "source": "unavailable"}, "irrigation": {"inferred": None, "source": "unknown"},
       "centroid": {"lat": 42.0, "lon": -1.0}, "country": "ES"}
STAGES = irr.load_stage_table()  # the cited FAO-56 table (data/fao56_stage_lengths.yaml)


def clim(et0=5.0):
    return Climatology({y: (array("f", [et0] * DAYS), array("f", [0.0] * DAYS)) for y in range(2005, 2020)})


def varying_clim():
    return Climatology({y: (array("f", [3.0 + 0.4 * i] * DAYS), array("f", [0.0] * DAYS))
                        for i, y in enumerate(range(2005, 2020))})


def test_contract_version_and_blockers():
    assert CONTRACT_VERSION == "2.4"
    assert BLOCKERS == frozenset({"soil_unsuitable", "soil_ph_unsuitable", "soil_texture_unsuitable",
                                  "soil_drainage_unsuitable", "frost_risk"})
    assert "irrigation_need_tight" in INFORMATIVE


def test_p50_above_allotment_is_a_caveat_not_a_blocker():
    reasons = allotment_reasons(400, 480, 300)
    assert [r["code"] for r in reasons] == ["irrigation_need_exceeds_allotment"] and reasons[0]["gate"] == "water"
    assert verdict_of(reasons) == "caveats"


def test_allotment_between_p50_and_p90_is_tight_and_keeps_fits():
    reasons = allotment_reasons(280, 350, 300)
    assert [r["code"] for r in reasons] == ["irrigation_need_tight"]
    assert verdict_of(reasons) == "fits"


def test_allotment_above_p90_or_absent_gives_no_reason():
    assert allotment_reasons(280, 350, 400) == []
    assert allotment_reasons(280, 350, None) == []
    assert allotment_reasons(None, None, 300) == []
    assert allotment_reasons(300, 350, 300) != [] and allotment_reasons(300, 350, 300)[0]["code"] == "irrigation_need_tight"


async def _fit(monkeypatch, *, regime="irrigated", raw=None, climatology=None, status="ready", stages=STAGES,
               centroid=None):
    dao = AsyncMock()
    dao.get_parcel_environment.return_value = {**ENV, "irrigation_allotment_raw": raw,
                                               **({"centroid": centroid} if centroid else {})}
    monkeypatch.setattr(service, "fetch_crop_cycles", AsyncMock(return_value=None))
    monkeypatch.setattr(service, "load_reference", AsyncMock(return_value=REF))
    monkeypatch.setattr(service, "load_stage_table", lambda: stages)
    monkeypatch.setattr(service, "schedule_climatology", lambda cell: None)
    monkeypatch.setattr(service, "get_climatology", lambda cell: climatology)
    monkeypatch.setattr(service, "climatology_status", lambda cell: status)
    out = await service.parcel_fit(dao=dao, source=KnowledgeSource("global", None), parcel_id="p1", tenant_id="t",
                                   campaign=2027, regime=regime)
    FitResponse.model_validate(out)
    return out, next(s for s in out["species"] if s["species"] == "wheat")


def codes(sp):
    return {r["code"] for r in sp["reasons"]}


async def test_irrigated_fit_carries_need_and_allotment_gate(monkeypatch):
    _, base = await _fit(monkeypatch, climatology=clim())
    need = base["irrigation_need"]
    assert need["unit"] == "mm" and need["n_years"] >= 5 and base["irrigation_need_unavailable"] is None
    p50 = need["percentiles"]["p50"]
    assert need["method"]["pe_method"] == "usda_scs_decadal" and need["method"]["kc_source"].startswith("FAO-56 Table 11 'Winter Wheat'")
    assert not codes(base) & {"irrigation_need_exceeds_allotment", "irrigation_need_tight"}

    out, over = await _fit(monkeypatch, climatology=clim(),
                           raw={"type": "Property", "value": p50 - 100, "unitCode": "MMT"})
    assert "irrigation_need_exceeds_allotment" in codes(over) and over["verdict"] == "caveats"
    assert out["facts"]["irrigation_allotment_mm"] == pytest.approx(p50 - 100)


async def test_tight_allotment_does_not_change_verdict(monkeypatch):
    _, base = await _fit(monkeypatch, climatology=clim())
    _, tight = await _fit(monkeypatch, climatology=clim(),
                          raw={"type": "Property", "value": base["irrigation_need"]["percentiles"]["p50"], "unitCode": "MMT"})
    # Constant weather: P50 == P90, so an allotment equal to P50 is not tight (P90 > allotment is false).
    assert "irrigation_need_tight" not in codes(tight) and tight["verdict"] == base["verdict"]


async def test_wrong_unit_allotment_is_ignored(monkeypatch):
    out, sp = await _fit(monkeypatch, climatology=clim(), raw={"type": "Property", "value": 1, "unitCode": "MTQ"})
    assert out["facts"]["irrigation_allotment_mm"] is None
    assert "irrigation_need_exceeds_allotment" not in codes(sp)


async def test_rainfed_has_no_need(monkeypatch):
    _, sp = await _fit(monkeypatch, regime="rainfed", climatology=clim())
    assert sp["irrigation_need"] is None and sp["irrigation_need_unavailable"] == "regime_not_irrigated"


async def test_unavailable_reasons(monkeypatch):
    _, sp = await _fit(monkeypatch, climatology=None, status="loading")
    assert sp["irrigation_need_unavailable"] == "pending"
    _, sp = await _fit(monkeypatch, climatology=None, status="absent")
    assert sp["irrigation_need_unavailable"] == "pending"
    _, sp = await _fit(monkeypatch, climatology=None, status="failed")
    assert sp["irrigation_need_unavailable"] == "weather_unavailable"
    _, sp = await _fit(monkeypatch, climatology=Climatology({2019: clim().year_series(2005)}))
    assert sp["irrigation_need_unavailable"] == "weather_unavailable"  # under 5 valid years
    _, sp = await _fit(monkeypatch, climatology=clim(), stages={k: v for k, v in STAGES.items() if k != "wheat"})
    assert sp["irrigation_need_unavailable"] == "no_stage_lengths"  # not in the FAO-56 table


async def test_tight_allotment_between_p50_and_p90_keeps_the_verdict(monkeypatch):
    _, base = await _fit(monkeypatch, climatology=varying_clim())
    p = base["irrigation_need"]["percentiles"]
    assert p["p50"] < p["p90"]
    mid = (p["p50"] + p["p90"]) / 2
    out, tight = await _fit(monkeypatch, climatology=varying_clim(), raw={"type": "Property", "value": mid, "unitCode": "MMT"})
    assert "irrigation_need_tight" in codes(tight) and "irrigation_need_exceeds_allotment" not in codes(tight)
    assert tight["verdict"] == base["verdict"]
    assert out["facts"]["irrigation_allotment_mm"] == pytest.approx(mid)


async def test_no_sowing_date(monkeypatch):
    monkeypatch.setattr(service, "sowing_info", lambda *a, **k: service.NO_SOWING)
    monkeypatch.setattr(service.ggcmi_calendar, "lookup", lambda *a, **k: None)
    _, sp = await _fit(monkeypatch, climatology=clim())
    assert sp["irrigation_need"] is None and sp["irrigation_need_unavailable"] == "no_sowing_date"


async def test_irrigated_parcel_without_centroid(monkeypatch):
    _, sp = await _fit(monkeypatch, climatology=clim(), centroid={"lat": None, "lon": None})
    assert sp["irrigation_need"] is None and sp["irrigation_need_unavailable"] == "weather_unavailable"


async def test_stage_read_failure_is_kc_unavailable(monkeypatch):
    dao = AsyncMock()
    dao.get_parcel_environment.return_value = ENV
    monkeypatch.setattr(service, "fetch_crop_cycles", AsyncMock(return_value=None))
    monkeypatch.setattr(service, "load_reference", AsyncMock(return_value=REF))
    def broken():
        raise ValueError("fao56_stage_lengths.yaml: no crops")
    monkeypatch.setattr(service, "load_stage_table", broken)
    monkeypatch.setattr(service, "schedule_climatology", lambda cell: None)
    monkeypatch.setattr(service, "get_climatology", lambda cell: clim())
    monkeypatch.setattr(service, "climatology_status", lambda cell: "ready")
    out = await service.parcel_fit(dao=dao, source=KnowledgeSource("global", None), parcel_id="p1", tenant_id="t",
                                   campaign=2027, regime="irrigated")
    wheat = next(s for s in out["species"] if s["species"] == "wheat")
    assert wheat["irrigation_need_unavailable"] == "kc_unavailable"


async def test_unknown_regime_has_no_need_and_no_reason(monkeypatch):
    _, sp = await _fit(monkeypatch, regime=None, climatology=clim())
    assert sp["irrigation_need"] is None and sp["irrigation_need_unavailable"] is None
    assert "regime_missing" in codes(sp)


async def test_need_carries_licensed_attributions(monkeypatch):
    _, sp = await _fit(monkeypatch, climatology=clim())
    ids = {a["source_id"] for a in sp["irrigation_need"]["attributions"]}
    assert ids == {"open_meteo_historical", "copernicus_era5", "fao56"}
    texts = " ".join(a["text"] for a in sp["irrigation_need"]["attributions"])
    assert "Open-Meteo.com" in texts and "Copernicus Climate Change Service" in texts
