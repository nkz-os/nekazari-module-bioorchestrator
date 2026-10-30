from unittest.mock import AsyncMock

import pytest

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


async def test_unplaceable_cycle_makes_rotation_unknown_even_with_rules(monkeypatch):
    # A cycle whose species or campaign cannot be placed hides a possible restriction.
    from app.planning.rotation import Constraint

    ref = Reference(tolerances={}, trial_eppo=frozenset({"TRZAX"}), constraints=(
        Constraint("wheat", "wheat", "restriction", 1, "disease carry-over", "src"),))
    history = {"cycles": [
        {"crop_id": "c1", "species": "TRZAX", "status": "harvested", "campaign_label": 2026,
         "start": {"date": "2025-11-02", "provenance": "actual"}, "end": {"date": "2026-07-01"}},
        {"crop_id": "c2", "species": "Unobtainium plantae", "status": "harvested", "campaign_label": 2025,
         "start": {"date": "2024-11-02", "provenance": "actual"}, "end": {"date": "2025-07-01"}},
    ]}
    dao = AsyncMock()
    dao.get_parcel_environment.return_value = BARE_ENV
    monkeypatch.setattr(service, "fetch_crop_cycles", AsyncMock(return_value=history))
    monkeypatch.setattr(service, "load_reference", AsyncMock(return_value=ref))
    out = await service.parcel_fit(**_fit_args(dao))
    for s in out["species"]:
        codes = {r["code"] for r in s["reasons"]}
        assert "rotation_unknown" in codes and "rotation_restriction" not in codes


# --- final-review fixes -------------------------------------------------------------------------

def _suitable(*_args, **_kwargs):
    return {"verdict": "suitable"}


def _soil_env(climate_class=None, climate_detail=None):
    return {**BARE_ENV, "climate_class": climate_class, "climate_detail": climate_detail,
            "soil": {"data_available": True, "source": "test", "ph": 7.0, "texture": "loam"}}


def _tolerant_ref(frost_c=-15.0):
    from app.planning.reference import SpeciesTolerance
    from app.planning.rotation import Constraint

    soil = {"ph_min": 5.5, "ph_max": 8.0, "textures": ["loam"], "drainage": []}
    slugs = ("soybean", "sunflower", "strawberry", "alfalfa", "wheat")
    return Reference(
        tolerances={s: SpeciesTolerance(soil=soil, frost_damage_c=frost_c, soil_source="t", frost_source="t")
                    for s in slugs},
        constraints=(Constraint("rapeseed", "rapeseed", "restriction", 3, "clubroot", "FAO"),),
        trial_eppo=frozenset())


def _setup(monkeypatch, env, *, ref=None, cycles=None, regime_ref=None):
    dao = AsyncMock()
    dao.get_parcel_environment.return_value = env
    monkeypatch.setattr(service, "fetch_crop_cycles", AsyncMock(return_value={"cycles": []} if cycles is None else cycles))
    monkeypatch.setattr(service, "load_reference", AsyncMock(return_value=ref or _tolerant_ref()))
    monkeypatch.setattr(service, "assess_soil_suitability", _suitable)
    return dao


def _codes(out, slug):
    return {r["code"] for r in next(s for s in out["species"] if s["species"] == slug)["reasons"]}


@pytest.mark.parametrize("detail", [None, {}, {"source": "x"}])
async def test_unknown_climate_is_a_caveat_so_nothing_fits(monkeypatch, detail):
    # Soil, regime and history all known; no climate: it must not read as "fits".
    dao = _setup(monkeypatch, _soil_env(climate_class=None, climate_detail=detail))
    out = await service.parcel_fit(**{**_fit_args(dao), "regime": "irrigated"})
    FitResponse.model_validate(out)
    assert all(s["verdict"] != "fits" for s in out["species"])
    for slug in ("soybean", "sunflower", "strawberry"):
        assert "climate_unknown" in _codes(out, slug)
    reason = next(r for r in next(s for s in out["species"] if s["species"] == "soybean")["reasons"]
                  if r["code"] == "climate_unknown")
    assert reason == {"code": "climate_unknown", "gate": "data", "params": {}}


async def test_climate_class_without_detail_is_still_unknown(monkeypatch):
    dao = _setup(monkeypatch, _soil_env(climate_class="Csa", climate_detail=None))
    out = await service.parcel_fit(**{**_fit_args(dao), "regime": "irrigated"})
    assert "climate_unknown" in _codes(out, "soybean")


async def test_known_climate_has_no_climate_unknown(monkeypatch):
    detail = {"coldest_month_min_c": 3.0, "annual_rainfall_mm": 600.0, "annual_et0_mm": 1000.0, "source": "x"}
    dao = _setup(monkeypatch, _soil_env(climate_class="Csa", climate_detail=detail))
    out = await service.parcel_fit(**{**_fit_args(dao), "regime": "irrigated"})
    assert all("climate_unknown" not in {r["code"] for r in s["reasons"]} for s in out["species"])


COLD = {"coldest_month_min_c": -12.0, "annual_rainfall_mm": 600.0, "annual_et0_mm": 900.0, "source": "x"}
MILD = {"coldest_month_min_c": 3.0, "annual_rainfall_mm": 600.0, "annual_et0_mm": 900.0, "source": "x"}


async def test_perennial_gets_the_frost_gate_evaluated(monkeypatch):
    # strawberry stands through winter: coldest -12 - margin 5 <= threshold -10 -> frost_risk.
    dao = _setup(monkeypatch, _soil_env("Dfb", COLD), ref=_tolerant_ref(frost_c=-10.0))
    out = await service.parcel_fit(**{**_fit_args(dao), "regime": "irrigated"})
    strawberry = next(s for s in out["species"] if s["species"] == "strawberry")
    assert "frost_risk" in _codes(out, "strawberry") and strawberry["verdict"] == "no_fit"


async def test_perennial_with_climate_and_tolerance_has_no_frost_unknown(monkeypatch):
    dao = _setup(monkeypatch, _soil_env("Csa", MILD), ref=_tolerant_ref(frost_c=-15.0))
    out = await service.parcel_fit(**{**_fit_args(dao), "regime": "irrigated"})
    codes = _codes(out, "alfalfa")
    assert "frost_unknown" not in codes and "frost_risk" not in codes


async def test_failing_reference_load_does_not_take_the_fit_down(monkeypatch, caplog):
    dao = _setup(monkeypatch, _soil_env("Csa", MILD))
    monkeypatch.setattr(service, "load_reference", AsyncMock(side_effect=RuntimeError("graph down")))
    with caplog.at_level("ERROR", logger=service.logger.name):
        out = await service.parcel_fit(**{**_fit_args(dao), "regime": "irrigated"})
    FitResponse.model_validate(out)
    assert out["species"] and all(s["verdict"] != "fits" for s in out["species"])
    codes = _codes(out, "wheat")
    assert {"rotation_unknown", "soil_unknown"} <= codes
    assert any("graph down" in r.getMessage() or r.exc_info for r in caplog.records)


@pytest.mark.parametrize("answer", [[], "oops", 3, ["cycles"]])
async def test_crop_cycles_answer_that_is_not_an_object_is_history_unavailable(monkeypatch, answer):
    dao = _setup(monkeypatch, _soil_env("Csa", MILD), cycles=answer)
    out = await service.parcel_fit(**{**_fit_args(dao), "regime": "irrigated"})
    assert out["facts"]["history_available"] is False
    assert all("rotation_unknown" in {r["code"] for r in s["reasons"]} for s in out["species"])


async def test_cover_only_species_are_flagged(monkeypatch):
    dao = AsyncMock()
    dao.get_parcel_environment.return_value = BARE_ENV
    monkeypatch.setattr(service, "fetch_crop_cycles", AsyncMock(return_value=None))
    monkeypatch.setattr(service, "load_reference", AsyncMock(return_value=EMPTY_REF))
    out = await service.parcel_fit(dao=dao, source=KnowledgeSource("global", None), parcel_id="p1",
                                   tenant_id="t", campaign=2027, regime=None)
    FitResponse.model_validate(out)
    by_slug = {s["species"]: s for s in out["species"]}
    assert by_slug["hairy_vetch"]["cover_only"] is True and by_slug["crimson_clover"]["cover_only"] is True
    assert by_slug["wheat"]["cover_only"] is False and by_slug["vetch"]["cover_only"] is False
