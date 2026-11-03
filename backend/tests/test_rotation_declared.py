"""Rotation gate reads crops a farmer declared for past campaigns (no dates)."""
from datetime import date
from unittest.mock import AsyncMock

from nkz_platform_sdk.crop_cycles import resolve_crop_cycles

from app.planning import service
from app.planning.history import history_complete, past_crops
from app.planning.reference import Reference
from app.planning.rotation import Constraint, PastCrop
from tests.planning.test_fit_service import BARE_ENV, _fit_args

P = "urn:ngsi-ld:AgriParcel:p1"
AT = date(2026, 11, 3)


def _crop(cid, **a):
    return {"id": f"urn:ngsi-ld:AgriCrop:acme:{cid}", "type": "AgriCrop", "hasAgriParcel": P, **a}


def _declared(cid, year, species="wheat"):
    return _crop(cid, status="harvested", species=species, **{"nkz:declaredCampaign": year})


def _dated(cid, planted, harvested, species="barley"):
    return _crop(cid, status="harvested", species=species, plantingDate=planted, harvestDate=harvested,
                 plantingDateSource="manual")


def _payload(crops):
    return resolve_crop_cycles(P, crops, [], AT).to_dict()


def test_declared_cycles_become_past_crops_and_complete_history():
    payload = _payload([_declared("a", 2024), _declared("b", 2025, "barley")])
    assert history_complete(payload)
    assert sorted(past_crops(payload, 2027), key=lambda p: p.campaign) == [
        PastCrop("wheat", 2024), PastCrop("barley", 2025)]


def test_dated_cycle_wins_over_declared_in_same_campaign():
    payload = _payload([_declared("a", 2025, "wheat"), _dated("d", "2024-10-20", "2025-07-10", "barley")])
    assert past_crops(payload, 2027) == [PastCrop("barley", 2025)]
    # order of the list does not matter
    payload["cycles"].reverse()
    assert past_crops(payload, 2027) == [PastCrop("barley", 2025)]


def test_declared_alone_is_kept_when_dated_is_other_campaign():
    payload = _payload([_declared("a", 2024), _dated("d", "2024-10-20", "2025-07-10", "barley")])
    assert sorted(p.campaign for p in past_crops(payload, 2027)) == [2024, 2025]


def _ref():
    return Reference(tolerances={}, trial_eppo=frozenset(), constraints=(
        Constraint("wheat", "wheat", "restriction", 1, "disease carry-over", "src"),
        Constraint("wheat", "barley", "benefit", 1, "break crop", "src")))


async def _fit(monkeypatch, cycles):
    dao = AsyncMock()
    dao.get_parcel_environment.return_value = BARE_ENV
    monkeypatch.setattr(service, "fetch_crop_cycles", AsyncMock(return_value=cycles))
    monkeypatch.setattr(service, "load_reference", AsyncMock(return_value=_ref()))
    out = await service.parcel_fit(**{**_fit_args(dao), "campaign": 2026})
    return out, lambda slug: {r["code"] for r in next(s for s in out["species"] if s["species"] == slug)["reasons"]}


async def test_gate_uses_declared_crops(monkeypatch):
    out, codes = await _fit(monkeypatch, _payload([_declared("a", 2024, "barley"), _declared("b", 2025)]))
    assert "rotation_restriction" in codes("wheat")
    assert "rotation_benefit" in codes("barley")
    assert not any("rotation_unknown" in codes(s["species"]) for s in out["species"])
    assert out["facts"]["history_available"] is True


async def test_nothing_declared_is_rotation_unknown(monkeypatch):
    out, codes = await _fit(monkeypatch, None)
    assert all("rotation_unknown" in codes(s["species"]) for s in out["species"])
    assert out["facts"]["history_available"] is False


async def test_one_declared_campaign_behaves_like_a_dated_one(monkeypatch):
    _out, codes = await _fit(monkeypatch, _payload([_declared("b", 2025)]))
    assert "rotation_restriction" in codes("wheat")
    assert "rotation_unknown" not in codes("wheat")


def _prev(crops, campaign=2027):
    return service.previous_crop(_payload(crops), campaign, maturity_for=lambda slug, start: None)


def test_previous_crop_later_declared_beats_older_dated():
    got = _prev([_declared("a", 2025, "wheat"), _dated("d", "2023-10-20", "2024-07-10", "barley")])
    assert got.crop == "wheat" and got.end_source == "campaign_start"


def test_previous_crop_same_campaign_dated_wins():
    got = _prev([_declared("a", 2024, "wheat"), _dated("d", "2023-10-20", "2024-07-10", "barley")])
    assert got.crop == "barley" and got.end == date(2024, 7, 10)


def test_previous_crop_dated_only_and_declared_only():
    assert _prev([_dated("d", "2022-10-20", "2023-07-10", "barley")]).crop == "barley"
    got = _prev([_declared("a", 2025)])
    assert (got.crop, got.end, got.end_source) == ("wheat", None, "campaign_start")
