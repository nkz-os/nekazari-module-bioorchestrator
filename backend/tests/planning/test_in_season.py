"""F5: in-season inputs come from facts only; every gap is a reason code."""
from datetime import date

import pytest

from app.planning.in_season import MAX_SEASON_DAYS, InSeasonInputs, resolve_in_season

TODAY = date(2026, 3, 15)
PARCEL = {"id": "urn:ngsi-ld:AgriParcel:t:p1", "irrigationRegime": "rainfed",
          "location": {"type": "Polygon", "coordinates": [[[-1.6, 42.8], [-1.5, 42.8], [-1.5, 42.9],
                                                          [-1.6, 42.9], [-1.6, 42.8]]]}}
CROP_ID = "urn:ngsi-ld:AgriCrop:t:c1"
CROPS = {CROP_ID: {"id": CROP_ID, "species": "TRZAX", "irrigationRegime": "rainfed"}}


def cycles(provenance="actual", day="2025-10-20", species="TRZAX", crop_id=CROP_ID):
    return {"current": {"crop_id": crop_id, "species": species, "status": "active", "lifecycle": "annual",
                        "start": {"date": day, "provenance": provenance},
                        "end": {"date": None, "provenance": None}}}


def test_a_sown_wheat_parcel_resolves_every_input():
    got = resolve_in_season(PARCEL, CROPS, cycles(), TODAY)
    assert got == InSeasonInputs(crop_slug="wheat", eppo="TRZAX", regime="rainfed", regime_source="crop",
                                 country="ES", sowing_date=date(2025, 10, 20), sowing_provenance="actual")


def test_a_declared_start_counts_as_sown():
    assert resolve_in_season(PARCEL, CROPS, cycles(provenance="declared"), TODAY).sowing_provenance == "declared"


@pytest.mark.parametrize("provenance", ["planned", "placeholder", "unknown", None])
def test_a_start_that_is_not_a_fact_is_not_sown(provenance):
    # The core may link a PLANNED cycle whose planned start already passed as the current crop.
    assert resolve_in_season(PARCEL, CROPS, cycles(provenance=provenance), TODAY) == "not_sown"


def test_core_unreachable_is_said_not_guessed():
    assert resolve_in_season(PARCEL, CROPS, None, TODAY) == "cycles_unavailable"


def test_no_current_cycle_is_no_committed_crop():
    assert resolve_in_season(PARCEL, CROPS, {"current": None}, TODAY) == "no_committed_crop"


def test_a_pending_planned_cycle_is_not_sown():
    # Core (2026-10-13): a planned cycle whose date passed without a sowing is "pending", not current.
    pending = {"crop_id": CROP_ID, "species": "TRZAX", "start": {"date": "2025-10-20", "provenance": "planned"}}
    assert resolve_in_season(PARCEL, CROPS, {"current": None, "pending": pending}, TODAY) == "not_sown"


def test_a_fact_without_a_date_has_no_sowing_date():
    assert resolve_in_season(PARCEL, CROPS, cycles(day=None), TODAY) == "no_sowing_date"


@pytest.mark.parametrize("species", ["TRZAX", "wheat", "Triticum aestivum"])
def test_species_resolves_from_eppo_slug_or_name(species):
    assert resolve_in_season(PARCEL, CROPS, cycles(species=species), TODAY).eppo == "TRZAX"


def test_an_unknown_species_is_unknown_crop():
    assert resolve_in_season(PARCEL, CROPS, cycles(species="Dragonfruit"), TODAY) == "unknown_crop"


def test_a_deleted_crop_entity_falls_back_to_the_cycle_species_and_the_parcel_regime():
    got = resolve_in_season(PARCEL, {}, cycles(crop_id="urn:ngsi-ld:AgriCrop:t:gone"), TODAY)
    assert (got.eppo, got.regime, got.regime_source) == ("TRZAX", "rainfed", "parcel:irrigationRegime")


def test_the_crop_regime_wins_and_legacy_words_are_read():
    parcel = {**PARCEL, "irrigationRegime": "regadío"}
    crops = {CROP_ID: {**CROPS[CROP_ID], "irrigationRegime": "secano"}}
    assert resolve_in_season(parcel, crops, cycles(), TODAY).regime == "rainfed"


def test_no_regime_anywhere_is_never_supposed():
    parcel = {k: v for k, v in PARCEL.items() if k != "irrigationRegime"}
    crops = {CROP_ID: {"id": CROP_ID, "species": "TRZAX"}}
    assert resolve_in_season(parcel, crops, cycles(), TODAY) == "no_regime"


def test_barley_is_not_validated():
    crops = {CROP_ID: {"id": CROP_ID, "species": "HORVX", "irrigationRegime": "rainfed"}}
    assert resolve_in_season(PARCEL, crops, cycles(species="HORVX"), TODAY) == "not_validated"


def test_a_parcel_without_location_is_not_validated():
    parcel = {k: v for k, v in PARCEL.items() if k != "location"}
    assert resolve_in_season(parcel, CROPS, cycles(), TODAY) == "not_validated"


def test_a_future_sowing_is_preseason():
    assert resolve_in_season(PARCEL, CROPS, cycles(provenance="declared", day="2026-04-01"),
                             TODAY) == "sowing_in_future"


def test_a_stale_current_cycle_is_season_too_old():
    old = date.fromordinal(TODAY.toordinal() - MAX_SEASON_DAYS - 1).isoformat()
    assert resolve_in_season(PARCEL, CROPS, cycles(day=old), TODAY) == "season_too_old"
