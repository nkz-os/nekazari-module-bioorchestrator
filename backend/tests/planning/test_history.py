from app.planning.history import (
    cell_from_cycle,
    cycle_campaigns,
    history_complete,
    past_crops,
    plain,
)

PAYLOAD = {"cycles": [
    {"crop_id": "c1", "role": "main_crop", "species": "TRZAX", "status": "harvested",
     "start": {"date": "2025-11-02", "provenance": "actual"}, "end": {"date": "2026-07-01", "provenance": "actual"},
     "campaign_label": 2026},
    {"crop_id": "c2", "role": "main_crop", "species": "Zea mays", "status": "planned",
     "start": {"date": "2027-04-20", "provenance": "planned"}, "end": {"date": "2027-10-01", "provenance": "planned"},
     "campaign_label": 2027},
]}


def test_past_crops_are_slugs_before_the_campaign():
    assert [(p.species, p.campaign) for p in past_crops(PAYLOAD, 2027)] == [("wheat", 2026)]


def test_cell_reads_variety_and_provenance():
    cell = cell_from_cycle(PAYLOAD["cycles"][1], {"variety": {"type": "Property", "value": "P1524"}})
    assert (cell["species"], cell["variety"], cell["provenance"]) == ("maize", "P1524", "planned")
    assert cell["names"]["es"]


ALFALFA = {"crop_id": "a1", "role": "main_crop", "species": "MEDSA", "status": "active", "lifecycle": "perennial",
           "start": {"date": "2024-09-15", "provenance": "actual"}, "end": {"date": None, "provenance": None},
           "campaign_label": 2024}


def test_open_perennial_occupies_every_campaign_until_the_target():
    # Alfalfa is planned like any crop but spans campaigns.
    assert cycle_campaigns(ALFALFA, until=2027) == [2025, 2026, 2027]
    assert [(p.species, p.campaign) for p in past_crops({"cycles": [ALFALFA]}, 2027)] == [("alfalfa", 2025), ("alfalfa", 2026)]
    cell = cell_from_cycle(ALFALFA, None)
    assert (cell["lifecycle"], cell["first_campaign"]) == ("perennial", 2025)


def test_plain_unwraps_dates_and_properties():
    assert plain({"@type": "Date", "@value": "2027-04-20"}) == "2027-04-20"
    assert plain({"type": "Property", "value": "x"}) == "x"
    assert plain("y") == "y"


def test_history_is_complete_only_when_every_cycle_is_placed():
    assert history_complete(PAYLOAD) and history_complete({"cycles": [ALFALFA]})
    assert history_complete({"cycles": []})  # no cycles: a known, empty history
    unknown_species = {**PAYLOAD["cycles"][0], "species": "Unobtainium plantae"}
    assert not history_complete({"cycles": [PAYLOAD["cycles"][0], unknown_species]})
    no_campaign = {k: v for k, v in PAYLOAD["cycles"][0].items() if k != "campaign_label"}
    assert not history_complete({"cycles": [no_campaign]})
    assert not history_complete(None)
