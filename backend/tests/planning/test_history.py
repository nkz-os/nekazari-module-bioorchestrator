from datetime import date

from app.planning.history import (
    cell_from_cycle,
    cycle_campaigns,
    history_complete,
    missing_history_campaigns,
    past_crops,
    plain,
)

NOV, MAR = date(2026, 11, 3), date(2026, 3, 1)

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


def _main(label, **over):
    return {"crop_id": f"c{label}", "role": "main_crop", "species": "TRZAX", "status": "harvested",
            "campaign_label": label, **over}


def test_missing_history_is_none_when_the_read_failed():
    assert missing_history_campaigns(None, 2027) is None


def test_missing_history_is_every_lookback_campaign_when_there_are_no_cycles():
    assert missing_history_campaigns({"cycles": []}, 2027, NOV) == [2025, 2026]


def test_missing_history_ignores_campaigns_with_a_main_crop_dated_or_declared():
    declared = _main(2025, start={"date": None}, end={"date": None}, declared_campaign=2025)
    assert missing_history_campaigns({"cycles": [_main(2026), declared]}, 2027, NOV) == []
    assert missing_history_campaigns({"cycles": [_main(2026)]}, 2027, NOV) == [2025]


def test_missing_history_does_not_count_covers_cancelled_or_later_cycles():
    cover = _main(2026, role="cover_crop")
    cancelled = _main(2025, status="cancelled")
    planned_later = _main(2027, status="planned")
    assert missing_history_campaigns({"cycles": [cover, cancelled, planned_later]}, 2027, NOV) == [2025, 2026]


def test_missing_history_counts_a_perennial_spanning_the_campaigns():
    alfalfa = {"crop_id": "a1", "role": "main_crop", "species": "MEDSA", "status": "active", "lifecycle": "perennial",
               "start": {"date": "2024-09-15"}, "end": {"date": None}, "campaign_label": 2025}
    assert missing_history_campaigns({"cycles": [alfalfa]}, 2027, NOV) == []


def test_missing_history_never_offers_a_campaign_the_platform_would_refuse():
    # Planning moves to 2027 from July: 2026 is over in November, still running in March.
    assert missing_history_campaigns({"cycles": []}, 2027, NOV) == [2025, 2026]
    assert missing_history_campaigns({"cycles": []}, 2027, MAR) == [2025]
    assert missing_history_campaigns({"cycles": []}, 2026, MAR) == [2024, 2025]
    assert missing_history_campaigns({"cycles": []}, 2027, date(2026, 6, 30)) == [2025]
    assert missing_history_campaigns({"cycles": []}, 2027, date(2026, 7, 1)) == [2025, 2026]


def test_a_declared_only_cycle_is_history_only_when_closed():
    def declared(status):
        return _main(2025, start={"date": None}, end={"date": None}, declared_campaign=2025, status=status)
    for status in ("harvested", "terminated"):
        assert missing_history_campaigns({"cycles": [declared(status)]}, 2027, NOV) == [2026]
        assert [(p.species, p.campaign) for p in past_crops({"cycles": [declared(status)]}, 2027)] == [("wheat", 2025)]
    for status in ("active", "planned", None):
        assert missing_history_campaigns({"cycles": [declared(status)]}, 2027, NOV) == [2025, 2026]
        assert past_crops({"cycles": [declared(status)]}, 2027) == []
