import datetime

import pytest

import app.planning.varieties as varieties_mod
from app.planning.varieties import fw_slope, stability_class, variety_stats


def _rows(env, values):
    site, year = env
    return [{"variety": v, "site": site, "year": year, "source_id": "GENVCE", "yield_kg_ha": y} for v, y in values.items()]


def test_relative_yield_cancels_the_environment():
    rows = _rows(("A", 2024), {"V1": 6000, "V2": 5000, "V3": 4000}) + _rows(("B", 2025), {"V1": 3000, "V2": 2500, "V3": 2000})
    stats, n_env = variety_stats(rows)
    assert n_env == 2
    v1 = next(s for s in stats if s["variety"] == "V1")
    assert v1["relative_yield_pct"] == 120.0 and v1["n_environments"] == 2 and v1["years"] == [2024, 2025]
    assert v1["few_environments"] is True and v1["fw_slope"] is None


def test_environment_with_too_few_varieties_is_ignored():
    stats, n_env = variety_stats(_rows(("A", 2024), {"V1": 6000, "V2": 5000}))
    assert (stats, n_env) == ([], 0)


def test_finlay_wilkinson_slope_and_bands():
    assert fw_slope([1.0, 2.0, 3.0], [2.0, 4.0, 6.0]) == 2.0
    assert fw_slope([2.0, 2.0], [1.0, 3.0]) is None
    assert (stability_class(0.7), stability_class(1.0), stability_class(1.3), stability_class(None)) == \
        ("stable", "average", "responsive", None)


def test_many_environment_varieties_rank_before_few_environment_ones():
    rows = []
    for i in range(3):
        rows += _rows((f"S{i}", 2023 + i), {"OLD": 5000, "X": 5000, "Y": 5000})
    rows += _rows(("S9", 2025), {"NEW": 9000, "X": 5000, "Y": 5000})
    stats, _ = variety_stats(rows, ref_year=2025)  # every variety is current: only the environment count sorts
    assert stats[0]["variety"] != "NEW" and stats[-1]["variety"] == "NEW"


def test_duplicate_rows_of_a_variety_in_one_environment_are_averaged():
    # V1 has two rows in A (4000 and 8000): the mean 6000 counts, not the max 8000.
    rows = _rows(("A", 2024), {"V1": 4000, "V2": 5000, "V3": 6000}) + _rows(("A", 2024), {"V1": 8000})
    stats, n_env = variety_stats(rows)
    v1 = next(s for s in stats if s["variety"] == "V1")
    assert n_env == 1 and v1["mean_kg_ha"] == 6000 and v1["relative_yield_pct"] == 105.9


def test_rows_without_site_or_year_define_no_environment():
    ok = _rows(("A", 2024), {"V1": 6000, "V2": 5000, "V3": 4000})
    no_site = [{"variety": v, "site": None, "year": 2025, "source_id": "GENVCE", "yield_kg_ha": 5000}
               for v in ("V1", "V2", "V3")]
    no_year = [{"variety": v, "site": "B", "year": None, "source_id": "GENVCE", "yield_kg_ha": 5000}
               for v in ("V1", "V2", "V3")]
    stats, n_env = variety_stats(ok + no_site + no_year)
    assert n_env == 1 and all(s["n_environments"] == 1 and s["years"] == [2024, 2024] for s in stats)


def _five_environments():
    rows = []
    for i in range(5):
        rows += _rows((f"S{i}", 2020 + i), {"V1": 5000, "V2": 5000, "V3": 5000})
    return rows


def test_stability_is_classified_on_the_rounded_slope(monkeypatch):
    # 0.8496 is shown as 0.85, which is "average": a shown 0.85 is never "stable".
    monkeypatch.setattr(varieties_mod, "fw_slope", lambda *_: 0.8496)
    v1 = next(s for s in variety_stats(_five_environments())[0] if s["variety"] == "V1")
    assert v1["fw_slope"] == 0.85 and v1["stability"] == "average"
    # 1.1504 is shown as 1.15, which is "average": a shown 1.15 is never "responsive".
    monkeypatch.setattr(varieties_mod, "fw_slope", lambda *_: 1.1504)
    v1 = next(s for s in variety_stats(_five_environments())[0] if s["variety"] == "V1")
    assert v1["fw_slope"] == 1.15 and v1["stability"] == "average"
    monkeypatch.setattr(varieties_mod, "fw_slope", lambda *_: 0.8449)
    v1 = next(s for s in variety_stats(_five_environments())[0] if s["variety"] == "V1")
    assert v1["fw_slope"] == 0.84 and v1["stability"] == "stable"


def _low_high_rows(high_means=(5000, 6000, 7000, 8000)):
    """Two low-yield environments where A holds (+20 %) and B drops (-20 %), then high ones where all tie."""
    rows = []
    for i, m in enumerate((2000, 2500)):
        rows += _rows((f"L{i}", 2020 + i), {"A": m * 1.2, "B": m * 0.8, "C": m})
    for i, m in enumerate(high_means):
        rows += _rows((f"H{i}", 2022 + i), {"A": m, "B": m, "C": m})
    return rows


def test_low_environment_floor_tells_who_holds_in_poor_trials():
    stats, _ = variety_stats(_low_high_rows())
    by = {s["variety"]: s for s in stats}
    assert by["A"]["low_env_relative_yield_pct"] == 120.0 and by["A"]["n_low_environments"] == 2
    assert by["B"]["low_env_relative_yield_pct"] == 80.0
    assert by["C"]["low_env_relative_yield_pct"] == 100.0


def test_low_environment_floor_needs_enough_environments():
    # 4 environments < MIN_FW_ENVIRONMENTS: no floor, as with the stability slope.
    stats, _ = variety_stats(_low_high_rows(high_means=(5000, 6000)))
    assert all(s["low_env_relative_yield_pct"] is None for s in stats)
    assert all(s["n_low_environments"] == 2 for s in stats)


def test_low_environment_floor_needs_a_spread_of_environment_means():
    rows = []
    for i in range(6):
        rows += _rows((f"S{i}", 2020 + i), {"A": 6000, "B": 5000, "C": 4000})
    stats, _ = variety_stats(rows)
    assert all(s["low_env_relative_yield_pct"] is None and s["n_low_environments"] == 0 for s in stats)


def test_low_environment_floor_needs_two_low_observations_of_the_variety():
    rows = _rows(("L0", 2020), {"A": 2400, "B": 1600, "C": 2000})
    rows += _rows(("L1", 2021), {"B": 2000, "C": 2500, "D": 2500})  # A not in the other poor trials
    rows += _rows(("L2", 2022), {"B": 2200, "C": 2600, "D": 2600})
    for i, m in enumerate((6000, 7000, 8000, 9000)):
        rows += _rows((f"H{i}", 2023 + i), {"A": m, "B": m, "C": m, "D": m})
    by = {s["variety"]: s for s in variety_stats(rows)[0]}
    assert by["A"]["n_environments"] == 5 and by["A"]["n_low_environments"] == 1
    assert by["A"]["low_env_relative_yield_pct"] is None
    assert by["B"]["n_low_environments"] == 3 and by["B"]["low_env_relative_yield_pct"] is not None


def test_response_model_keeps_the_floor():
    from app.planning.models import VarietyStat

    stats, _ = variety_stats(_low_high_rows())
    dumped = VarietyStat.model_validate(stats[0]).model_dump()
    assert dumped["low_env_relative_yield_pct"] == stats[0]["low_env_relative_yield_pct"]
    assert dumped["n_low_environments"] == 2


# --- recency weighting (half-life RECENCY_HALF_LIFE_YEARS, relative yield only) --------------

def test_relative_yield_weights_an_observation_eight_years_older_at_half():
    # V1: 130 % in the old environment, 100 % in the recent one; V2/V3 fill the environments.
    rows = _rows(("A", 2016), {"V1": 6500, "V2": 5000, "V3": 3500}) + _rows(("B", 2024), {"V1": 5000, "V2": 5000, "V3": 5000})
    flat, _ = variety_stats(rows, ref_year=2016)  # nothing is older than the reference: every weight is 1
    weighted, _ = variety_stats(rows, ref_year=2024)
    v1_flat = next(s for s in flat if s["variety"] == "V1")["relative_yield_pct"]
    v1_weighted = next(s for s in weighted if s["variety"] == "V1")["relative_yield_pct"]
    assert v1_flat == pytest.approx(115.0)
    assert v1_weighted == pytest.approx((0.5 * 1.3 + 1.0 * 1.0) / 1.5 * 100, abs=0.05)  # 110.0
    assert varieties_mod.RECENCY_HALF_LIFE_YEARS == 8.0


def test_ref_year_defaults_to_the_current_year():
    this_year = datetime.datetime.now(datetime.UTC).year
    rows = _rows(("A", this_year - 8), {"V1": 6500, "V2": 5000, "V3": 3500}) + _rows(("B", this_year), {"V1": 5000, "V2": 5000, "V3": 5000})
    default, _ = variety_stats(rows)
    explicit, _ = variety_stats(rows, ref_year=this_year)
    assert default[0]["relative_yield_pct"] == explicit[0]["relative_yield_pct"]


def test_recency_leaves_the_low_environment_floor_and_the_mean_flat():
    # Six environments; the only low-yield (dry) ones are old. The floor and mean_kg_ha must not move.
    rows = []
    for i, (year, level) in enumerate([(2012, 2000), (2013, 2200), (2018, 5000), (2020, 5200), (2022, 5400), (2024, 5600)]):
        rows += _rows((f"S{i}", year), {"V1": level * 1.2, "V2": level, "V3": level * 0.8})
    flat, _ = variety_stats(rows, ref_year=2012)
    weighted, _ = variety_stats(rows, ref_year=2024)
    f = next(s for s in flat if s["variety"] == "V1")
    w = next(s for s in weighted if s["variety"] == "V1")
    assert w["low_env_relative_yield_pct"] == f["low_env_relative_yield_pct"] == pytest.approx(120.0)
    assert w["mean_kg_ha"] == f["mean_kg_ha"] and w["fw_slope"] == f["fw_slope"]


# --- actuality: a variety without a recent trial is demoted, never hidden -------------------------

def _grid(years, names, site="A"):
    rows = []
    for y in years:
        rows += _rows((site, y), {n: 5000 + 100 * i for i, n in enumerate(names)})
    return rows


def test_variety_without_a_trial_in_the_last_three_campaigns_is_stale_and_demoted():
    # OLD: best yields, but last trialled in 2021. NEW: trialled every year through 2025.
    rows = _grid(range(2016, 2022), ["LOW", "MID", "OLD"]) + _grid(range(2019, 2026), ["LOW", "MID", "NEW"])
    stats, _ = variety_stats(rows, ref_year=2025)
    by = {s["variety"]: s for s in stats}
    assert by["OLD"]["stale"] is True and by["OLD"]["last_year"] == 2021
    assert by["NEW"]["stale"] is False and by["NEW"]["last_year"] == 2025
    assert stats[-1]["variety"] == "OLD"  # demoted to the end
    assert varieties_mod.RECENT_TRIAL_YEARS == 3


def test_the_actuality_window_counts_campaigns_inclusively():
    rows = _grid(range(2016, 2024), ["LOW", "MID", "EDGE"]) + _grid(range(2019, 2026), ["LOW", "MID", "NEW"])
    by = {s["variety"]: s for s in variety_stats(rows, ref_year=2025)[0]}
    assert by["EDGE"]["last_year"] == 2023 and by["EDGE"]["stale"] is False  # 2023, 2024, 2025 = 3 campaigns
    by = {s["variety"]: s for s in variety_stats(rows, ref_year=2026)[0]}
    assert by["EDGE"]["stale"] is True


def test_stale_varieties_rank_after_recent_ones_even_with_few_environments():
    rows = _grid(range(2016, 2022), ["LOW", "MID", "OLD"]) + _grid([2025], ["LOW", "MID", "FEW"])
    stats, _ = variety_stats(rows, ref_year=2025)
    names = [s["variety"] for s in stats]
    assert names.index("FEW") < names.index("OLD")


def test_a_reference_year_before_every_trial_disables_the_rule():
    rows = _grid(range(2016, 2022), ["LOW", "MID", "OLD"]) + _grid(range(2019, 2026), ["LOW", "MID", "NEW"])
    stats, _ = variety_stats(rows, ref_year=0)
    assert all(s["stale"] is False for s in stats) and stats[0]["variety"] in ("OLD", "NEW")
