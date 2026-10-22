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
        rows += _rows((f"S{i}", 2020 + i), {"OLD": 5000, "X": 5000, "Y": 5000})
    rows += _rows(("S9", 2025), {"NEW": 9000, "X": 5000, "Y": 5000})
    stats, _ = variety_stats(rows)
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
