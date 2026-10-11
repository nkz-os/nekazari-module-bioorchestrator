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
