from app.planning.regional_yield import regional_yield
from app.services.eurostat_yields import PlausibleRange


def rows(nuts, values, start=2000, hum=None):
    return [{"nuts": nuts, "year": start + i, "yieldTHa": v, "humidityPct": hum}
            for i, v in enumerate(values)]


def test_twelve_years_uses_last_ten():
    ry, reason = regional_yield(rows("ES11", [1.0] * 2 + [2.0] * 10), rows("ES", [2.0] * 12))
    assert reason is None
    assert (ry.first_year, ry.last_year, ry.n_years) == (2002, 2011, 10)
    assert ry.percentiles.p50 == 2.0
    assert ry.nuts == "ES11"


def test_short_region_series():
    assert regional_yield(rows("ES11", [2.0] * 4), rows("ES", [2.0] * 4)) == (None, "region_series_short")


def test_invalid_years_do_not_count():
    r = rows("ES11", [2.0, 2.0, 2.0, 2.0, None, 0, True])
    assert regional_yield(r, rows("ES", [2.0] * 7)) == (None, "region_series_short")


def test_year_above_registry_max_excluded():
    rng = PlausibleRange(min_t_ha=0.5, max_t_ha=10.0, range_id="x", status="assumption")
    ry, _ = regional_yield(rows("ES11", [2.0, 2.0, 2.0, 2.0, 2.0, 50.0]), rows("ES", [2.0] * 6), plausible=rng)
    assert ry.n_years == 5 and ry.last_year == 2004
    assert ry.percentiles.p90 == 2.0


def test_year_below_min_stays():
    rng = PlausibleRange(min_t_ha=0.5, max_t_ha=10.0, range_id="x", status="assumption")
    ry, _ = regional_yield(rows("ES11", [2.0] * 5 + [0.1]), rows("ES", [2.0] * 6), plausible=rng)
    assert ry.n_years == 6


def test_short_country_gives_number_without_index():
    ry, reason = regional_yield(rows("ES11", [3.0] * 8), rows("ES", [2.0] * 4, start=2004))
    assert ry is not None and ry.relative_index is None
    assert reason == "national_series_short"


def test_index_uses_same_years_only():
    # country has extra old years with a very different level: they must not enter the base
    region = rows("ES11", [3.0] * 6, start=2005)
    country = rows("ES", [100.0] * 5, start=2000) + rows("ES", [2.0] * 6, start=2005)
    ry, reason = regional_yield(region, country)
    assert reason is None
    assert ry.relative_index == 1.5


def test_labels():
    ry, _ = regional_yield(rows("ES11", [2.0] * 5, hum=13.0), rows("ES", [2.0] * 5))
    assert ry.moisture.basis == "eu_standard" and ry.moisture.pct == 13.0
    assert ry.regimes == "mixed" and ry.basis == "regional_statistics"
    assert ry.provider_id == "eurostat_nuts2" and ry.unit == "t/ha"
