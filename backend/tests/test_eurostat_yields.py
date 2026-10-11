"""Eurostat apro_cpshr parsing and yield derivation (pure)."""
import pytest

from app.services.eurostat_yields import (
    AREA,
    HUMIDITY,
    MIN_MEDIAN_YEARS,
    PRODUCTION,
    PlausibleRange,
    build_series,
    parse_jsonstat,
    request_url,
    to_node_props,
    yields_payload,
)


def _doc(values, status=None):
    # dims: freq(1) × crops(2) × strucpro(3) × geo(1) × time(2) — row-major flat index
    return {
        "version": "2.0", "class": "dataset", "updated": "2026-09-08T23:00:00+0200",
        "id": ["freq", "crops", "strucpro", "geo", "time"],
        "size": [1, 2, 3, 1, 2],
        "dimension": {
            "freq": {"category": {"index": {"A": 0}}},
            "crops": {"category": {"index": {"C1110": 0, "C1120": 1}}},
            "strucpro": {"category": {"index": {AREA: 0, HUMIDITY: 1, PRODUCTION: 2}}},
            "geo": {"category": {"index": {"ES22": 0}}},
            "time": {"category": {"index": {"2023": 0, "2024": 1}}},
        },
        "value": values,
        "status": status or {},
    }


# flat index = crop*6 + strucpro*2 + time
COMMON_WHEAT = {"0": 81.36, "1": 76.6, "2": 14.0, "3": 14.0, "4": 331.95, "5": 431.91}


def test_parse_decodes_flat_indices():
    cells = parse_jsonstat(_doc(COMMON_WHEAT))
    area_2024 = next(c for c in cells if c.strucpro == AREA and c.year == 2024)
    assert (area_2024.crop, area_2024.geo, area_2024.value) == ("C1110", "ES22", 76.6)


def test_parse_keeps_status_only_cells():
    cells = parse_jsonstat(_doc(COMMON_WHEAT, status={"6": "c"}))
    flagged = [c for c in cells if c.flag]
    assert flagged[0].crop == "C1120" and flagged[0].value is None and flagged[0].flag == "c"


def test_parse_rejects_other_dimensions():
    doc = _doc(COMMON_WHEAT)
    doc["id"] = ["freq", "unit", "geo", "time"]
    with pytest.raises(ValueError):
        parse_jsonstat(doc)


def test_parse_rejects_extra_multi_valued_dimension():
    doc = _doc(COMMON_WHEAT)
    doc["dimension"]["freq"]["category"]["index"] = {"A": 0, "Q": 1}
    doc["size"][0] = 2
    with pytest.raises(ValueError):
        parse_jsonstat(doc)


def test_yield_is_production_over_area_in_eu_standard_humidity():
    rows = build_series(parse_jsonstat(_doc(COMMON_WHEAT)), {"C1110": "TRZAX"})
    r2024 = next(r for r in rows if r.year == 2024)
    assert r2024.area_ha == pytest.approx(76600)
    assert r2024.production_t == pytest.approx(431910)
    assert r2024.humidity_pct == 14.0
    assert r2024.yield_t_ha == pytest.approx(431910 / 76600)
    assert r2024.eppo == "TRZAX" and r2024.nuts == "ES22"


def test_unmapped_crops_are_skipped_and_missing_area_gives_no_yield():
    values = {**COMMON_WHEAT, "10": 2.0}             # durum production 2023 without area
    rows = build_series(parse_jsonstat(_doc(values)), {"C1110": "TRZAX", "C1120": "TRZDU"})
    durum = [r for r in rows if r.eppo == "TRZDU"]
    assert durum[0].yield_t_ha is None and durum[0].production_t == pytest.approx(2000)
    assert {r.eppo for r in build_series(parse_jsonstat(_doc(values)), {"C1110": "TRZAX"})} \
        == {"TRZAX"}


def _nodes():
    rows = build_series(parse_jsonstat(_doc(COMMON_WHEAT)), {"C1110": "TRZAX"})
    return [to_node_props(r, dataset_updated="2026-09-08T23:00:00+0200",
                          extracted_at="2026-10-11T10:00:00+00:00",
                          nuts_version="as published") for r in rows]


def test_node_props_carry_provenance_and_basis():
    n = _nodes()[1]
    assert n["key"] == "apro_cpshr|ES22|C1110|2024"
    assert (n["humidityBasis"], n["yieldMethod"], n["sourceId"]) == (
        "eu_standard", "production_over_area", "eurostat_apro_cpshr")
    assert n["datasetUpdated"] and n["extractedAt"] and n["nutsVersion"]


def test_payload_series_and_recent_years():
    body = yields_payload(list(reversed(_nodes())), recent=5)
    assert [s["year"] for s in body["series"]] == [2023, 2024]
    assert body["recent_years"] == [2023, 2024]
    assert body["humidity_basis"] == "eu_standard"
    assert yields_payload(_nodes(), recent=1)["recent_years"] == [2024]
    with pytest.raises(ValueError):
        yields_payload([], recent=5)


def _three_year_nodes():
    # 2022-2024 yields of 4.0, 5.0 and 6.0 t/ha
    nodes = []
    for year, yld in [(2022, 4.0), (2023, 5.0), (2024, 6.0)]:
        n = dict(_nodes()[0])
        n.update(year=year, yieldTHa=yld, areaHa=1000.0, productionT=yld * 1000.0)
        nodes.append(n)
    return nodes


def test_payload_median_needs_the_minimum_years():
    assert MIN_MEDIAN_YEARS == 3
    body = yields_payload(list(reversed(_three_year_nodes())), recent=5)
    assert body["recent_years"] == [2022, 2023, 2024]
    assert body["years_used"] == 3 and body["latest_year"] == 2024
    assert body["recent_median_t_ha"] == pytest.approx(5.0)
    # two yield years in the window: the years are listed, the median is withheld
    two = yields_payload(_three_year_nodes(), recent=2)
    assert two["recent_years"] == [2023, 2024] and two["years_used"] == 2
    assert two["recent_median_t_ha"] is None
    assert yields_payload(_nodes(), recent=5)["recent_median_t_ha"] is None


def test_payload_single_yield_year_has_no_median():
    node = dict(_nodes()[0])
    body = yields_payload([node], recent=5)
    assert body["recent_median_t_ha"] is None
    assert body["years_used"] == 1
    assert body["latest_year"] == node["year"]
    assert body["recent_years"] == [node["year"]]


def test_payload_without_any_yield_has_no_latest_year():
    node = dict(_nodes()[0], yieldTHa=None)
    body = yields_payload([node], recent=5)
    assert body["years_used"] == 0 and body["latest_year"] is None
    assert body["recent_median_t_ha"] is None and body["recent_years"] == []


def test_payload_latest_year_is_the_newest_with_a_yield():
    nodes = _three_year_nodes()
    nodes.append(dict(nodes[-1], year=2025, yieldTHa=None))
    body = yields_payload(nodes, recent=5)
    assert body["latest_year"] == 2024 and body["years_used"] == 3
    assert body["series"][-1]["year"] == 2025


def test_request_url_pins_codes_and_level():
    url = request_url(["C1120", "C1110"])
    assert url.startswith("https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/apro_cpshr?")
    assert "geoLevel=nuts2" in url and url.index("crops=C1110") < url.index("crops=C1120")
    for code in (AREA, PRODUCTION, HUMIDITY):
        assert f"strucpro={code}" in url
    with pytest.raises(ValueError):
        request_url([])


def test_humidity_only_groups_are_dropped():
    values = {"2": 14.0, "3": 14.0}                     # common wheat: humidity, no area/production
    assert build_series(parse_jsonstat(_doc(values)), {"C1110": "TRZAX"}) == []


def test_not_significant_zero_production_gives_no_yield():
    # production 0.0 flagged "n" (not significant) with a reported area: a gap, not a failure
    values = {"0": 81.36, "1": 76.6, "4": 0.0, "5": 431.91}
    rows = build_series(parse_jsonstat(_doc(values, status={"4": "n"})), {"C1110": "TRZAX"})
    r2023 = next(r for r in rows if r.year == 2023)
    assert r2023.yield_t_ha is None
    assert r2023.area_ha == pytest.approx(81360) and r2023.production_t == 0.0
    assert f"{PRODUCTION}:n" in r2023.flags
    assert next(r for r in rows if r.year == 2024).yield_t_ha == pytest.approx(431910 / 76600)


def test_zero_production_without_flag_gives_no_yield():
    values = {"0": 81.36, "1": 76.6, "4": 0.0, "5": 431.91}
    rows = build_series(parse_jsonstat(_doc(values)), {"C1110": "TRZAX"})
    r2023 = next(r for r in rows if r.year == 2023)
    assert r2023.yield_t_ha is None and r2023.production_t == 0.0 and r2023.flags == ()


def test_not_significant_area_gives_no_yield():
    values = {"0": 81.36, "1": 76.6, "4": 331.95, "5": 431.91}
    rows = build_series(parse_jsonstat(_doc(values, status={"0": "n"})), {"C1110": "TRZAX"})
    r2023 = next(r for r in rows if r.year == 2023)
    assert r2023.yield_t_ha is None and f"{AREA}:n" in r2023.flags
    assert r2023.area_ha == pytest.approx(81360)


# ── plausibility bounds (only the maximum excludes a year) ───────────────────

RANGE = PlausibleRange(min_t_ha=0.5, max_t_ha=14.0, range_id="crop_yield.TRZAX.grain",
                       status="assumption")


def _year_nodes(yields_by_year):
    nodes = []
    for year, yld in yields_by_year:
        n = dict(_nodes()[0])
        n.update(year=year, yieldTHa=yld, areaHa=1000.0,
                 productionT=yld * 1000.0 if yld is not None else None)
        nodes.append(n)
    return nodes


def test_plausible_range_dataclass_is_frozen():
    assert (RANGE.min_t_ha, RANGE.max_t_ha, RANGE.range_id, RANGE.status) == (
        0.5, 14.0, "crop_yield.TRZAX.grain", "assumption")
    with pytest.raises(AttributeError):
        RANGE.max_t_ha = 99.0


def test_above_max_year_is_flagged_kept_in_series_and_dropped_from_the_median():
    nodes = _year_nodes([(2021, 4.0), (2022, 5.0), (2023, 20.0), (2024, 6.0)])
    body = yields_payload(nodes, recent=5, plausible=RANGE)
    assert [(s["year"], s["range_check"]) for s in body["series"]] == [
        (2021, "ok"), (2022, "ok"), (2023, "above_max"), (2024, "ok")]
    assert body["series"][2]["yield_t_ha"] == 20.0       # the value itself is never altered
    assert body["recent_years"] == [2021, 2022, 2024]
    assert body["years_used"] == 3 and body["latest_year"] == 2024
    assert body["recent_median_t_ha"] == pytest.approx(5.0)


def test_above_max_newest_year_does_not_become_the_latest_year():
    nodes = _year_nodes([(2021, 4.0), (2022, 5.0), (2023, 6.0), (2024, 20.0)])
    body = yields_payload(nodes, recent=5, plausible=RANGE)
    assert body["latest_year"] == 2023 and body["recent_years"] == [2021, 2022, 2023]
    assert body["series"][-1]["year"] == 2024 and body["series"][-1]["range_check"] == "above_max"


def test_window_takes_the_last_recent_years_among_the_plausible_ones():
    nodes = _year_nodes([(2020, 3.0), (2021, 4.0), (2022, 20.0), (2023, 5.0), (2024, 6.0)])
    body = yields_payload(nodes, recent=3, plausible=RANGE)
    assert body["recent_years"] == [2021, 2023, 2024]
    assert body["recent_median_t_ha"] == pytest.approx(5.0)


def test_below_min_year_is_flagged_but_stays_in_the_median():
    nodes = _year_nodes([(2021, 4.0), (2022, 0.3), (2023, 5.0), (2024, 6.0)])
    body = yields_payload(nodes, recent=5, plausible=RANGE)
    assert body["series"][1]["range_check"] == "below_min"
    assert body["recent_years"] == [2021, 2022, 2023, 2024] and body["years_used"] == 4
    assert body["recent_median_t_ha"] == pytest.approx(4.5)     # median of 0.3, 4, 5, 6


def test_bounds_are_inclusive():
    nodes = _year_nodes([(2022, 0.5), (2023, 14.0), (2024, 5.0)])
    body = yields_payload(nodes, recent=5, plausible=RANGE)
    assert [s["range_check"] for s in body["series"]] == ["ok", "ok", "ok"]
    assert body["years_used"] == 3


def test_a_year_without_a_yield_has_no_range_check():
    nodes = _year_nodes([(2022, 4.0), (2023, None), (2024, 5.0)])
    body = yields_payload(nodes, recent=5, plausible=RANGE)
    assert [s["range_check"] for s in body["series"]] == ["ok", None, "ok"]


def test_without_a_range_nothing_is_checked_and_the_median_is_unchanged():
    nodes = _year_nodes([(2021, 4.0), (2022, 5.0), (2023, 20.0), (2024, 6.0)])
    body = yields_payload(nodes, recent=5, plausible=None)
    assert [s["range_check"] for s in body["series"]] == [None, None, None, None]
    assert body["plausible_range"] is None
    assert body["recent_years"] == [2021, 2022, 2023, 2024]
    assert body["recent_median_t_ha"] == pytest.approx(5.5)
    default = yields_payload(nodes, recent=5)
    assert default["plausible_range"] is None and default["recent_median_t_ha"] == pytest.approx(5.5)


def test_plausible_range_is_reported_with_its_provenance():
    body = yields_payload(_three_year_nodes(), recent=5, plausible=RANGE)
    assert body["plausible_range"] == {
        "min_t_ha": 0.5, "max_t_ha": 14.0, "range_id": "crop_yield.TRZAX.grain",
        "status": "assumption", "excludes": "above_max"}


def test_excluded_years_can_leave_too_few_for_a_median():
    nodes = _year_nodes([(2022, 4.0), (2023, 20.0), (2024, 6.0)])
    body = yields_payload(nodes, recent=5, plausible=RANGE)
    assert body["recent_years"] == [2022, 2024] and body["years_used"] == 2
    assert body["recent_median_t_ha"] is None
    assert body["latest_year"] == 2024


def test_all_years_above_max_gives_no_window_and_no_latest_year():
    nodes = _year_nodes([(2022, 20.0), (2023, 30.0)])
    body = yields_payload(nodes, recent=5, plausible=RANGE)
    assert body["recent_years"] == [] and body["years_used"] == 0
    assert body["latest_year"] is None and body["recent_median_t_ha"] is None
