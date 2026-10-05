"""get_available_crops groups by EPPO only; localized names must not duplicate crops."""
from unittest.mock import AsyncMock, MagicMock

from app.graph.dao import GraphDAO


class _Res:
    def __init__(self, rows):
        self._rows = rows

    def __aiter__(self):
        self._it = iter(self._rows)
        return self

    async def __anext__(self):
        try:
            return next(self._it)
        except StopIteration:
            raise StopAsyncIteration


def _dao(rows):
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    calls = []
    params_seen = []

    async def run(query, **params):
        calls.append(query)
        params_seen.append(params)
        return _Res(rows)

    session.run = run
    driver = MagicMock()
    driver.session.return_value = session
    dao = GraphDAO(driver)
    dao._calls = calls
    dao._params = params_seen
    return dao


def _row(eppo, names, varieties, trials, first, last):
    """One row per EPPO, as the aggregating query returns it."""
    return {"eppo_code": eppo, "names": names, "variety_count": varieties,
            "trial_count": trials, "first_year": first, "last_year": last}


async def test_query_aggregates_per_eppo_with_distinct_varieties():
    dao = _dao([_row("TRZAX", ["Triticum aestivum"], 3, 4, 2000, 2001)])
    await dao.get_available_crops()
    q = dao._calls[0]
    assert "count(DISTINCT vt.variety) AS variety_count" in q
    # name must not be a grouping key: it is only collected
    assert "collect(" in q and "AS scientific_name" not in q


async def test_name_variants_collapse_and_distinct_varieties_not_summed():
    # one variety shared by two name variants counts once: DB reports 4, not 4+4
    dao = _dao([
        _row("TRZAX", ["Blé tendre"] * 3 + ["Triticum aestivum"] * 9 + ["(unknown)"] * 2,
             4, 14, 2000, 2024),
        _row("HORVX", ["Orge"] * 2, 4, 2, 2012, 2020),
        _row("SOLTU", ["Patates"], 3, 1, 2015, 2016),
    ])
    out = await dao.get_available_crops()
    by = {c["eppo_code"]: c for c in out}
    assert len(out) == 3
    t = by["TRZAX"]
    assert t["scientific_name"] == "Triticum aestivum"
    assert t["variety_count"] == 4 and t["trial_count"] == 14
    assert t["first_year"] == 2000 and t["last_year"] == 2024
    assert set(t) == {"eppo_code", "scientific_name", "variety_count", "trial_count",
                      "first_year", "last_year"}


async def test_ordered_by_trial_count_desc():
    dao = _dao([_row("A", ["a"], 1, 5, 2000, 2001), _row("B", ["b"], 1, 99, 2000, 2001)])
    out = await dao.get_available_crops()
    assert [c["eppo_code"] for c in out] == ["B", "A"]


async def test_empty_or_null_eppo_excluded():
    dao = _dao([_row("", ["x"], 1, 5, 2000, 2001), _row(None, ["y"], 1, 5, 2000, 2001),
                _row("TRZAX", ["Triticum aestivum"], 1, 5, 2000, 2001)])
    out = await dao.get_available_crops()
    assert [c["eppo_code"] for c in out] == ["TRZAX"]


async def test_only_unknown_name_falls_back_to_unknown():
    dao = _dao([_row("ZZZZZ", ["(unknown)"], 1, 5, 2000, 2001)])
    assert (await dao.get_available_crops())[0]["scientific_name"] == "(unknown)"


async def test_name_tie_break_is_deterministic():
    dao = _dao([_row("A", ["b", "a"], 1, 2, None, None)])
    out = await dao.get_available_crops()
    assert out[0]["scientific_name"] == "b" and out[0]["first_year"] is None


async def test_exact_sibling_codes_are_merged_in_the_query_and_nothing_else():
    dao = _dao([_row("ZEAMX", ["Zea mays"], 3, 5, 2019, 2024)])
    out = await dao.get_available_crops()
    assert out[0]["eppo_code"] == "ZEAMX" and out[0]["variety_count"] == 3
    q = dao._calls[0]
    # grouped by the listed code, so distinct varieties are counted across the siblings
    assert "coalesce($catalog_codes[vt.cropEppo], vt.cropEppo) AS eppo_code" in q
    assert dao._params[0]["catalog_codes"] == {"ZEAMA": "ZEAMX"}
