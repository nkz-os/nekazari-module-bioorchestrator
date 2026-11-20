import pytest

import app.planning.variety_catalog as vc
from app.planning.variety_catalog import build_catalog, crop_variety_catalog


@pytest.fixture(autouse=True)
def _fresh_cache():
    vc.clear_cache()
    yield
    vc.clear_cache()


def _names(entries):
    return [(e["name"], e["key"]) for e in entries]


def test_one_entry_per_key_most_frequent_spelling():
    out = build_catalog([("Filón", 2), ("FILON", 5), ("filon (T)", 1)], None)
    assert _names(out) == [("FILON", "FILON")]


def test_untagged_spelling_wins_over_more_frequent_tagged():
    assert _names(build_catalog([("HISPANIC (T)", 9), ("HISPANIC", 1)], None)) == [("HISPANIC", "HISPANIC")]
    assert _names(build_catalog([("ORVAL (DI618)", 2)], None)) == [("ORVAL (DI618)", "ORVAL")]


def test_tie_takes_alphabetical_first():
    assert _names(build_catalog([("Avispa", 3), ("AVISPA", 3)], None)) == [("AVISPA", "AVISPA")]


def test_sorted_by_key():
    out = build_catalog([("Zeta", 1), ("alfa", 1), ("Meseta", 1)], None)
    assert [e["key"] for e in out] == ["ALFA", "MESETA", "ZETA"]


def test_blank_names_dropped():
    assert build_catalog([("", 3), ("  ", 2), (None, 1), ("Kiko", 1)], None) == \
        [{"name": "Kiko", "key": "KIKO", "cereal_type": None}]


def test_cereal_type_from_classes():
    out = build_catalog([("FILON", 1), ("Kiko Nick", 1), ("Avispa", 1)],
                        [("FILON", "invierno"), ("KIKO  NICK", "primavera")])
    assert {e["key"]: e["cereal_type"] for e in out} == {"FILON": "winter", "KIKO NICK": "spring", "AVISPA": None}


def test_two_classes_is_none():
    out = build_catalog([("FILON", 1)], [("FILON", "invierno"), ("Filón", "primavera")])
    assert out[0]["cereal_type"] is None


def test_facultative_is_none():
    out = build_catalog([("FILON", 1)], [("FILON", "facultativo")])
    assert out[0]["cereal_type"] is None


def test_no_classes_for_other_crops():
    out = build_catalog([("FILON", 1)], None)
    assert out[0]["cereal_type"] is None


class _Result:
    def __init__(self, rows):
        self._rows = rows

    async def data(self):
        return self._rows


class _Session:
    def __init__(self, driver):
        self.driver = driver

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def run(self, query, **params):
        self.driver.queries.append((query, params))
        if self.driver.fail:
            raise RuntimeError("graph down")
        if "cycle_class" in query:
            return _Result([{"name": "FILON", "cls": "invierno"}])
        return _Result([{"name": "FILON", "n": 3}, {"name": "Kiko", "n": 1}])


class _Driver:
    def __init__(self, fail=False):
        self.queries = []
        self.fail = fail

    def session(self):
        return _Session(self)


async def test_wheat_reads_names_and_classes():
    d = _Driver()
    out = await crop_variety_catalog(d, eppo="TRZAX", graph_id="g")
    assert {e["key"]: e["cereal_type"] for e in out} == {"FILON": "winter", "KIKO": None}
    assert len(d.queries) == 2 and d.queries[0][1] == {"crop": "TRZAX"}


async def test_other_crop_skips_class_lookup():
    d = _Driver()
    out = await crop_variety_catalog(d, eppo="ZEAMX", graph_id="g")
    assert len(d.queries) == 1 and all(e["cereal_type"] is None for e in out)


async def test_cache_by_graph_and_crop():
    d = _Driver()
    clock = [1000.0]
    now = lambda: clock[0]
    await crop_variety_catalog(d, eppo="ZEAMX", graph_id="g", now=now)
    await crop_variety_catalog(d, eppo="ZEAMX", graph_id="g", now=now)
    assert len(d.queries) == 1
    await crop_variety_catalog(d, eppo="ZEAMX", graph_id="other", now=now)
    assert len(d.queries) == 2
    clock[0] += 3601
    await crop_variety_catalog(d, eppo="ZEAMX", graph_id="g", now=now)
    assert len(d.queries) == 3


async def test_graph_error_is_not_cached():
    d = _Driver(fail=True)
    with pytest.raises(RuntimeError):
        await crop_variety_catalog(d, eppo="ZEAMX", graph_id="g")
    d.fail = False
    out = await crop_variety_catalog(d, eppo="ZEAMX", graph_id="g")
    assert [e["key"] for e in out] == ["FILON", "KIKO"] and len(d.queries) == 2


class _ClassFailDriver(_Driver):
    def session(self):
        driver = self

        class _S(_Session):
            async def run(self, query, **params):
                if "cycle_class" in query:
                    driver.queries.append((query, params))
                    raise RuntimeError("class lookup down")
                return await super().run(query, **params)
        return _S(self)


async def test_class_lookup_failure_keeps_the_names_and_is_not_cached():
    d = _ClassFailDriver()
    out = await crop_variety_catalog(d, eppo="TRZAX", graph_id="g")
    assert [e["key"] for e in out] == ["FILON", "KIKO"] and all(e["cereal_type"] is None for e in out)
    await crop_variety_catalog(d, eppo="TRZAX", graph_id="g")
    assert len(d.queries) == 4  # names + classes, twice: a degraded answer is not cached


async def test_class_lookup_timeout_keeps_the_names(monkeypatch):
    import asyncio

    async def slow(eppo):
        await asyncio.sleep(1)
        return [("FILON", "invierno")]
    monkeypatch.setattr(vc, "graph_class_fetcher", lambda driver: slow)
    monkeypatch.setattr(vc, "CLASS_TIMEOUT_S", 0.01)
    out = await crop_variety_catalog(_Driver(), eppo="TRZAX", graph_id="g")
    assert [e["cereal_type"] for e in out] == [None, None]
