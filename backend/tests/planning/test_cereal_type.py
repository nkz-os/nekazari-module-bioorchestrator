import asyncio

from app.planning.cereal_type import (
    CerealType,
    cereal_group,
    graph_class_fetcher,
    resolve_cereal_type,
)


def _fetcher(rows, calls=None):
    async def fetch(eppo):
        if calls is not None:
            calls.append(eppo)
        return rows
    return fetch


async def test_variety_normalised_match():
    calls = []
    res = await resolve_cereal_type("wheat", "  nemó  (T)", fetch_classes=_fetcher([("NEMO", "invierno")], calls))
    assert res == CerealType("winter", "variety_graph", "  nemó  (T)", None)
    assert calls == ["TRZAX"]
    res = await resolve_cereal_type("barley", "Alba  Uno", fetch_classes=_fetcher([("ALBA UNO", "primavera")]))
    assert res.value == "spring" and res.source == "variety_graph"


async def test_variety_both_classes_returns_none():
    res = await resolve_cereal_type("wheat", "Mixto", fetch_classes=_fetcher(
        [("MIXTO", "invierno"), ("Mixto", "primavera"), ("Otra", "invierno")]))
    assert res.value is None and res.source is None and res.reason == "no_cereal_type"


async def test_no_match_is_no_cereal_type():
    res = await resolve_cereal_type("wheat", "Local", fetch_classes=_fetcher([("Otra", "invierno")]))
    assert res.value is None and res.reason == "no_cereal_type"


async def test_no_variety_skips_graph():
    calls = []
    for v in (None, "", "   "):
        res = await resolve_cereal_type("wheat", v, fetch_classes=_fetcher([("X", "invierno")], calls))
        assert res.value is None and res.reason == "no_cereal_type"
    assert calls == []


async def test_graph_timeout_reason():
    async def slow(eppo):
        await asyncio.sleep(1)
        return []
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    res = await resolve_cereal_type("wheat", "Nemo", fetch_classes=slow, timeout_s=0.05)
    assert loop.time() - t0 < 0.5
    assert res.value is None and res.reason == "cereal_type_lookup_failed"


async def test_graph_error_reason():
    async def boom(eppo):
        raise RuntimeError("down")
    res = await resolve_cereal_type("wheat", "Nemo", fetch_classes=boom)
    assert res.reason == "cereal_type_lookup_failed" and res.value is None


async def test_facultative_not_supported():
    res = await resolve_cereal_type("wheat", "Nemo", fetch_classes=_fetcher([("Nemo", "facultative")]))
    assert res.value == "facultative" and res.reason == "cereal_type_not_supported"


async def test_catalogue_ciclo_ignored():
    res = await resolve_cereal_type("wheat", "Nemo", fetch_classes=_fetcher([("Nemo", "corto"), ("Nemo", "largo")]))
    assert res.value is None and res.reason == "no_cereal_type"


async def test_durum_no_lookup():
    calls = []
    res = await resolve_cereal_type("durum_wheat", "Nemo", fetch_classes=_fetcher([("Nemo", "invierno")], calls))
    assert res.value is None and res.reason is None and calls == []
    assert cereal_group("durum_wheat", None) == "durum"


async def test_unsupported_species_is_no_cereal_type():
    res = await resolve_cereal_type("maize", "Nemo", fetch_classes=_fetcher([("Nemo", "invierno")]))
    assert res.reason == "no_cereal_type"


def test_cereal_group():
    assert cereal_group("wheat", "winter") == "wheat_winter"
    assert cereal_group("wheat", "spring") == "wheat_spring"
    assert cereal_group("barley", "winter") == "barley_winter"
    assert cereal_group("barley", "spring") == "barley_spring"
    assert cereal_group("wheat", None) is None
    assert cereal_group("wheat", "facultative") is None
    assert cereal_group("maize", "winter") is None


async def test_graph_class_fetcher_runs_read_query():
    seen = {}

    class R:
        async def data(self):
            return [{"name": "A", "cls": "invierno"}, {"name": None, "cls": "x"}]

    class S:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *e):
            return False
        async def run(self, q, **kw):
            seen["q"], seen["kw"] = q, kw
            return R()

    class D:
        def session(self):
            return S()

    rows = await graph_class_fetcher(D())("TRZAX")
    assert rows == [("A", "invierno")]
    assert seen["kw"]["eppo"] == "TRZAX" and "GENVCE-TRIALS" in seen["q"]
