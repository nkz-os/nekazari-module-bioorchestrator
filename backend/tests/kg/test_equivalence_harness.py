"""The equivalence collector's own guarantees (no database): it cannot write, and it classifies by cause."""
from __future__ import annotations

import asyncio

import pytest

from tests.kg import equivalence_harness as h


def _rec(crop, tier, n, kg, trust="low", top="V"):
    return {"crop": {"eppo": crop}, "evidence": {"tier": tier, "trial_count": n}, "yield": {"expected_kg_ha": kg},
            "trust": {"level": trust}, "varieties": [{"variety": top}]}


@pytest.mark.parametrize("cypher", [
    "CREATE (n:X)", "MATCH (n) SET n.a = 1", "MATCH (n) DETACH DELETE n", "MERGE (n:X {a: 1})",
    "match (n) remove n.a", "CALL apoc.cypher.doIt('x', {})", "LOAD CSV FROM 'x' AS r RETURN r",
    "MATCH (n) FOREACH (x IN [1] | SET n.a = x)",
])
def test_writes_are_refused_before_they_are_sent(cypher):
    class Inner:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return None

        async def run(self, *a, **k):
            raise AssertionError("sent")

    class Driver:
        def session(self, **kw):
            assert kw["default_access_mode"] == h.READ_ACCESS
            return Inner()

    async def go():
        async with h.ReadOnlyDriver(Driver()).session() as s:
            await s.run(cypher)

    with pytest.raises(h.ReadOnlyViolation):
        asyncio.run(go())


def test_reads_pass_even_when_a_string_contains_a_write_word():
    seen = []

    class Inner:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return None

        async def run(self, query, *a, **k):
            seen.append(query)

    class Driver:
        def session(self, **kw):
            return Inner()

    async def go():
        async with h.ReadOnlyDriver(Driver()).session() as s:
            await s.run("MATCH (t:TrialSite) WHERE t.name = 'Set and Create' RETURN t")

    asyncio.run(go())
    assert len(seen) == 1


def test_transactions_are_not_available():
    class Inner:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return None

    class Driver:
        def session(self, **kw):
            return Inner()

    async def go():
        async with h.ReadOnlyDriver(Driver()).session() as s:
            s.begin_transaction  # noqa: B018

    with pytest.raises(h.ReadOnlyViolation):
        asyncio.run(go())


def test_the_classifier_names_a_cause_for_each_kind_of_difference_and_flags_the_rest():
    def resp(*recs):
        return {"recommendations": list(recs)}

    prod = {
        "recommend|ES-Csa|main|None": resp(_rec("HORVX", "regional", 9, 8000.0), _rec("TRZAX", "regional", 5, 7000.0),
                                           _rec("ZEAMX", "field", 8, 19000.0, "medium")),
        "recommend|ES-Cfb|main|None": resp(),
        "recommend|IT-Csa|main|None": resp(_rec("HORVX", "regional", 9, 8000.0)),
        "recommend|ES-Csa|main|secano": resp(_rec("HORVX", "regional", 6, 8000.0)),
    }
    new = {
        "recommend|ES-Csa|main|None": resp(_rec("HORVX", "regional", 10, 8100.0), _rec("TRZAX", "regional", 5, 7000.0),
                                           _rec("ZEAMX", "regional", 8, 19000.0)),
        "recommend|ES-Cfb|main|None": resp(_rec("HORVX", "regional", 10, 8100.0)),
        "recommend|IT-Csa|main|None": resp(),
        "recommend|ES-Csa|main|secano": resp(),
    }
    causes, unexplained = h.classify_recommendations(prod, new)
    assert causes == {"zones": 1, "identical": 1, "coverage": 1, "country": 1, "irrigation": 1, "both empty": 10}
    assert [(u[0], u[1]) for u in unexplained] == [("recommend|ES-Csa|main|None", "ZEAMX")]  # field -> regional
