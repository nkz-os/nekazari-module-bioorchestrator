"""Link and verify (plan task 8): every unit hangs on its site, variety, crop, study and document.

The loader already writes these relationships. This step is the independent second look: it
re-links idempotently from the bundle (MERGE, never a duplicate), falling back to the sites
registry's aliases for a unit whose ``site_key`` is empty but whose observed ``raw_site`` is a
registered name, and then *verifies* in the graph that every unit of the bundle has exactly one
``TRIAL_AT`` (when the bundle gives it a site) and exactly one ``OF_CROP`` / ``IN_STUDY`` /
``SOURCED_FROM`` / ``OF_VARIETY`` (when it has a variety). Nothing is deleted: a unit with a second
site is reported (a stale link from an older load is a decision, not a silent cleanup).
"""
from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any

from . import identity
from .contracts import Bundle
from .loader import _RELATIONSHIPS, DEFAULT_BATCH_SIZE, LoadError, _chunks, _run_step
from .registries import Registries, load_registries

logger = logging.getLogger(__name__)

VERIFY_CHUNK = 5000

# link kind -> relationship type, checked for "exactly one" per unit
_EXACTLY_ONE = {
    "site": "TRIAL_AT", "crop": "OF_CROP", "study": "IN_STUDY", "document": "SOURCED_FROM", "variety": "OF_VARIETY",
}


@dataclass(frozen=True)
class LinkReport:
    source_id: str
    units: int
    relinked: dict[str, int]  # plan name -> relationships created by this pass (0 when the loader had them all)
    alias_resolved: int  # units whose site came from the registry alias fallback
    unresolved_sites: dict[str, int]  # raw site as printed -> units left without a site
    units_without_site: int  # no site_key and no observed name: an honest gap
    wrong_cardinality: dict[str, int]  # link kind -> units whose count is not exactly one

    @property
    def problems(self) -> list[str]:
        out = [f"{n} unit(s) without exactly one {_EXACTLY_ONE[kind]}" for kind, n in self.wrong_cardinality.items() if n]
        out += [f"site {name!r}: {n} unit(s) unresolved" for name, n in self.unresolved_sites.items()]
        return out

    @property
    def ok(self) -> bool:
        return not self.problems


def expected_sites(bundle: Bundle, registries: Registries) -> tuple[dict[str, str], int, Counter[str], int]:
    """unit_key -> site key for every unit that has one, how many came from an alias, the unresolved, the gaps."""
    sites = {identity.site_key(s) for s in bundle.sites}
    out: dict[str, str] = {}
    alias = 0
    unresolved: Counter[str] = Counter()
    gaps = 0
    for unit in bundle.units:
        key = identity.unit_key(unit)
        if unit.site_key is not None:
            out[key] = unit.site_key
        elif unit.raw_site is None:
            gaps += 1
        else:
            found = registries.site(unit.raw_site)
            if found is not None and found.id in sites:
                out[key] = found.id
                alias += 1
            else:
                unresolved[unit.raw_site] += 1
    return out, alias, unresolved, gaps


def _cardinality_query(rel: str) -> str:
    return (f"UNWIND $keys AS k MATCH (u:ObservationUnit {{unitKey: k}}) "
            f"WHERE size([(u)-[:{rel}]->() | 1]) <> 1 RETURN count(u) AS bad")


async def _count(driver: Any, database: str | None, statement: str, **params: Any) -> int:
    async with driver.session(database=database) as session:
        result = await session.run(statement, **params)
        record = await result.single()
    return int(record["bad"]) if record is not None else 0


def _keys_chunks(keys: Sequence[str]) -> Iterator[Sequence[str]]:
    yield from _chunks(keys, VERIFY_CHUNK)


async def link(
    bundle: Bundle, driver: Any, *, registries: Registries | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE, database: str | None = None,
) -> LinkReport:
    """Re-link a loaded bundle's units and verify the cardinalities in the graph."""
    registries = registries or load_registries()
    if batch_size < 1:
        raise LoadError("batch_size must be at least 1")
    site_of, alias, unresolved, gaps = expected_sites(bundle, registries)
    unit_keys = sorted(identity.unit_key(u) for u in bundle.units)

    rows: dict[str, list[dict[str, Any]]] = {
        "trial_at": [{"a": k, "b": s} for k, s in sorted(site_of.items())],
        "unit_crop": [], "unit_study": [], "unit_document": [], "unit_variety": [],
    }
    for unit in sorted(bundle.units, key=identity.unit_key):
        key = identity.unit_key(unit)
        rows["unit_crop"].append({"a": key, "b": unit.crop_eppo})
        if unit.study_key is not None:
            rows["unit_study"].append({"a": key, "b": unit.study_key})
        rows["unit_document"].append({"a": key, "b": unit.document_key})
        if unit.variety_key is not None:
            rows["unit_variety"].append({"a": key, "b": unit.variety_key})

    statements = dict(_RELATIONSHIPS)
    relinked: dict[str, int] = {}
    async with driver.session(database=database) as session:
        for name in ("unit_crop", "unit_variety", "unit_study", "unit_document", "trial_at"):
            step = await _run_step(session, f"link_{name}", statements[name], rows[name], batch_size,
                                   all_must_match=True)
            relinked[name] = step.relationships_created

    expect = {"crop": unit_keys, "document": unit_keys,
              "study": sorted(r["a"] for r in rows["unit_study"]),
              "variety": sorted(r["a"] for r in rows["unit_variety"]),
              "site": sorted(site_of)}
    wrong: dict[str, int] = {}
    for kind, keys in expect.items():
        wrong[kind] = sum([await _count(driver, database, _cardinality_query(_EXACTLY_ONE[kind]), keys=list(c))
                           for c in _keys_chunks(keys)])
    report = LinkReport(
        source_id=bundle.source_id, units=len(unit_keys), relinked=relinked, alias_resolved=alias,
        unresolved_sites=dict(sorted(unresolved.items())), units_without_site=gaps, wrong_cardinality=wrong,
    )
    logger.info("kg link source=%s units=%d relinked=%s alias_resolved=%d without_site=%d wrong=%s",
                report.source_id, report.units, relinked, alias, gaps, wrong)
    return report


__all__ = ["LinkReport", "expected_sites", "link"]
