"""Variety names of a crop, as bioorch writes them, for forms that ask the user for a variety.

The names come from ``VarietyTrial.variety`` (the names variety recommendations use), one entry per
``normalise_name`` key, so a form can store the graph's spelling instead of a typo. Global catalog:
nothing here is tenant data. The cereal type is the one ``resolve_cereal_type`` would give.
"""
from __future__ import annotations

import re
import time
from collections import defaultdict
from collections.abc import Callable

from app.graph.dao import _CROP_MATCH_PREDICATE
from app.planning.cereal_type import _CLASS, graph_class_fetcher, normalise_name

CACHE_TTL_S = 3600.0
_TYPED_CROPS = ("TRZAX", "HORVX")  # the crops with a winter/spring model group
_TAG = re.compile(r"\([^)]*\)\s*$")

VARIETY_NAMES_CYPHER = f"""
MATCH (vt:VarietyTrial)
WHERE {_CROP_MATCH_PREDICATE} AND vt.variety IS NOT NULL
RETURN vt.variety AS name, count(*) AS n
"""

_cache: dict[tuple[str, str], tuple[float, list[dict]]] = {}


def clear_cache() -> None:
    _cache.clear()


def _display_name(spellings: dict[str, int]) -> str:
    """Untagged spellings first ("HISPANIC" over "HISPANIC (T)"), then the most frequent, then A-Z."""
    return min(spellings, key=lambda s: (bool(_TAG.search(s)), -spellings[s], s))


def build_catalog(name_rows: list[tuple[str | None, int]],
                  class_rows: list[tuple[str, str]] | None) -> list[dict]:
    spellings: dict[str, dict[str, int]] = defaultdict(dict)
    for raw, count in name_rows:
        if not isinstance(raw, str) or not raw.strip():
            continue
        name = raw.strip()
        key = normalise_name(name)
        if key:
            spellings[key][name] = spellings[key].get(name, 0) + int(count or 0)
    classes: dict[str, set[str]] = defaultdict(set)
    for name, cls in class_rows or []:
        mapped = _CLASS.get(cls.strip().lower()) if isinstance(cls, str) else None
        if mapped:
            classes[normalise_name(name)].add(mapped)
    out = []
    for key in sorted(spellings):
        found = classes.get(key, set())
        value = next(iter(found)) if len(found) == 1 else None
        out.append({"name": _display_name(spellings[key]), "key": key,
                    "cereal_type": value if value in ("winter", "spring") else None})
    return out


async def crop_variety_catalog(driver, *, eppo: str, graph_id: str,
                               now: Callable[[], float] = time.monotonic) -> list[dict]:
    """Catalog of one crop, cached per (graph, crop). A graph error propagates and is not cached."""
    hit = _cache.get((graph_id, eppo))
    if hit is not None and now() < hit[0]:
        return hit[1]
    async with driver.session() as session:
        result = await session.run(VARIETY_NAMES_CYPHER, crop=eppo)
        rows = await result.data()
    names = [(r.get("name"), r.get("n") or 0) for r in rows]
    class_rows = await graph_class_fetcher(driver)(eppo) if eppo in _TYPED_CROPS else None
    entries = build_catalog(names, class_rows)
    _cache[(graph_id, eppo)] = (now() + CACHE_TTL_S, entries)
    return entries
