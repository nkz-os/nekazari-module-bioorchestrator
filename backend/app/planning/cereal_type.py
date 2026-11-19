"""Cereal type (winter / spring) of a sown variety, from the knowledge graph.

The type comes only from ``cycle_class`` observations of the GENVCE-TRIALS source
(``invierno`` / ``primavera``). The GENVCE catalogue writes a *cycle duration* (corto/largo) under
the same variable id: it is filtered out by source and by value and is never a habit.
"""
from __future__ import annotations

import asyncio
import logging
import re
import unicodedata
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from app.ingestion.normalization_registry import normalize_variety_name

logger = logging.getLogger(__name__)

_EPPO = {"wheat": "TRZAX", "barley": "HORVX"}
_DURUM_SLUG = "durum_wheat"
_CLASS = {"invierno": "winter", "primavera": "spring", "winter": "winter", "spring": "spring",
          "facultative": "facultative", "facultativo": "facultative"}

CYCLE_CLASS_CYPHER = """
MATCH (o:Observation)-[:OF_VARIABLE]->(:Variable {variableId: 'cycle_class'})
WHERE o.valueText IN ['invierno', 'primavera']
MATCH (o)-[:ON_UNIT]->(u:ObservationUnit)-[:IN_STUDY]->(:Study)-[:PART_OF]->(s:Source {sourceId: 'GENVCE-TRIALS'})
MATCH (u)-[:OF_VARIETY]->(v:Variety {cropEppo: $eppo})
UNWIND [v.name, u.varietyNormalized] AS name
WITH name, o.valueText AS cls
WHERE name IS NOT NULL
RETURN DISTINCT name, cls
"""

FetchClasses = Callable[[str], Awaitable[list[tuple[str, str]]]]


@dataclass(frozen=True)
class CerealType:
    value: str | None  # "winter" | "spring" | "facultative" | None
    source: str | None  # "variety_graph" only when a class was found
    variety: str | None
    reason: str | None  # no_cereal_type | cereal_type_not_supported | cereal_type_lookup_failed | None


def normalise_name(name: str | None) -> str:
    """Registry normalisation plus accent folding and whitespace collapse; applied to both sides."""
    base = normalize_variety_name(name) or ""
    folded = "".join(c for c in unicodedata.normalize("NFKD", base) if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", folded).strip().upper()


def cereal_group(slug: str, value: str | None) -> str | None:
    """Model group for a species and cereal type, or None when it has none."""
    if slug == _DURUM_SLUG:
        return "durum"
    if slug in _EPPO and value in ("winter", "spring"):
        return f"{'wheat' if slug == 'wheat' else 'barley'}_{value}"
    return None


async def resolve_cereal_type(slug: str, variety: str | None, *, fetch_classes: FetchClasses,
                              timeout_s: float = 2.0) -> CerealType:
    if slug == _DURUM_SLUG:
        return CerealType(None, None, variety, None)  # single group, no lookup
    eppo = _EPPO.get(slug)
    wanted = normalise_name(variety) if isinstance(variety, str) else ""
    if eppo is None or not wanted:
        return CerealType(None, None, variety, "no_cereal_type")
    try:
        rows = await asyncio.wait_for(fetch_classes(eppo), timeout=timeout_s)
    except asyncio.TimeoutError:
        logger.warning("cereal_type_lookup_failed slug=%s code=timeout", slug)
        return CerealType(None, None, variety, "cereal_type_lookup_failed")
    except Exception as exc:  # noqa: BLE001 - any lookup failure degrades only this field
        logger.warning("cereal_type_lookup_failed slug=%s code=%s", slug, type(exc).__name__)
        return CerealType(None, None, variety, "cereal_type_lookup_failed")
    classes = set()
    for name, cls in rows:
        mapped = _CLASS.get(cls.strip().lower()) if isinstance(cls, str) else None
        if mapped and normalise_name(name) == wanted:
            classes.add(mapped)
    if len(classes) != 1:  # none (unknown / local / catalogue-only) or both: never guess
        return CerealType(None, None, variety, "no_cereal_type")
    value = next(iter(classes))
    if value == "facultative":
        return CerealType(value, "variety_graph", variety, "cereal_type_not_supported")
    return CerealType(value, "variety_graph", variety, None)


def graph_class_fetcher(driver) -> FetchClasses:
    """``fetch_classes`` backed by the neo4j async driver: distinct (variety name, class) per crop."""
    async def fetch(eppo: str) -> list[tuple[str, str]]:
        async with driver.session() as session:
            result = await session.run(CYCLE_CLASS_CYPHER, eppo=eppo)
            data = await result.data()
        return [(r["name"], r["cls"]) for r in data if r.get("name") and r.get("cls")]
    return fetch
