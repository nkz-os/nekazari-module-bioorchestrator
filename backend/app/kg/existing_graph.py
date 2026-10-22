"""Read-only Neo4j view of the units a graph already holds, for the gate's content-duplicate check.

Implements :class:`app.kg.gate.ExistingGraph` over a (synchronous) Neo4j driver. It opens sessions in
``READ_ACCESS`` mode and runs only ``MATCH ... RETURN``, in keyset pages ordered by ``unitKey`` so no
query holds the whole source in memory (the production heap is small). Units are rebuilt from the
properties the loader wrote (:func:`app.kg.loader.unit_properties`), then keyed with the gate's own
:func:`~app.kg.gate.unit_content_key`, so the two sides can never disagree on what "same content" means.
A node without a ``unitKey`` (a legacy ``VarietyTrial``) is not an observation unit of the new model and is
not read.
"""
from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

from neo4j import READ_ACCESS

from .gate import unit_content_key
from .model import FactorLevel, UnitRow

PAGE_SIZE = 2000

_PAGE = """
MATCH (u:ObservationUnit)
WHERE u.source_id = $source_id AND u.unitKey > $after
RETURN u.unitKey AS unitKey, u.documentKey AS documentKey, u.cropEppo AS cropEppo, u.variety AS rawVariety,
       u.rawSite AS rawSite, u.rawSeason AS rawSeason, u.rawIrrigation AS rawIrrigation,
       u.rawProductionSystem AS rawProductionSystem, u.factorLevels AS factorLevels,
       u.rootstock AS rootstock, u.clone AS clone, u.plantingYear AS plantingYear,
       u.yieldKgHa AS yieldKgHa
ORDER BY u.unitKey
LIMIT $limit
"""


def _factor_levels(texts: list[str] | None) -> tuple[FactorLevel, ...]:
    levels = []
    for text in texts or ():
        factor, level, unit = json.loads(text)["t"]
        levels.append(FactorLevel(factor=factor, level=level, unit=unit))
    return tuple(levels)


def _unit_from_record(source_id: str, record: Any) -> UnitRow:
    # model_construct: the node was validated when it was loaded; only the content-key fields matter here
    return UnitRow.model_construct(
        source_id=source_id, document_key=record["documentKey"], crop_eppo=record["cropEppo"],
        raw_variety=record["rawVariety"], raw_site=record["rawSite"], raw_season=record["rawSeason"],
        raw_irrigation=record["rawIrrigation"], raw_production_system=record["rawProductionSystem"],
        factor_levels=_factor_levels(record["factorLevels"]), rootstock=record["rootstock"],
        clone=record["clone"], planting_year=record["plantingYear"], yield_kg_ha=record["yieldKgHa"])


class Neo4jExistingGraph:
    """``ExistingGraph`` over a Neo4j driver; never writes."""

    def __init__(self, driver: Any, *, database: str | None = None, page_size: int = PAGE_SIZE) -> None:
        if page_size < 1:
            raise ValueError("page_size must be at least 1")
        self._driver = driver
        self._database = database
        self._page_size = page_size

    def unit_identities(self, source_id: str) -> Iterator[tuple[str, str]]:
        after = ""
        while True:
            with self._driver.session(database=self._database, default_access_mode=READ_ACCESS) as session:
                records = list(session.run(_PAGE, source_id=source_id, after=after, limit=self._page_size))
            for record in records:
                yield record["unitKey"], unit_content_key(_unit_from_record(source_id, record))
            if len(records) < self._page_size:
                return
            after = records[-1]["unitKey"]
