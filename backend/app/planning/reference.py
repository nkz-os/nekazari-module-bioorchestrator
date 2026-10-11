"""Reference knowledge the planner reads in bulk: species tolerances, rotation rules and the
crops that have variety trials.

One query per kind instead of one per species: the fit evaluates every registry species
on each call. Cached per graph for REFERENCE_TTL_S, since only a graph load changes it.
"""
from __future__ import annotations

import time
from dataclasses import dataclass

from app.core.knowledge import KnowledgeSource
from app.planning.rotation import Constraint

REFERENCE_TTL_S = 600.0

_SPECIES_QUERY = """
MATCH (s:Species)
OPTIONAL MATCH (s)-[:HAS_SOIL_SUITABILITY]->(ss:CropSoilSuitability)
OPTIONAL MATCH (s)-[:HAS_HEAT_TOLERANCE]->(h:CropHeatTolerance)
WITH s, head(collect(ss)) AS ss, head(collect(h)) AS h
RETURN s.name AS species,
       ss.phMin AS ph_min, ss.phMax AS ph_max, ss.textures AS textures, ss.drainage AS drainage,
       ss.sourceShort AS soil_source,
       h.frostDamageThresholdC AS frost_damage_c, h.heatDamageThresholdC AS heat_damage_c,
       h.sourceShort AS frost_source
"""
_ROTATION_QUERY = """
MATCH (rc:RotationConstraint)
RETURN rc.cropA AS crop_a, rc.cropB AS crop_b, coalesce(rc.effect, 'restriction') AS effect,
       rc.intervalYears AS interval_years, rc.reason AS reason, rc.sourceShort AS source
"""
_TRIAL_CROPS_QUERY = """
MATCH (vt:VarietyTrial) WHERE vt.cropEppo IS NOT NULL AND vt.cropEppo <> ''
RETURN DISTINCT vt.cropEppo AS eppo
"""
_SOIL_KEYS = ("ph_min", "ph_max", "textures", "drainage")


@dataclass(frozen=True)
class SpeciesTolerance:
    soil: dict | None
    frost_damage_c: float | None
    soil_source: str | None
    frost_source: str | None


@dataclass(frozen=True)
class Reference:
    tolerances: dict[str, SpeciesTolerance]
    constraints: tuple[Constraint, ...]
    trial_eppo: frozenset[str]


_cache: dict[str, tuple[float, Reference]] = {}


def clear_cache() -> None:
    _cache.clear()


async def load_reference(source: KnowledgeSource, now: float | None = None) -> Reference:
    t = time.monotonic() if now is None else now
    hit = _cache.get(source.graph_id)
    if hit is not None and t - hit[0] < REFERENCE_TTL_S:
        return hit[1]
    async with source.driver.session() as session:
        species = await (await session.run(_SPECIES_QUERY)).data()
        rotation = await (await session.run(_ROTATION_QUERY)).data()
        trials = await (await session.run(_TRIAL_CROPS_QUERY)).data()
    built = Reference(
        tolerances={row["species"]: _tolerance(row) for row in species if row.get("species")},
        constraints=tuple(_constraint(row) for row in rotation if row.get("crop_a") and row.get("crop_b")),
        trial_eppo=frozenset(row["eppo"] for row in trials if row.get("eppo")),
    )
    _cache[source.graph_id] = (t, built)
    return built


def _tolerance(row: dict) -> SpeciesTolerance:
    soil = {k: row.get(k) for k in _SOIL_KEYS}
    has_soil = any(v not in (None, "", []) for v in soil.values())
    return SpeciesTolerance(soil=soil if has_soil else None, frost_damage_c=row.get("frost_damage_c"),
                            soil_source=row.get("soil_source"), frost_source=row.get("frost_source"))


def _constraint(row: dict) -> Constraint:
    return Constraint(crop_a=row["crop_a"], crop_b=row["crop_b"], effect=row.get("effect") or "restriction",
                      interval_years=int(row.get("interval_years") or 0), reason=row.get("reason"),
                      source=row.get("source"))
