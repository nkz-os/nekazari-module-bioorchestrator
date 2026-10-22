"""Schema migration of a NON-empty restored copy of the served graph (T12, option b).

The build applies the migrations only on an empty target (a data migration rewrites loaded units). A restored
copy is not empty, and some constraints of the F1 schema may not hold on its data. ``preflight`` therefore reads
(READ_ACCESS only) every UNIQUE constraint the migrations would create that the copy does not have yet, and
reports

* each constraint whose keys are duplicated in the copy (up to :data:`SAMPLE` offending keys, and the number of
  duplicated groups), and
* each plain index that would block a UNIQUE constraint on the same label and properties (Neo4j answers
  ``IndexAlreadyExists``; the index must be dropped by hand, nothing here drops anything).

``migrate`` runs the preflight and, only if it is clean, applies the migrations **schema-only**: constraints and
indexes, never the data statements. The copy already holds whatever the data migrations did to the served graph;
running them again would rewrite its legacy trials. Idempotent: constraints that exist are skipped by the
preflight and answered "already exists" by the runner.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

from neo4j import READ_ACCESS

from .migrations import (
    MigrationFile,
    MigrationReport,
    apply_migrations,
    discover_migrations,
)

logger = logging.getLogger(__name__)

SAMPLE = 10
_UNIQUE = re.compile(
    r"^CREATE\s+CONSTRAINT\s+(?P<name>\w+)\s+(?:IF\s+NOT\s+EXISTS\s+)?FOR\s+\(\s*\w+\s*:\s*(?P<label>\w+)\s*\)\s+"
    r"REQUIRE\s+(?P<props>.+?)\s+IS\s+UNIQUE$", re.IGNORECASE | re.DOTALL)


class PreflightFailed(RuntimeError):
    """The copy violates a constraint the migrations would create; nothing was written."""

    def __init__(self, violations: list[Violation]) -> None:
        super().__init__(f"{len(violations)} constraint(s) cannot be created on this graph: "
                         + "; ".join(v.summary() for v in violations))
        self.violations = violations


@dataclass(frozen=True)
class Wanted:
    name: str
    label: str
    properties: tuple[str, ...]
    file: str


@dataclass
class Violation:
    constraint: str
    label: str
    properties: tuple[str, ...]
    kind: str  # "duplicate" | "blocking-index"
    groups: int = 0
    sample: list[Any] = field(default_factory=list)
    index: str | None = None

    def summary(self) -> str:
        if self.kind == "blocking-index":
            return f"{self.constraint}: index {self.index} on {self.label}{list(self.properties)} blocks it"
        return f"{self.constraint}: {self.groups} duplicated key group(s) on {self.label}{list(self.properties)}"

    def to_dict(self) -> dict[str, Any]:
        return {"constraint": self.constraint, "label": self.label, "properties": list(self.properties),
                "kind": self.kind, "groups": self.groups, "sample": self.sample, "index": self.index}


@dataclass
class Preflight:
    already_present: list[str]
    to_create: list[str]
    violations: list[Violation]

    @property
    def clean(self) -> bool:
        return not self.violations

    def to_dict(self) -> dict[str, Any]:
        return {"clean": self.clean, "constraints_already_present": self.already_present,
                "constraints_to_create": self.to_create, "violations": [v.to_dict() for v in self.violations]}


def wanted_constraints(migrations: list[MigrationFile] | None = None) -> list[Wanted]:
    """Every UNIQUE constraint the migrations create (the only kind they declare on Community)."""
    out = []
    for mf in migrations if migrations is not None else discover_migrations():
        for statement in mf.statements:
            match = _UNIQUE.match(statement.strip())
            if match is None:
                continue
            props = tuple(p.strip().split(".", 1)[1] for p in match["props"].strip("() ").split(","))
            out.append(Wanted(match["name"], match["label"], props, mf.name))
    return out


def _clip(value: Any) -> Any:
    return value[:120] if isinstance(value, str) else value


async def _read(driver: Any, database: str | None, cypher: str, **params: Any) -> list[dict[str, Any]]:
    async def work(tx: Any) -> list[dict[str, Any]]:
        return [r.data() for r in await (await tx.run(cypher, **params)).fetch(10_000_000)]

    async with driver.session(database=database, default_access_mode=READ_ACCESS) as session:
        return await session.execute_read(work)


async def preflight(driver: Any, *, database: str | None = None,
                    migrations: list[MigrationFile] | None = None) -> Preflight:
    """Read-only check of every constraint the migrations would create that the graph does not have."""
    constraints = await _read(driver, database,
                              "SHOW CONSTRAINTS YIELD name, type, labelsOrTypes, properties "
                              "RETURN name, type, labelsOrTypes, properties")
    existing = {(c["labelsOrTypes"][0], tuple(c["properties"])) for c in constraints
                if c["type"] == "UNIQUENESS" and len(c["labelsOrTypes"]) == 1}
    indexes = await _read(driver, database,
                          "SHOW INDEXES YIELD name, type, labelsOrTypes, properties, owningConstraint "
                          "WHERE type <> 'LOOKUP' AND owningConstraint IS NULL "
                          "RETURN name, labelsOrTypes, properties")
    plain = {(i["labelsOrTypes"][0], tuple(i["properties"])): i["name"] for i in indexes
             if len(i["labelsOrTypes"]) == 1}
    present: list[str] = []
    create: list[str] = []
    violations: list[Violation] = []
    for want in wanted_constraints(migrations):
        if (want.label, want.properties) in existing:
            present.append(want.name)
            continue
        create.append(want.name)
        blocking = plain.get((want.label, want.properties))
        if blocking is not None:
            violations.append(Violation(want.name, want.label, want.properties, "blocking-index", index=blocking))
        keys = ", ".join(f"n.`{p}` AS k{i}" for i, p in enumerate(want.properties))
        present_all = " AND ".join(f"n.`{p}` IS NOT NULL" for p in want.properties)
        groups = await _read(
            driver, database,
            f"MATCH (n:`{want.label}`) WHERE {present_all} WITH {keys}, count(*) AS c WHERE c > 1 "
            "RETURN " + ", ".join(f"k{i}" for i in range(len(want.properties))) + ", c "
            f"ORDER BY c DESC LIMIT {SAMPLE}")
        if groups:
            total = (await _read(
                driver, database,
                f"MATCH (n:`{want.label}`) WHERE {present_all} WITH {keys}, count(*) AS c WHERE c > 1 "
                "RETURN count(*) AS groups"))[0]["groups"]
            sample = [{"key": [_clip(g[f"k{i}"]) for i in range(len(want.properties))], "nodes": g["c"]}
                      for g in groups]
            violations.append(Violation(want.name, want.label, want.properties, "duplicate", groups=total,
                                        sample=sample))
    logger.info("kg migrate-restored preflight present=%d to_create=%d violations=%d",
                len(present), len(create), len(violations))
    return Preflight(present, create, violations)


async def migrate(driver: Any, *, database: str | None = None) -> tuple[Preflight, MigrationReport]:
    """Preflight, then schema-only migrations. Raises :class:`PreflightFailed` before writing anything."""
    checked = await preflight(driver, database=database)
    if not checked.clean:
        raise PreflightFailed(checked.violations)
    return checked, await apply_migrations(driver, include_data=False, database=database)


__all__ = ["Preflight", "PreflightFailed", "Violation", "migrate", "preflight", "wanted_constraints"]
