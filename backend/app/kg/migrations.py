"""Strict Cypher migration runner (single implementation for app startup, ops script and KG build).

Replaces the two earlier runners, which shared defect P4: the startup runner split on ``;`` and
discarded every chunk that *began* with a comment (so the first statement after each comment
block never ran) and swallowed every error, and the ops script only stripped ``//`` per line
(mangling ``'http://...'`` strings) and did not handle ``/* */``.

Behaviour
  * Comments (``//`` and ``/* */``) are removed by a small lexer that knows about quoted strings
    and backtick identifiers; ``;`` splits statements only outside them. An unterminated string,
    identifier or block comment is an error, never silently accepted.
  * Every file is parsed before anything is executed: a malformed file aborts the run with the
    database untouched.
  * Files run in numeric order (``NNN_name.cypher``, top level of the migrations directory).
    ``enterprise/`` holds Enterprise-only schema rules and is skipped unless asked for.
  * Statements run in order. "An equivalent schema rule already exists" counts as success (matched
    on the server error *code*, not on message text); any other error stops the run with a
    :class:`MigrationError` that names the file and statement. Schema statements are not
    transactional in Neo4j, so a failed run can have applied the statements before the failing one;
    the failed file is then not recorded, and re-running is safe because every statement is
    idempotent.
  * ``include_data=False`` runs only schema statements (``CREATE``/``DROP`` of constraints and
    indexes) and reports the data statements it skipped. App startup uses it: data migrations
    (``MERGE``/``SET``/...) have never run at startup and must not start running at every boot.
  * After each file the runner upserts ``(:SchemaVersion {file, sha256, appliedAt, mode})``.
    ``mode`` is ``full`` or ``schema-only`` (data statements skipped). The record is an audit trail
    and drift detector, not a gate: statements are always re-applied, so a constraint that was
    dropped or never restored is recreated on the next run.

Migrations must stay idempotent (``IF NOT EXISTS`` / ``IF EXISTS`` / ``MERGE``).
"""
from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from neo4j.exceptions import Neo4jError

logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "cypher_migrations"
ENTERPRISE_SUBDIR = "enterprise"

SCHEMA_VERSION_LABEL = "SchemaVersion"
SCHEMA_VERSION_CONSTRAINT = (
    "CREATE CONSTRAINT schema_version_file IF NOT EXISTS "
    "FOR (v:SchemaVersion) REQUIRE v.file IS UNIQUE"
)

MODE_FULL = "full"
MODE_SCHEMA_ONLY = "schema-only"

# Server codes meaning "this schema rule is already there": success, not failure.
_ALREADY_EXISTS_CODES = frozenset(
    {
        "Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists",
        "Neo.ClientError.Schema.ConstraintAlreadyExists",
        "Neo.ClientError.Schema.IndexAlreadyExists",
    }
)

_FILE_RE = re.compile(r"^(\d{3})_[A-Za-z0-9_.-]+\.cypher$")
_SCHEMA_STMT_RE = re.compile(
    r"^(?:CREATE\s+(?:OR\s+REPLACE\s+)?|DROP\s+)"
    r"(?:(?:RANGE|TEXT|POINT|LOOKUP|FULLTEXT|VECTOR)\s+)?(?:CONSTRAINT|INDEX)\b",
    re.IGNORECASE,
)


class MigrationError(RuntimeError):
    """A migration could not be applied. Carries the file and statement that failed."""

    def __init__(
        self,
        message: str,
        *,
        file: str | None = None,
        statement_no: int | None = None,
        statement: str | None = None,
        code: str | None = None,
    ) -> None:
        super().__init__(message)
        self.file = file
        self.statement_no = statement_no
        self.statement = statement
        self.code = code


class MigrationParseError(MigrationError):
    """A migration file could not be split into statements (unterminated string/comment)."""


def split_statements(text: str) -> list[str]:
    """Strip ``//`` and ``/* */`` comments and split on ``;`` outside strings and backticks."""
    statements: list[str] = []
    buf: list[str] = []
    n = len(text)
    i = 0

    def flush() -> None:
        stmt = "".join(buf).strip()
        buf.clear()
        if stmt:
            statements.append(stmt)

    while i < n:
        ch = text[i]
        nxt = text[i + 1] if i + 1 < n else ""
        if ch == "/" and nxt == "/":  # line comment: keep the newline so tokens stay apart
            end = text.find("\n", i)
            i = n if end == -1 else end
            buf.append(" ")
        elif ch == "/" and nxt == "*":
            end = text.find("*/", i + 2)
            if end == -1:
                raise MigrationParseError(f"unterminated /* comment at offset {i}")
            i = end + 2
            buf.append(" ")
        elif ch in ("'", '"', "`"):
            j = i + 1
            while j < n and text[j] != ch:
                j += 2 if (text[j] == "\\" and ch != "`") else 1
            if j >= n:
                kind = "identifier" if ch == "`" else "string"
                raise MigrationParseError(f"unterminated {kind} starting at offset {i}")
            buf.append(text[i : j + 1])
            i = j + 1
        elif ch == ";":
            flush()
            i += 1
        else:
            buf.append(ch)
            i += 1
    flush()
    return statements


def is_schema_statement(statement: str) -> bool:
    """True for ``CREATE``/``DROP`` of a constraint or index (the only DDL the runner knows)."""
    return _SCHEMA_STMT_RE.match(statement.lstrip()) is not None


@dataclass(frozen=True)
class MigrationFile:
    """One migration file, parsed. ``name`` is the path relative to the migrations directory."""

    name: str
    sha256: str
    statements: tuple[str, ...]

    @property
    def data_statements(self) -> tuple[str, ...]:
        return tuple(s for s in self.statements if not is_schema_statement(s))


@dataclass(frozen=True)
class AppliedMigration:
    """Outcome for one file."""

    file: str
    sha256: str
    mode: str
    executed: int  # statements sent to the server (already-exists included)
    already_present: int  # of those, answered "equivalent rule already exists"
    skipped_data: int  # data statements not run because include_data=False
    recorded: bool  # the SchemaVersion node was created or updated


@dataclass(frozen=True)
class MigrationReport:
    applied: tuple[AppliedMigration, ...]

    @property
    def files(self) -> tuple[str, ...]:
        return tuple(a.file for a in self.applied)

    @property
    def skipped_data(self) -> int:
        return sum(a.skipped_data for a in self.applied)


def discover_migrations(
    directory: Path | None = None, *, include_enterprise: bool = False
) -> list[MigrationFile]:
    """Parse the migration files in run order. Raises before any DB access on a bad layout."""
    base = Path(directory) if directory is not None else MIGRATIONS_DIR
    if not base.is_dir():
        raise MigrationError(f"migrations directory not found: {base.name}")

    top: list[Path] = []
    seen: dict[str, str] = {}
    for path in sorted(base.glob("*.cypher")):
        match = _FILE_RE.match(path.name)
        if match is None:
            raise MigrationError(
                f"unexpected migration file name (want NNN_name.cypher): {path.name}",
                file=path.name,
            )
        prefix = match.group(1)
        if prefix in seen:
            raise MigrationError(
                f"duplicate migration number {prefix}: {seen[prefix]} and {path.name}",
                file=path.name,
            )
        seen[prefix] = path.name
        top.append(path)

    paths = [(p.name, p) for p in top]
    if include_enterprise:
        ent_dir = base / ENTERPRISE_SUBDIR
        if ent_dir.is_dir():
            paths += [(f"{ENTERPRISE_SUBDIR}/{p.name}", p) for p in sorted(ent_dir.glob("*.cypher"))]

    parsed: list[MigrationFile] = []
    for name, path in paths:
        raw = path.read_bytes()
        try:
            statements = split_statements(raw.decode("utf-8"))
        except MigrationParseError as exc:
            raise MigrationParseError(f"{name}: {exc}", file=name) from exc
        parsed.append(MigrationFile(name, hashlib.sha256(raw).hexdigest(), tuple(statements)))
    return parsed


async def _run_statement(session: Any, statement: str) -> bool:
    """Execute one statement. Return True if the server said the rule already existed."""
    try:
        result = await session.run(statement)
        await result.consume()
    except Neo4jError as exc:
        if exc.code in _ALREADY_EXISTS_CODES:
            return True
        raise
    return False


async def _record(session: Any, file: str, sha256: str, mode: str) -> bool:
    """Upsert the SchemaVersion node; True if it was created or changed."""
    result = await session.run(
        "MATCH (v:SchemaVersion {file: $file}) RETURN v.sha256 AS sha256, v.mode AS mode",
        file=file,
    )
    previous = await result.single()
    if previous is not None and previous["sha256"] == sha256 and previous["mode"] == mode:
        return False
    if previous is not None and previous["sha256"] != sha256:
        logger.warning(
            "migration file changed since last applied file=%s previous_sha256=%s sha256=%s",
            file, previous["sha256"], sha256,
        )
    result = await session.run(
        "MERGE (v:SchemaVersion {file: $file}) "
        "SET v.sha256 = $sha256, v.mode = $mode, v.appliedAt = datetime()",
        file=file, sha256=sha256, mode=mode,
    )
    await result.consume()
    return True


async def apply_migrations(
    driver: Any,
    directory: Path | None = None,
    *,
    include_data: bool = True,
    include_enterprise: bool = False,
    database: str | None = None,
) -> MigrationReport:
    """Apply every migration in order; raise :class:`MigrationError` on the first real failure."""
    files = discover_migrations(directory, include_enterprise=include_enterprise)
    applied: list[AppliedMigration] = []

    async with driver.session(database=database) as session:
        try:
            await _run_statement(session, SCHEMA_VERSION_CONSTRAINT)
        except Neo4jError as exc:
            raise MigrationError(
                f"cannot create the SchemaVersion constraint: {exc.code}",
                statement=SCHEMA_VERSION_CONSTRAINT, code=exc.code,
            ) from exc

        for mf in files:
            executed = present = skipped = 0
            for no, statement in enumerate(mf.statements, start=1):
                if not include_data and not is_schema_statement(statement):
                    skipped += 1
                    continue
                try:
                    existed = await _run_statement(session, statement)
                except Neo4jError as exc:
                    logger.error(
                        "migration failed file=%s statement_no=%d code=%s message=%s",
                        mf.name, no, exc.code, str(exc).replace("\n", " ")[:300],
                    )
                    raise MigrationError(
                        f"{mf.name} statement {no} failed: {exc.code}: {str(exc)[:300]}",
                        file=mf.name, statement_no=no, statement=statement[:300], code=exc.code,
                    ) from exc
                executed += 1
                present += int(existed)

            mode = MODE_SCHEMA_ONLY if skipped else MODE_FULL
            try:
                recorded = await _record(session, mf.name, mf.sha256, mode)
            except Neo4jError as exc:
                raise MigrationError(
                    f"{mf.name}: could not record SchemaVersion: {exc.code}",
                    file=mf.name, code=exc.code,
                ) from exc
            if skipped:
                logger.info(
                    "migration data statements skipped file=%s skipped=%d (schema-only run)",
                    mf.name, skipped,
                )
            logger.info(
                "migration applied file=%s sha256=%s mode=%s executed=%d already_present=%d recorded=%s",
                mf.name, mf.sha256, mode, executed, present, recorded,
            )
            applied.append(
                AppliedMigration(mf.name, mf.sha256, mode, executed, present, skipped, recorded)
            )
    return MigrationReport(tuple(applied))
