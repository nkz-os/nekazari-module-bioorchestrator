"""Source adapters: the raw format of one source -> raw rows (plain dicts) for the contract engine.

An adapter knows the quirks of its source's files and nothing else. It does not map fields to the
canonical model (the contract does), it never reads a registry, and it never invents a value: where
the source or the extraction states nothing, the field is absent and the adapter says so in a warning.

What an adapter may do, and must report each time it does it:

* flatten the layout of the files into one row per published table row;
* clean the *format* of a value (``"-2"`` printed as text is the number -2);
* resolve a scale the extraction lost, from evidence in the source document that is quoted next to
  the rule (never from the magnitude of a value);
* leave out a value the extraction assumed instead of reading it, and count it;
* exclude rows that are not the source's data (a mislabelled paper), listing each one.

Everything it did is returned as :class:`AdapterWarning` entries, so the build manifest and the
quality gate can show it. Anything the adapter does not understand is an :class:`AdapterError`: an
unknown key or shape stops the build instead of being dropped.
"""
from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


class AdapterError(ValueError):
    """A raw file does not have the shape the adapter understands; nothing is guessed."""


@dataclass(frozen=True)
class AdapterWarning:
    """One thing the adapter did that the reader of the bundle must be able to see.

    ``code`` is a stable token; ``count`` the number of rows (or values) concerned; ``where`` up to a
    few places (``file[row]``) to look at first.
    """

    code: str
    message: str
    count: int = 1
    where: tuple[str, ...] = ()


@dataclass(frozen=True)
class AdapterResult:
    """The raw rows of one source, the warnings of the reading, and the files they came from."""

    rows: tuple[dict[str, Any], ...]
    warnings: tuple[AdapterWarning, ...]
    inputs: tuple[tuple[str, str], ...] = field(default=())  # (path relative to the source folder, sha256)


class WarningLog:
    """Collects warnings by code so thousands of equal events become one entry with a count."""

    EXAMPLES = 5

    def __init__(self) -> None:
        self._entries: dict[str, list[Any]] = {}

    def add(self, code: str, message: str, where: str | None = None, count: int = 1, *, keep_all: bool = False) -> None:
        """Record an event; ``keep_all`` lists every place (for the few events that must each be seen)."""
        entry = self._entries.setdefault(code, [message, 0, []])
        entry[1] += count
        if where is not None and (keep_all or len(entry[2]) < self.EXAMPLES):
            entry[2].append(where)

    def result(self) -> tuple[AdapterWarning, ...]:
        return tuple(
            AdapterWarning(code=code, message=message, count=count, where=tuple(where))
            for code, (message, count, where) in sorted(self._entries.items())
        )


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sorted_files(directory: Path, pattern: str) -> list[Path]:
    """The files of a glob in a fixed order, so a build never depends on the file system."""
    return sorted(directory.glob(pattern), key=lambda p: p.name)


def fingerprints(source_dir: Path, files: Iterable[Path]) -> tuple[tuple[str, str], ...]:
    return tuple((path.relative_to(source_dir).as_posix(), file_sha256(path)) for path in files)
