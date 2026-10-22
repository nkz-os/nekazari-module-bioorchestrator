"""Fixtures shared by the KG tests."""
from __future__ import annotations

import logging

import pytest


class _ListHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@pytest.fixture
def log_records():
    """Capture records straight from a named logger.

    ``caplog`` relies on the logger being enabled and propagating; other tests run
    ``logging.config.dictConfig``, which disables every logger that already exists. Attaching to
    the logger itself (and re-enabling it for the test) is independent of test order.
    """
    attached: list[tuple[logging.Logger, _ListHandler, int, bool]] = []

    def _capture(name: str, level: int = logging.DEBUG) -> list[logging.LogRecord]:
        lg = logging.getLogger(name)
        handler = _ListHandler()
        attached.append((lg, handler, lg.level, lg.disabled))
        lg.addHandler(handler)
        lg.setLevel(level)
        lg.disabled = False
        return handler.records

    yield _capture
    for lg, handler, old_level, was_disabled in attached:
        lg.removeHandler(handler)
        lg.setLevel(old_level)
        lg.disabled = was_disabled
