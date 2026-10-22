"""Typed errors of the crop simulation: a stable ``code`` plus a readable message.

``status_code`` is the HTTP status the route answers with: 422 for input the
caller or the data cannot satisfy, 503 for an upstream that is unavailable.
"""
from __future__ import annotations


class SimulationError(Exception):
    """A simulation input is invalid/missing (422) or an upstream is down (503)."""

    def __init__(self, code: str, message: str, status_code: int = 422):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.status_code = status_code

    def detail(self) -> dict[str, str]:
        return {"code": self.code, "message": self.message}
