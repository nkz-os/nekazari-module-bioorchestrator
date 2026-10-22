"""Mandatory source attributions: re-export of ``app.common.attribution``.

The helpers live in ``app.common`` so service layers (``app.planning``) can use them without
importing the ``app.api`` package, whose ``__init__`` imports those same services.
"""

from __future__ import annotations

from app.common.attribution import (
    attach_attributions,
    recommendation_source_ids,
    rows_source_ids,
)

__all__ = ["attach_attributions", "recommendation_source_ids", "rows_source_ids"]
