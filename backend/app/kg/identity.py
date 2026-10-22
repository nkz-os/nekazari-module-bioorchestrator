"""Deterministic identity of KG nodes.

The natural key of a node is a function of **values observed in the source only**, in canonical
form. Never of a normalised or derived field (a site alias, the variety normaliser, an irrigation
vocabulary, a registry id): changing a normalisation table must not change an identity, or a
re-ingest would duplicate every node it touched.

Canonical form, fixed by :data:`KEY_VERSION`:

* text: Unicode NFC, trimmed; blank is missing (``None``). Case and inner spacing are kept: raw
  means raw, and only registries fold them.
* numbers: ``int`` as its decimal text; a float as the decimal text of the integer when it has
  one, else ``repr(float(x))``. So ``8``, ``8.0`` and ``-0.0``/``0`` agree, and distinct floats stay
  distinct. Numbers are tagged (``{"n": "8"}``) so the number 8 and the text "8" never agree; text
  is never parsed as a number. NaN and infinity are refused.
* the whole key input is one JSON text with sorted keys, no spaces, UTF-8, carrying the node kind
  and the key version; the key is its sha256 hex digest. Nothing is delimiter-joined, so field
  boundaries cannot be shifted.

Which fields of each row are key fields is declared next to the row (``*_KEY_FIELDS`` in
:mod:`app.kg.model`) and checked there at import. ``key_payload(row)`` returns the exact text that
is hashed, which is what to look at when two keys differ unexpectedly.

``site_key`` is the one key that is not a hash: the canonical id of the sites registry already is
the identity of a place, stable and readable. A place the registry does not know has no key and
cannot be loaded.
"""
from __future__ import annotations

import hashlib
import json
import math
import numbers
import unicodedata
from collections.abc import Mapping
from typing import Any

from .model import (
    DOCUMENT_KEY_FIELDS,
    OBSERVATION_KEY_FIELDS,
    STUDY_KEY_FIELDS,
    UNIT_KEY_FIELDS,
    DocumentRow,
    ObservationRow,
    SiteRow,
    StudyRow,
    UnitRow,
    VarietyRow,
)
from .registries import lookup_key

# Bump only on a deliberate change of the canonical form: every key changes with it.
KEY_VERSION = 1


def _canon_text(value: str) -> str | None:
    text = unicodedata.normalize("NFC", value).strip()
    return text or None


def _canon_number(value: numbers.Real) -> dict[str, str]:
    if isinstance(value, numbers.Integral):
        return {"n": str(int(value))}
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"cannot key a non-finite number: {value!r}; a key needs a finite value")
    return {"n": str(int(number)) if number.is_integer() else repr(number)}


def _canon(value: Any) -> Any:
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, str):
        return _canon_text(value)
    if isinstance(value, numbers.Real):
        return _canon_number(value)
    if isinstance(value, (list, tuple)):
        return [_canon(item) for item in value]
    raise TypeError(f"cannot key a value of type {type(value).__name__}: {value!r}")


def _dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _canon_mapping(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {str(name): _canon(value) for name, value in payload.items()}


def canonical_json(payload: Mapping[str, Any]) -> str:
    """The canonical text of a mapping of observed values (see the module docstring)."""
    return _dumps(_canon_mapping(payload))


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _levels(unit: UnitRow) -> list[list[Any]]:
    """Factor levels in canonical order, so the order the source lists them in is irrelevant."""
    triples = [[level.factor, level.level, level.unit] for level in unit.factor_levels]
    return sorted(triples, key=lambda triple: _dumps(_canon(triple)))


def _fields(row: Any, names: tuple[str, ...]) -> dict[str, Any]:
    return {name: getattr(row, name) for name in names}


def key_payload(row: DocumentRow | StudyRow | UnitRow | ObservationRow | VarietyRow) -> str:
    """The exact canonical text hashed into the key of a row; ``TypeError`` for any other row."""
    if isinstance(row, UnitRow):
        fields = _fields(row, UNIT_KEY_FIELDS)
        fields["factor_levels"] = _levels(row)
        kind = "unit"
    elif isinstance(row, ObservationRow):
        fields = _fields(row, OBSERVATION_KEY_FIELDS)
        fields["date"] = row.date.isoformat() if row.date is not None else None
        kind = "observation"
    elif isinstance(row, DocumentRow):
        fields, kind = _fields(row, DOCUMENT_KEY_FIELDS), "document"
    elif isinstance(row, StudyRow):
        fields, kind = _fields(row, STUDY_KEY_FIELDS), "study"
    elif isinstance(row, VarietyRow):
        # the normalised name is the identity of a variety (spec 4.3); it is never part of a unit key
        fields, kind = {"crop_eppo": row.crop_eppo, "name": lookup_key(row.name)}, "variety"
    else:
        raise TypeError(f"{type(row).__name__} has no hashed key")
    # kind and version are fixed text and a bare integer, outside the canonicalisation of values
    return _dumps({"kind": kind, "v": KEY_VERSION, **_canon_mapping(fields)})


def _key(row: Any, expected: type) -> str:
    if not isinstance(row, expected):
        raise TypeError(f"expected a {expected.__name__}, got {type(row).__name__}")
    return _digest(key_payload(row))


def document_key(row: DocumentRow) -> str:
    """``ArticleSource.documentKey``: source, title, issue and year, as printed."""
    return _key(row, DocumentRow)


def study_key(row: StudyRow) -> str:
    """``Study.studyKey``: source, study type, canonical crop, season and scope, as printed."""
    return _key(row, StudyRow)


def unit_key(row: UnitRow) -> str:
    """``ObservationUnit.unitKey``: the raw observed fields, the source, the document and the crop."""
    return _key(row, UnitRow)


def obs_key(row: ObservationRow) -> str:
    """``Observation.obsKey``: the unit's key, the variable, the stage or date and the qualifier."""
    return _key(row, ObservationRow)


def variety_key(row: VarietyRow) -> str:
    """``Variety.varietyKey``: the canonical crop and the normalised variety name."""
    return _key(row, VarietyRow)


def site_key(row: SiteRow) -> str:
    """``TrialSite.siteKey``: the canonical site id of the sites registry, as is."""
    if not isinstance(row, SiteRow):
        raise TypeError(f"expected a SiteRow, got {type(row).__name__}")
    return row.site_id


__all__ = [
    "KEY_VERSION",
    "canonical_json",
    "document_key",
    "key_payload",
    "obs_key",
    "site_key",
    "study_key",
    "unit_key",
    "variety_key",
]
