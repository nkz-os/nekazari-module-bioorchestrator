"""Pure builders for multi-segment crop plan AgriCrop entities (no I/O)."""
from __future__ import annotations

VALID_ROLES = {"cover_crop", "main_crop", "catch_crop"}
VALID_TERMINATION = {"roller_crimper", "harvest", "incorporate", "grazing"}


def segment_urn(tenant_id: str, parcel_id: str, season: str, seq: int) -> str:
    parcel_short = parcel_id.split(":")[-1]
    return f"urn:ngsi-ld:AgriCrop:{tenant_id}:{parcel_short}:{season}:{seq}"


def _date(v):
    return {"type": "Property", "value": {"@type": "Date", "@value": v}}


def _prop(v):
    return {"type": "Property", "value": v}


def build_segment_entity(tenant_id: str, parcel_id: str, season: str, seq: int, segment: dict) -> dict:
    """Build the NGSI-LD AgriCrop dict for one PLANNED segment.

    Sets planned windows only; actual plantingDate/terminationDate are absent
    until advance. status='planned'. @context is injected by OrionClient.
    """
    window = segment.get("sowing_window") or [None, None]
    entity: dict = {
        "id": segment_urn(tenant_id, parcel_id, season, seq),
        "type": "AgriCrop",
        "hasAgriParcel": {"type": "Relationship", "object": parcel_id},
        "role": _prop(segment.get("role")),
        "cropSeason": _prop(season),
        "seq": _prop(seq),
        "status": _prop("planned"),
        "terminationMethod": _prop(segment.get("termination_method")),
    }
    if segment.get("crop"):
        entity["species"] = _prop(segment["crop"])
    if segment.get("variety"):
        entity["variety"] = _prop(segment["variety"])
    if window[0]:
        entity["sowingWindowStart"] = _date(window[0])
    if window[1]:
        entity["sowingWindowEnd"] = _date(window[1])
    if segment.get("expected_termination"):
        entity["expectedTerminationDate"] = _date(segment["expected_termination"])
    return entity


def _windows_overlap(a: list | None, b: list | None) -> bool:
    """True if two [start, end] ISO-date windows intersect.

    Missing/partial windows are treated as non-overlapping (fail-safe: we
    only warn when we have enough information to be confident).
    """
    if not a or not b:
        return False
    a_start, a_end = a[0] if len(a) > 0 else None, a[1] if len(a) > 1 else None
    b_start, b_end = b[0] if len(b) > 0 else None, b[1] if len(b) > 1 else None
    if not a_start or not a_end or not b_start or not b_end:
        return False
    return a_start <= b_end and b_start <= a_end


def sanity_warnings(segments: list[dict]) -> list[dict]:
    """Fail-safe, non-blocking sanity checks for a crop plan's segments.

    Checks (commit-time, never block):
      - invalid `role` (not in VALID_ROLES)
      - invalid `termination_method` (not in VALID_TERMINATION)
      - overlapping sowing windows between any two segments

    Agronomic rotation-constraint checking is deferred to SP2 and is
    intentionally NOT performed here.
    """
    warnings: list[dict] = []
    for i, seg in enumerate(segments):
        role = seg.get("role")
        if role is not None and role not in VALID_ROLES:
            warnings.append({"type": "invalid_role", "seq": i, "value": role})
        method = seg.get("termination_method")
        if method is not None and method not in VALID_TERMINATION:
            warnings.append({"type": "invalid_termination_method", "seq": i, "value": method})

    for i in range(len(segments)):
        for j in range(i + 1, len(segments)):
            if _windows_overlap(segments[i].get("sowing_window"), segments[j].get("sowing_window")):
                warnings.append({"type": "window_overlap", "seq": [i, j]})

    return warnings
