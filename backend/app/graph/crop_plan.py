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
        "species": _prop(segment.get("crop")),
        "role": _prop(segment.get("role")),
        "cropSeason": _prop(season),
        "seq": _prop(seq),
        "status": _prop("planned"),
        "terminationMethod": _prop(segment.get("termination_method")),
    }
    if segment.get("variety"):
        entity["variety"] = _prop(segment["variety"])
    if window[0]:
        entity["sowingWindowStart"] = _date(window[0])
    if window[1]:
        entity["sowingWindowEnd"] = _date(window[1])
    if segment.get("expected_termination"):
        entity["expectedTerminationDate"] = _date(segment["expected_termination"])
    return entity
