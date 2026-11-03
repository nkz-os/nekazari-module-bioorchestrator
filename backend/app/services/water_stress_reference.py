"""Water-stress reference: non-water-stressed baselines (NWSB), upper limits and stage flags.

Pure functions: load and validate the curated YAML, and build the API payload for one species.
Only rows read in their primary source AND carrying a published upper limit are served: without
an upper limit no CWSI can be computed. Other rows stay in the file as data but are not served.
"""
from __future__ import annotations

import math
from pathlib import Path

import yaml

UPPER_FORMS = frozenset({"constant", "linear_vpd"})
EVIDENCE = frozenset({"primary_read", "secondary"})
# Registry licence states that may be served. `unknown` is accepted while permissions are pending.
ACCEPTED_USE = frozenset({"allowed", "permission_granted", "unknown"})
A0_RANGE = (-10.0, 15.0)
A1_RANGE = (-5.0, 1.0)


def load(path: Path) -> dict:
    doc = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(doc, dict):
        raise TypeError(f"{path}: expected a mapping at the top level")
    return doc


def _num(value: object, low: float, high: float) -> bool:
    return (not isinstance(value, bool) and isinstance(value, (int, float))
            and math.isfinite(value) and low <= value <= high)


def _check_source(where: str, source_id: object, known: dict[str, str], errors: list[str]) -> None:
    if not isinstance(source_id, str) or not source_id:
        errors.append(f"{where}: source_id missing")
    elif source_id not in known:
        errors.append(f"{where}: source {source_id!r} not registered")
    elif known[source_id] not in ACCEPTED_USE:
        errors.append(f"{where}: source {source_id!r} licence {known[source_id]!r} not accepted")


def _check_upper(where: str, upper: object, errors: list[str]) -> None:
    if upper is None:
        return
    if not isinstance(upper, dict) or upper.get("form") not in UPPER_FORMS:
        errors.append(f"{where}.upper: form must be one of {sorted(UPPER_FORMS)}")
        return
    if upper["form"] == "constant":
        if not _num(upper.get("value_c"), *A0_RANGE):
            errors.append(f"{where}.upper: constant needs value_c in {A0_RANGE}")
    else:
        if not _num(upper.get("a0_c"), *A0_RANGE):
            errors.append(f"{where}.upper: linear_vpd needs a0_c in {A0_RANGE}")
        if not _num(upper.get("a1_c_per_kpa"), *A1_RANGE):
            errors.append(f"{where}.upper: linear_vpd needs a1_c_per_kpa in {A1_RANGE}")


def _check_valid(where: str, valid: object, errors: list[str]) -> None:
    if not isinstance(valid, dict) or not isinstance(valid.get("clear_sky"), bool):
        errors.append(f"{where}.valid: clear_sky must be true or false")
        return
    for key, low, high in (("solar_hour_from", 0, 24), ("solar_hour_to", 0, 24),
                           ("vpd_kpa_min", 0, 20), ("vpd_kpa_max", 0, 20)):
        if valid.get(key) is not None and not _num(valid[key], low, high):
            errors.append(f"{where}.valid.{key}: out of range")


def _check_baseline(slug: str, b: object, known: dict[str, str], seen: set[str],
                    errors: list[str]) -> None:
    bid = b.get("id") if isinstance(b, dict) else None
    where = f"{slug}.{bid or '?'}"
    if not isinstance(b, dict) or not isinstance(bid, str) or not bid:
        errors.append(f"{where}: baseline needs an id")
        return
    if bid in seen:
        errors.append(f"{where}: duplicate id")
    seen.add(bid)
    if not _num(b.get("a0_c"), *A0_RANGE):
        errors.append(f"{where}.a0_c: must be a number in {A0_RANGE}")
    if not _num(b.get("a1_c_per_kpa"), *A1_RANGE):
        errors.append(f"{where}.a1_c_per_kpa: must be a number in {A1_RANGE}")
    stages = b.get("stages")
    if stages is not None and (not isinstance(stages, list) or not stages
                               or not all(isinstance(s, str) and s for s in stages)):
        errors.append(f"{where}.stages: null (all) or a non-empty list of names")
    if "upper" not in b:
        errors.append(f"{where}.upper: missing (use null when none is published)")
    _check_upper(where, b.get("upper"), errors)
    _check_valid(where, b.get("valid"), errors)
    if b.get("evidence") not in EVIDENCE:
        errors.append(f"{where}.evidence: must be one of {sorted(EVIDENCE)}")
    _check_source(where, b.get("source_id"), known, errors)
    if not b.get("ref"):
        errors.append(f"{where}.ref: missing")


def _check_stage(where: str, flags: object, known: dict[str, str], errors: list[str]) -> None:
    if not isinstance(flags, dict):
        errors.append(f"{where}: must be a mapping")
        return
    ws = flags.get("water_sensitive")
    if not (isinstance(ws, bool) or ws == "unknown"):
        errors.append(f"{where}.water_sensitive: true, false or 'unknown'")
    if not isinstance(flags.get("rdi_tolerant"), bool):
        errors.append(f"{where}.rdi_tolerant: must be true or false")
    ids = flags.get("source_ids")
    if not isinstance(ids, list):
        errors.append(f"{where}.source_ids: must be a list")
        ids = []
    for sid in ids:
        _check_source(where, sid, known, errors)
    if not flags.get("ref"):
        errors.append(f"{where}.ref: missing")
    if flags.get("water_sensitive") is False and not ids:
        errors.append(f"{where}: water_sensitive false needs a source")


def validate(doc: dict, known_sources: dict[str, str]) -> list[str]:
    """Return the errors of the document. `known_sources` maps source_id to commercial_use."""
    errors: list[str] = []
    species = doc.get("species") if isinstance(doc, dict) else None
    if not isinstance(species, dict) or not species:
        return ["species: missing or empty"]
    for slug, block in species.items():
        if not isinstance(block, dict):
            errors.append(f"{slug}: must be a mapping")
            continue
        seen: set[str] = set()
        for b in block.get("baselines") or []:
            _check_baseline(slug, b, known_sources, seen, errors)
        for name, flags in (block.get("stages") or {}).items():
            _check_stage(f"{slug}.stages.{name}", flags, known_sources, errors)
    return errors


def _drop_none(d: dict) -> dict:
    return {k: v for k, v in d.items() if v is not None}


def _baseline_payload(b: dict) -> dict:
    upper, valid = b["upper"], b["valid"]
    return _drop_none({
        "id": b["id"], "cultivar": b.get("cultivar"), "stages": b.get("stages"),
        "condition": b.get("condition"), "a0C": b["a0_c"], "a1CPerKpa": b["a1_c_per_kpa"],
        "upper": _drop_none({"form": upper["form"], "valueC": upper.get("value_c"),
                             "a0C": upper.get("a0_c"), "a1CPerKpa": upper.get("a1_c_per_kpa")}),
        "valid": _drop_none({"solarHourFrom": valid.get("solar_hour_from"),
                             "solarHourTo": valid.get("solar_hour_to"),
                             "vpdKpaMin": valid.get("vpd_kpa_min"),
                             "vpdKpaMax": valid.get("vpd_kpa_max"),
                             "clearSky": valid["clear_sky"]}),
        "site": b.get("site"), "evidence": b["evidence"], "sourceId": b["source_id"],
        "ref": b["ref"], "assumption": b.get("assumption"),
    })


def species_payload(doc: dict, slug: str) -> dict | None:
    """API payload for a species, or None when nothing is served for it.

    Served: baselines read in their primary source that carry an upper limit. Secondary rows and
    rows without an upper limit stay in the file but are not served.
    """
    block = (doc.get("species") or {}).get(slug)
    if not isinstance(block, dict):
        return None
    baselines = [_baseline_payload(b) for b in block.get("baselines") or []
                 if b.get("evidence") == "primary_read" and b.get("upper") is not None]
    stages = {
        name: {"waterSensitive": f["water_sensitive"], "rdiTolerant": f["rdi_tolerant"],
               "sourceIds": list(f["source_ids"]), "ref": f["ref"]}
        for name, f in (block.get("stages") or {}).items()
    }
    if not baselines and not stages:
        return None
    ids = {b["sourceId"] for b in baselines}
    for f in stages.values():
        ids.update(f["sourceIds"])
    return {"baselines": baselines, "stages": stages, "sourceIds": sorted(ids)}
