"""Water-stress reference: non-water-stressed baselines (NWSB), upper limits and stage flags.

Pure functions: load and validate the curated YAML, and build the API payload for one species.
Only rows read in their primary source AND carrying a published upper limit are served: without
an upper limit no CWSI can be computed. Other rows stay in the file as data but are not served.
"""
from __future__ import annotations

import logging
import math
from collections.abc import Callable, Mapping
from pathlib import Path

import yaml

UPPER_FORMS = frozenset({"constant", "linear_vpd"})
EVIDENCE = frozenset({"primary_read", "secondary"})
# Registry licence states that may be served. `unknown` is accepted while permissions are pending.
ACCEPTED_USE = frozenset({"allowed", "permission_granted", "unknown"})
A0_RANGE = (-10.0, 15.0)
A1_RANGE = (-5.0, 1.0)
# `post_maturity` is a reserved key under `stages`: it is not a stage_detection name.
RESERVED_STAGES = frozenset({"post_maturity"})
SENESCENCE_METHODS = frozenset({"timesat_amplitude"})

logger = logging.getLogger(__name__)


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
    for low_key, high_key in (("solar_hour_from", "solar_hour_to"), ("vpd_kpa_min", "vpd_kpa_max")):
        low, high = valid.get(low_key), valid.get(high_key)
        if _num(low, -1e9, 1e9) and _num(high, -1e9, 1e9) and low > high:
            errors.append(f"{where}.valid: {low_key} greater than {high_key}")


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


def _check_senescence(where: str, sen: object, known: dict[str, str],
                      stage_names: set[str] | None, errors: list[str]) -> None:
    if not isinstance(sen, dict):
        errors.append(f"{where}: must be a mapping")
        return
    if sen.get("method") not in SENESCENCE_METHODS:
        errors.append(f"{where}.method: must be one of {sorted(SENESCENCE_METHODS)}")
    if not _num(sen.get("end_fraction"), 0.0, 1.0) or sen["end_fraction"] in (0, 1):
        errors.append(f"{where}.end_fraction: must be a number with 0 < value < 1")
    gate = sen.get("gate_stage")
    if not isinstance(gate, str) or not gate:
        errors.append(f"{where}.gate_stage: missing")
    elif stage_names is not None and gate not in stage_names:
        errors.append(f"{where}.gate_stage: {gate!r} is not a stage of the species")
    _check_source(where, sen.get("source_id"), known, errors)
    if not sen.get("ref"):
        errors.append(f"{where}.ref: missing")


def validate(doc: dict, known_sources: dict[str, str],
             stage_detection: Mapping[str, Mapping] | None = None) -> list[str]:
    """Return the errors of the document. `known_sources` maps source_id to commercial_use.

    `stage_detection` (slug -> block with `stages`) enables the check that a senescence
    `gate_stage` is a stage name of the species; without it that check is skipped.
    """
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
        if block.get("senescence") is not None:
            names = None
            if stage_detection is not None:
                names = set((stage_detection.get(slug) or {}).get("stages") or {})
            _check_senescence(f"{slug}.senescence", block["senescence"], known_sources, names, errors)
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


def _registry_citation(source_id: str) -> dict | None:
    from app.common.source_registry import get_source

    try:
        src = get_source(source_id)
    except KeyError:
        return None
    text = src.get("attribution_text") or (src.get("attribution") or {}).get("en")
    url = src.get("attribution_url") or src.get("url")
    if not text:
        return None
    return {"sourceId": source_id, "text": text, "url": url}


def species_payload(doc: dict, slug: str,
                    citation_lookup: Callable[[str], dict | None] = _registry_citation) -> dict | None:
    """API payload for a species, or None when nothing is served for it.

    Served: baselines read in their primary source that carry an upper limit. Secondary rows and
    rows without an upper limit stay in the file but are not served.
    `citations` lists every source id of the payload from the registry's attribution text, whatever
    its commercial_use: citing is always allowed (the licence attribution list skips unknown ones).
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
    sen = block.get("senescence")
    senescence = None if not isinstance(sen, dict) else {
        "method": sen["method"], "endFraction": sen["end_fraction"], "gateStage": sen["gate_stage"],
        "sourceId": sen["source_id"], "ref": sen["ref"]}
    if not baselines and not stages and senescence is None:
        return None
    ids = {b["sourceId"] for b in baselines}
    for f in stages.values():
        ids.update(f["sourceIds"])
    if senescence is not None:
        ids.add(senescence["sourceId"])
    citations = []
    for sid in sorted(ids):
        cite = citation_lookup(sid)
        if cite is None:
            logger.warning("water stress reference: no citation for source %s", sid)
        else:
            citations.append(cite)
    out = {"baselines": baselines, "stages": stages, "sourceIds": sorted(ids), "citations": citations}
    if senescence is not None:
        out["senescence"] = senescence
    return out
