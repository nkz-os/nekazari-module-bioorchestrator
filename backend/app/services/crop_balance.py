"""Per-crop coefficients of the N-P-K balance (FertiliCalc schema) with per-value provenance.

Pure functions: validate the curated YAML, flatten a crop into node properties, and rebuild the
API payload from a node. P and K are elemental; concentrations are % of dry matter.
The harvest index here is a nutrient-balance coefficient (`balanceHarvestIndexPct`), not a
crop-model parameter: crop simulation must not read it.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass

CROP_TYPES = frozenset({
    "cereal", "industrial", "tuber_root", "forage_legume", "grain_legume", "tree",
    "vegetable", "forage",
})

# field -> (node property, lower bound exclusive?, low, high, unit)
FIELDS: dict[str, tuple[str, bool, float, float, str]] = {
    "dm_product_pct": ("dmProductPct", True, 0.0, 100.0, "% fresh weight"),
    "n_product_pct": ("nProductPct", True, 0.0, 10.0, "% DM"),
    "p_product_pct": ("pProductPct", True, 0.0, 10.0, "% DM"),
    "k_product_pct": ("kProductPct", True, 0.0, 10.0, "% DM"),
    "dm_residue_pct": ("dmResiduePct", True, 0.0, 100.0, "% fresh weight"),
    "n_residue_pct": ("nResiduePct", True, 0.0, 10.0, "% DM"),
    "p_residue_pct": ("pResiduePct", True, 0.0, 10.0, "% DM"),
    "k_residue_pct": ("kResiduePct", True, 0.0, 10.0, "% DM"),
    "balance_harvest_index_pct": ("balanceHarvestIndexPct", True, 0.0, 100.0, "%"),
    "residue_left_pct": ("residueLeftPct", False, 0.0, 100.0, "%"),
}
# Licences that forbid redistribution: never accepted, whatever the registry says.
BANNED_SOURCE_PREFIXES = ("ipcc", "fao_bulletin")


@dataclass(frozen=True)
class CoefficientError:
    eppo: str
    field: str
    message: str

    def __str__(self) -> str:
        return f"{self.eppo}.{self.field}: {self.message}"


def _in_range(value: object, exclusive_low: bool, low: float, high: float) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)) \
            or not math.isfinite(value):
        return False
    return (low < value if exclusive_low else low <= value) and value <= high


def validate(doc: dict, allowed_sources: set[str]) -> list[CoefficientError]:
    """Every value needs a registered, redistributable source, a reference and a transformation."""
    errors: list[CoefficientError] = []
    if not isinstance(doc.get("dataset_version"), str) or not doc["dataset_version"]:
        errors.append(CoefficientError("*", "dataset_version", "missing"))
    crops = doc.get("crops")
    if not isinstance(crops, dict) or not crops:
        # The seed prunes nodes absent from the file: an empty list must not validate vacuously.
        errors.append(CoefficientError("*", "crops", "missing or empty"))
        crops = {}
    for eppo, crop in crops.items():
        if crop.get("crop_type") not in CROP_TYPES:
            errors.append(CoefficientError(eppo, "crop_type", "unknown crop type"))
        if not isinstance(crop.get("n_fixing"), bool):
            errors.append(CoefficientError(eppo, "n_fixing", "must be true or false"))
        # residue_product is a display-only label: optional when no source names the residue.
        if not crop.get("harvest_product"):
            errors.append(CoefficientError(eppo, "harvest_product", "missing"))
        for field, entry in (crop.get("values") or {}).items():
            if field not in FIELDS:
                errors.append(CoefficientError(eppo, field, "unknown field"))
                continue
            _prop, excl, low, high, _unit = FIELDS[field]
            if not _in_range(entry.get("value"), excl, low, high):
                errors.append(CoefficientError(eppo, field, f"value out of range {low}-{high}"))
            source = entry.get("source_id") or ""
            if source.startswith(BANNED_SOURCE_PREFIXES):
                errors.append(CoefficientError(eppo, field, f"source {source} not redistributable"))
            elif source not in allowed_sources:
                errors.append(CoefficientError(eppo, field, f"source {source!r} not registered"))
            for key in ("ref", "transformation"):
                if not entry.get(key):
                    errors.append(CoefficientError(eppo, field, f"{key} missing"))
    return errors


def to_node_props(eppo: str, crop: dict, dataset_version: str) -> dict:
    """Flat properties for a (:CropBalanceCoefficients) node."""
    values = crop.get("values") or {}
    props: dict = {
        "eppo": eppo,
        "datasetVersion": dataset_version,
        "cropType": crop["crop_type"],
        "harvestProduct": crop["harvest_product"],
        "nFixing": crop["n_fixing"],
        "sourceIds": sorted({e["source_id"] for e in values.values()}),
        "provenance": json.dumps(
            {f: {k: e[k] for k in ("source_id", "ref", "raw", "transformation", "note")
                 if e.get(k) not in (None, "")}
             for f, e in sorted(values.items())},
            ensure_ascii=False, sort_keys=True,
        ),
    }
    residue_product = crop.get("residue_product")
    if isinstance(residue_product, str) and residue_product:
        props["residueProduct"] = residue_product
    for field, entry in values.items():
        props[FIELDS[field][0]] = entry["value"]
    return props


def payload_from_node(node: dict) -> dict:
    """API body (without attributions) from a node's properties."""
    provenance = json.loads(node.get("provenance") or "{}")
    values, missing = {}, []
    for field, (prop, _e, _l, _h, unit) in FIELDS.items():
        if node.get(prop) is None:
            missing.append(field)
            continue
        values[field] = {"value": node[prop], "unit": unit, **provenance.get(field, {})}
    return {
        "eppo": node["eppo"],
        "crop_type": node.get("cropType"),
        "harvest_product": node.get("harvestProduct"),
        "residue_product": node.get("residueProduct"),
        "n_fixing": node.get("nFixing"),
        "dataset_version": node.get("datasetVersion"),
        "values": values,
        "missing": missing,
        "source_ids": list(node.get("sourceIds") or []),
    }
