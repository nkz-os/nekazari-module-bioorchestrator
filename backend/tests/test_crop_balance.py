"""Crop balance coefficients: validation, node flattening and payload rebuilding (pure)."""
import copy
import json

import pytest

from app.services.crop_balance import FIELDS, payload_from_node, to_node_props, validate

ALLOWED = {"mapa_guia_2010", "panagos_2022", "essd_2024_s3", "ludemann_cc0"}


def _entry(value, source="mapa_guia_2010"):
    return {"value": value, "source_id": source, "ref": "Tabla 16.2, p. 211",
            "raw": str(value), "transformation": "none"}


DOC = {
    "dataset_version": "2026-10-11.1",
    "crops": {
        "TRZAX": {
            "crop_type": "cereal", "harvest_product": "grain", "residue_product": "straw",
            "n_fixing": False,
            "values": {
                "dm_product_pct": _entry(88.0),
                "n_product_pct": _entry(2.0, "ludemann_cc0"),
                "p_product_pct": _entry(0.356, "panagos_2022"),
                "balance_harvest_index_pct": _entry(51.0, "panagos_2022"),
                "residue_left_pct": _entry(50.0, "panagos_2022"),
            },
        },
    },
}


def test_valid_document_has_no_errors():
    assert validate(DOC, ALLOWED) == []


@pytest.mark.parametrize("field,value", [
    ("n_product_pct", 0.0), ("n_product_pct", 12.0), ("dm_product_pct", float("nan")),
    ("balance_harvest_index_pct", 0.0), ("residue_left_pct", -1.0), ("n_product_pct", True),
])
def test_out_of_range_or_non_numeric_rejected(field, value):
    doc = copy.deepcopy(DOC)
    doc["crops"]["TRZAX"]["values"].setdefault(field, _entry(value))["value"] = value
    errors = validate(doc, ALLOWED)
    assert [e.field for e in errors] == [field]


def test_zero_residue_concentration_rejected():
    doc = copy.deepcopy(DOC)
    doc["crops"]["TRZAX"]["values"]["n_residue_pct"] = _entry(0.0, "ludemann_cc0")
    assert [str(e) for e in validate(doc, ALLOWED)] == [
        "TRZAX.n_residue_pct: value out of range 0.0-10.0"]


def test_unregistered_or_banned_source_rejected():
    doc = copy.deepcopy(DOC)
    doc["crops"]["TRZAX"]["values"]["n_product_pct"]["source_id"] = "ipcc_2019_t11_1a"
    doc["crops"]["TRZAX"]["values"]["p_product_pct"]["source_id"] = "someone_else"
    messages = [str(e) for e in validate(doc, ALLOWED | {"ipcc_2019_t11_1a"})]
    assert messages == ["TRZAX.n_product_pct: source ipcc_2019_t11_1a not redistributable",
                        "TRZAX.p_product_pct: source 'someone_else' not registered"]


def test_reference_and_transformation_required():
    doc = copy.deepcopy(DOC)
    del doc["crops"]["TRZAX"]["values"]["dm_product_pct"]["ref"]
    doc["crops"]["TRZAX"]["values"]["n_product_pct"]["transformation"] = ""
    assert [str(e) for e in validate(doc, ALLOWED)] == [
        "TRZAX.dm_product_pct: ref missing", "TRZAX.n_product_pct: transformation missing"]


def test_unknown_field_and_crop_type_rejected():
    doc = copy.deepcopy(DOC)
    doc["crops"]["TRZAX"]["crop_type"] = "weed"
    doc["crops"]["TRZAX"]["values"]["n_min_product_pct"] = _entry(1.8)
    assert {e.field for e in validate(doc, ALLOWED)} == {"crop_type", "n_min_product_pct"}


def test_node_props_round_trip_to_payload():
    props = to_node_props("TRZAX", DOC["crops"]["TRZAX"], DOC["dataset_version"])
    assert props["balanceHarvestIndexPct"] == 51.0
    assert props["sourceIds"] == ["ludemann_cc0", "mapa_guia_2010", "panagos_2022"]
    assert "harvestIndexPct" not in props            # never the crop-model name
    json.loads(props["provenance"])
    body = payload_from_node(props)
    assert body["values"]["p_product_pct"] == {
        "value": 0.356, "unit": "% DM", "source_id": "panagos_2022",
        "ref": "Tabla 16.2, p. 211", "raw": "0.356", "transformation": "none"}
    assert set(body["missing"]) == set(FIELDS) - set(DOC["crops"]["TRZAX"]["values"])


def _without_residue_product(value):
    doc = copy.deepcopy(DOC)
    crop = doc["crops"]["TRZAX"]
    if value is None:
        del crop["residue_product"]
    else:
        crop["residue_product"] = value
    return doc


@pytest.mark.parametrize("value", [None, ""])
def test_residue_product_is_optional(value):
    # Display-only label: a crop whose sources name no residue still validates.
    doc = _without_residue_product(value)
    assert validate(doc, ALLOWED) == []
    props = to_node_props("TRZAX", doc["crops"]["TRZAX"], doc["dataset_version"])
    assert "residueProduct" not in props
    assert payload_from_node(props)["residue_product"] is None


def test_harvest_product_still_required():
    doc = copy.deepcopy(DOC)
    del doc["crops"]["TRZAX"]["harvest_product"]
    assert [str(e) for e in validate(doc, ALLOWED)] == ["TRZAX.harvest_product: missing"]


def test_note_is_carried_to_the_payload():
    crop = copy.deepcopy(DOC["crops"]["TRZAX"])
    crop["values"]["n_product_pct"]["note"] = "MAPA T17.6 vs T17.2 disagree; T17.6 kept"
    body = payload_from_node(to_node_props("TRZAX", crop, "v"))
    assert body["values"]["n_product_pct"]["note"].startswith("MAPA T17.6")
    assert "note" not in body["values"]["p_product_pct"]


@pytest.mark.parametrize("doc", [
    {"dataset_version": "v"},
    {"dataset_version": "v", "crops": {}},
])
def test_missing_or_empty_crops_rejected(doc):
    assert [str(e) for e in validate(doc, ALLOWED)] == ["*.crops: missing or empty"]
