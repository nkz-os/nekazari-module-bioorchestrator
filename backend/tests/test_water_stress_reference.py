"""Water-stress reference (NWSB baselines, upper limits, stage flags): validator, loader, API shape."""
import copy
from pathlib import Path

import pytest
import yaml

from app.kg.registries import DEFAULT_REGISTRIES_PATH, load_registries
from app.services.water_stress_reference import load, species_payload, validate
from app.species_registry import resolve_species

DATA = Path(__file__).resolve().parents[1] / "data"
PATH = DATA / "water_stress_reference.yaml"
DOC = load(PATH)
KNOWN = {"idso1982": "unknown", "egea2017": "unknown", "bellvert2015": "unknown",
         "sanchezpinero2022": "unknown", "fao33": "unknown", "allowed1": "allowed"}


def _real_known():
    return {s.source_id: s.licence.commercial_use for s in load_registries(DEFAULT_REGISTRIES_PATH).sources}


def _doc():
    return {"dataset_version": "t", "species": {"olive": {"baselines": [{
        "id": "b1", "cultivar": None, "stages": None, "condition": "sunlit",
        "a0_c": 2.5, "a1_c_per_kpa": -0.36,
        "upper": {"form": "constant", "value_c": 5.0},
        "valid": {"clear_sky": True}, "site": "x", "evidence": "primary_read",
        "source_id": "egea2017", "ref": "Table 2"}], "stages": {}}}}


def _errors(doc):
    return validate(doc, KNOWN)


def _first(doc):
    return doc["species"]["olive"]["baselines"][0]


def test_real_file_validates():
    assert validate(DOC, _real_known()) == []


def test_real_file_species_and_stages_exist():
    detection = yaml.safe_load((DATA / "phenology_sources.yaml").read_text(encoding="utf-8"))["stage_detection"]
    for slug, block in DOC["species"].items():
        assert resolve_species(slug) == slug
        names = set(detection.get(slug, {}).get("stages", {}))
        used = set(block.get("stages") or {})
        for b in block["baselines"]:
            used |= set(b["stages"] or [])
        assert used <= names, (slug, used - names)


def test_valid_doc_passes():
    assert _errors(_doc()) == []


def test_rejects_denied_source():
    d = _doc()
    assert validate(d, {**KNOWN, "egea2017": "denied"})


def test_rejects_unknown_source():
    d = _doc()
    _first(d)["source_id"] = "nope"
    assert any("nope" in e for e in _errors(d))


def test_accepts_allowed_and_permission_granted():
    d = _doc()
    _first(d)["source_id"] = "allowed1"
    assert _errors(d) == []
    assert validate(d, {"allowed1": "permission_granted"}) == []


def test_rejects_unknown_upper_form():
    d = _doc()
    _first(d)["upper"] = {"form": "idso1981"}
    assert _errors(d)


def test_upper_form_needs_its_numbers():
    d = _doc()
    _first(d)["upper"] = {"form": "linear_vpd", "a0_c": 4.0}
    assert _errors(d)
    d = _doc()
    _first(d)["upper"] = {"form": "constant"}
    assert _errors(d)


def test_upper_null_is_allowed():
    d = _doc()
    _first(d)["upper"] = None
    assert _errors(d) == []


@pytest.mark.parametrize("key", ["ref", "source_id"])
def test_requires_ref_and_source(key):
    d = _doc()
    _first(d)[key] = ""
    assert _errors(d)
    d = _doc()
    del _first(d)[key]
    assert _errors(d)


@pytest.mark.parametrize("a0,a1,ok", [
    (-10, -5, True), (15, 1, True), (-10.1, -1, False), (15.1, -1, False),
    (2, -5.1, False), (2, 1.1, False), (True, -1, False), ("2", -1, False),
])
def test_a0_a1_ranges(a0, a1, ok):
    d = _doc()
    _first(d)["a0_c"], _first(d)["a1_c_per_kpa"] = a0, a1
    assert (_errors(d) == []) is ok


def test_rejects_bad_evidence_and_duplicate_ids():
    d = _doc()
    _first(d)["evidence"] = "guess"
    assert _errors(d)
    d = _doc()
    d["species"]["olive"]["baselines"].append(copy.deepcopy(_first(d)))
    assert _errors(d)


def test_stage_flags_validated():
    d = _doc()
    d["species"]["olive"]["stages"] = {"pit_hardening": {
        "water_sensitive": "maybe", "rdi_tolerant": True, "source_ids": ["egea2017"], "ref": "r"}}
    assert _errors(d)
    d["species"]["olive"]["stages"]["pit_hardening"]["water_sensitive"] = "unknown"
    assert _errors(d) == []
    d["species"]["olive"]["stages"]["pit_hardening"]["source_ids"] = ["nope"]
    assert _errors(d)
    # A `false` flag without a source is an unsourced claim.
    d["species"]["olive"]["stages"]["pit_hardening"].update(water_sensitive=False, source_ids=[])
    assert _errors(d)


def test_empty_doc_is_an_error():
    assert validate({}, KNOWN)


def test_secondary_rows_not_served():
    d = _doc()
    _first(d)["evidence"] = "secondary"
    assert species_payload(d, "olive") is None or species_payload(d, "olive")["baselines"] == []


def test_rows_without_upper_not_served():
    d = _doc()
    _first(d)["upper"] = None
    p = species_payload(d, "olive")
    assert p is None or p["baselines"] == []
    # ...but the real file keeps them.
    stored = DOC["species"]["almond"]["baselines"]
    assert stored and all(b["upper"] is None for b in stored)
    assert species_payload(DOC, "almond") is None


def test_idso_wheat_pre_heading():
    rows = {b["id"]: b for b in DOC["species"]["durum_wheat"]["baselines"]}
    pre = rows["idso1982-durum-pre-heading"]
    assert (pre["a0_c"], pre["a1_c_per_kpa"]) == (3.38, -3.25)
    assert pre["source_id"] == "idso1982" and pre["ref"] == "Table I p.62"
    assert pre["stages"] == ["emergence", "tillering", "stem_elongation"]
    assert pre["upper"] is None


def test_soft_wheat_marked_assumption():
    rows = DOC["species"]["wheat"]["baselines"]
    assert rows and all(b.get("assumption") for b in rows)
    assert all("durum" in b["assumption"] for b in rows)


def test_payload_shape_camel_case():
    p = species_payload(DOC, "olive")
    assert set(p) == {"baselines", "stages", "sourceIds"}
    b = p["baselines"][0]
    assert {"id", "a0C", "a1CPerKpa", "upper", "valid", "evidence", "sourceId", "ref"} <= set(b)
    assert b["upper"] == {"form": "constant", "valueC": 5.0}
    assert b["valid"]["clearSky"] is True
    assert all("a0_c" not in x and x["evidence"] == "primary_read" and x["upper"] for x in p["baselines"])
    assert p["stages"]["pit_hardening"]["rdiTolerant"] is True
    assert p["stages"]["pit_hardening"]["waterSensitive"] == "unknown"
    assert set(p["sourceIds"]) >= {"egea2017", "sanchezpinero2022"}


def test_grapevine_linear_vpd_upper_served():
    p = species_payload(DOC, "grapevine")
    ups = {b["upper"]["form"] for b in p["baselines"]}
    assert ups == {"linear_vpd"}
    assert all({"a0C", "a1CPerKpa"} <= set(b["upper"]) for b in p["baselines"])


def test_unknown_species_is_none():
    assert species_payload(DOC, "nonexistent") is None


def test_load_rejects_non_mapping(tmp_path):
    f = tmp_path / "x.yaml"
    f.write_text("- a\n", encoding="utf-8")
    with pytest.raises(TypeError):
        load(f)
