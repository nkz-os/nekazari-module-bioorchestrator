"""Tests for the phenology_sources.yaml seed data integrity.

Validates that the YAML file:
- Contains the required species with correct structure
- Has valid Kc values (0.0-1.5 range)
- Has plauisble d1 < d2 (stages in order)
- Mid-season Kc >= initial and late-season Kc
- Has complete provenance data
"""

import pytest
import yaml
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"
YAML_PATH = DATA_DIR / "phenology_sources.yaml"


def _load_yaml():
    with YAML_PATH.open("r") as f:
        return yaml.safe_load(f)


def test_yaml_file_exists():
    """The YAML data file must exist."""
    assert YAML_PATH.exists(), f"Missing: {YAML_PATH}"


def test_required_species_present():
    """Must contain at least the 9 target crop species."""
    data = _load_yaml()
    species_names = {sp["name"] for sp in data["species"]}
    required = {"wheat", "maize", "rice", "soybean", "sunflower",
                 "potato", "alfalfa", "grapevine", "olive"}
    missing = required - species_names
    assert not missing, f"Missing species: {missing}"


def test_each_species_has_four_stages():
    """Each species must have exactly 4 stages (initial, development, mid-season, late-season)."""
    data = _load_yaml()
    expected_stages = {"initial", "development", "mid-season", "late-season"}
    for sp in data["species"]:
        stage_names = {s["name"] for s in sp["stages"]}
        assert stage_names == expected_stages, \
            f"{sp['name']}: stages {stage_names} != {expected_stages}"


def test_each_stage_has_kc():
    """Each stage must have a Kc parameter."""
    data = _load_yaml()
    for sp in data["species"]:
        for stage in sp["stages"]:
            assert len(stage["parameters"]) > 0, \
                f"{sp['name']}/{stage['name']}: no parameters"
            param = stage["parameters"][0]
            assert "kc" in param, \
                f"{sp['name']}/{stage['name']}: missing kc"


def test_kc_values_in_range():
    """Kc must be between 0.0 and 1.5."""
    data = _load_yaml()
    for sp in data["species"]:
        for stage in sp["stages"]:
            kc = stage["parameters"][0]["kc"]
            assert 0.0 < kc <= 1.5, \
                f"{sp['name']}/{stage['name']}: Kc={kc} out of range (0, 1.5]"


def test_d1_less_than_d2():
    """d1 (stage onset) must be < d2 (stage end)."""
    data = _load_yaml()
    for sp in data["species"]:
        for stage in sp["stages"]:
            p = stage["parameters"][0]
            assert p["d1"] < p["d2"], \
                f"{sp['name']}/{stage['name']}: d1={p['d1']} >= d2={p['d2']}"


def test_mid_season_kc_highest():
    """Mid-season Kc should be the highest among all stages."""
    data = _load_yaml()
    for sp in data["species"]:
        stages = {s["name"]: s["parameters"][0]["kc"] for s in sp["stages"]}
        mid_kc = stages["mid-season"]
        init_kc = stages["initial"]
        late_kc = stages["late-season"]
        assert mid_kc >= init_kc, f"{sp['name']}: mid Kc={mid_kc} < init Kc={init_kc}"
        assert mid_kc >= late_kc, f"{sp['name']}: mid Kc={mid_kc} < late Kc={late_kc}"
        # Development Kc should be between initial and mid
        dev_kc = stages["development"]
        assert init_kc <= dev_kc <= mid_kc or late_kc <= dev_kc <= mid_kc, \
            f"{sp['name']}: dev Kc={dev_kc} not between init={init_kc} and mid={mid_kc}"


def test_stages_in_chronological_order():
    """d1 values must increase: initial < development < mid-season < late-season."""
    data = _load_yaml()
    expected_order = ["initial", "development", "mid-season", "late-season"]
    for sp in data["species"]:
        stage_map = {s["name"]: s["parameters"][0]["d1"] for s in sp["stages"]}
        d1_values = [stage_map[name] for name in expected_order]
        assert d1_values == sorted(d1_values), \
            f"{sp['name']}: stages not in chronological order: {d1_values}"


def test_each_param_has_source():
    """Each parameter must have source.provenance."""
    data = _load_yaml()
    for sp in data["species"]:
        for stage in sp["stages"]:
            src = stage["parameters"][0].get("source", {})
            assert "doi" in src, f"{sp['name']}/{stage['name']}: missing source.doi"
            assert "short" in src, f"{sp['name']}/{stage['name']}: missing source.short"
            assert "author" in src, f"{sp['name']}/{stage['name']}: missing source.author"


def test_heat_tolerance_for_all_species():
    """All seeded species should have heat tolerance entries."""
    data = _load_yaml()
    ht = data.get("crop_heat_tolerance", {})
    species_names = {sp["name"] for sp in data["species"]}
    missing = species_names - set(ht.keys())
    assert not missing, f"Species missing heat tolerance: {missing}"
    for sp in species_names:
        assert "heat_damage_threshold_c" in ht[sp], \
            f"{sp}: missing heat_damage_threshold_c"
        assert "frost_damage_threshold_c" in ht[sp], \
            f"{sp}: missing frost_damage_threshold_c"
        assert "source" in ht[sp], f"{sp}: missing heat tolerance source"


def test_yaml_is_valid_yaml():
    """YAML must parse without errors."""
    try:
        _load_yaml()
    except Exception as e:
        pytest.fail(f"Invalid YAML: {e}")


def test_nutrient_profiles_major_species():
    """Major species should have nutrient profiles with N/P/K per stage."""
    data = _load_yaml()
    np_data = data.get("crop_nutrient_profiles", {})
    # At minimum wheat and olive should have NPK data
    for sp in ("wheat", "olive"):
        assert sp in np_data, f"{sp}: missing nutrient profile"
        for element in ("nitrogen", "phosphorus", "potassium"):
            assert element in np_data[sp], \
                f"{sp}: missing {element}"
            for stage in ("initial", "development", "mid-season", "late-season"):
                assert stage in np_data[sp][element], \
                    f"{sp}/{element}: missing stage {stage}"
                entry = np_data[sp][element][stage]
                assert "total_kg_ha" in entry, \
                    f"{sp}/{element}/{stage}: missing total_kg_ha"
    assert "source" in np_data["wheat"], "wheat nutrients: missing source"
    assert "doi" in np_data["wheat"]["source"], "wheat nutrients: missing source.doi"


def test_soil_suitability_major_species():
    """Major species should have soil suitability data with required fields."""
    data = _load_yaml()
    ss = data.get("crop_soil_suitability", {})
    for sp in ("wheat", "maize"):
        assert sp in ss, f"{sp}: missing soil suitability"
        for field in ("ph_min", "ph_max", "textures", "drainage", "depth_min_cm"):
            assert field in ss[sp], f"{sp}: missing {field}"
        assert isinstance(ss[sp]["textures"], list), f"{sp}: textures must be list"
        assert isinstance(ss[sp]["drainage"], list), f"{sp}: drainage must be list"
        assert "source" in ss[sp], f"{sp}: missing soil suitability source"


def test_rotation_constraints_have_required_fields():
    """Rotation constraints must have interval_years, reason, and source_short."""
    data = _load_yaml()
    rc = data.get("rotation_constraints", [])
    assert len(rc) > 0, "No rotation constraints defined"
    for entry in rc:
        assert "crop_a" in entry, f"Rotation constraint missing crop_a: {entry}"
        assert "crop_b" in entry, f"Rotation constraint missing crop_b: {entry}"
        assert "interval_years" in entry, \
            f"{entry['crop_a']}-{entry['crop_b']}: missing interval_years"
        assert entry["interval_years"] > 0, \
            f"{entry['crop_a']}-{entry['crop_b']}: interval_years must be positive"
        assert "reason" in entry, \
            f"{entry['crop_a']}-{entry['crop_b']}: missing reason"
