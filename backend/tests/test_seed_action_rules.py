from pathlib import Path
import yaml


def test_seed_yaml_rules_are_well_formed():
    data = yaml.safe_load(Path("data/action_rules.yaml").read_text())
    rules = data["rules"]
    ids = {r["id"] for r in rules}
    assert "cover_crop_termination_flowering" in ids
    for r in rules:
        assert r["id"] and r["category"] and isinstance(r["conditions"], dict)
        assert isinstance(r["action"], dict) and r["action"].get("operation_type")
        assert r.get("source_short"), f"rule {r['id']} missing provenance"
