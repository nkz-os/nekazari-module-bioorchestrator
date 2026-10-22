import json
from pathlib import Path

_REG = Path(__file__).resolve().parent.parent / "data" / "sources_registry.json"


def test_ecocrop_and_cpvo_registered():
    data = json.loads(_REG.read_text())
    blob = json.dumps(data).lower()
    assert "ecocrop" in blob and "gaez" in blob
    assert "cpvo" in blob
