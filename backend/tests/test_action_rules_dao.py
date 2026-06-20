import json, pytest
from app.graph.dao import GraphDAO


class _Res:
    def __init__(self, rows): self._rows = rows
    async def data(self): return self._rows


class _Session:
    def __init__(self, rows): self._rows = rows; self.ran = []
    async def run(self, q, **kw): self.ran.append((q, kw)); return _Res(self._rows)
    async def __aenter__(self): return self
    async def __aexit__(self, *a): return False


@pytest.mark.asyncio
async def test_get_action_rules_parses_json_and_includes_generic(monkeypatch):
    rows = [{
        "id": "cover_crop_termination_flowering", "name": "T", "category": "termination",
        "priority": 10, "active": True,
        "conditions": json.dumps({"all": [{"field": "crop.role", "op": "eq", "value": "cover_crop"}]}),
        "action": json.dumps({"operation_type": "tillage", "urgency": "high", "window_days": 7,
                              "description_template": "x"}),
        "source_doi": "10.x", "source_short": "INTIA",
    }]
    dao = GraphDAO.__new__(GraphDAO)
    sess = _Session(rows)
    monkeypatch.setattr(dao, "_driver", type("D", (), {"session": lambda self, **k: sess})(), raising=False)
    out = await dao.get_action_rules(species="Vicia sativa", stage=None, role="cover_crop")
    assert out[0]["id"] == "cover_crop_termination_flowering"
    assert out[0]["conditions"] == {"all": [{"field": "crop.role", "op": "eq", "value": "cover_crop"}]}
    assert out[0]["action"]["operation_type"] == "tillage"
