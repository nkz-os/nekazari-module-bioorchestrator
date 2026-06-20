import pytest
from app.graph.dao import GraphDAO


class _FakeOrion:
    def __init__(self):
        self.created = []
        self.patched = []
        self.entities = []
    async def create_entity(self, e): self.created.append(e)
    async def update_entity_attrs(self, eid, attrs): self.patched.append((eid, attrs))
    async def query_entities(self, **kw): return self.entities
    async def close(self): pass


@pytest.fixture
def dao(monkeypatch):
    d = GraphDAO.__new__(GraphDAO)
    fake = _FakeOrion()
    import app.graph.dao as dao_mod
    monkeypatch.setattr(dao_mod, "OrionClient", lambda *a, **k: fake)
    return d, fake


@pytest.mark.asyncio
async def test_create_plan_creates_one_agricrop_per_segment_none_active(dao):
    d, fake = dao
    segs = [
        {"crop": "Vicia sativa", "role": "cover_crop", "sowing_window": ["2025-11-01", "2025-11-20"],
         "termination_method": "roller_crimper", "expected_termination": "2026-03-15"},
        {"crop": "Zea mays", "variety": "MAS 26 T", "role": "main_crop",
         "sowing_window": ["2026-04-10", "2026-05-01"], "termination_method": "harvest",
         "expected_termination": "2026-09-20"},
    ]
    out = await d.create_crop_plan("urn:ngsi-ld:AgriParcel:montiko:p-1", "2026", segs, "montiko")
    assert len(fake.created) == 2
    assert all(e["status"]["value"] == "planned" for e in fake.created)
    assert fake.created[0]["seq"]["value"] == 0 and fake.created[1]["seq"]["value"] == 1
    # parcel season bounds patched, hasAgriCrop NOT patched (nothing active yet)
    parcel_patches = [p for p in fake.patched if p[0] == "urn:ngsi-ld:AgriParcel:montiko:p-1"]
    assert parcel_patches and "cropSeasonStart" in parcel_patches[0][1]
    assert not any("hasAgriCrop" in attrs for _, attrs in fake.patched)
    assert out["status"] == "committed" and len(out["segments"]) == 2
