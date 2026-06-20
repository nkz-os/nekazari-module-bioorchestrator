import httpx
import pytest
from app.graph.dao import GraphDAO


class _FakeOrion:
    def __init__(self):
        self.created = []
        self.patched = []
        self.entities = []
        self.fail_create_on_seq = None
    async def create_entity(self, e):
        if self.fail_create_on_seq is not None and e["seq"]["value"] == self.fail_create_on_seq:
            raise httpx.ConnectError("boom")
        self.created.append(e)
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


@pytest.mark.asyncio
async def test_create_plan_segment_transport_error_is_collected_not_aborted(dao):
    d, fake = dao
    fake.fail_create_on_seq = 0  # first segment raises a transport error
    segs = [
        {"crop": "Vicia sativa", "role": "cover_crop", "sowing_window": ["2025-11-01", "2025-11-20"],
         "termination_method": "roller_crimper", "expected_termination": "2026-03-15"},
        {"crop": "Zea mays", "variety": "MAS 26 T", "role": "main_crop",
         "sowing_window": ["2026-04-10", "2026-05-01"], "termination_method": "harvest",
         "expected_termination": "2026-09-20"},
    ]
    # Must not raise — a transport error on one segment cannot abort the batch.
    out = await d.create_crop_plan("urn:ngsi-ld:AgriParcel:montiko:p-1", "2026", segs, "montiko")

    assert out["status"] == "committed"
    # seq 0 failed and was collected as a warning, never created
    assert any(w.get("seq") == 0 and "boom" in w.get("error", "") for w in out["warnings"])
    # seq 1 succeeded despite seq 0's failure
    assert len(fake.created) == 1
    assert fake.created[0]["seq"]["value"] == 1
    assert len(out["segments"]) == 1


@pytest.mark.asyncio
async def test_get_crop_plan_orders_by_seq_and_resolves_active(dao):
    d, fake = dao
    fake.entities = [
        {"id": "urn:ngsi-ld:AgriCrop:montiko:p-1:2026:1", "seq": 1, "status": "planned"},
        {"id": "urn:ngsi-ld:AgriCrop:montiko:p-1:2026:0", "seq": 0, "status": "active"},
    ]
    out = await d.get_crop_plan("urn:ngsi-ld:AgriParcel:montiko:p-1", "2026", "montiko")

    assert [s["seq"] for s in out["segments"]] == [0, 1]
    assert out["active"] == "urn:ngsi-ld:AgriCrop:montiko:p-1:2026:0"


@pytest.mark.asyncio
async def test_advance_activates_target_demotes_prior_patches_hasagricrop(dao):
    d, fake = dao
    # prior active = seg 0 (cover_crop, roller_crimper); advancing to seg 1 (main, harvest)
    fake.entities = [
        {"id": "urn:ngsi-ld:AgriCrop:montiko:p-1:2026:0", "seq": 0, "status": "active",
         "terminationMethod": "roller_crimper"},
        {"id": "urn:ngsi-ld:AgriCrop:montiko:p-1:2026:1", "seq": 1, "status": "planned",
         "terminationMethod": "harvest"},
    ]
    out = await d.advance_segment("urn:ngsi-ld:AgriParcel:montiko:p-1", "2026", 1, "2026-04-15", "montiko")
    patched = dict((eid, attrs) for eid, attrs in fake.patched)
    # target activated with real plantingDate
    tgt = patched["urn:ngsi-ld:AgriCrop:montiko:p-1:2026:1"]
    assert tgt["status"]["value"] == "active"
    assert tgt["plantingDate"]["value"]["@value"] == "2026-04-15"
    # prior demoted: roller_crimper -> terminated, terminationDate set
    prior = patched["urn:ngsi-ld:AgriCrop:montiko:p-1:2026:0"]
    assert prior["status"]["value"] == "terminated"
    assert prior["terminationDate"]["value"]["@value"] == "2026-04-15"
    # parcel hasAgriCrop -> target
    parcel = patched["urn:ngsi-ld:AgriParcel:montiko:p-1"]
    assert parcel["hasAgriCrop"]["object"] == "urn:ngsi-ld:AgriCrop:montiko:p-1:2026:1"
    assert out["status"] == "advanced"
