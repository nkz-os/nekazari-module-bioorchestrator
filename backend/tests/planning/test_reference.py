from app.core.knowledge import KnowledgeSource
from app.planning import reference as ref

SPECIES = [
    {"species": "wheat", "ph_min": 5.5, "ph_max": 8.0, "textures": ["loam"], "drainage": None,
     "soil_source": "EcoCrop", "frost_damage_c": -15.0, "heat_damage_c": 34.0, "frost_source": "X"},
    {"species": "onion", "ph_min": None, "ph_max": None, "textures": None, "drainage": None,
     "soil_source": None, "frost_damage_c": None, "heat_damage_c": None, "frost_source": None},
]
ROTATION = [{"crop_a": "wheat", "crop_b": "wheat", "effect": "restriction", "interval_years": 2,
             "reason": "take-all", "source": "FAO"}]
TRIALS = [{"eppo": "TRZAX"}, {"eppo": "ZEAMX"}]


async def test_reads_once_and_caches(fake_driver_cls):
    ref.clear_cache()
    drv = fake_driver_cls([("MATCH (s:Species)", SPECIES), ("RotationConstraint", ROTATION),
                           ("VarietyTrial", TRIALS)])
    src = KnowledgeSource("global", drv)
    first = await ref.load_reference(src, now=0.0)
    again = await ref.load_reference(src, now=100.0)
    assert again is first and drv.sessions == 1
    assert first.tolerances["wheat"].soil == {"ph_min": 5.5, "ph_max": 8.0, "textures": ["loam"], "drainage": None}
    assert first.tolerances["onion"].soil is None  # no tolerance at all -> None, never an all-None dict
    assert first.constraints[0].interval_years == 2
    assert first.trial_eppo == frozenset({"TRZAX", "ZEAMX"})
    await ref.load_reference(src, now=ref.REFERENCE_TTL_S + 1.0)
    assert drv.sessions == 2
