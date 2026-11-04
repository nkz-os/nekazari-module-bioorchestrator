// 011 - One WaterStressReference node per species (payload = raw species block from
// data/water_stress_reference.yaml). Idempotent.

CREATE CONSTRAINT water_stress_reference_species IF NOT EXISTS
FOR (n:WaterStressReference) REQUIRE n.species IS UNIQUE;
