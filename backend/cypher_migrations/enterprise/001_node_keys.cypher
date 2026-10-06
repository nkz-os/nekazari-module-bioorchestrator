// ═══════════════════════════════════════════════════════════════════════════
// ENTERPRISE ONLY — NODE KEY constraints removed from 001 (Community cannot create them)
// ═══════════════════════════════════════════════════════════════════════════
//
// The migration runner never reads this directory. These are the original NODE KEY statements
// of 001_schema_constraints.cypher, kept for a deployment that runs Neo4j Enterprise Edition.
// Neo4j Community rejects them ("Node Key constraint requires Neo4j Enterprise Edition"), which
// is why 001 declares the same properties as UNIQUE and the loader/gate check existence.
//
// Status: scaffolded. Never executed against an Enterprise server (none is available to this
// project); only the statements' text is preserved.
//
// Using it on a FRESH Enterprise instance: apply THIS file first (cypher-shell), then the normal
// migrations. The Community UNIQUE of the same name is then a no-op, because CREATE ... IF NOT
// EXISTS matches by constraint name. On an instance that already has the Community UNIQUE
// constraints, drop each one first (DROP CONSTRAINT <name>); IF NOT EXISTS would otherwise leave
// the UNIQUE in place.
//
// Deliberately NOT here:
//   trial_site_name_municipality  replaced by UNIQUE(siteKey) in 002 and dropped there.
//   variety_trial_key             VarietyTrial.mergeKey is indexed (010), not constrained: legacy
//                                 data holds mergeKey generations that may collide.
//   capability_entity_attr        UNIQUE (entityType, attributeName) is capability_key_unique
//                                 in 006.
// ═══════════════════════════════════════════════════════════════════════════

CREATE CONSTRAINT stage_species_name IF NOT EXISTS
FOR (st:PhenologyStage) REQUIRE (st.speciesName, st.name) IS NODE KEY;

CREATE CONSTRAINT heat_tolerance_species IF NOT EXISTS
FOR (ht:CropHeatTolerance) REQUIRE (ht.species) IS NODE KEY;

CREATE CONSTRAINT frost_tolerance_species IF NOT EXISTS
FOR (ft:CropFrostTolerance) REQUIRE (ft.species) IS NODE KEY;

CREATE CONSTRAINT cropcoeff_crop IF NOT EXISTS
FOR (cc:CropCoefficient) REQUIRE (cc.cropCommonName) IS NODE KEY;

CREATE CONSTRAINT nutrient_profile_species_stage IF NOT EXISTS
FOR (np:CropNutrientProfile) REQUIRE (np.species, np.stage) IS NODE KEY;

CREATE CONSTRAINT soil_suitability_species IF NOT EXISTS
FOR (ss:CropSoilSuitability) REQUIRE (ss.species) IS NODE KEY;

CREATE CONSTRAINT management_trial_key IF NOT EXISTS
FOR (mt:ManagementTrial) REQUIRE (mt.mergeKey) IS NODE KEY;

CREATE CONSTRAINT harvest_data_key IF NOT EXISTS
FOR (hd:HarvestData) REQUIRE (hd.mergeKey) IS NODE KEY;

CREATE CONSTRAINT article_source_key IF NOT EXISTS
FOR (as:ArticleSource) REQUIRE (as.mergeKey) IS NODE KEY;

CREATE CONSTRAINT gdd_model_pest_stage IF NOT EXISTS
FOR (g:GDDModel) REQUIRE (g.pestEppo, g.stageName) IS NODE KEY;

CREATE CONSTRAINT companion_relation_pair IF NOT EXISTS
FOR (cr:CompanionRelation) REQUIRE (cr.cropA, cr.cropB) IS NODE KEY;

CREATE CONSTRAINT host_association_pair IF NOT EXISTS
FOR (ha:HostAssociation) REQUIRE (ha.pestEppo, ha.hostEppo) IS NODE KEY;

CREATE CONSTRAINT mrl_substance_crop IF NOT EXISTS
FOR (mrl:MRLEntry) REQUIRE (mrl.substanceCode, mrl.cropEppo) IS NODE KEY;

CREATE CONSTRAINT rotation_constraint_pair IF NOT EXISTS
FOR (rc:RotationConstraint) REQUIRE (rc.cropA, rc.cropB) IS NODE KEY;
