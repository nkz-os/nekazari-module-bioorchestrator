// ═══════════════════════════════════════════════════════════════════════════
// BioOrchestrator Schema v2 — Domain-agnostic knowledge graph foundation
// ═══════════════════════════════════════════════════════════════════════════
//
// This migration creates constraints & indices for ALL known entity types
// across agriculture, livestock, forestry, and agroforestry domains.
//
// Labels follow the pattern :Domain:EntityType for domain grouping.
// Nodes can carry multiple labels (e.g., :Agriculture:Crop:Species).
//
// Design principles:
//  1. Domain-agnostic infrastructure — adding a new domain means adding
//     constraints/indices, not changing existing ones.
//  2. ICASA variables stored as rdfs:label annotations on properties.
//  3. All URIs use canonical forms (AGROVOC, EPPO, QUDT, etc.).
//  4. MERGE keys are composite (natural keys) for idempotent ingestion.
//
// Run:  cat 001_schema_constraints.cypher | cypher-shell -u neo4j -p $PASS
// Or via Python driver in BioOrchestrator startup.
// ═══════════════════════════════════════════════════════════════════════════

// ── Global uniqueness constraints ─────────────────────────────────────────

// URI is the universal identifier for any resource node
CREATE CONSTRAINT resource_uri IF NOT EXISTS
FOR (r:Resource) REQUIRE r.uri IS UNIQUE;

// ── Agriculture domain ────────────────────────────────────────────────────

// Species (from AGROVOC / EPPO / EcoCrop)
CREATE CONSTRAINT species_name IF NOT EXISTS
FOR (s:Species) REQUIRE s.name IS UNIQUE;
CREATE CONSTRAINT species_eppo IF NOT EXISTS
FOR (s:Species) REQUIRE s.eppoCode IS UNIQUE;

// AgriCrop (from Orion-LD / EcoCrop / CPVO)
CREATE CONSTRAINT agricrop_uri IF NOT EXISTS
FOR (ac:AgriCrop) REQUIRE ac.uri IS UNIQUE;

// AgriCropVariety (from CPVO)
CREATE CONSTRAINT variety_uri IF NOT EXISTS
FOR (v:AgriCropVariety) REQUIRE v.uri IS UNIQUE;

// PhenologyStage
CREATE CONSTRAINT stage_species_name IF NOT EXISTS
FOR (st:PhenologyStage) REQUIRE (st.speciesName, st.name) IS NODE KEY;

// PhenologyParams (NO constraint — multiple params per species+stage exist
// with different cultivars/management. Dedup via MERGE on composite key.)

// CropHeatTolerance
CREATE CONSTRAINT heat_tolerance_species IF NOT EXISTS
FOR (ht:CropHeatTolerance) REQUIRE (ht.species) IS NODE KEY;

// CropFrostTolerance
CREATE CONSTRAINT frost_tolerance_species IF NOT EXISTS
FOR (ft:CropFrostTolerance) REQUIRE (ft.species) IS NODE KEY;

// CropCoefficient (FAO-56)
CREATE CONSTRAINT cropcoeff_crop IF NOT EXISTS
FOR (cc:CropCoefficient) REQUIRE (cc.cropCommonName) IS NODE KEY;

// CropNutrientProfile (per species + stage)
CREATE CONSTRAINT nutrient_profile_species_stage IF NOT EXISTS
FOR (np:CropNutrientProfile) REQUIRE (np.species, np.stage) IS NODE KEY;

// CropSoilSuitability
CREATE CONSTRAINT soil_suitability_species IF NOT EXISTS
FOR (ss:CropSoilSuitability) REQUIRE (ss.species) IS NODE KEY;

// ── Navarra Agraria / Trial types ────────────────────────────────────────

// TrialSite (geolocated experimental station)
CREATE CONSTRAINT trial_site_name_municipality IF NOT EXISTS
FOR (ts:TrialSite) REQUIRE (ts.name, ts.municipality) IS NODE KEY;

// VarietyTrial (one row = one variety × site × year × regime)
CREATE CONSTRAINT variety_trial_key IF NOT EXISTS
FOR (vt:VarietyTrial) REQUIRE (vt.mergeKey) IS NODE KEY;

// ManagementTrial (fertilization, irrigation, pest control experiment)
CREATE CONSTRAINT management_trial_key IF NOT EXISTS
FOR (mt:ManagementTrial) REQUIRE (mt.mergeKey) IS NODE KEY;

// HarvestData (campaign-level summary)
CREATE CONSTRAINT harvest_data_key IF NOT EXISTS
FOR (hd:HarvestData) REQUIRE (hd.mergeKey) IS NODE KEY;

// ArticleSource (bibliographic provenance)
CREATE CONSTRAINT article_source_key IF NOT EXISTS
FOR (as:ArticleSource) REQUIRE (as.mergeKey) IS NODE KEY;

// ── Pest & Biocontrol (from IkerKeta EPPO/CABI/USPEST) ───────────────────

// Pest (phytosanitary organism)
CREATE CONSTRAINT pest_eppo IF NOT EXISTS
FOR (p:Pest) REQUIRE p.eppoCode IS UNIQUE;

// GDDModel (degree-day model for pest lifecycle stage)
CREATE CONSTRAINT gdd_model_pest_stage IF NOT EXISTS
FOR (g:GDDModel) REQUIRE (g.pestEppo, g.stageName) IS NODE KEY;

// NaturalEnemy (biological control agent from CABI)
CREATE CONSTRAINT natural_enemy_eppo IF NOT EXISTS
FOR (ne:NaturalEnemy) REQUIRE ne.eppoCode IS UNIQUE;

// ── Associations (from IkerKeta companion planting / CABI) ──────────────

// CompanionRelation (crop↔crop companion planting)
CREATE CONSTRAINT companion_relation_pair IF NOT EXISTS
FOR (cr:CompanionRelation) REQUIRE (cr.cropA, cr.cropB) IS NODE KEY;

// HostAssociation (pest→host plant from EPPO)
CREATE CONSTRAINT host_association_pair IF NOT EXISTS
FOR (ha:HostAssociation) REQUIRE (ha.pestEppo, ha.hostEppo) IS NODE KEY;

// ── Regulatory (from IkerKeta DG SANTE / FiBL) ──────────────────────────

// ActiveSubstance (EU-approved pesticide active ingredient)
CREATE CONSTRAINT active_substance_code IF NOT EXISTS
FOR (asub:ActiveSubstance) REQUIRE asub.substanceCode IS UNIQUE;

// MRLEntry (Maximum Residue Limit per crop)
CREATE CONSTRAINT mrl_substance_crop IF NOT EXISTS
FOR (mrl:MRLEntry) REQUIRE (mrl.substanceCode, mrl.cropEppo) IS NODE KEY;

// ── Rotation rules (from IkerKeta AgroPortal / expert knowledge) ────────

// RotationConstraint
CREATE CONSTRAINT rotation_constraint_pair IF NOT EXISTS
FOR (rc:RotationConstraint) REQUIRE (rc.cropA, rc.cropB) IS NODE KEY;

// ── Capability Registry (module metadata) ────────────────────────────────

// Module
CREATE CONSTRAINT module_id IF NOT EXISTS
FOR (m:Module) REQUIRE m.id IS UNIQUE;

// Capability
CREATE CONSTRAINT capability_entity_attr IF NOT EXISTS
FOR (c:Capability) REQUIRE (c.entityType, c.attributeName) IS NODE KEY;

// ═══════════════════════════════════════════════════════════════════════════
// Indices (for query performance)
// ═══════════════════════════════════════════════════════════════════════════

// Species lookup by name (frequent in phenology queries)
CREATE INDEX species_name_lookup IF NOT EXISTS FOR (s:Species) ON (s.name);
CREATE INDEX species_scientific_name IF NOT EXISTS FOR (s:Species) ON (s.scientificName);

// TrialSite lookup by climate/soil (extrapolation queries)
CREATE INDEX trial_site_climate IF NOT EXISTS FOR (ts:TrialSite) ON (ts.climateClass);
CREATE INDEX trial_site_soil IF NOT EXISTS FOR (ts:TrialSite) ON (ts.soilType);
CREATE INDEX trial_site_rainfall IF NOT EXISTS FOR (ts:TrialSite) ON (ts.annualRainfallMm);

// VarietyTrial lookup by crop + year (common filter)
CREATE INDEX variety_trial_crop_year IF NOT EXISTS FOR (vt:VarietyTrial) ON (vt.cropEppo, vt.year);
CREATE INDEX variety_trial_yield IF NOT EXISTS FOR (vt:VarietyTrial) ON (vt.yieldKgHa);

// ManagementTrial by experiment type
CREATE INDEX mgmt_trial_exp_type IF NOT EXISTS FOR (mt:ManagementTrial) ON (mt.experimentType);

// Pest by name for quick lookup
CREATE INDEX pest_name IF NOT EXISTS FOR (p:Pest) ON (p.prefName);

// ActiveSubstance by name
CREATE INDEX active_substance_name IF NOT EXISTS FOR (asub:ActiveSubstance) ON (asub.commonName);

// ArticleSource by year
CREATE INDEX article_source_year IF NOT EXISTS FOR (as:ArticleSource) ON (as.year);

// PhenologyStage by species (GDD auto-detection)
CREATE INDEX phenology_stage_species IF NOT EXISTS FOR (st:PhenologyStage) ON (st.speciesName);

// HarvestData by crop
CREATE INDEX harvest_data_crop IF NOT EXISTS FOR (hd:HarvestData) ON (hd.cropEppo);
