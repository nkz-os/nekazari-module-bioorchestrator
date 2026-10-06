// ═══════════════════════════════════════════════════════════════════════════
// 010 — Identity constraints and indexes for the KG ingestion model
// ═══════════════════════════════════════════════════════════════════════════
//
// Natural keys of the new nodes (Study, ObservationUnit, Observation, Variety, Crop, Variable,
// Source) and the document key on ArticleSource. UNIQUE only: Community cannot enforce that the
// key property exists, so the loader and the quality gate refuse a row without its key.
//
// Idempotent: every statement is IF NOT EXISTS. Labels that have no nodes yet (everything except
// ArticleSource and TrialSite) make the constraints trivially satisfiable.
// ═══════════════════════════════════════════════════════════════════════════

CREATE CONSTRAINT observation_unit_unitkey IF NOT EXISTS
FOR (u:ObservationUnit) REQUIRE u.unitKey IS UNIQUE;

CREATE CONSTRAINT observation_obskey IF NOT EXISTS
FOR (o:Observation) REQUIRE o.obsKey IS UNIQUE;

CREATE CONSTRAINT variety_varietykey IF NOT EXISTS
FOR (v:Variety) REQUIRE v.varietyKey IS UNIQUE;

CREATE CONSTRAINT crop_eppo IF NOT EXISTS
FOR (c:Crop) REQUIRE c.eppo IS UNIQUE;

CREATE CONSTRAINT variable_variableid IF NOT EXISTS
FOR (v:Variable) REQUIRE v.variableId IS UNIQUE;

CREATE CONSTRAINT source_sourceid IF NOT EXISTS
FOR (s:Source) REQUIRE s.sourceId IS UNIQUE;

CREATE CONSTRAINT study_studykey IF NOT EXISTS
FOR (s:Study) REQUIRE s.studyKey IS UNIQUE;

CREATE CONSTRAINT article_source_documentkey IF NOT EXISTS
FOR (a:ArticleSource) REQUIRE a.documentKey IS UNIQUE;

// Same name and schema as 002: a no-op wherever 002 has run, kept so this file stands alone.
CREATE CONSTRAINT trial_site_sitekey IF NOT EXISTS
FOR (ts:TrialSite) REQUIRE ts.siteKey IS UNIQUE;

// Legacy VarietyTrial lookups (the loader MERGEs by mergeKey; hot queries filter by crop).
CREATE INDEX variety_trial_merge_key IF NOT EXISTS
FOR (vt:VarietyTrial) ON (vt.mergeKey);

CREATE INDEX variety_trial_crop_eppo IF NOT EXISTS
FOR (vt:VarietyTrial) ON (vt.cropEppo);

CREATE INDEX observation_variable_id IF NOT EXISTS
FOR (o:Observation) ON (o.variableId);
