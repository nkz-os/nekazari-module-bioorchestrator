// Migration 008: Add ky (yield response factor) to PhenologyParams nodes
// Backfills from FAO-33 (Doorenbos-Kassam 1979) defaults per stage
// Run: cypher-shell -u neo4j -p bioorchestrator -f 008_add_phenology_ky.cypher

MATCH (p:PhenologyParams)
WHERE p.ky IS NULL
SET p.ky = CASE
  WHEN p.stage = 'vegetative' THEN 0.45
  WHEN p.stage = 'germination' THEN 0.40
  WHEN p.stage = 'emergence' THEN 0.40
  WHEN p.stage = 'stem_elongation' THEN 0.60
  WHEN p.stage = 'booting' THEN 0.80
  WHEN p.stage = 'heading' THEN 0.95
  WHEN p.stage = 'flowering' THEN 1.15
  WHEN p.stage = 'anthesis' THEN 1.15
  WHEN p.stage = 'grain_filling' THEN 0.85
  WHEN p.stage = 'fruit_set' THEN 1.00
  WHEN p.stage = 'pit_hardening' THEN 0.85
  WHEN p.stage = 'kernel_fill' THEN 0.85
  WHEN p.stage = 'fruit_growth' THEN 0.70
  WHEN p.stage = 'veraison' THEN 0.85
  WHEN p.stage = 'ripening' THEN 0.50
  WHEN p.stage = 'maturity' THEN 0.35
  WHEN p.stage = 'senescence' THEN 0.30
  WHEN p.stage = 'dormancy' THEN 0.10
  ELSE 0.45
END
RETURN count(p) AS updated_count;
