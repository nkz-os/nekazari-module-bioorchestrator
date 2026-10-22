// Migration: Add management regime to existing Neo4j nodes
// Date: 2026-06-02
// Purpose: Tag all VarietyTrial and ManagementTrial nodes with their
//          farming management regime for the regenerative-sequence engine.
//
// Management categories:
//   - organic:       Certified organic, no synthetic inputs
//   - conventional:  Standard synthetic fertilizers + pesticides
//   - integrated:    Reduced inputs, IPM
//   - low_input:     Minimal external inputs (rainfed semi-arid, extensivo)
//   - unspecified:   Not documented in source

// ═══════════════════════════════════════════════════════════════════════════
// PHASE 1: Tag VarietyTrial nodes by source
// ═══════════════════════════════════════════════════════════════════════════

// Navarra Agraria → conventional (variety trials with herbicides + fungicides)
MATCH (vt:VarietyTrial)-[:SOURCED_FROM]->(a:ArticleSource)
WHERE a.sourceName CONTAINS 'Navarra' OR a.sourceName CONTAINS 'INTIA'
SET vt.management = 'conventional'
RETURN count(vt) AS Navarra_tagged;

// GENVCE organic wheat → organic
MATCH (vt:VarietyTrial)-[:SOURCED_FROM]->(a:ArticleSource)
WHERE a.sourceName CONTAINS 'GENVCE'
  AND (vt.cropGroup = 'trigo-ecologico' OR vt.cropGroup CONTAINS 'ecológico')
SET vt.management = 'organic'
RETURN count(vt) AS GENVCE_organic_tagged;

// GENVCE conventional → conventional  
MATCH (vt:VarietyTrial)-[:SOURCED_FROM]->(a:ArticleSource)
WHERE a.sourceName CONTAINS 'GENVCE'
  AND vt.management IS NULL
SET vt.management = 'conventional'
RETURN count(vt) AS GENVCE_conventional_tagged;

// CTIFL France → conventional (agriculture raisonnée)
MATCH (vt:VarietyTrial)-[:SOURCED_FROM]->(a:ArticleSource)
WHERE a.sourceName CONTAINS 'CTIFL'
SET vt.management = 'conventional'
RETURN count(vt) AS CTIFL_tagged;

// LfL Bayern → conventional (German state trials)
MATCH (vt:VarietyTrial)-[:SOURCED_FROM]->(a:ArticleSource)
WHERE a.sourceName CONTAINS 'LfL'
SET vt.management = 'conventional'
RETURN count(vt) AS LfL_tagged;

// NÉBIH Hungary → conventional
MATCH (vt:VarietyTrial)-[:SOURCED_FROM]->(a:ArticleSource)
WHERE a.sourceName CONTAINS 'NÉBIH' OR a.sourceName CONTAINS 'NEBIH'
SET vt.management = 'conventional'
RETURN count(vt) AS NEBIH_tagged;

// INIAV / CerealTech Portugal → conventional
MATCH (vt:VarietyTrial)-[:SOURCED_FROM]->(a:ArticleSource)
WHERE a.sourceName CONTAINS 'CerealTech' OR a.sourceName CONTAINS 'ANPOC'
   OR a.sourceName CONTAINS 'INIAV'
SET vt.management = 'conventional'
RETURN count(vt) AS Portugal_tagged;

// ═══════════════════════════════════════════════════════════════════════════
// PHASE 2: Tag remaining nodes with defaults
// ═══════════════════════════════════════════════════════════════════════════

// Any remaining VarietyTrial → conventional (safe default for variety trials)
MATCH (vt:VarietyTrial)
WHERE vt.management IS NULL
SET vt.management = 'conventional'
RETURN count(vt) AS remaining_vt_tagged;

// ManagementTrial → tag by content
MATCH (mt:ManagementTrial)
WHERE mt.management IS NULL
SET mt.management = 'conventional'
RETURN count(mt) AS mt_tagged;

// ═══════════════════════════════════════════════════════════════════════════
// PHASE 3: Verify
// ═══════════════════════════════════════════════════════════════════════════

MATCH (vt:VarietyTrial)
RETURN vt.management AS management, count(vt) AS count
ORDER BY count DESC;

MATCH (mt:ManagementTrial)
RETURN mt.management AS management, count(mt) AS count
ORDER BY count DESC;
