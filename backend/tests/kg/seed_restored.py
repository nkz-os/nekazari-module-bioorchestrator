"""A synthetic 'restored production' graph for the T12 tests: legacy trials of several sources, reference
knowledge and a few F1 units. Nothing here is real data; names are generic."""
from __future__ import annotations

SEED = """
CREATE (:Species {name: 'wheat', eppoCode: 'TRZAX'}), (:Species {name: 'maize', eppoCode: 'ZEAMX'})
CREATE (:PhenologyStage {name: 'flowering', speciesName: 'wheat'}), (:Pest {eppoCode: 'PEST1'})
CREATE (:CropNutrientProfile {species: 'wheat', stage: 'tillering', element: 'nitrogen'})
CREATE (:RotationConstraint {cropA: 'TRZAX', cropB: 'ZEAMX'}), (:ClimateCell {key: 'c1'})
CREATE (:ManagementTrial {mergeKey: 'mt1', source_id: 'NAVARRA-AGRARIA'})

// sites: only GENVCE, shared with Navarra, only Navarra, empty GENVCE-tagged, empty foreign-tagged
CREATE (cordoba:TrialSite {siteKey: 'city-a', name: 'City A', source_id: 'GENVCE', sourceIds: ['GENVCE', 'IFAPA']})
CREATE (shared:TrialSite {siteKey: 'shared-1', name: 'Shared', source_id: 'NAVARRA-AGRARIA', sourceIds: ['NAVARRA-AGRARIA']})
CREATE (nav:TrialSite {siteKey: 'nav-1', name: 'Nav only', source_id: 'NAVARRA-AGRARIA'})
CREATE (:TrialSite {siteKey: 'empty-genvce', name: 'Empty G', sourceIds: ['GENVCE']})
CREATE (:TrialSite {siteKey: 'empty-ifapa', name: 'Empty I', dataSource: 'IFAPA trials'})
CREATE (crea_site:TrialSite {siteKey: 'crea-field', name: 'CREA field', source_id: 'CREA'})

// documents
CREATE (dg:ArticleSource {mergeKey: 'doc-g', articleTitle: 'GENVCE report'})
CREATE (dn:ArticleSource {mergeKey: 'doc-n', articleTitle: 'Navarra article'})
CREATE (dshared:ArticleSource {mergeKey: 'doc-s', articleTitle: 'Shared doc'})

// legacy trials
WITH cordoba, shared, nav, crea_site, dg, dn, dshared
UNWIND range(1, 12) AS i
CREATE (g:VarietyTrial {mergeKey: 'g' + i, source_id: 'GENVCE', dataSource: 'GENVCE', cropEppo: 'TRZAX', year: 2020})
CREATE (g)-[:TRIAL_AT]->(cordoba) CREATE (g)-[:SOURCED_FROM]->(dg)
WITH shared, nav, crea_site, dn, dshared, i
WHERE i <= 4
CREATE (gs:VarietyTrial {mergeKey: 'gs' + i, source_id: 'GENVCE', dataSource: 'genvce', cropEppo: 'TRZAX', year: 2021})
CREATE (gs)-[:TRIAL_AT]->(shared) CREATE (gs)-[:SOURCED_FROM]->(dshared)
CREATE (n:VarietyTrial {mergeKey: 'n' + i, source_id: 'NAVARRA-AGRARIA', dataSource: 'Navarra Agraria', cropEppo: 'TRZAX'})
CREATE (n)-[:TRIAL_AT]->(shared) CREATE (n)-[:SOURCED_FROM]->(dshared)
CREATE (n2:VarietyTrial {mergeKey: 'nn' + i, source_id: 'NAVARRA-AGRARIA', cropEppo: 'ZEAMX'})
CREATE (n2)-[:TRIAL_AT]->(nav) CREATE (n2)-[:SOURCED_FROM]->(dn)
CREATE (c:VarietyTrial {mergeKey: 'c' + i, source_id: 'CREA', dataSource: 'CREA', cropEppo: 'ZEAMX'})
CREATE (c)-[:TRIAL_AT]->(crea_site)
CREATE (z:VarietyTrial {mergeKey: 'z' + i, source_id: 'CREA', dataSource: 'CREA', cropEppo: 'ZEAMA'})
CREATE (z)-[:TRIAL_AT]->(crea_site)
CREATE (l:VarietyTrial {mergeKey: 'l' + i, source_id: 'LEGACY', cropEppo: 'TRZAX'})
"""

# F1 units of GENVCE (with observations and a study) and of a second, untouchable source
F1 = """
MATCH (cordoba:TrialSite {siteKey: 'city-a'})
CREATE (st:Study {studyKey: 'st-g', source_id: 'GENVCE'}), (sto:Study {studyKey: 'st-o', source_id: 'OTHER'})
CREATE (doc:ArticleSource {documentKey: 'dk-g', source_id: 'GENVCE', mergeKey: 'dk-g'})
WITH cordoba, st, sto, doc
UNWIND range(1, 5) AS i
CREATE (u:ObservationUnit:VarietyTrial {unitKey: 'ug' + i, source_id: 'GENVCE', dataSource: 'GENVCE', cropEppo: 'HORVX'})
CREATE (u)-[:IN_STUDY]->(st) CREATE (u)-[:SOURCED_FROM]->(doc) CREATE (u)-[:TRIAL_AT]->(cordoba)
CREATE (:Observation {obsKey: 'og' + i + 'a'})-[:ON_UNIT]->(u)
CREATE (:Observation {obsKey: 'og' + i + 'b'})-[:ON_UNIT]->(u)
CREATE (o:ObservationUnit:VarietyTrial {unitKey: 'uo' + i, source_id: 'OTHER', dataSource: 'OTHER', cropEppo: 'HORVX'})
CREATE (o)-[:IN_STUDY]->(sto)
CREATE (:Observation {obsKey: 'oo' + i})-[:ON_UNIT]->(o)
"""

# 1500 more legacy GENVCE trials, to cross the 500-per-transaction batch
BULK = ("UNWIND range(1, 1500) AS i CREATE (:VarietyTrial {mergeKey: 'bulk' + i, source_id: 'GENVCE', "
        "dataSource: 'GENVCE', cropEppo: 'TRZAX'})")


def seed(run, *, f1: bool = True, bulk: bool = True) -> None:
    """``run`` executes one Cypher statement (the tests' ``_q``)."""
    run(SEED)
    if f1:
        run(F1)
    if bulk:
        run(BULK)
