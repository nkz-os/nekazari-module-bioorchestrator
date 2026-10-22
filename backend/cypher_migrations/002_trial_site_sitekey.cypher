// ═══════════════════════════════════════════════════════════════════════════
// 002 — Source-agnostic TrialSite identity (spec §4.5)
// ═══════════════════════════════════════════════════════════════════════════
//
// Replaces the old (name, municipality) NODE KEY with a UNIQUE(siteKey)
// constraint so TrialSite identity is source-agnostic and byte-compatible
// with base_ingester (which MERGEs by siteKey) and the in-place migration
// (scripts/migrate_site_identity.py).
//
// APPLY AFTER the data migration (migrate_site_identity.py --execute) so no
// duplicate siteKey values exist when the constraint is created.
//
// Idempotent: safe to re-run (IF NOT EXISTS).
// ═══════════════════════════════════════════════════════════════════════════

CREATE CONSTRAINT trial_site_sitekey IF NOT EXISTS
FOR (ts:TrialSite) REQUIRE ts.siteKey IS UNIQUE;

CREATE INDEX trial_site_municipality_key IF NOT EXISTS
FOR (ts:TrialSite) ON (ts.municipalityKey);

DROP CONSTRAINT trial_site_name_municipality IF EXISTS;
