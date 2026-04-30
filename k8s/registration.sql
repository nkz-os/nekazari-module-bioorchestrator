-- =============================================================================
-- BioOrchestrator — Marketplace Registration
-- =============================================================================
-- Run once per environment to register this module in marketplace_modules.
-- This module has both a backend (FastAPI, port 8420) and a frontend IIFE bundle.
-- =============================================================================

INSERT INTO marketplace_modules (
    id,
    name,
    display_name,
    description,
    remote_entry_url,
    version,
    author,
    category,
    route_path,
    label,
    module_type,
    required_plan_type,
    pricing_tier,
    is_local,
    is_active,
    required_roles,
    metadata
) VALUES (
    'bioorchestrator',
    'nekazari-module-bioorchestrator',
    'BioOrchestrator',
    'Multi-domain biodiversity ETL pipeline for regenerative agriculture intelligence. '
    'Integrates data sources across agriculture, livestock, forestry, and agroforestry.',
    '/modules/bioorchestrator/nekazari-module.js',
    '0.1.0',
    'nkz-os',
    'analytics',
    '/bioorchestrator',
    'BioOrchestrator',
    'ADDON_FREE',
    'basic',
    'FREE',
    false,
    true,
    ARRAY['Farmer', 'TenantAdmin', 'PlatformAdmin'],
    '{
        "icon": "🌿",
        "color": "#059669",
        "features": [
            "19 data source connectors",
            "Multi-domain: agriculture, livestock, forestry, agroforestry",
            "Phenology parameters for Crop Health engine",
            "DAD-IS breed discovery",
            "Knowledge graph with Neo4j + n10s JSON-LD"
        ]
    }'::jsonb
) ON CONFLICT (id) DO UPDATE SET
    display_name   = EXCLUDED.display_name,
    description    = EXCLUDED.description,
    is_active      = true,
    updated_at     = NOW();
