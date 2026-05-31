"""
Semantic mappings: ICASA variable codes, QUDT/UCUM units, AGROVOC concepts.

This module is the canonical registry for all international ontology mappings
used across BioOrchestrator ingestion pipelines. Every variable entering the
knowledge graph MUST be mapped through these dictionaries.

Standards referenced:
  - ICASA Master Variable List (White et al. 2013, AgMIP harmonization)
  - QUDT (Quantities, Units, Dimensions and Types) — http://qudt.org
  - UCUM (Unified Code for Units of Measure) — https://ucum.org
  - AGROVOC (FAO Agricultural Thesaurus) — http://aims.fao.org/aos/agrovoc/
  - EPPO (European Plant Protection Organization) — https://gd.eppo.int
  - WRB (World Reference Base for Soil Resources) — FAO
  - Köppen climate classification
"""

from __future__ import annotations

# ═══════════════════════════════════════════════════════════════════════════════
# ICASA Variable Codes → Spanish metric names (Navarra Agraria extraction)
# ═══════════════════════════════════════════════════════════════════════════════
#
# Format: "icasa_code": ["spanish_metric_1", "spanish_metric_2", ...]
#
# When a ManagementTrial.result_metric matches any of the Spanish names,
# the corresponding ICASA code is used as the canonical variable identifier
# in the knowledge graph.

ICASA_VARIABLE_MAP: dict[str, list[str]] = {
    # ── Yield ────────────────────────────────────────────────────────────
    "YAMH": [
        "rendimiento_kg_ha",
        "rendimiento_grano",
        "rendimiento_grano_medio",
        "produccion_ms_kg_ha",
    ],
    "HWAM": [
        "produccion_media_gr_m2",
        "peso_biomasa_seca_kg_m2",
        "peso_biomasa_verde_kg_m2",
    ],
    "BWAH": [
        "peso_medio_planta_gr",
    ],

    # ── Quality ──────────────────────────────────────────────────────────
    "THWT": [
        # Thousand grain weight — ICASA THWT
    ],
    "PLHT": [
        # Plant height — ICASA PLHT
    ],
    "BDSF": [
        # Bulk density (specific weight) — ICASA BDSF
    ],

    # ── Phenology ────────────────────────────────────────────────────────
    "MDAT": [
        "ciclo_dias",
    ],
    "SDAT": [
        # Sowing date — ICASA SDAT
    ],
    "HDAT": [
        # Harvest date — ICASA HDAT
    ],

    # ── Nutrients / Composition ──────────────────────────────────────────
    "NUPF": [
        "nitrogeno_total_porcentaje_smf",
        "nitrogeno_amoniacal_porcentaje_smf",
        "n_total_aplicado",
    ],
    "PUPF": [
        "fosforo_porcentaje_smf",
    ],
    "KUPF": [
        "potasio_porcentaje_smf",
    ],
    "CAF": [
        "calcio_porcentaje_smf",
    ],
    "MGF": [
        "magnesio_porcentaje_smf",
    ],
    "NAF": [
        "sodio_mg_kg_smf",
    ],
    "CUF": [
        "cobre_mg_kg_smf",
    ],
    "ZNF": [
        "zinc_mg_kg_smf",
    ],

    # ── Organic / Feed ───────────────────────────────────────────────────
    "OM%": [
        "materia_organica_porcentaje_smf",
    ],
    "DM%": [
        "materia_seca_porcentaje",
    ],
    "CP%": [
        "proteina_bruta_porcentaje_sms",
    ],
    "CFAT": [
        # Crude fat (leche_AGM, queso_AGM)
    ],
    "NDF": [
        "digestibilidad_materia_organica_porcentaje",
    ],

    # ── Energy (feed) ────────────────────────────────────────────────────
    "ME": [
        "concentracion_energetica_UFL_kg_ms",
    ],

    # ── Pest / Disease ───────────────────────────────────────────────────
    "NOPC": [
        "capturas_pulgones",
        "total_pulgon_verde",
        "total_pulgon_brassicae",
        "total_colonias_pulgon",
        "total_mosca_blanca",
        "total_polilla",
    ],
    "DAM%": [
        "porcentaje_aceitunas_danadas",
        "porcentaje_plantas_ocupadas",
    ],

    # ── Economic ─────────────────────────────────────────────────────────
    "COST": [
        "coste_por_tonelada_compost_producido",
    ],

    # ── Milk / Dairy quality ─────────────────────────────────────────────
    "MILK_FAT": [
        "leche_AGM",
        "queso_AGM",
    ],
    "MILK_PROTEIN": [
        "leche_AGP",
        "queso_AGP",
    ],
    "MILK_SNF": [
        "leche_AGS",
        "queso_AGS",
    ],
    "MILK_CLA": [
        "leche_CLA",
        "queso_CLA",
    ],
    "MILK_OM3": [
        "leche_n3",
        "queso_n3",
    ],
    "MILK_OM6": [
        "leche_n6",
        "queso_n6",
    ],
    "MILK_OM63": [
        "leche_n6_n3",
        "queso_n6_n3",
        "leche_AGP_AGS",
        "queso_AGP_AGS",
    ],

    # ── Other ────────────────────────────────────────────────────────────
    "ONTP": [
        "otros_tipos_nitrogeno_principalmente_organico_porcentaje_smf",
    ],
}


def get_icasa_code(spanish_metric: str) -> str | None:
    """Resolve a Spanish metric name to its ICASA variable code.

    Args:
        spanish_metric: The result_metric string from Navarra Agraria extraction.

    Returns:
        ICASA code (e.g., 'YAMH', 'NUPF') or None if no mapping exists.
    """
    metric_lower = spanish_metric.lower().strip()
    for icasa_code, spanish_names in ICASA_VARIABLE_MAP.items():
        for name in spanish_names:
            if name.lower() == metric_lower:
                return icasa_code
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# QUDT / UCUM Unit Mappings
# ═══════════════════════════════════════════════════════════════════════════════
#
# Maps Spanish/legacy unit strings to QUDT unit URIs (preferred) and UCUM codes (fallback).
# QUDT URIs are stable, dereferenceable RDF resources.
# UCUM codes (https://ucum.org) are the ISO 80000-compliant grammar for units.

QUDT_UNIT_MAP: dict[str, dict[str, str]] = {
    # ── SI base & derived ────────────────────────────────────────────────
    "kg.ha-1": {
        "qudt_uri": "http://qudt.org/vocab/unit/KiloGM-PER-HA",
        "ucum": "kg.ha-1",
        "label": "kilogram per hectare",
    },
    "kg/ha": {
        "qudt_uri": "http://qudt.org/vocab/unit/KiloGM-PER-HA",
        "ucum": "kg.ha-1",
        "label": "kilogram per hectare",
    },
    "t.ha-1": {
        "qudt_uri": "http://qudt.org/vocab/unit/TONNE-PER-HA",
        "ucum": "t.ha-1",
        "label": "tonne per hectare",
    },
    "t/ha": {
        "qudt_uri": "http://qudt.org/vocab/unit/TONNE-PER-HA",
        "ucum": "t.ha-1",
        "label": "tonne per hectare",
    },
    "kg.hl-1": {
        "qudt_uri": "http://qudt.org/vocab/unit/KiloGM-PER-HectoL",
        "ucum": "kg.hl-1",
        "label": "kilogram per hectoliter",
    },
    "kg/hl": {
        "qudt_uri": "http://qudt.org/vocab/unit/KiloGM-PER-HectoL",
        "ucum": "kg.hl-1",
        "label": "kilogram per hectoliter",
    },
    "g.m-2": {
        "qudt_uri": "http://qudt.org/vocab/unit/GM-PER-M2",
        "ucum": "g.m-2",
        "label": "gram per square meter",
    },
    "g/m2": {
        "qudt_uri": "http://qudt.org/vocab/unit/GM-PER-M2",
        "ucum": "g.m-2",
        "label": "gram per square meter",
    },
    "gr/m2": {
        "qudt_uri": "http://qudt.org/vocab/unit/GM-PER-M2",
        "ucum": "g.m-2",
        "label": "gram per square meter",
    },
    "kg.m-2": {
        "qudt_uri": "http://qudt.org/vocab/unit/KiloGM-PER-M2",
        "ucum": "kg.m-2",
        "label": "kilogram per square meter",
    },
    "kg/m2": {
        "qudt_uri": "http://qudt.org/vocab/unit/KiloGM-PER-M2",
        "ucum": "kg.m-2",
        "label": "kilogram per square meter",
    },
    "kg.m-3": {
        "qudt_uri": "http://qudt.org/vocab/unit/KiloGM-PER-M3",
        "ucum": "kg.m-3",
        "label": "kilogram per cubic meter",
    },
    "kg/m3": {
        "qudt_uri": "http://qudt.org/vocab/unit/KiloGM-PER-M3",
        "ucum": "kg.m-3",
        "label": "kilogram per cubic meter",
    },

    # ── Mass ─────────────────────────────────────────────────────────────
    "g": {
        "qudt_uri": "http://qudt.org/vocab/unit/GM",
        "ucum": "g",
        "label": "gram",
    },
    "gr": {
        "qudt_uri": "http://qudt.org/vocab/unit/GM",
        "ucum": "g",
        "label": "gram",
    },
    "kg": {
        "qudt_uri": "http://qudt.org/vocab/unit/KiloGM",
        "ucum": "kg",
        "label": "kilogram",
    },
    "t": {
        "qudt_uri": "http://qudt.org/vocab/unit/TONNE",
        "ucum": "t",
        "label": "tonne (metric ton)",
    },

    # ── Length ───────────────────────────────────────────────────────────
    "mm": {
        "qudt_uri": "http://qudt.org/vocab/unit/MilliM",
        "ucum": "mm",
        "label": "millimeter",
    },
    "cm": {
        "qudt_uri": "http://qudt.org/vocab/unit/CentiM",
        "ucum": "cm",
        "label": "centimeter",
    },
    "m": {
        "qudt_uri": "http://qudt.org/vocab/unit/M",
        "ucum": "m",
        "label": "meter",
    },

    # ── Area ─────────────────────────────────────────────────────────────
    "ha": {
        "qudt_uri": "http://qudt.org/vocab/unit/HA",
        "ucum": "ha",
        "label": "hectare",
    },
    "m-2": {
        "qudt_uri": "http://qudt.org/vocab/unit/PER-M2",
        "ucum": "m-2",
        "label": "per square meter",
    },

    # ── Percent / dimensionless ──────────────────────────────────────────
    "%": {
        "qudt_uri": "http://qudt.org/vocab/unit/PERCENT",
        "ucum": "%",
        "label": "percent",
    },
    "1": {
        "qudt_uri": "http://qudt.org/vocab/unit/UNITLESS",
        "ucum": "1",
        "label": "dimensionless / count",
    },
    "adimensional": {
        "qudt_uri": "http://qudt.org/vocab/unit/UNITLESS",
        "ucum": "1",
        "label": "dimensionless",
    },

    # ── Time ─────────────────────────────────────────────────────────────
    "días": {
        "qudt_uri": "http://qudt.org/vocab/unit/DAY",
        "ucum": "d",
        "label": "day",
    },
    "d": {
        "qudt_uri": "http://qudt.org/vocab/unit/DAY",
        "ucum": "d",
        "label": "day",
    },

    # ── Agri-specific (no standard QUDT entry — minted under nkz) ───────
    "% sms": {
        "qudt_uri": None,  # Mint: nkz:unit/PercentDryMatter
        "ucum": "g.kg-1",
        "label": "percent on dry matter basis → g/kg",
    },
    "%smf": {
        "qudt_uri": None,  # Mint: nkz:unit/PercentFreshMatter
        "ucum": "g.kg-1",
        "label": "percent on fresh matter basis → g/kg",
    },
    "mg/kg smf": {
        "qudt_uri": None,
        "ucum": "mg.kg-1",
        "label": "milligram per kilogram fresh matter",
    },
    "kg ms/ha": {
        "qudt_uri": None,
        "ucum": "kg.ha-1",
        "label": "kilogram dry matter per hectare",
    },
    "UFL/kg ms": {
        "qudt_uri": None,  # Mint: nkz:unit/UFL-PER-KiloGM-DM
        "ucum": None,
        "label": "Forage Milk Unit per kilogram dry matter (INRAE UFL system)",
    },
    "euros/t": {
        "qudt_uri": None,  # Mint: nkz:unit/EUR-PER-TONNE
        "ucum": None,
        "label": "euro per metric ton",
    },
    "g/100g grasa": {
        "qudt_uri": None,  # Mint: nkz:unit/G-PER-100G-FAT
        "ucum": "g.100g-1",
        "label": "gram per 100 gram fat",
    },
    "individuos/placa": {
        "qudt_uri": None,  # Mint: nkz:unit/CountPerPlate
        "ucum": None,
        "label": "individuals per plate count",
    },
    "número total": {
        "qudt_uri": None,  # Mint: nkz:unit/TotalCount
        "ucum": "1",
        "label": "total count (dimensionless)",
    },
    "% del testigo uniforme": {
        "qudt_uri": None,
        "ucum": "%",
        "label": "percent of uniform control",
    },
    "QM/HA": {
        "qudt_uri": "http://qudt.org/vocab/unit/KiloGM-PER-HA",
        "ucum": "kg.ha-1",
        "label": "metric quintal per hectare → kg/ha",
    },
}


def get_qudt_unit(unit_str: str) -> dict | None:
    """Resolve a unit string to its QUDT/UCUM canonical form.

    Returns:
        Dict with keys: qudt_uri (str|None), ucum (str|None), label (str).
        None if the unit is completely unrecognized.
    """
    if not unit_str:
        return None
    # Exact match
    if unit_str in QUDT_UNIT_MAP:
        return QUDT_UNIT_MAP[unit_str]
    # Lowercase match
    lower = unit_str.lower().strip()
    for key, val in QUDT_UNIT_MAP.items():
        if key.lower().strip() == lower:
            return val
    # Partial match for compound units
    for key, val in QUDT_UNIT_MAP.items():
        if lower in key.lower() or key.lower() in lower:
            return val
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# AGROVOC Concept Mappings
# ═══════════════════════════════════════════════════════════════════════════════

AGROVOC_ZONES: dict[str, str] = {
    "Montaña": "http://aims.fao.org/aos/agrovoc/c_3615",          # highlands
    "Zona Media": "http://aims.fao.org/aos/agrovoc/c_5182",        # mid-altitude zones
    "Ribera": "http://aims.fao.org/aos/agrovoc/c_6617",            # riparian zones
    "Secano fresco": "http://aims.fao.org/aos/agrovoc/c_2405",     # dry farming
    "Secano semiárido": "http://aims.fao.org/aos/agrovoc/c_2405",
    "Secano árido": "http://aims.fao.org/aos/agrovoc/c_2405",
    "Regadío": "http://aims.fao.org/aos/agrovoc/c_3954",           # irrigated land
}

AGROVOC_IRRIGATION: dict[str, str] = {
    "secano": "http://aims.fao.org/aos/agrovoc/c_6436",            # rainfed agriculture
    "regadío": "http://aims.fao.org/aos/agrovoc/c_3954",           # irrigated land
    "riego deficitario": "http://aims.fao.org/aos/agrovoc/c_8919", # deficit irrigation
}

# Experiment type → AGROVOC (where standard concept exists) or nkz: fallback
EXPERIMENT_TYPE_MAP: dict[str, str] = {
    "riego_deficitario": "http://aims.fao.org/aos/agrovoc/c_8919",    # deficit irrigation
    "fertilizacion": "http://aims.fao.org/aos/agrovoc/c_10795",       # fertilizer application
    "control_plagas": "http://aims.fao.org/aos/agrovoc/c_5726",       # pest control
    "manejo_suelo": "http://aims.fao.org/aos/agrovoc/c_7176",         # soil management
    "fecha_siembra": "http://aims.fao.org/aos/agrovoc/c_32894",       # sowing date
    "densidad_siembra": "http://aims.fao.org/aos/agrovoc/c_7154",     # sowing density
    "cultivo_ecologico": "http://aims.fao.org/aos/agrovoc/c_5380",    # organic agriculture
    "otro": "https://nkz.robotika.cloud/ngsi-ld/OtherTrial",          # nkz fallback
}


def get_agrovoc_zone(zone_name: str) -> str | None:
    """Resolve agroclimatic zone name to AGROVOC URI."""
    return AGROVOC_ZONES.get(zone_name)


def get_agrovoc_irrigation(regime: str) -> str | None:
    """Resolve irrigation regime to AGROVOC URI."""
    return AGROVOC_IRRIGATION.get(regime.lower().strip())


def get_experiment_uri(exp_type: str) -> str | None:
    """Resolve experiment type to AGROVOC URI (with nkz fallback)."""
    return EXPERIMENT_TYPE_MAP.get(exp_type)
