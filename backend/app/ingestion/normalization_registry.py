"""
Semantic normalization registry for all BioOrchestrator ingestion pipelines.

Every source-specific trait key, disease score, location, and scale is mapped
to a canonical vocabulary (AGROVOC + ICASA + nkz namespace) so that data from
different sources (BSL German, GENVCE Spanish, NÉBIH Hungarian, etc.) becomes
directly comparable.

Usage:
    from app.ingestion.normalization_registry import (
        normalize_variety_name,
        normalize_location,
        eppo_to_scientific,
        normalize_merge_key,
        transform_traits_to_unified,
    )

Rules for adding a new source:
    1. Add EPPO_TO_SCIENTIFIC entries for any novel crop codes
    2. Add source trait keys inside each TRAIT_REGISTRY entry
    3. Add source disease keys inside DISEASE_REGISTRY (if applicable)
    4. Add trial location names to LOCATION_NORMALIZATION
    5. Run: python -m app.ingestion.validate_source NEW_SRC /path/to/trials.jsonld
"""

from __future__ import annotations

import json
import re


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Trait mappings: source language → canonical AGROVOC
# ═══════════════════════════════════════════════════════════════════════════════
#
# Canonical key → {sources: {source_id: original_key_name}, scale, direction}

TRAIT_REGISTRY: dict[str, dict] = {
    "cold_sensitivity": {
        "canonical": "cold_sensitivity",
        "agrovoc": "http://aims.fao.org/aos/agrovoc/c_1765",
        "description": "Sensibilidad al frío / Frost sensitivity",
        "sources": {"BSL": "kaelteempfindlichkeit"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "lodging_susceptibility": {
        "canonical": "lodging_susceptibility",
        "agrovoc": None,
        "description": "Propensión al encamado / Lodging tendency",
        "sources": {"BSL": "neigung_zu_lager"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "plant_height": {
        "canonical": "plant_height",
        "agrovoc": "http://aims.fao.org/aos/agrovoc/c_5969",
        "description": "Altura de planta / Plant height",
        "sources": {"BSL": "pflanzenlaenge"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "grain_maturity_number": {
        "canonical": "grain_maturity_number",
        "agrovoc": None,
        "description": "Número de madurez de grano (código alemán: K 210) / Grain maturity number code",
        "sources": {"BSL": "koernerreifezahl"},
        "scale": "categorical",
        "higherIs": None,
        "domain": None,
    },
    "silage_maturity_number": {
        "canonical": "silage_maturity_number",
        "agrovoc": None,
        "description": "Número de madurez para ensilado (código alemán: S 250) / Silage maturity number code",
        "sources": {"BSL": "siloreifezahl"},
        "scale": "categorical",
        "higherIs": None,
        "domain": None,
    },
    "flowering_time_female": {
        "canonical": "flowering_time_female",
        "agrovoc": "http://aims.fao.org/aos/agrovoc/c_15954",
        "description": "Floración femenina / Female flowering time",
        "sources": {"BSL": "zeitpunkt_weibliche_bluete"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "tillering_tendency": {
        "canonical": "tillering_tendency",
        "agrovoc": "http://aims.fao.org/aos/agrovoc/c_7775",
        "description": "Tendencia al ahijamiento / Tillering tendency",
        "sources": {"BSL": "neigung_zu_bestockung"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "leaf_senescence_rating": {
        "canonical": "leaf_senescence_rating",
        "agrovoc": None,
        "description": "Senescencia foliar / Leaf senescence rating",
        "sources": {"BSL": "abreifegrad_der_blaetter"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "silage_maturity_rating": {
        "canonical": "silage_maturity_rating",
        "agrovoc": None,
        "description": "Madurez para ensilado / Silage maturity number",
        "sources": {"BSL": "siloreifezahl"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "maturity_group": {
        "canonical": "maturity_group",
        "agrovoc": "http://aims.fao.org/aos/agrovoc/c_331039",
        "description": "Grupo de madurez / Maturity group",
        "sources": {"BSL": "reifegruppe"},
        "scale": "categorical",
        "higherIs": None,
        "domain": None,
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Disease score mappings
# ═══════════════════════════════════════════════════════════════════════════════

DISEASE_REGISTRY: dict[str, dict] = {
    "stem_rot_resistance": {
        "canonical": "stem_rot_resistance",
        "description": "Resistencia a podredumbre de tallo / Stem rot resistance",
        "sources": {"BSL": "staengelfaeule"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Scale normalizers
# ═══════════════════════════════════════════════════════════════════════════════

SCALE_NORMALIZERS: dict[str, callable] = {}


def _norm_bsl_1_9(value, higher_is="better"):
    """BSL 1-9 → 0-1 normalized.

    higherIs="better": 1→1.0 (best), 9→0.0 (worst)
    higherIs="worse":  1→0.0 (best), 9→1.0 (worst)

    Returns None for non-numeric values (they are categorical, not 1-9 scale).
    """
    if value is None:
        return None
    try:
        v = max(1.0, min(9.0, float(value)))
    except (ValueError, TypeError):
        # Non-numeric value (e.g. "K 210" categorical code) — skip numeric norm
        return None
    if higher_is == "better":
        return 1.0 - ((v - 1.0) / 8.0)
    return (v - 1.0) / 8.0


SCALE_NORMALIZERS["1-9_bsl"] = _norm_bsl_1_9


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Location normalization
# ═══════════════════════════════════════════════════════════════════════════════
#
# Maps trialLocation strings (lowercase) to canonical {name, country, climateClass}.
# Multiple raw strings mapping to the same canonical entry = same physical site.

LOCATION_NORMALIZATION: dict[str, dict] = {
    # ── Alemania ────────────────────────────────────────────────────────
    "bsl deutschland cfb": {
        "name": "BSL Alemania Cfb", "country": "Alemania", "climateClass": "Cfb",
    },
    "bsl deutschland dfb": {
        "name": "BSL Alemania Dfb", "country": "Alemania", "climateClass": "Dfb",
    },
    "bsl deutschland uebergang": {
        "name": "BSL Alemania Transición", "country": "Alemania", "climateClass": None,
    },
    "bundesweit": {
        "name": "Alemania (promedio nacional)", "country": "Alemania", "climateClass": None,
    },
    "bundesweit (deutschland)": {
        "name": "Alemania (promedio nacional)", "country": "Alemania", "climateClass": None,
    },
    # ── Francia ─────────────────────────────────────────────────────────
    "ctifl balandran": {
        "name": "CTIFL Balandran", "country": "Francia", "climateClass": "Csa",
    },
    "balandran (bellegarde)": {
        "name": "CTIFL Balandran", "country": "Francia", "climateClass": "Csa",
    },
    "balandran (ctifl)": {
        "name": "CTIFL Balandran", "country": "Francia", "climateClass": "Csa",
    },
    "balandran": {
        "name": "CTIFL Balandran", "country": "Francia", "climateClass": "Csa",
    },
    "ctifl lanxade": {
        "name": "CTIFL Lanxade", "country": "Francia", "climateClass": "Cfb",
    },
    "avignon": {
        "name": "Avignon", "country": "Francia", "climateClass": "Csa",
    },
    # ── Hungría ─────────────────────────────────────────────────────────
    "hungary (average)": {
        "name": "Hungría (promedio nacional)", "country": "Hungría", "climateClass": "Cfb",
    },
    "hungary (multiple locations)": {
        "name": "Hungría (promedio nacional)", "country": "Hungría", "climateClass": "Cfb",
    },
    # ── España / Navarra ────────────────────────────────────────────────
    "imarcoain": {
        "name": "Imarcoain", "country": "España", "climateClass": "Cfb",
    },
    "navarra (zonas húmedas de secano)": {
        "name": "Navarra (zonas húmedas de secano)", "country": "España", "climateClass": "Cfb",
    },
    "larraga": {
        "name": "Larraga", "country": "España", "climateClass": "Cfb",
    },
    "arazuri": {
        "name": "Arazuri", "country": "España", "climateClass": "BSk",
    },
    "cadreita": {
        "name": "Cadreita", "country": "España", "climateClass": "BSk",
    },
    "sartaguda": {
        "name": "Sartaguda", "country": "España", "climateClass": "BSk",
    },
    "olite": {
        "name": "Olite", "country": "España", "climateClass": "BSk",
    },
    "tafalla": {
        "name": "Tafalla", "country": "España", "climateClass": "Cfb",
    },
    "unciti": {
        "name": "Unciti", "country": "España", "climateClass": "Cfb",
    },
    "tulebras": {
        "name": "Tulebras", "country": "España", "climateClass": "BSk",
    },
    "cárcar": {
        "name": "Cárcar", "country": "España", "climateClass": "BSk",
    },
    "lumbier": {
        "name": "Lumbier", "country": "España", "climateClass": "Cfb",
    },
    "mendióroz": {
        "name": "Mendióroz", "country": "España", "climateClass": "Cfb",
    },
    "doneztebe": {
        "name": "Doneztebe", "country": "España", "climateClass": "Cfb",
    },
    "doneztebe/santesteban": {
        "name": "Doneztebe", "country": "España", "climateClass": "Cfb",
    },
    # ── Portugal ────────────────────────────────────────────────────────
    "elvas": {
        "name": "Elvas", "country": "Portugal", "climateClass": "Csa",
    },
    "beja": {
        "name": "Beja", "country": "Portugal", "climateClass": "Csa",
    },
    # ── Italia / CREA ───────────────────────────────────────────────────
    "villafranca piemonte (to)": {
        "name": "Villafranca Piemonte", "country": "Italia", "climateClass": "Cfa",
    },
    "media 14 località": {
        "name": "CREA Italia (media 14 località)", "country": "Italia", "climateClass": None,
    },
    "media 13 località": {
        "name": "CREA Italia (media 13 località)", "country": "Italia", "climateClass": None,
    },
    "media 10 località": {
        "name": "CREA Italia (media 10 località)", "country": "Italia", "climateClass": None,
    },
    "media 8 località": {
        "name": "CREA Italia (media 8 località)", "country": "Italia", "climateClass": None,
    },
    # ── España / INTIA-ITACyL-IFAPA ───────────────────────────────────────
    "zamadueñas": {
        "name": "Zamadueñas", "country": "España", "climateClass": "Cfb",
    },
    "valladolid": {
        "name": "Valladolid", "country": "España", "climateClass": "Cfb",
    },
    "lleida": {
        "name": "Lleida", "country": "España", "climateClass": "Csa",
    },
    "córdoba": {
        "name": "Córdoba", "country": "España", "climateClass": "Csa",
    },
    "lugo": {
        "name": "Lugo", "country": "España", "climateClass": "Cfb",
    },
    "palencia": {
        "name": "Palencia", "country": "España", "climateClass": "Cfb",
    },
    "ciudad real": {
        "name": "Ciudad Real", "country": "España", "climateClass": "BSk",
    },
    "cuenca": {
        "name": "Cuenca", "country": "España", "climateClass": "Cfb",
    },
    "biota": {
        "name": "Biota", "country": "España", "climateClass": "BSk",
    },
    "esteras de lubia": {
        "name": "Esteras de Lubia", "country": "España", "climateClass": "Cfb",
    },
    "zael": {
        "name": "Zael", "country": "España", "climateClass": "Cfb",
    },
    "belorado": {
        "name": "Belorado", "country": "España", "climateClass": "Cfb",
    },
    "azpa": {
        "name": "Azpa", "country": "España", "climateClass": "Cfb",
    },
    "torres de elorz": {
        "name": "Torres de Elorz", "country": "España", "climateClass": "Cfb",
    },
    "uroz": {
        "name": "Uroz", "country": "España", "climateClass": "Cfb",
    },
    "oskotz": {
        "name": "Oskotz", "country": "España", "climateClass": "Cfb",
    },
    "aldaba": {
        "name": "Aldaba", "country": "España", "climateClass": "Cfb",
    },
    "sesma (navarra)": {
        "name": "Sesma (Navarra)", "country": "España", "climateClass": "BSk",
    },
    "rípodas (navarra)": {
        "name": "Rípodas (Navarra)", "country": "España", "climateClass": "Cfb",
    },
    "cabra (córdoba)": {
        "name": "Cabra (Córdoba)", "country": "España", "climateClass": "Csa",
    },
    "carmona (tomejil)": {
        "name": "Carmona (Tomejil)", "country": "España", "climateClass": "Csa",
    },
    "almería (la mojonera)": {
        "name": "Almería (La Mojonera)", "country": "España", "climateClass": "Csa",
    },
    "córdoba (alameda del obispo)": {
        "name": "Córdoba (Alameda del Obispo)", "country": "España", "climateClass": "Csa",
    },
    "andalucía": {
        "name": "Andalucía", "country": "España", "climateClass": "Csa",
    },
    "castilla y león": {
        "name": "Castilla y León", "country": "España", "climateClass": "Cfb",
    },
    "babilafuente": {
        "name": "Babilafuente", "country": "España", "climateClass": "Cfb",
    },
    "casasola de arión": {
        "name": "Casasola de Arión", "country": "España", "climateClass": "Cfb",
    },
    "fresnillo de las dueñas": {
        "name": "Fresnillo de las Dueñas", "country": "España", "climateClass": "Cfb",
    },
    "villamuriel de cerrato": {
        "name": "Villamuriel de Cerrato", "country": "España", "climateClass": "Cfb",
    },
    "torrecilla de la abadesa": {
        "name": "Torrecilla de la Abadesa", "country": "España", "climateClass": "Cfb",
    },
    "peñaflor de hornija": {
        "name": "Peñaflor de Hornija", "country": "España", "climateClass": "Cfb",
    },
    "portugal": {
        "name": "Portugal (promedio nacional)", "country": "Portugal", "climateClass": None,
    },
    # ── Alemania / LfL Bayern ─────────────────────────────────────────────
    "freising": {
        "name": "Freising", "country": "Alemania", "climateClass": "Dfb",
    },
    "amberg": {
        "name": "Amberg", "country": "Alemania", "climateClass": "Dfb",
    },
    "würzburg": {
        "name": "Würzburg", "country": "Alemania", "climateClass": "Dfb",
    },
    "straßmoos": {
        "name": "Straßmoos", "country": "Alemania", "climateClass": "Dfb",
    },
    "großbreitenbronn": {
        "name": "Großbreitenbronn", "country": "Alemania", "climateClass": "Dfb",
    },
    "rotthalmünster": {
        "name": "Rotthalmünster", "country": "Alemania", "climateClass": "Dfb",
    },
    "almesbach": {
        "name": "Almesbach", "country": "Alemania", "climateClass": "Dfb",
    },
    "frankendorf": {
        "name": "Frankendorf", "country": "Alemania", "climateClass": "Dfb",
    },
    "hausen": {
        "name": "Hausen", "country": "Alemania", "climateClass": "Dfb",
    },
    "landsberg": {
        "name": "Landsberg", "country": "Alemania", "climateClass": "Dfb",
    },
    "osterseon": {
        "name": "Osterseon", "country": "Alemania", "climateClass": "Dfb",
    },
    "köfering": {
        "name": "Köfering", "country": "Alemania", "climateClass": "Dfb",
    },
    "anbaugebiete süddeutschland": {
        "name": "Alemania Sur (anbaugebiete)", "country": "Alemania", "climateClass": "Cfb",
    },
    # ── Hungría / NÉBIH ───────────────────────────────────────────────────
    "bóly": {
        "name": "Bóly", "country": "Hungría", "climateClass": "Dfb",
    },
    "püski": {
        "name": "Püski", "country": "Hungría", "climateClass": "Cfb",
    },
    "szombathely": {
        "name": "Szombathely", "country": "Hungría", "climateClass": "Dfb",
    },
    "abaújszántó": {
        "name": "Abaújszántó", "country": "Hungría", "climateClass": "Cfb",
    },
    "eszterágpuszta": {
        "name": "Eszterágpuszta", "country": "Hungría", "climateClass": "Cfb",
    },
    "gyulatanya": {
        "name": "Gyulatanya", "country": "Hungría", "climateClass": "Cfb",
    },
    "jászboldogháza": {
        "name": "Jászboldogháza", "country": "Hungría", "climateClass": "Dfa",
    },
    "újfehértó": {
        "name": "Újfehértó", "country": "Hungría", "climateClass": "Cfb",
    },
    "szarvas": {
        "name": "Szarvas", "country": "Hungría", "climateClass": "Dfa",
    },
    "hanságliget": {
        "name": "Hanságliget", "country": "Hungría", "climateClass": "Cfb",
    },
    "pápa": {
        "name": "Pápa", "country": "Hungría", "climateClass": "Cfb",
    },
    "kéthely": {
        "name": "Kéthely", "country": "Hungría", "climateClass": "Cfb",
    },
    "taktaharkány": {
        "name": "Taktaharkány", "country": "Hungría", "climateClass": "Cfb",
    },
    "sopronhorpács": {
        "name": "Sopronhorpács", "country": "Hungría", "climateClass": "Dfb",
    },
    "bödönhely": {
        "name": "Bödönhely", "country": "Hungría", "climateClass": "Cfb",
    },
    "vámosszabadi": {
        "name": "Vámosszabadi", "country": "Hungría", "climateClass": "Cfb",
    },
    "iregszemcse": {
        "name": "Iregszemcse", "country": "Hungría", "climateClass": "Cfb",
    },
    "tordas": {
        "name": "Tordas", "country": "Hungría", "climateClass": "Cfb",
    },
    "székkutas": {
        "name": "Székkutas", "country": "Hungría", "climateClass": "Dfa",
    },
    "hajdúböszörmény": {
        "name": "Hajdúböszörmény", "country": "Hungría", "climateClass": "Dfa",
    },
    "jánoshalma": {
        "name": "Jánoshalma", "country": "Hungría", "climateClass": "Dfa",
    },
    "mezőhegyes": {
        "name": "Mezőhegyes", "country": "Hungría", "climateClass": "Dfa",
    },
    "mezőfalva": {
        "name": "Mezőfalva", "country": "Hungría", "climateClass": "Dfa",
    },
    "mosonmagyaróvár": {
        "name": "Mosonmagyaróvár", "country": "Hungría", "climateClass": "Cfb",
    },
    "országos": {
        "name": "Hungría (promedio nacional)", "country": "Hungría", "climateClass": None,
    },
    "országos átlag": {
        "name": "Hungría (promedio nacional)", "country": "Hungría", "climateClass": None,
    },
    "magyarország": {
        "name": "Hungría (promedio nacional)", "country": "Hungría", "climateClass": None,
    },
    "átlag (8 helyszín)": {
        "name": "Hungría (media 8 ubicaciones)", "country": "Hungría", "climateClass": None,
    },
    "átlag (9 helyszín)": {
        "name": "Hungría (media 9 ubicaciones)", "country": "Hungría", "climateClass": None,
    },
    "átlag": {
        "name": "Hungría (promedio)", "country": "Hungría", "climateClass": None,
    },
    "average 10 locations": {
        "name": "Hungría (media 10 ubicaciones)", "country": "Hungría", "climateClass": None,
    },
    "múltiples localidades": {
        "name": "Hungría (múltiples localidades)", "country": "Hungría", "climateClass": None,
    },
    # ── Reino Unido / AHDB ────────────────────────────────────────────────
    "uk national list": {
        "name": "UK National List (promedio)", "country": "Reino Unido", "climateClass": "Cfb",
    },
    # ── Francia / CTIFL ───────────────────────────────────────────────────
    "pleumeur-gautier (terre d'essais)": {
        "name": "CTIFL Pleumeur-Gautier", "country": "Francia", "climateClass": "Cfb",
    },
    "bassin sud-est": {
        "name": "Francia Bassin Sud-Est", "country": "Francia", "climateClass": "Csa",
    },
    "la morinière": {
        "name": "CTIFL La Morinière", "country": "Francia", "climateClass": "Cfb",
    },
    # ── Genéricos ─────────────────────────────────────────────────────────
    "unknown": {
        "name": "Unknown", "country": None, "climateClass": None,
    },
    "not specified": {
        "name": "Not specified", "country": None, "climateClass": None,
    },
}


def normalize_location(raw_location: str | None) -> dict | None:
    """Resolve a raw trialLocation to its canonical form.

    Args:
        raw_location: Raw trialLocation string from any source.

    Returns:
        Dict with keys {name, country, climateClass} or None if unrecognised.
    """
    if not raw_location:
        return None
    key = raw_location.strip().lower()
    # Exact match
    if key in LOCATION_NORMALIZATION:
        return dict(LOCATION_NORMALIZATION[key])
    # Partial match (raw_key in key, or key in raw_key)
    for raw_key, info in LOCATION_NORMALIZATION.items():
        if raw_key in key or key in raw_key:
            return dict(info)
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Variety name normalizer
# ═══════════════════════════════════════════════════════════════════════════════

def normalize_variety_name(name: str | None) -> str | None:
    """Normalise a variety name: uppercase + strip trailing parenthetical tags.

    E.g.:  "Hispanic"  → "HISPANIC"
           "MAS 26 T"  → "MAS 26 T"
           "HISPANIC (T)" → "HISPANIC"
           None         → None
    """
    if not name:
        return None
    name_upper = name.strip().upper()
    # Strip trailing parenthetical tags like "(T)", "(TEST)"
    name_clean = re.sub(r'\s*\([^)]*\)\s*$', '', name_upper).strip()
    return name_clean if name_clean else name_upper


# ═══════════════════════════════════════════════════════════════════════════════
# 6. EPPO → Scientific name (reverse lookup)
# ═══════════════════════════════════════════════════════════════════════════════

EPPO_TO_SCIENTIFIC: dict[str, str] = {
    "ZEAMX": "Zea mays",
    "TRZAX": "Triticum aestivum",
    "TRZAW": "Triticum aestivum",
    "TRZDU": "Triticum durum",
    "HORVX": "Hordeum vulgare",
    "BRSNN": "Brassica napus",
    "BRSOX": "Brassica oleracea",
    "HELAN": "Helianthus annuus",
    "GLXMA": "Glycine max",
    "PISSA": "Pisum sativum",
    "VICSA": "Vicia sativa",
    "LENCU": "Lens culinaris",
    "CIEAR": "Cicer arietinum",
    "AVESA": "Avena sativa",
    "SECCE": "Secale cereale",
    "TTLSS": "Triticosecale",
    "SOLTU": "Solanum tuberosum",
    "LYPES": "Solanum lycopersicum",
    "CAPAN": "Capsicum annuum",
    "VICFA": "Vicia faba",
    "LUPAL": "Lupinus albus",
    "MALDO": "Malus domestica",
    "FRAAN": "Fragaria × ananassa",
    "PRUDU": "Prunus dulcis",
    "OLEAE": "Olea europaea",
    "VITVI": "Vitis vinifera",
    "ORYSA": "Oryza sativa",
}


def eppo_to_scientific(eppo_code: str | None) -> str | None:
    """Resolve an EPPO code (e.g. 'ZEAMX') to its scientific name.

    Strips 'eppo:' prefix automatically.
    """
    if not eppo_code:
        return None
    clean = eppo_code.replace("eppo:", "").replace("EPPO:", "").strip().upper()
    return EPPO_TO_SCIENTIFIC.get(clean)


# ═══════════════════════════════════════════════════════════════════════════════
# 7. MergeKey normalizer
# ═══════════════════════════════════════════════════════════════════════════════

def normalize_merge_key(
    source_id: str,
    eppo: str | None,
    variety: str | None,
    year: int | None,
    location: str | None,
) -> str:
    """Generate a canonical, source-independent mergeKey.

    Format: ``SOURCE|eppo:XXXXX|VARIETY|COUNTRY|YYYY``

    The country is resolved from the location when possible;
    the variety name is normalised (upper, no parenthetical suffixes).
    """
    src = source_id.upper() if source_id else "UNKN"
    e = eppo.replace("eppo:", "").replace("EPPO:", "").strip().upper() if eppo else "NOEPPO"
    v = normalize_variety_name(variety) or "NOVAR"
    loc_info = normalize_location(location)
    country = loc_info["country"] if loc_info else "NOCOUNTRY"
    y = str(year) if year and year > 1900 else "NOYEAR"
    return f"{src}|eppo:{e}|{v}|{country}|{y}"


# ═══════════════════════════════════════════════════════════════════════════════
# 8. Trait transformation engine
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_json_field(value: str | dict | None) -> dict:
    """Safely parse a field that may be None, a dict, or a JSON string."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {}
    return {}


def transform_traits_to_unified(
    traits_raw: str | dict | None,
    disease_raw: str | dict | None,
    source_id: str,
) -> tuple[str | None, str | None]:
    """Transform raw (source-language) traits and disease scores into unified form.

    For each canonical trait in the registry, looks up the source-specific key
    in the raw data, extracts the value, applies scale normalisation, and
    produces a JSON string with the unified representation.

    Returns:
        (traits_json, disease_json) — each a JSON string or None.
    """
    traits_map = _parse_json_field(traits_raw)
    disease_map = _parse_json_field(disease_raw)

    unified_traits: dict = {}
    unified_disease: dict = {}

    for canonical, config in TRAIT_REGISTRY.items():
        source_key = config["sources"].get(source_id)
        if source_key is not None and source_key in traits_map:
            raw_value = traits_map[source_key]
            scale = config["scale"]
            higher_is = config["higherIs"]

            if scale in SCALE_NORMALIZERS:
                normalised_value = SCALE_NORMALIZERS[scale](raw_value, higher_is)
            else:
                normalised_value = raw_value

            unified_traits[canonical] = {
                "value": normalised_value,
                "rawValue": raw_value,
                "scale": scale,
                "sourceKey": source_key,
                "agrovoc": config.get("agrovoc"),
            }

    for canonical, config in DISEASE_REGISTRY.items():
        source_key = config["sources"].get(source_id)
        if source_key is not None and source_key in disease_map:
            raw_value = disease_map[source_key]
            scale = config["scale"]
            higher_is = config["higherIs"]
            if scale in SCALE_NORMALIZERS:
                normalised_value = SCALE_NORMALIZERS[scale](raw_value, higher_is)
            else:
                normalised_value = raw_value
            unified_disease[canonical] = {
                "value": normalised_value,
                "rawValue": raw_value,
                "scale": scale,
                "sourceKey": source_key,
            }

    traits_json = json.dumps(unified_traits, ensure_ascii=False) if unified_traits else None
    disease_json = json.dumps(unified_disease, ensure_ascii=False) if unified_disease else None
    return traits_json, disease_json
