"""EPPO code → common name mapping for display in UI.

EPPO codes are the canonical crop identifiers in the knowledge graph.
This mapping provides human-readable names in English and Spanish.
Sources: EPPO Global Database (https://gd.eppo.int), FAO.
"""

EPPO_COMMON_NAMES: dict[str, dict[str, str]] = {
    "TRZAX": {"en": "Wheat", "es": "Trigo"},
    "ZEAMX": {"en": "Maize", "es": "Maíz"},
    "ORYSX": {"en": "Rice", "es": "Arroz"},
    "GLXMA": {"en": "Soybean", "es": "Soja"},
    "HELAN": {"en": "Sunflower", "es": "Girasol"},
    "GOSHI": {"en": "Cotton", "es": "Algodón"},
    "MEDSA": {"en": "Alfalfa", "es": "Alfalfa"},
    "OLEAE": {"en": "Olive", "es": "Olivo"},
    "PRNDU": {"en": "Almond", "es": "Almendro"},
    "VITVI": {"en": "Wine grape", "es": "Vid"},
    "LYPES": {"en": "Tomato", "es": "Tomate"},
    "SOLTU": {"en": "Potato", "es": "Patata"},
    "ALLCE": {"en": "Onion", "es": "Cebolla"},
    "VICFX": {"en": "Faba bean", "es": "Haba"},
    "CIEAR": {"en": "Chickpea", "es": "Garbanzo"},
    "LENCU": {"en": "Lentil", "es": "Lenteja"},
    "PIBAR": {"en": "Pea", "es": "Guisante"},
    "BRSNN": {"en": "Rapeseed", "es": "Colza"},
    "SACHU": {"en": "Sugarcane", "es": "Caña de azúcar"},
    "SORVU": {"en": "Sorghum", "es": "Sorgo"},
    "PENMI": {"en": "Millet", "es": "Mijo"},
    "HORVX": {"en": "Barley", "es": "Cebada"},
    "AVESA": {"en": "Oat", "es": "Avena"},
    "SECCE": {"en": "Rye", "es": "Centeno"},
    "SOYBN": {"en": "Soybean", "es": "Soja"},
}


def get_common_name(eppo_code: str, lang: str = "en") -> str | None:
    """Look up common name for an EPPO code. Returns None if not found."""
    names = EPPO_COMMON_NAMES.get(eppo_code.upper())
    if names:
        return names.get(lang)
    return None
