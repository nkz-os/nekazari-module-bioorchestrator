"""Crop reference data — carbon, operations, nutrient requirements.

IPCC 2019 Tier 1 defaults + standard Mediterranean agronomic practices.
Used by compare-crops and rotation-plan endpoints.

19 crops: cereals (TRZAX, HORVX, AVESA, SECCE, ZEAXX), legumes (CIEAR, PIBSX,
PIBAR, VICFX, LENCU, GLXMA, VICSA, VICVI, VICER, LTHSA), forage (MEDSA),
tubers (SOLTU), oilseed (BRUn).
"""

CROP_REFERENCE: dict[str, dict] = {
    # Cereals
    "TRZAX": {"carbon_fixed_tco2e_ha": 2.8, "operations_count": 7, "n_requirement_kg_ha": 180, "n_fixation_kg_ha": 0, "growing_season_days": 210},  # trigo blando
    "HORVX": {"carbon_fixed_tco2e_ha": 2.3, "operations_count": 6, "n_requirement_kg_ha": 140, "n_fixation_kg_ha": 0, "growing_season_days": 190},  # cebada
    "AVESA": {"carbon_fixed_tco2e_ha": 2.0, "operations_count": 5, "n_requirement_kg_ha": 120, "n_fixation_kg_ha": 0, "growing_season_days": 180},  # avena
    "SECCE": {"carbon_fixed_tco2e_ha": 2.1, "operations_count": 5, "n_requirement_kg_ha": 110, "n_fixation_kg_ha": 0, "growing_season_days": 200},  # centeno
    "ZEAXX": {"carbon_fixed_tco2e_ha": 4.0, "operations_count": 8, "n_requirement_kg_ha": 250, "n_fixation_kg_ha": 0, "growing_season_days": 180},  # maíz
    "SOLTU": {"carbon_fixed_tco2e_ha": 1.8, "operations_count": 9, "n_requirement_kg_ha": 200, "n_fixation_kg_ha": 0, "growing_season_days": 160},  # patata
    # Legumes (grain)
    "CIEAR": {"carbon_fixed_tco2e_ha": 0.9, "operations_count": 4, "n_requirement_kg_ha": 30, "n_fixation_kg_ha": 80, "growing_season_days": 150},  # garbanzo
    "PIBSX": {"carbon_fixed_tco2e_ha": 1.2, "operations_count": 4, "n_requirement_kg_ha": 20, "n_fixation_kg_ha": 100, "growing_season_days": 140},  # guisante proteico
    "PIBAR": {"carbon_fixed_tco2e_ha": 1.1, "operations_count": 4, "n_requirement_kg_ha": 15, "n_fixation_kg_ha": 110, "growing_season_days": 140},  # guisante
    "VICFX": {"carbon_fixed_tco2e_ha": 1.5, "operations_count": 4, "n_requirement_kg_ha": 20, "n_fixation_kg_ha": 130, "growing_season_days": 160},  # haba
    "LENCU": {"carbon_fixed_tco2e_ha": 0.7, "operations_count": 3, "n_requirement_kg_ha": 15, "n_fixation_kg_ha": 60, "growing_season_days": 130},  # lenteja
    "GLXMA": {"carbon_fixed_tco2e_ha": 2.5, "operations_count": 5, "n_requirement_kg_ha": 30, "n_fixation_kg_ha": 180, "growing_season_days": 160},  # soja
    "VICSA": {"carbon_fixed_tco2e_ha": 1.0, "operations_count": 3, "n_requirement_kg_ha": 10, "n_fixation_kg_ha": 110, "growing_season_days": 150},  # veza común
    "VICVI": {"carbon_fixed_tco2e_ha": 1.2, "operations_count": 3, "n_requirement_kg_ha": 10, "n_fixation_kg_ha": 120, "growing_season_days": 160},  # veza vellosa
    "VICER": {"carbon_fixed_tco2e_ha": 0.9, "operations_count": 3, "n_requirement_kg_ha": 15, "n_fixation_kg_ha": 100, "growing_season_days": 140},  # yero
    "LTHSA": {"carbon_fixed_tco2e_ha": 0.8, "operations_count": 3, "n_requirement_kg_ha": 10, "n_fixation_kg_ha": 90, "growing_season_days": 130},  # almorta
    # Forage legumes
    "MEDSA": {"carbon_fixed_tco2e_ha": 3.0, "operations_count": 5, "n_requirement_kg_ha": 10, "n_fixation_kg_ha": 200, "growing_season_days": 250},  # alfalfa
    # Other
    "BRUn":  {"carbon_fixed_tco2e_ha": 5.2, "operations_count": 6, "n_requirement_kg_ha": 200, "n_fixation_kg_ha": 0, "growing_season_days": 250},  # brassica/colza
}

DEFAULT_REFERENCE = {"carbon_fixed_tco2e_ha": 1.5, "operations_count": 5, "n_requirement_kg_ha": 100, "n_fixation_kg_ha": 0, "growing_season_days": 180}


def get_crop_ref(eppo: str) -> dict:
    """Get reference data for a crop EPPO code, falling back to defaults."""
    return CROP_REFERENCE.get(eppo, DEFAULT_REFERENCE)
