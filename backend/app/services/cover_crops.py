"""Cover crop and protein legume reference data — open-licensed European sources.

All parameters are backed exclusively by sources audited with
``commercial_use: allowed`` in ``backend/data/registries/sources.yaml``:
- ``mapa_guia_2010``: Guía práctica de la fertilización racional de los cultivos en España
  (MAPA, NIPO 770-10-150-4, Ley 37/2007 reuse allowed)
- ``fertilicalc_script_2024``: FertiliCalc reference script (UCO / IAS-CSIC, F. J. Villalobos,
  GPL-3.0 compatible with AGPL-3.0 under section 13)
- ``essd_2024_s3``: Ludemann et al. (2024), Earth Syst. Sci. Data 16, 525 (CC-BY-4.0)
- ``ludemann_cc0``: Ludemann et al., Dryad global crop nutrient concentration and harvest indices (CC0-1.0)

Zero data from restricted sources (INTIA, IFAPA, ITACyL).

Usage:
    from app.services.cover_crops import lookup, select_cover_crops

    params = lookup("VICSA", "Csa")
    candidates = select_cover_crops(climate_class="Csa", management="organic")
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

# ── Base temperature for cool-season cover crops (Trudgill et al. 2005) ──────
BASE_TEMP_COOL_C = 4.0
BASE_TEMP_WARM_C = 10.0

# ── Organic yield reduction factor ──────────────────────────────────────────
# Meta-analysis: Seufert et al. 2012, Ponisio et al. 2015
# Organic systems yield 75-80% of conventional for grain legumes
ORGANIC_YIELD_FACTOR = 0.80

# Allowed source_ids in this module (all commercial_use: allowed in sources.yaml)
ALLOWED_COVER_SOURCES: frozenset[str] = frozenset({
    "mapa_guia_2010",
    "fertilicalc_script_2024",
    "essd_2024_s3",
    "ludemann_cc0",
})

# ── Complete European data matrix ──────────────────────────────────────────
# Format per species:
#   "EPPO": {
#       "scientific": str,
#       "common_name": str,
#       "type": "legume" | "grass",
#       "climates": {
#           "Csa": {param: {"value": float, "min": float, "max": float, "source": str, "source_id": str}},
#           ...
#       }
#   }

COVER_CROPS: dict[str, dict[str, Any]] = {
    "VICSA": {
        "scientific": "Vicia sativa L.",
        "common_name": "Common vetch",
        "type": "legume",
        "kill_method": "roller_crimper",
        "climates": {
            "Csa": {
                "biomass_t_ha": {"value": 4.5, "min": 3.5, "max": 5.5, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "c_n_ratio": {"value": 12, "min": 10, "max": 15, "source": "FertiliCalc (UCO/CSIC)", "source_id": "fertilicalc_script_2024"},
                "gdd_to_termination": {"value": 1150, "min": 1000, "max": 1300, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "frost_tolerance_c": {"value": -8, "min": -10, "max": -6, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_fixation_kg_ha": {"value": 90, "min": 60, "max": 130, "source": "ESSD 2024 (Ludemann et al.)", "source_id": "essd_2024_s3"},
                "n_content_pct": {"value": 3.5, "min": 3.0, "max": 4.2, "source": "FertiliCalc (UCO/CSIC)", "source_id": "fertilicalc_script_2024"},
            },
            "BSk": {
                "biomass_t_ha": {"value": 3.8, "min": 2.8, "max": 4.5, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "c_n_ratio": {"value": 12, "min": 10, "max": 14, "source": "FertiliCalc (UCO/CSIC)", "source_id": "fertilicalc_script_2024"},
                "gdd_to_termination": {"value": 1250, "min": 1100, "max": 1400, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "frost_tolerance_c": {"value": -8, "min": -10, "max": -6, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_fixation_kg_ha": {"value": 80, "min": 60, "max": 110, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_content_pct": {"value": 3.5, "min": 3.0, "max": 4.0, "source": "FertiliCalc (UCO/CSIC)", "source_id": "fertilicalc_script_2024"},
            },
            "Cfb": {
                "biomass_t_ha": {"value": 5.8, "min": 4.5, "max": 7.0, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "c_n_ratio": {"value": 11, "min": 9, "max": 14, "source": "FertiliCalc (UCO/CSIC)", "source_id": "fertilicalc_script_2024"},
                "gdd_to_termination": {"value": 1350, "min": 1200, "max": 1500, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "frost_tolerance_c": {"value": -8, "min": -10, "max": -6, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_fixation_kg_ha": {"value": 120, "min": 80, "max": 160, "source": "ESSD 2024 (Ludemann et al.)", "source_id": "essd_2024_s3"},
            },
        },
    },
    "VICVI": {
        "scientific": "Vicia villosa Roth",
        "common_name": "Hairy vetch",
        "type": "legume",
        "kill_method": "roller_crimper",
        "climates": {
            "Csa": {
                "biomass_t_ha": {"value": 5.5, "min": 4.0, "max": 7.0, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "c_n_ratio": {"value": 11, "min": 9, "max": 13, "source": "FertiliCalc (UCO/CSIC)", "source_id": "fertilicalc_script_2024"},
                "gdd_to_termination": {"value": 1300, "min": 1150, "max": 1450, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "frost_tolerance_c": {"value": -15, "min": -20, "max": -10, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_fixation_kg_ha": {"value": 120, "min": 80, "max": 170, "source": "ESSD 2024 (Ludemann et al.)", "source_id": "essd_2024_s3"},
                "n_content_pct": {"value": 3.8, "min": 3.2, "max": 4.5, "source": "FertiliCalc (UCO/CSIC)", "source_id": "fertilicalc_script_2024"},
            },
            "BSk": {
                "biomass_t_ha": {"value": 4.2, "min": 3.0, "max": 5.5, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "c_n_ratio": {"value": 11, "min": 9, "max": 13, "source": "FertiliCalc (UCO/CSIC)", "source_id": "fertilicalc_script_2024"},
                "gdd_to_termination": {"value": 1400, "min": 1200, "max": 1550, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "frost_tolerance_c": {"value": -18, "min": -22, "max": -12, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_fixation_kg_ha": {"value": 105, "min": 80, "max": 140, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
            },
            "Cfb": {
                "biomass_t_ha": {"value": 7.0, "min": 5.5, "max": 8.5, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "c_n_ratio": {"value": 10, "min": 8, "max": 12, "source": "FertiliCalc (UCO/CSIC)", "source_id": "fertilicalc_script_2024"},
                "gdd_to_termination": {"value": 1500, "min": 1350, "max": 1650, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "frost_tolerance_c": {"value": -18, "min": -22, "max": -14, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_fixation_kg_ha": {"value": 160, "min": 110, "max": 200, "source": "ESSD 2024 (Ludemann et al.)", "source_id": "essd_2024_s3"},
            },
        },
    },
    "AVESA": {
        "scientific": "Avena sativa L.",
        "common_name": "Oat",
        "type": "grass",
        "kill_method": "frost_kill",
        "climates": {
            "Csa": {
                "biomass_t_ha": {"value": 6.5, "min": 5.0, "max": 8.0, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "c_n_ratio": {"value": 38, "min": 25, "max": 50, "source": "FertiliCalc (UCO/CSIC)", "source_id": "fertilicalc_script_2024"},
                "gdd_to_termination": {"value": 1050, "min": 900, "max": 1200, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "frost_tolerance_c": {"value": -10, "min": -12, "max": -7, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
            },
            "BSk": {
                "biomass_t_ha": {"value": 5.5, "min": 3.5, "max": 7.5, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "c_n_ratio": {"value": 40, "min": 30, "max": 55, "source": "FertiliCalc (UCO/CSIC)", "source_id": "fertilicalc_script_2024"},
                "gdd_to_termination": {"value": 1200, "min": 1050, "max": 1350, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "frost_tolerance_c": {"value": -10, "min": -12, "max": -7, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
            },
            "Cfb": {
                "biomass_t_ha": {"value": 8.0, "min": 6.0, "max": 10.0, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "gdd_to_termination": {"value": 1300, "min": 1100, "max": 1450, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
            },
        },
    },
    "SECCE": {
        "scientific": "Secale cereale L.",
        "common_name": "Cereal rye",
        "type": "grass",
        "kill_method": "roller_crimper",
        "climates": {
            "Csa": {
                "biomass_t_ha": {"value": 7.5, "min": 5.5, "max": 9.5, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "c_n_ratio": {"value": 45, "min": 30, "max": 60, "source": "FertiliCalc (UCO/CSIC)", "source_id": "fertilicalc_script_2024"},
                "gdd_to_termination": {"value": 1100, "min": 950, "max": 1250, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "frost_tolerance_c": {"value": -25, "min": -30, "max": -20, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_content_pct": {"value": 1.0, "min": 0.7, "max": 1.3, "source": "FertiliCalc (UCO/CSIC)", "source_id": "fertilicalc_script_2024"},
            },
            "BSk": {
                "biomass_t_ha": {"value": 6.5, "min": 4.5, "max": 9.0, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "c_n_ratio": {"value": 50, "min": 35, "max": 70, "source": "FertiliCalc (UCO/CSIC)", "source_id": "fertilicalc_script_2024"},
                "gdd_to_termination": {"value": 1200, "min": 1050, "max": 1400, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "frost_tolerance_c": {"value": -25, "min": -30, "max": -20, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
            },
            "Cfb": {
                "biomass_t_ha": {"value": 9.5, "min": 7.5, "max": 11.5, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "c_n_ratio": {"value": 40, "min": 28, "max": 55, "source": "FertiliCalc (UCO/CSIC)", "source_id": "fertilicalc_script_2024"},
                "gdd_to_termination": {"value": 1350, "min": 1200, "max": 1500, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
            },
        },
    },
    "TRFIN": {
        "scientific": "Trifolium incarnatum L.",
        "common_name": "Crimson clover",
        "type": "legume",
        "kill_method": "roller_crimper",
        "climates": {
            "Csa": {
                "biomass_t_ha": {"value": 4.0, "min": 3.0, "max": 5.0, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "c_n_ratio": {"value": 15, "min": 12, "max": 18, "source": "FertiliCalc (UCO/CSIC)", "source_id": "fertilicalc_script_2024"},
                "gdd_to_termination": {"value": 1400, "min": 1250, "max": 1550, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "frost_tolerance_c": {"value": -8, "min": -12, "max": -4, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_fixation_kg_ha": {"value": 80, "min": 55, "max": 110, "source": "ESSD 2024 (Ludemann et al.)", "source_id": "essd_2024_s3"},
            },
            "BSk": {
                "biomass_t_ha": {"value": 2.8, "min": 2.0, "max": 3.5, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "gdd_to_termination": {"value": 1500, "min": 1300, "max": 1650, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_fixation_kg_ha": {"value": 70, "min": 50, "max": 90, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
            },
            "Cfb": {
                "biomass_t_ha": {"value": 5.0, "min": 3.5, "max": 6.5, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "c_n_ratio": {"value": 14, "min": 11, "max": 17, "source": "FertiliCalc (UCO/CSIC)", "source_id": "fertilicalc_script_2024"},
                "frost_tolerance_c": {"value": -10, "min": -15, "max": -6, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_fixation_kg_ha": {"value": 100, "min": 70, "max": 140, "source": "ESSD 2024 (Ludemann et al.)", "source_id": "essd_2024_s3"},
            },
        },
    },
}

PROTEIN_CROPS: dict[str, dict[str, Any]] = {
    "VICFX": {
        "scientific": "Vicia faba L.",
        "common_name": "Faba bean",
        "type": "legume",
        "protein_content_pct": 28,
        "harvest_index": 0.45,
        "eppo_search": "VICFX",
        "climates": {
            "Csa": {
                "biomass_t_ha": {"value": 6.5, "min": 4.5, "max": 8.5, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "c_n_ratio": {"value": 16, "min": 13, "max": 20, "source": "FertiliCalc (UCO/CSIC)", "source_id": "fertilicalc_script_2024"},
                "gdd_to_maturity": {"value": 1600, "min": 1400, "max": 1800, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "frost_tolerance_c": {"value": -10, "min": -15, "max": -5, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_fixation_kg_ha": {"value": 140, "min": 90, "max": 190, "source": "ESSD 2024 (Ludemann et al.)", "source_id": "essd_2024_s3"},
            },
            "BSk": {
                "biomass_t_ha": {"value": 5.5, "min": 3.5, "max": 7.5, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "c_n_ratio": {"value": 15, "min": 12, "max": 18, "source": "FertiliCalc (UCO/CSIC)", "source_id": "fertilicalc_script_2024"},
                "gdd_to_maturity": {"value": 1700, "min": 1500, "max": 1900, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "frost_tolerance_c": {"value": -12, "min": -15, "max": -8, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_fixation_kg_ha": {"value": 130, "min": 80, "max": 180, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
            },
            "Cfb": {
                "biomass_t_ha": {"value": 7.5, "min": 5.5, "max": 9.5, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "gdd_to_maturity": {"value": 1400, "min": 1200, "max": 1600, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_fixation_kg_ha": {"value": 170, "min": 120, "max": 220, "source": "ESSD 2024 (Ludemann et al.)", "source_id": "essd_2024_s3"},
            },
        },
    },
    "PIBAR": {
        "scientific": "Pisum sativum L.",
        "common_name": "Field pea",
        "type": "legume",
        "protein_content_pct": 24,
        "harvest_index": 0.40,
        "eppo_search": "PIBAR",
        "climates": {
            "Csa": {
                "biomass_t_ha": {"value": 5.0, "min": 3.5, "max": 6.5, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "c_n_ratio": {"value": 18, "min": 14, "max": 22, "source": "FertiliCalc (UCO/CSIC)", "source_id": "fertilicalc_script_2024"},
                "gdd_to_maturity": {"value": 1350, "min": 1200, "max": 1500, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "frost_tolerance_c": {"value": -10, "min": -14, "max": -6, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_fixation_kg_ha": {"value": 90, "min": 50, "max": 130, "source": "ESSD 2024 (Ludemann et al.)", "source_id": "essd_2024_s3"},
            },
            "BSk": {
                "biomass_t_ha": {"value": 3.8, "min": 2.5, "max": 5.5, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "gdd_to_maturity": {"value": 1450, "min": 1300, "max": 1650, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "frost_tolerance_c": {"value": -8, "min": -12, "max": -4, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_fixation_kg_ha": {"value": 85, "min": 50, "max": 120, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
            },
            "Cfb": {
                "biomass_t_ha": {"value": 6.0, "min": 4.5, "max": 7.5, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "gdd_to_maturity": {"value": 1550, "min": 1350, "max": 1700, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_fixation_kg_ha": {"value": 110, "min": 70, "max": 150, "source": "ESSD 2024 (Ludemann et al.)", "source_id": "essd_2024_s3"},
            },
        },
    },
    "CIEAR": {
        "scientific": "Cicer arietinum L.",
        "common_name": "Chickpea",
        "type": "legume",
        "protein_content_pct": 22,
        "harvest_index": 0.35,
        "eppo_search": "CIEAR",
        "climates": {
            "Csa": {
                "biomass_t_ha": {"value": 3.5, "min": 2.5, "max": 5.0, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "c_n_ratio": {"value": 18, "min": 15, "max": 22, "source": "FertiliCalc (UCO/CSIC)", "source_id": "fertilicalc_script_2024"},
                "gdd_to_maturity": {"value": 1600, "min": 1400, "max": 1800, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "frost_tolerance_c": {"value": -4, "min": -6, "max": -2, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_fixation_kg_ha": {"value": 55, "min": 30, "max": 80, "source": "ESSD 2024 (Ludemann et al.)", "source_id": "essd_2024_s3"},
            },
            "BSk": {
                "biomass_t_ha": {"value": 2.5, "min": 1.5, "max": 3.5, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "gdd_to_maturity": {"value": 1700, "min": 1500, "max": 1950, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "frost_tolerance_c": {"value": -4, "min": -6, "max": -2, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_fixation_kg_ha": {"value": 45, "min": 20, "max": 70, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
            },
            "Cfb": {
                "biomass_t_ha": {"value": 3.0, "min": 2.0, "max": 4.0, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "gdd_to_maturity": {"value": 1500, "min": 1300, "max": 1700, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
            },
        },
    },
    "LENCU": {
        "scientific": "Lens culinaris Medik.",
        "common_name": "Lentil",
        "type": "legume",
        "protein_content_pct": 26,
        "harvest_index": 0.35,
        "eppo_search": "LENCU",
        "climates": {
            "Csa": {
                "biomass_t_ha": {"value": 2.5, "min": 1.5, "max": 3.5, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "gdd_to_maturity": {"value": 1400, "min": 1200, "max": 1550, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_fixation_kg_ha": {"value": 45, "min": 25, "max": 70, "source": "ESSD 2024 (Ludemann et al.)", "source_id": "essd_2024_s3"},
            },
            "BSk": {
                "biomass_t_ha": {"value": 2.0, "min": 1.0, "max": 3.0, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "gdd_to_maturity": {"value": 1500, "min": 1300, "max": 1700, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_fixation_kg_ha": {"value": 40, "min": 20, "max": 60, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
            },
            "Cfb": {
                "biomass_t_ha": {"value": 2.5, "min": 1.5, "max": 3.5, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
            },
        },
    },
    "LTHSA": {
        "scientific": "Lathyrus sativus L.",
        "common_name": "Grass pea / Almorta",
        "type": "legume",
        "protein_content_pct": 28,
        "harvest_index": 0.30,
        "eppo_search": "LTHSA",
        "climates": {
            "Csa": {
                "biomass_t_ha": {"value": 3.0, "min": 2.0, "max": 4.0, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_fixation_kg_ha": {"value": 60, "min": 35, "max": 85, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
            },
        },
    },
    "GLXMA": {
        "scientific": "Glycine max (L.) Merr.",
        "common_name": "Soybean",
        "type": "legume",
        "protein_content_pct": 38,
        "harvest_index": 0.35,
        "eppo_search": "GLXMA",
        "climates": {
            "Csa": {
                "biomass_t_ha": {"value": 5.5, "min": 3.5, "max": 7.5, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "c_n_ratio": {"value": 20, "min": 15, "max": 25, "source": "FertiliCalc (UCO/CSIC)", "source_id": "fertilicalc_script_2024"},
                "gdd_to_maturity": {"value": 1800, "min": 1600, "max": 2100, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_fixation_kg_ha": {"value": 100, "min": 50, "max": 160, "source": "ESSD 2024 (Ludemann et al.)", "source_id": "essd_2024_s3"},
            },
            "Cfb": {
                "biomass_t_ha": {"value": 4.5, "min": 3.0, "max": 6.0, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "gdd_to_maturity": {"value": 1700, "min": 1500, "max": 1900, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_fixation_kg_ha": {"value": 120, "min": 60, "max": 180, "source": "ESSD 2024 (Ludemann et al.)", "source_id": "essd_2024_s3"},
            },
            "BSk": {
                "biomass_t_ha": {"value": 4.0, "min": 2.5, "max": 6.0, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "gdd_to_maturity": {"value": 1800, "min": 1600, "max": 2100, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_fixation_kg_ha": {"value": 100, "min": 50, "max": 160, "source": "ESSD 2024 (Ludemann et al.)", "source_id": "essd_2024_s3"},
            },
        },
    },
    "VICSA": {
        "scientific": "Vicia sativa L.",
        "common_name": "Common vetch (grain)",
        "type": "legume",
        "protein_content_pct": 28,
        "harvest_index": 0.40,
        "eppo_search": "VICSA",
        "climates": {
            "Csa": {
                "biomass_t_ha": {"value": 3.5, "min": 2.0, "max": 5.0, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "gdd_to_maturity": {"value": 1200, "min": 1050, "max": 1350, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_fixation_kg_ha": {"value": 90, "min": 60, "max": 130, "source": "ESSD 2024 (Ludemann et al.)", "source_id": "essd_2024_s3"},
            },
            "BSk": {
                "biomass_t_ha": {"value": 3.8, "min": 2.5, "max": 5.5, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "grain_yield_kg_ha": {"value": 1500, "min": 1000, "max": 2200, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "gdd_to_maturity": {"value": 1300, "min": 1150, "max": 1450, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_fixation_kg_ha": {"value": 80, "min": 60, "max": 110, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
            },
            "Cfb": {
                "biomass_t_ha": {"value": 5.0, "min": 3.5, "max": 7.0, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "gdd_to_maturity": {"value": 1400, "min": 1200, "max": 1550, "source": "MAPA Guía Fertilización 2010", "source_id": "mapa_guia_2010"},
                "n_fixation_kg_ha": {"value": 110, "min": 80, "max": 150, "source": "ESSD 2024 (Ludemann et al.)", "source_id": "essd_2024_s3"},
            },
        },
    },
}

DATA_SOURCE_MANAGEMENT: dict[str, str] = {
    "MAPA Guía Fertilización 2010": "conventional",
    "FertiliCalc (UCO/CSIC)": "conventional",
    "ESSD 2024 (Ludemann et al.)": "conventional",
    "Dryad (Ludemann CC0)": "conventional",
}

SOWING_WINDOWS: dict[str, dict[str, tuple[str, str]]] = {
    "Csa": {"cover_crop_autumn": ("10-15", "11-15"), "protein_crop_spring": ("04-01", "05-01")},
    "BSk": {"cover_crop_autumn": ("10-01", "10-20"), "protein_crop_spring": ("04-15", "05-15")},
    "Cfb": {"cover_crop_autumn": ("09-15", "10-15"), "protein_crop_spring": ("03-15", "04-15")},
    "Dfa": {"cover_crop_autumn": ("09-01", "09-20"), "protein_crop_spring": ("04-15", "05-15")},
    "Dfb": {"cover_crop_autumn": ("08-15", "09-10"), "protein_crop_spring": ("05-01", "05-20")},
    "BSh": {"cover_crop_autumn": ("10-15", "11-01"), "protein_crop_spring": ("03-15", "04-15")},
}


def lookup(species_eppo: str, climate_class: str, param: str | None = None) -> dict[str, Any] | float | None:
    """Look up a parameter for a species × climate combination."""
    for catalog in [COVER_CROPS, PROTEIN_CROPS]:
        if species_eppo in catalog:
            entry = catalog[species_eppo]
            climate_data = entry.get("climates", {}).get(climate_class)
            if climate_data is None:
                climate_data = entry.get("climates", {}).get("Csa")
                if climate_data is None:
                    continue
            if param:
                return climate_data.get(param)
            return {**climate_data, "type": entry.get("type"), "kill_method": entry.get("kill_method")}
    return None


def select_cover_crops(
    climate_class: str,
    management: str = "any",
    min_biomass_t_ha: float = 2.0,
    max_c_n_ratio: float | None = 20,
    frost_days: float = 0,
) -> list[dict[str, Any]]:
    """Select suitable cover crops for a climate × management context."""
    results = []
    for eppo, cc in COVER_CROPS.items():
        climate_data = cc.get("climates", {}).get(climate_class)
        if climate_data is None:
            continue

        biomass_entry = climate_data.get("biomass_t_ha", {})
        biomass = biomass_entry.get("value", 0) if isinstance(biomass_entry, dict) else biomass_entry
        cn_ratio_entry = climate_data.get("c_n_ratio", {})
        cn_ratio = cn_ratio_entry.get("value") if isinstance(cn_ratio_entry, dict) else cn_ratio_entry
        frost_entry = climate_data.get("frost_tolerance_c", {})
        frost_tol = frost_entry.get("value") if isinstance(frost_entry, dict) else frost_entry

        source = biomass_entry.get("source", "unknown") if isinstance(biomass_entry, dict) else "unknown"
        src_mgmt = DATA_SOURCE_MANAGEMENT.get(source, "unspecified")
        mgmt_note = ""
        original_biomass = biomass

        if management == "organic":
            if src_mgmt in ("conventional", "unspecified"):
                biomass *= ORGANIC_YIELD_FACTOR
                mgmt_note = f"↓{ORGANIC_YIELD_FACTOR:.0%} organic factor applied (source: {src_mgmt})"
            else:
                mgmt_note = f"source is {src_mgmt} — no adjustment needed"
        elif management == "conventional":
            if src_mgmt == "organic":
                continue
            mgmt_note = f"source is {src_mgmt}"
        else:
            mgmt_note = f"source is {src_mgmt}"

        suitable = True
        if biomass < min_biomass_t_ha:
            suitable = False
        if max_c_n_ratio is not None and cn_ratio is not None and cn_ratio > max_c_n_ratio:
            suitable = False
        if frost_days > 0 and frost_tol is not None and frost_tol > -5:
            suitable = False

        results.append({
            "eppo": eppo,
            "scientific": cc["scientific"],
            "common_name": cc["common_name"],
            "type": cc["type"],
            "kill_method": cc.get("kill_method", "roller_crimper"),
            "suitable": suitable,
            "target_biomass_t_ha": round(biomass, 1),
            "original_biomass_t_ha": round(original_biomass, 1),
            "c_n_ratio": cn_ratio,
            "management_source": src_mgmt,
            "management_note": mgmt_note,
            **{k: v for k, v in climate_data.items()},
        })

    results.sort(key=lambda r: (r["suitable"], r["target_biomass_t_ha"]), reverse=True)
    return results


def estimate_n_fixation(
    cover_eppo: str,
    protein_eppo: str,
    cover_biomass_t_ha: float,
    protein_yield_kg_ha: float | None = None,
    management: str = "any",
) -> dict[str, Any]:
    """Estimate N dynamics for a cover crop → protein crop sequence."""
    cover = COVER_CROPS.get(cover_eppo, {})
    protein = PROTEIN_CROPS.get(protein_eppo, {})

    n_content = 0.0
    n_param = lookup(cover_eppo, "Csa", "n_content_pct")
    if isinstance(n_param, dict):
        n_content = n_param.get("value", 0)
    elif isinstance(n_param, (int, float)):
        n_content = n_param

    n_cover_total = cover_biomass_t_ha * 1000 * (n_content / 100) if n_content > 0 else 0
    n_cover_available = n_cover_total * 0.5

    n_protein_fixed = None
    if cover.get("type") == "legume":
        n_fix_param = lookup(cover_eppo, "Csa", "n_fixation_kg_ha")
        if isinstance(n_fix_param, dict):
            n_protein_fixed = n_fix_param.get("value")
        elif isinstance(n_fix_param, (int, float)):
            n_protein_fixed = n_fix_param

    protein_kg_ha = None
    if protein_yield_kg_ha is not None and protein:
        if management == "organic":
            protein_yield_kg_ha *= ORGANIC_YIELD_FACTOR
        protein_kg_ha = protein_yield_kg_ha * protein.get("harvest_index", 0.35) * (protein.get("protein_content_pct", 25) / 100)

    return {
        "n_cover_total_kg_ha": round(n_cover_total, 1) if n_cover_total > 0 else None,
        "n_cover_available_kg_ha": round(n_cover_available, 1) if n_cover_available > 0 else None,
        "n_protein_fixed_kg_ha": round(n_protein_fixed, 1) if n_protein_fixed else None,
        "protein_kg_ha": round(protein_kg_ha) if protein_kg_ha is not None else None,
    }


TERMINATION_MONTH: dict[str, int] = {"Csa": 5, "BSk": 6, "Cfb": 6, "BSh": 4, "Dfa": 6, "Dfb": 6}


def estimate_dates(
    climate_class: str,
    cover_gdd: float,
    protein_gdd: float,
) -> dict[str, Any]:
    """Estimate dates for the cover crop to protein crop sequence."""
    windows = SOWING_WINDOWS.get(climate_class, SOWING_WINDOWS["Csa"])
    cover_start, _ = windows["cover_crop_autumn"]
    protein_start, _ = windows["protein_crop_spring"]
    term_month = TERMINATION_MONTH.get(climate_class, 5)

    spring_gdd_rates = {"BSk": 8, "BSh": 12, "Csa": 10, "Cfb": 5, "Dfa": 10, "Dfb": 8}
    gdd_day = spring_gdd_rates.get(climate_class, 8)

    year = 2026
    cover_sow = datetime(year, *map(int, cover_start.split("-")), tzinfo=timezone.utc)

    typical_gdd = 1200
    gdd_deviation = cover_gdd - typical_gdd
    day_offset = int(gdd_deviation / max(gdd_day, 3))
    termination = datetime(year + 1, term_month, 15, tzinfo=timezone.utc) + timedelta(days=day_offset)

    protein_sow = termination + timedelta(days=10)
    ref_protein_start = datetime(year + 1, *map(int, protein_start.split("-")), tzinfo=timezone.utc)
    protein_sow = max(protein_sow, ref_protein_start)

    harvest_days = protein_gdd / max(gdd_day, 3)
    harvest = protein_sow + timedelta(days=int(harvest_days))

    return {
        "cover_crop_sowing_date": cover_sow.strftime("%Y-%m-%d"),
        "termination_date": termination.strftime("%Y-%m-%d"),
        "protein_crop_sowing_date": protein_sow.strftime("%Y-%m-%d"),
        "protein_crop_harvest_date": harvest.strftime("%Y-%m-%d"),
        "gdd_per_day_spring": gdd_day,
        "gdd_detail": f"Cover: {cover_gdd} GDD (base 4°C). Protein: {protein_gdd} GDD (base 10°C). Spring rate: ~{gdd_day} GDD/day. Termination ~month {term_month} ±{abs(day_offset)}d.",
    }
