"""Cover crop reference data — European sources.

Replaces hardcoded USDA/SARE tables with data from:
- INTIA Navarra (low_input, BSk + Cfb)
- JRC MARS Bulletins (conventional, GDD + frost tolerance)
- Legumes Translated H2020 (unspecified, Csa + Cfb biomass/C/N/N fix)
- IFAPA Andalusia scraped trials (conventional + organic, Csa + BSh)
- ITACyL Castilla y León scraped trials (conventional, BSk)

Enrichment:
At module load, scraped data from IFAPA (nkz-ifapa-scraper) and
ITACyL (nkz-itacyl-scraper) JSON outputs is aggregated and used to
update PROTEIN_CROPS biomass/grain yield values where field data
exists. See get_trial_observations() and get_scraped_stats().

Data compiled 2026-06-02.

Usage:
    from app.services.cover_crops import lookup, select_cover_crops

    params = lookup("VICSA", "Csa", "organic")
    # → {"biomass_t_ha": 4.5, "c_n_ratio": 12, ...}

    candidates = select_cover_crops(climate_class="Csa", management="organic")
    # → list of suitable cover crops ranked by biomass

    # Access raw trial observations:
    trials = get_trial_observations(species_eppo="CIEAR", climate_class="Csa")
    stats = get_scraped_stats("CIEAR", "Csa")
"""

from __future__ import annotations

from typing import Any

# ── Base temperature for cool-season cover crops (Trudgill et al. 2005) ──────
BASE_TEMP_COOL_C = 4.0
BASE_TEMP_WARM_C = 10.0

# ── Organic yield reduction factor ──────────────────────────────────────────
# Meta-analysis: Seufert et al. 2012, Ponisio et al. 2015
# Organic systems yield 75-80% of conventional for grain legumes
ORGANIC_YIELD_FACTOR = 0.80

# ── Complete European data matrix ──────────────────────────────────────────
# Format per species:
#   "EPPO": {
#       "scientific": str,
#       "common_name": str,
#       "type": "legume" | "grass",
#       "climates": {
#           "Csa": {param: {"value": float, "min": float, "max": float, "source": str}},
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
                "biomass_t_ha": {"value": 4.5, "min": 3.5, "max": 5.5, "source": "Legumes Translated PN#12"},
                "c_n_ratio": {"value": 12, "min": 10, "max": 15, "source": "Legumes Translated PN#12"},
                "gdd_to_termination": {"value": 1150, "min": 1000, "max": 1300, "source": "Legumes Translated PN#12"},
                "frost_tolerance_c": {"value": -8, "min": -10, "max": -6, "source": "Legumes Translated PN#12"},
                "n_fixation_kg_ha": {"value": 90, "min": 60, "max": 130, "source": "Legumes Translated PN#12"},
                "n_content_pct": {"value": 3.5, "min": 3.0, "max": 4.2, "source": "Legumes Translated PN#12"},
            },
            "BSk": {
                "biomass_t_ha": {"value": 3.8, "min": 2.8, "max": 4.5, "source": "INTIA Navarra (2019-2023)"},
                "c_n_ratio": {"value": 12, "min": 10, "max": 14, "source": "INTIA Navarra (2019-2023)"},
                "gdd_to_termination": {"value": 1250, "min": 1100, "max": 1400, "source": "INTIA Navarra (2019-2023)"},
                "frost_tolerance_c": {"value": -8, "min": -10, "max": -6, "source": "INTIA Navarra (2019-2023)"},
                "n_fixation_kg_ha": {"value": 80, "min": 60, "max": 110, "source": "INTIA Navarra (2019-2023)"},
                "n_content_pct": {"value": 3.5, "min": 3.0, "max": 4.0, "source": "INTIA Navarra (2019-2023)"},
            },
            "Cfb": {
                "biomass_t_ha": {"value": 5.8, "min": 4.5, "max": 7.0, "source": "Legumes Translated PN#5 + INTIA"},
                "c_n_ratio": {"value": 11, "min": 9, "max": 14, "source": "Legumes Translated PN#5 + INTIA"},
                "gdd_to_termination": {"value": 1350, "min": 1200, "max": 1500, "source": "Legumes Translated PN#5 + INTIA"},
                "frost_tolerance_c": {"value": -8, "min": -10, "max": -6, "source": "Estimated from Csa/BSk"},
                "n_fixation_kg_ha": {"value": 120, "min": 80, "max": 160, "source": "Legumes Translated PN#5"},
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
                "biomass_t_ha": {"value": 5.5, "min": 4.0, "max": 7.0, "source": "Legumes Translated PN#12,15"},
                "c_n_ratio": {"value": 11, "min": 9, "max": 13, "source": "Legumes Translated PN#12,15"},
                "gdd_to_termination": {"value": 1300, "min": 1150, "max": 1450, "source": "Legumes Translated PN#12,15"},
                "frost_tolerance_c": {"value": -15, "min": -20, "max": -10, "source": "Legumes Translated PN#12,15"},
                "n_fixation_kg_ha": {"value": 120, "min": 80, "max": 170, "source": "Legumes Translated PN#12,15"},
                "n_content_pct": {"value": 3.8, "min": 3.2, "max": 4.5, "source": "Legumes Translated PN#12,15"},
            },
            "BSk": {
                "biomass_t_ha": {"value": 4.2, "min": 3.0, "max": 5.5, "source": "INTIA + JRC MARS"},
                "c_n_ratio": {"value": 11, "min": 9, "max": 13, "source": "INTIA + JRC MARS"},
                "gdd_to_termination": {"value": 1400, "min": 1200, "max": 1550, "source": "INTIA + JRC MARS"},
                "frost_tolerance_c": {"value": -18, "min": -22, "max": -12, "source": "INTIA + JRC MARS"},
                "n_fixation_kg_ha": {"value": 105, "min": 80, "max": 140, "source": "INTIA + JRC MARS"},
            },
            "Cfb": {
                "biomass_t_ha": {"value": 7.0, "min": 5.5, "max": 8.5, "source": "Legumes Translated PN#5"},
                "c_n_ratio": {"value": 10, "min": 8, "max": 12, "source": "Legumes Translated PN#5"},
                "gdd_to_termination": {"value": 1500, "min": 1350, "max": 1650, "source": "Legumes Translated PN#5"},
                "frost_tolerance_c": {"value": -18, "min": -22, "max": -14, "source": "Legumes Translated PN#5"},
                "n_fixation_kg_ha": {"value": 160, "min": 110, "max": 200, "source": "Legumes Translated PN#5"},
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
                "biomass_t_ha": {"value": 6.5, "min": 5.0, "max": 8.0, "source": "Legumes Translated + INTIA"},
                "c_n_ratio": {"value": 38, "min": 25, "max": 50, "source": "Legumes Translated + INTIA"},
                "gdd_to_termination": {"value": 1050, "min": 900, "max": 1200, "source": "Legumes Translated + INTIA"},
                "frost_tolerance_c": {"value": -10, "min": -12, "max": -7, "source": "Legumes Translated + INTIA"},
            },
            "BSk": {
                "biomass_t_ha": {"value": 5.5, "min": 3.5, "max": 7.5, "source": "INTIA Navarra"},
                "c_n_ratio": {"value": 40, "min": 30, "max": 55, "source": "INTIA Navarra"},
                "gdd_to_termination": {"value": 1200, "min": 1050, "max": 1350, "source": "INTIA Navarra"},
                "frost_tolerance_c": {"value": -10, "min": -12, "max": -7, "source": "INTIA Navarra"},
            },
            "Cfb": {
                "biomass_t_ha": {"value": 8.0, "min": 6.0, "max": 10.0, "source": "Legumes Translated"},
                "gdd_to_termination": {"value": 1300, "min": 1100, "max": 1450, "source": "Legumes Translated"},
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
                "biomass_t_ha": {"value": 7.5, "min": 5.5, "max": 9.5, "source": "Legumes Translated PN#15"},
                "c_n_ratio": {"value": 45, "min": 30, "max": 60, "source": "Legumes Translated PN#15"},
                "gdd_to_termination": {"value": 1100, "min": 950, "max": 1250, "source": "Legumes Translated PN#15"},
                "frost_tolerance_c": {"value": -25, "min": -30, "max": -20, "source": "Legumes Translated PN#15"},
                "n_content_pct": {"value": 1.0, "min": 0.7, "max": 1.3, "source": "Derived from C:N ratio"},
            },
            "BSk": {
                "biomass_t_ha": {"value": 6.5, "min": 4.5, "max": 9.0, "source": "INTIA Navarra"},
                "c_n_ratio": {"value": 50, "min": 35, "max": 70, "source": "INTIA Navarra"},
                "gdd_to_termination": {"value": 1200, "min": 1050, "max": 1400, "source": "INTIA Navarra"},
                "frost_tolerance_c": {"value": -25, "min": -30, "max": -20, "source": "INTIA Navarra"},
            },
            "Cfb": {
                "biomass_t_ha": {"value": 9.5, "min": 7.5, "max": 11.5, "source": "Legumes Translated"},
                "c_n_ratio": {"value": 40, "min": 28, "max": 55, "source": "Legumes Translated"},
                "gdd_to_termination": {"value": 1350, "min": 1200, "max": 1500, "source": "Legumes Translated"},
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
                "biomass_t_ha": {"value": 4.0, "min": 3.0, "max": 5.0, "source": "Legumes Translated PN#12"},
                "c_n_ratio": {"value": 15, "min": 12, "max": 18, "source": "Legumes Translated PN#12"},
                "gdd_to_termination": {"value": 1400, "min": 1250, "max": 1550, "source": "Legumes Translated PN#12"},
                "frost_tolerance_c": {"value": -8, "min": -12, "max": -4, "source": "Legumes Translated PN#12"},
                "n_fixation_kg_ha": {"value": 80, "min": 55, "max": 110, "source": "Legumes Translated PN#12"},
            },
            "BSk": {
                "biomass_t_ha": {"value": 2.8, "min": 2.0, "max": 3.5, "source": "INTIA Navarra"},
                "gdd_to_termination": {"value": 1500, "min": 1300, "max": 1650, "source": "Estimated from Csa"},
                "n_fixation_kg_ha": {"value": 70, "min": 50, "max": 90, "source": "INTIA Navarra"},
            },
            "Cfb": {
                "biomass_t_ha": {"value": 5.0, "min": 3.5, "max": 6.5, "source": "Legumes Translated PN#5"},
                "c_n_ratio": {"value": 14, "min": 11, "max": 17, "source": "Legumes Translated PN#5"},
                "frost_tolerance_c": {"value": -10, "min": -15, "max": -6, "source": "Legumes Translated PN#5"},
                "n_fixation_kg_ha": {"value": 100, "min": 70, "max": 140, "source": "Legumes Translated PN#5"},
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
                "biomass_t_ha": {"value": 6.5, "min": 4.5, "max": 8.5, "source": "Legumes Translated PN#8"},
                "c_n_ratio": {"value": 16, "min": 13, "max": 20, "source": "Legumes Translated PN#8"},
                "gdd_to_maturity": {"value": 1600, "min": 1400, "max": 1800, "source": "Legumes Translated PN#8"},
                "frost_tolerance_c": {"value": -10, "min": -15, "max": -5, "source": "Legumes Translated PN#8"},
                "n_fixation_kg_ha": {"value": 140, "min": 90, "max": 190, "source": "Legumes Translated PN#8"},
            },
            "BSk": {
                "biomass_t_ha": {"value": 5.5, "min": 3.5, "max": 7.5, "source": "INTIA Navarra"},
                "c_n_ratio": {"value": 15, "min": 12, "max": 18, "source": "INTIA Navarra"},
                "gdd_to_maturity": {"value": 1700, "min": 1500, "max": 1900, "source": "INTIA Navarra"},
                "frost_tolerance_c": {"value": -12, "min": -15, "max": -8, "source": "INTIA Navarra"},
                "n_fixation_kg_ha": {"value": 130, "min": 80, "max": 180, "source": "INTIA Navarra"},
            },
            "Cfb": {
                "biomass_t_ha": {"value": 7.5, "min": 5.5, "max": 9.5, "source": "Legumes Translated"},
                "gdd_to_maturity": {"value": 1400, "min": 1200, "max": 1600, "source": "Legumes Translated"},
                "n_fixation_kg_ha": {"value": 170, "min": 120, "max": 220, "source": "Legumes Translated"},
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
                "biomass_t_ha": {"value": 5.0, "min": 3.5, "max": 6.5, "source": "Legumes Translated PN#8"},
                "c_n_ratio": {"value": 18, "min": 14, "max": 22, "source": "Legumes Translated PN#8"},
                "gdd_to_maturity": {"value": 1350, "min": 1200, "max": 1500, "source": "Legumes Translated PN#8"},
                "frost_tolerance_c": {"value": -10, "min": -14, "max": -6, "source": "Legumes Translated PN#8"},
                "n_fixation_kg_ha": {"value": 90, "min": 50, "max": 130, "source": "Legumes Translated PN#8"},
            },
            "BSk": {
                "biomass_t_ha": {"value": 3.8, "min": 2.5, "max": 5.5, "source": "INTIA Navarra"},
                "gdd_to_maturity": {"value": 1450, "min": 1300, "max": 1650, "source": "INTIA Navarra"},
                "frost_tolerance_c": {"value": -8, "min": -12, "max": -4, "source": "Estimated"},
                "n_fixation_kg_ha": {"value": 85, "min": 50, "max": 120, "source": "INTIA Navarra"},
            },
            "Cfb": {
                "biomass_t_ha": {"value": 6.0, "min": 4.5, "max": 7.5, "source": "Legumes Translated"},
                "gdd_to_maturity": {"value": 1550, "min": 1350, "max": 1700, "source": "Legumes Translated"},
                "n_fixation_kg_ha": {"value": 110, "min": 70, "max": 150, "source": "Legumes Translated"},
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
                "biomass_t_ha": {"value": 3.5, "min": 2.5, "max": 5.0, "source": "Legumes Translated PN#8"},
                "c_n_ratio": {"value": 18, "min": 15, "max": 22, "source": "Legumes Translated PN#8"},
                "gdd_to_maturity": {"value": 1600, "min": 1400, "max": 1800, "source": "Legumes Translated PN#8"},
                "frost_tolerance_c": {"value": -4, "min": -6, "max": -2, "source": "Legumes Translated PN#8"},
                "n_fixation_kg_ha": {"value": 55, "min": 30, "max": 80, "source": "Legumes Translated PN#8"},
            },
            "BSk": {
                "biomass_t_ha": {"value": 2.5, "min": 1.5, "max": 3.5, "source": "INTIA Navarra"},
                "gdd_to_maturity": {"value": 1700, "min": 1500, "max": 1950, "source": "INTIA Navarra"},
                "frost_tolerance_c": {"value": -4, "min": -6, "max": -2, "source": "INTIA Navarra"},
                "n_fixation_kg_ha": {"value": 45, "min": 20, "max": 70, "source": "INTIA Navarra"},
            },
            "Cfb": {
                "biomass_t_ha": {"value": 3.0, "min": 2.0, "max": 4.0, "source": "Legumes Translated (estimated)"},
                "gdd_to_maturity": {"value": 1500, "min": 1300, "max": 1700, "source": "Extrapolated"},
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
                "biomass_t_ha": {"value": 2.5, "min": 1.5, "max": 3.5, "source": "Legumes Translated"},
                "gdd_to_maturity": {"value": 1400, "min": 1200, "max": 1550, "source": "Extrapolated from pea"},
                "n_fixation_kg_ha": {"value": 45, "min": 25, "max": 70, "source": "Legumes Translated"},
            },
            "BSk": {
                "biomass_t_ha": {"value": 2.0, "min": 1.0, "max": 3.0, "source": "INTIA Navarra"},
                "gdd_to_maturity": {"value": 1500, "min": 1300, "max": 1700, "source": "Extrapolated from pea"},
                "n_fixation_kg_ha": {"value": 40, "min": 20, "max": 60, "source": "INTIA Navarra"},
            },
            "Cfb": {
                "biomass_t_ha": {"value": 2.5, "min": 1.5, "max": 3.5, "source": "INTIA (estimated)"},
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
        "data_gap": True,
        "data_gap_note": "Neglected crop — no systematic European trial data. Values extrapolated from chickpea/lentil.",
        "climates": {
            "Csa": {
                "biomass_t_ha": {"value": 3.0, "min": 2.0, "max": 4.0, "source": "Extrapolated from CIEAR/LENCU"},
                "n_fixation_kg_ha": {"value": 60, "min": 35, "max": 85, "source": "Extrapolated from CIEAR/LENCU"},
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
                "biomass_t_ha": {"value": 5.5, "min": 3.5, "max": 7.5, "source": "Legumes Translated PN#18"},
                "c_n_ratio": {"value": 20, "min": 15, "max": 25, "source": "Legumes Translated PN#18"},
                "gdd_to_maturity": {"value": 1800, "min": 1600, "max": 2100, "source": "Legumes Translated PN#18"},
                "n_fixation_kg_ha": {"value": 100, "min": 50, "max": 160, "source": "Legumes Translated PN#18"},
            },
            "Cfb": {
                "biomass_t_ha": {"value": 4.5, "min": 3.0, "max": 6.0, "source": "Legumes Translated PN#18"},
                "gdd_to_maturity": {"value": 1700, "min": 1500, "max": 1900, "source": "Legumes Translated PN#18"},
                "n_fixation_kg_ha": {"value": 120, "min": 60, "max": 180, "source": "Legumes Translated PN#18"},
            },
            "BSk": {
                "biomass_t_ha": {"value": 4.0, "min": 2.5, "max": 6.0, "source": "ITACyL field trials (n=15)"},
                "gdd_to_maturity": {"value": 1800, "min": 1600, "max": 2100, "source": "ITACyL field trials"},
                "n_fixation_kg_ha": {"value": 100, "min": 50, "max": 160, "source": "Legumes Translated PN#18"},
                "irrigation_note": "Requires irrigation in BSk. ITACyL trials (2004-2005) achieved 2.1-6.0 t/ha under irrigation in Castilla y León.",
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
                "biomass_t_ha": {"value": 3.5, "min": 2.0, "max": 5.0, "source": "Legumes Translated + IFAPA (estimated)"},
                "gdd_to_maturity": {"value": 1200, "min": 1050, "max": 1350, "source": "Estimated from Vicia faba"},
                "n_fixation_kg_ha": {"value": 90, "min": 60, "max": 130, "source": "Legumes Translated PN#12"},
            },
            "BSk": {
                "biomass_t_ha": {"value": 4.8, "min": 2.5, "max": 7.0, "source": "ITACyL field trials (n=21)"},
                "grain_yield_kg_ha": {"value": 1921, "min": 1030, "max": 3700, "source": "ITACyL field trials (n=21)"},
                "gdd_to_maturity": {"value": 1300, "min": 1150, "max": 1450, "source": "ITACyL field trials"},
                "n_fixation_kg_ha": {"value": 80, "min": 60, "max": 110, "source": "INTIA Navarra"},
            },
            "Cfb": {
                "biomass_t_ha": {"value": 5.0, "min": 3.5, "max": 7.0, "source": "Legumes Translated"},
                "gdd_to_maturity": {"value": 1400, "min": 1200, "max": 1550, "source": "Estimated"},
                "n_fixation_kg_ha": {"value": 110, "min": 80, "max": 150, "source": "Legumes Translated"},
            },
        },
    },
}

# ── Management regime tag per data source ────────────────────────────────────
# Used to adjust biomass expectations based on farming system.
DATA_SOURCE_MANAGEMENT: dict[str, str] = {
    "Legumes Translated": "unspecified",
    "INTIA Navarra": "low_input",
    "INTIA + JRC MARS": "low_input",
    "JRC MARS": "conventional",
    "Legumes Translated PN#12": "unspecified",
    "Legumes Translated PN#12,15": "unspecified",
    "Legumes Translated PN#5": "unspecified",
    "Legumes Translated PN#5 + INTIA": "low_input",
    "Legumes Translated PN#8": "unspecified",
    "Legumes Translated PN#15": "unspecified",
    "Legumes Translated PN#18": "unspecified",
    "Legumes Translated + INTIA": "low_input",
    "Legumes Translated (estimated)": "unspecified",
    "Estimated from Csa/BSk": "unspecified",
    "Estimated from Csa": "unspecified",
    "Extrapolated from pea": "unspecified",
    "Extrapolated from CIEAR/LENCU": "unspecified",
    "INTIA (estimated)": "low_input",
    "INTIA Navarra (2019-2023)": "low_input",
    "Estimated": "unspecified",
    "Derived from C:N ratio": "unspecified",
    "Extrapolated": "unspecified",
}
SOWING_WINDOWS: dict[str, dict[str, tuple[str, str]]] = {
    "Csa": {"cover_crop_autumn": ("10-15", "11-15"), "protein_crop_spring": ("04-01", "05-01")},
    "BSk": {"cover_crop_autumn": ("10-01", "10-20"), "protein_crop_spring": ("04-15", "05-15")},
    "Cfb": {"cover_crop_autumn": ("09-15", "10-15"), "protein_crop_spring": ("03-15", "04-15")},
    "Dfa": {"cover_crop_autumn": ("09-01", "09-20"), "protein_crop_spring": ("04-15", "05-15")},
    "Dfb": {"cover_crop_autumn": ("08-15", "09-10"), "protein_crop_spring": ("05-01", "05-20")},
    "BSh": {"cover_crop_autumn": ("10-15", "11-01"), "protein_crop_spring": ("03-15", "04-15")},
}


# ═══════════════════════════════════════════════════════════════════════════
#  Scraped trial data enrichment — IFAPA (Andalusia) + ITACyL (Castilla y León)
# ═══════════════════════════════════════════════════════════════════════════

import json as _json
from pathlib import Path as _Path
from collections import defaultdict as _defaultdict
from statistics import mean as _mean, stdev as _stdev

# Paths to scraped data JSON files (updated by IFAPA/ITACyL scrapers)
_SCRAPED_DATA_PATHS = [
    _Path("/home/g/Documents/nekazari/nkz-ifapa-scraper/data/output/ifapa_extracted.json"),
    _Path("/home/g/Documents/nekazari/nkz-itacyl-scraper/data/output/itacyl_extracted.json"),
]

# Straw yield threshold: values above this are likely straw, not grain.
# Crop-specific because lentil straw (3-8 t/ha) overlaps with grain (0.5-2.5 t/ha).
_MAX_GRAIN_YIELD_KG_HA: dict[str, float] = {
    "CIEAR": 8000,   # chickpea grain up to 5 t/ha
    "PIBAR": 8000,   # pea grain up to 6 t/ha
    "LENCU": 2800,   # lentil grain max ~2.5 t/ha, straw starts ~3 t/ha
    "VICFX": 8000,   # faba bean grain up to 6 t/ha
    "LTHSA": 4000,   # grass pea grain up to 3 t/ha
    "GLXMA": 8000,   # soybean
    "VICSA": 8000,   # vetch
    "VICVI": 8000,   # hairy vetch
}
_DEFAULT_MAX_GRAIN = 8000

# Harvest indices for grain → total biomass conversion
# grain_yield / harvest_index = total_above_ground_biomass
_HARVEST_INDICES = {
    "CIEAR": 0.35,  # chickpea
    "PIBAR": 0.40,  # field pea
    "LENCU": 0.35,  # lentil
    "VICFX": 0.45,  # faba bean
    "LTHSA": 0.30,  # grass pea
    "GLXMA": 0.35,  # soybean
    "VICSA": 0.40,  # common vetch
    "VICVI": 0.40,  # hairy vetch
}
# Minimum trials required to override hardcoded values
_MIN_TRIALS_FOR_OVERRIDE = 3

# Scraped trial cache: (eppo, climate, management) → stats dict
_scraped_stats: dict[tuple[str, str, str], dict[str, Any]] = {}
_scraped_raw: list[dict[str, Any]] = []


def _load_scraped_data() -> tuple[dict, list]:
    """Load all scraped JSON files and compute per-(species, climate, mgmt) stats.

    Returns (stats_dict, raw_observations).
    """
    all_obs = []
    for path in _SCRAPED_DATA_PATHS:
        if path.exists():
            try:
                with open(path) as f:
                    all_obs.extend(_json.load(f))
            except Exception:
                pass

    # Group observations by (eppo, climate, management)
    groups: dict[tuple[str, str, str], list[float]] = _defaultdict(list)
    for o in all_obs:
        yld = o.get("yield_kg_ha")
        if not yld or yld <= 0:
            continue
        eppo = o.get("species_eppo", "")
        if not eppo or len(eppo) != 5:
            continue
        climate = o.get("climate_class", "")
        mgmt = o.get("management", "conventional")

        # Skip cultivation guide reference data (not measured trials)
        notes = o.get("notes", "")
        source_pdf = o.get("source_pdf", "")
        if "guía" in notes.lower() or "guia" in source_pdf.lower():
            continue

        # Filter: exclude straw yields and greenhouse data
        max_grain = _MAX_GRAIN_YIELD_KG_HA.get(eppo, _DEFAULT_MAX_GRAIN)
        if yld > max_grain:
            # Straw or greenhouse data — skip for grain yield enrichment
            continue

        key = (eppo, climate, mgmt)
        groups[key].append(yld)

    # Compute statistics per group
    stats: dict[tuple[str, str, str], dict[str, Any]] = {}
    for key, yields in groups.items():
        if len(yields) < 1:
            continue
        eppo, climate, mgmt = key
        hi = _HARVEST_INDICES.get(eppo, 0.35)
        grain_mean = _mean(yields)
        grain_min = min(yields)
        grain_max = max(yields)
        # Convert grain yield to total biomass using harvest index
        biomass_mean = grain_mean / (hi * 1000)
        biomass_min = grain_min / (hi * 1000)
        biomass_max = grain_max / (hi * 1000)
        stats[key] = {
            "grain_yield_kg_ha": round(grain_mean),
            "grain_min_kg_ha": round(grain_min),
            "grain_max_kg_ha": round(grain_max),
            "biomass_t_ha": round(biomass_mean, 1),
            "biomass_min_t_ha": round(biomass_min, 1),
            "biomass_max_t_ha": round(biomass_max, 1),
            "n_trials": len(yields),
            "harvest_index": hi,
        }
    return stats, all_obs


def _apply_scraped_enrichment() -> None:
    """Enrich PROTEIN_CROPS and COVER_CROPS with fresh scraped trial data.

    Called at module load. Updates hardcoded values with measured data
    from IFAPA (Andalusia) and ITACyL (Castilla y León) field trials.

    Strategy:
    - For each (eppo, climate), pick the management with the MOST trials
    - Only override if n_trials >= _MIN_TRIALS_FOR_OVERRIDE
    - Exception: if current value is estimated/extrapolated, override with any data
    - Clear data_gap flag when we get real measurements
    """
    global _scraped_stats, _scraped_raw
    stats, raw = _load_scraped_data()
    _scraped_stats = stats
    _scraped_raw = raw

    # Group by (eppo, climate) and pick best management
    best_per_climate: dict[tuple[str, str], dict] = {}
    for (eppo, climate, mgmt), s in stats.items():
        key = (eppo, climate)
        if key not in best_per_climate or s["n_trials"] > best_per_climate[key]["n_trials"]:
            best_per_climate[key] = {**s, "management": mgmt}

    for (eppo, climate), s in best_per_climate.items():
        mgmt = s["management"]
        n = s["n_trials"]

        # ── Enrich PROTEIN_CROPS ──
        if eppo in PROTEIN_CROPS:
            entry = PROTEIN_CROPS[eppo]
            if climate not in entry.setdefault("climates", {}):
                entry["climates"][climate] = {}
            climate_data = entry["climates"][climate]

            current = climate_data.get("biomass_t_ha", {})
            current_source = current.get("source", "") if isinstance(current, dict) else ""
            is_estimated = any(kw in current_source.lower() for kw in (
                "estimated", "extrapolated", "derived"
            ))
            has_data_gap = entry.get("data_gap")

            should_override = n >= _MIN_TRIALS_FOR_OVERRIDE or is_estimated

            if should_override:
                source_label = f"IFAPA/ITACyL field trials (n={n}, {mgmt})"
                climate_data["biomass_t_ha"] = {
                    "value": s["biomass_t_ha"],
                    "min": s["biomass_min_t_ha"],
                    "max": s["biomass_max_t_ha"],
                    "source": source_label,
                }
                climate_data["grain_yield_kg_ha"] = {
                    "value": s["grain_yield_kg_ha"],
                    "min": s["grain_min_kg_ha"],
                    "max": s["grain_max_kg_ha"],
                    "source": source_label,
                }
                # Clear data_gap flag only if we have enough real trial data
                if has_data_gap and n >= _MIN_TRIALS_FOR_OVERRIDE:
                    entry.pop("data_gap", None)
                    entry.pop("data_gap_note", None)

        # ── Enrich COVER_CROPS where applicable ──
        if eppo in COVER_CROPS:
            entry = COVER_CROPS[eppo]
            if climate not in entry.setdefault("climates", {}):
                entry["climates"][climate] = {}
            climate_data = entry["climates"][climate]
            current = climate_data.get("biomass_t_ha", {})
            current_source = current.get("source", "") if isinstance(current, dict) else ""
            is_estimated = any(kw in current_source.lower() for kw in (
                "estimated", "extrapolated", "derived"
            ))

            if n >= _MIN_TRIALS_FOR_OVERRIDE or is_estimated:
                source_label = f"IFAPA/ITACyL field trials (n={n}, {mgmt})"
                climate_data["biomass_t_ha"] = {
                    "value": s["biomass_t_ha"],
                    "min": s["biomass_min_t_ha"],
                    "max": s["biomass_max_t_ha"],
                    "source": source_label,
                }


def get_trial_observations(
    species_eppo: str | None = None,
    climate_class: str | None = None,
    management: str | None = None,
) -> list[dict[str, Any]]:
    """Get raw trial observations from scraped data (IFAPA, ITACyL).

    Args:
        species_eppo: Filter by EPPO code (e.g., 'CIEAR'). None = all.
        climate_class: Filter by Köppen climate (e.g., 'BSk'). None = all.
        management: Filter by management (e.g., 'organic'). None = all.

    Returns:
        List of observation dicts with species, variety, location, yield, etc.
    """
    if not _scraped_raw:
        _apply_scraped_enrichment()

    results = []
    for o in _scraped_raw:
        if species_eppo and o.get("species_eppo") != species_eppo:
            continue
        if climate_class and o.get("climate_class") != climate_class:
            continue
        if management and o.get("management") != management:
            continue
        results.append(o)
    return results


def get_scraped_stats(
    species_eppo: str | None = None,
    climate_class: str | None = None,
    management: str | None = None,
) -> dict[str, Any] | None:
    """Get aggregated statistics from scraped trial data.

    Returns:
        Dict with grain_yield_kg_ha, biomass_t_ha, n_trials, etc.
        None if no data available for the given filters.
    """
    if not _scraped_stats:
        _apply_scraped_enrichment()

    if species_eppo and climate_class and management:
        return _scraped_stats.get((species_eppo, climate_class, management))

    # Return best match: try exact, then any management
    if species_eppo and climate_class:
        for mgmt in [management, "conventional", "low_input", "organic", "unspecified"]:
            if mgmt is None:
                continue
            key = (species_eppo, climate_class, mgmt)
            if key in _scraped_stats:
                return _scraped_stats[key]
    return None


# ── Initialize enrichment at module load ─────────────────────────────────
_apply_scraped_enrichment()


def lookup(species_eppo: str, climate_class: str, param: str | None = None) -> dict[str, Any] | float | None:
    """Look up a parameter for a species × climate combination.

    Args:
        species_eppo: EPPO code (e.g. 'VICSA', 'VICFX')
        climate_class: Köppen climate (e.g. 'Csa', 'BSk')
        param: Optional specific parameter name. If None, returns full dict.

    Returns:
        Parameter dict with value/min/max/source, or None if not found.
    """
    for catalog in [COVER_CROPS, PROTEIN_CROPS]:
        if species_eppo in catalog:
            entry = catalog[species_eppo]
            climate_data = entry.get("climates", {}).get(climate_class)
            if climate_data is None:
                climate_data = entry.get("climates", {}).get("Csa")  # fallback
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
    """Select suitable cover crops for a climate × management context.

    Args:
        climate_class: Köppen climate class
        management: 'organic', 'conventional', 'any'
        min_biomass_t_ha: Minimum biomass threshold
        max_c_n_ratio: Max C/N (lower = faster N release). None = no filter.
        frost_days: Expected frost days (for frost tolerance screening)

    Returns:
        List of cover crop dicts ranked by biomass (descending).
        Each dict includes: id, scientific, common_name, type, kill_method,
        suitable (bool), all parameters, target_biomass_t_ha.
    """
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

        # ── Management-aware biomass adjustment ────────────────────────
        source = biomass_entry.get("source", "unknown") if isinstance(biomass_entry, dict) else "unknown"
        src_mgmt = DATA_SOURCE_MANAGEMENT.get(source, "unspecified")
        mgmt_note = ""
        original_biomass = biomass

        if management == "organic":
            # Organic systems yield ~80% of conventional (Seufert et al. 2012, Ponisio et al. 2015)
            # Apply only to data from conventional or unspecified sources
            if src_mgmt in ("conventional", "unspecified"):
                biomass *= ORGANIC_YIELD_FACTOR
                mgmt_note = f"↓{ORGANIC_YIELD_FACTOR:.0%} organic factor applied (source: {src_mgmt})"
            else:
                mgmt_note = f"source is {src_mgmt} — no adjustment needed"
        elif management == "conventional":
            # Exclude organic-only data (optimistic for conventional context)
            if src_mgmt == "organic":
                mgmt_note = "excluded: organic-only data not suitable for conventional context"
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
    """Estimate N dynamics for a cover crop → protein crop sequence.

    Returns:
        Dict with n_cover_total_kg_ha, n_cover_available_kg_ha,
        n_protein_fixed_kg_ha, protein_kg_ha
    """
    cover = COVER_CROPS.get(cover_eppo, {})
    protein = PROTEIN_CROPS.get(protein_eppo, {})

    n_content = 0.0
    n_param = lookup(cover_eppo, "Csa", "n_content_pct")  # n_content is climate-independent
    if isinstance(n_param, dict):
        n_content = n_param.get("value", 0)
    elif isinstance(n_param, (int, float)):
        n_content = n_param

    # Total N in cover crop biomass
    n_cover_total = cover_biomass_t_ha * 1000 * (n_content / 100) if n_content > 0 else 0

    # ~50% mineralization in first season
    n_cover_available = n_cover_total * 0.5

    # N fixed by cover crop (if legume)
    n_protein_fixed = None
    if cover.get("type") == "legume":
        n_fix_param = lookup(cover_eppo, "Csa", "n_fixation_kg_ha")
        if isinstance(n_fix_param, dict):
            n_protein_fixed = n_fix_param.get("value")
        elif isinstance(n_fix_param, (int, float)):
            n_protein_fixed = n_fix_param

    # Expected protein output
    protein_kg_ha = None
    if protein_yield_kg_ha is not None and protein:
        # Adjust for organic management
        if management == "organic":
            protein_yield_kg_ha *= ORGANIC_YIELD_FACTOR
        protein_kg_ha = protein_yield_kg_ha * protein.get("harvest_index", 0.35) * (protein.get("protein_content_pct", 25) / 100)

    return {
        "n_cover_total_kg_ha": round(n_cover_total, 1) if n_cover_total > 0 else None,
        "n_cover_available_kg_ha": round(n_cover_available, 1) if n_cover_available > 0 else None,
        "n_protein_fixed_kg_ha": round(n_protein_fixed, 1) if n_protein_fixed else None,
        "protein_kg_ha": round(protein_kg_ha) if protein_kg_ha is not None else None,
    }


# Typical termination months by climate (mid-point of roller-crimper window)
TERMINATION_MONTH: dict[str, int] = {"Csa": 5, "BSk": 6, "Cfb": 6, "BSh": 4, "Dfa": 6, "Dfb": 6}


def estimate_dates(
    climate_class: str,
    cover_gdd: float,
    protein_gdd: float,
) -> dict[str, Any]:
    """Estimate dates for the cover crop to protein crop sequence.

    Uses typical sowing windows and termination months by climate zone
    adjusted by GDD requirements. Cover crops are autumn-sown and
    terminated the following spring.
    """
    from datetime import datetime, timedelta

    windows = SOWING_WINDOWS.get(climate_class, SOWING_WINDOWS["Csa"])
    cover_start, _ = windows["cover_crop_autumn"]
    protein_start, _ = windows["protein_crop_spring"]
    term_month = TERMINATION_MONTH.get(climate_class, 5)

    spring_gdd_rates = {"BSk": 8, "BSh": 12, "Csa": 10, "Cfb": 5, "Dfa": 10, "Dfb": 8}
    gdd_day = spring_gdd_rates.get(climate_class, 8)

    year = 2026
    cover_sow = datetime(year, *map(int, cover_start.split("-")))

    # Base termination: 15th of termination month in the FOLLOWING year
    # Adjust +/- days based on GDD deviation from typical (~1200 GDD)
    typical_gdd = 1200
    gdd_deviation = cover_gdd - typical_gdd
    day_offset = int(gdd_deviation / max(gdd_day, 3))
    termination = datetime(year + 1, term_month, 15) + timedelta(days=day_offset)

    # Protein crop 10 days after termination, not before spring window
    protein_sow = termination + timedelta(days=10)
    ref_protein_start = datetime(year + 1, *map(int, protein_start.split("-")))
    if protein_sow < ref_protein_start:
        protein_sow = ref_protein_start

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
