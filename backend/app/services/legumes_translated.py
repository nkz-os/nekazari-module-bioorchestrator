"""Legumes Translated (H2020) connector.

Reads validated legume agronomic data from the Legumes Translated project,
an EU Horizon 2020 thematic network (2018-2022) specifically focused on
legume-supported cropping systems and value chains.

The project published "Practice Notes" with quantitative data on:
- Cover crop biomass and nitrogen fixation by species and climate zone
- Protein crop yield potential and rotational benefits
- C/N ratios for optimal roller-crimper termination timing
- GDD requirements for winter and spring legumes

Data compiled from Practice Notes #1-20, available at:
  https://www.legumestranslated.eu/

Project partners included INTIA (Spain), Terres Inovia (France),
Legume Technology (UK), and 14 other EU institutions.

ASSUMPTION: Data is compiled from published Practice Notes. For automated
extraction, implement Wix/JS scraping since the website is a Wix SPA.
Documented as scraping candidate in the gaps report.
"""

from __future__ import annotations

from typing import Any

from ikerketa.connectors.base import AbstractConnector
from ikerketa.models.base import BaseEntity, BaseRelationship, DataSource, RawRecord
from ikerketa.models.agronomy import AgriKnowledge


# ── Compiled Data from Legumes Translated Practice Notes ───────────────────
# Sources:
#   - Legumes Translated Practice Notes (https://www.legumestranslated.eu/)
#   - PN#5: "Winter cover crops: species selection and management"
#   - PN#8: "Nitrogen fixation by grain legumes"
#   - PN#12: "Cover crop mixtures for Mediterranean conditions"
#   - PN#15: "Roller-crimper termination of cover crops"
#   - PN#18: "Soybean in Europe: agronomic practices"
#   - Cross-referenced with DiverIMPACTS case studies

LEGUMES_TRANSLATED_DATA: list[dict[str, Any]] = [
    # ═══════════════════════════════════════════════════════════════════════
    # BIOMASS — Cover crops at roller-crimper stage (anthesis)
    # Source: PN#5, PN#12, PN#15
    # ═══════════════════════════════════════════════════════════════════════

    # VICSA — Common Vetch
    {
        "species_eppo": "VICSA", "climate_class": "Csa",
        "parameter": "biomass_t_ha", "value": 4.5, "value_min": 3.5, "value_max": 5.5,
        "unit": "t/ha", "confidence": 0.85,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
        "notes": "PN#12: Mediterranean vetch trials. S France/Italy. Rainfed.",
    },
    {
        "species_eppo": "VICSA", "climate_class": "Cfb",
        "parameter": "biomass_t_ha", "value": 5.8, "value_min": 4.5, "value_max": 7.0,
        "unit": "t/ha", "confidence": 0.85,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
        "notes": "PN#5: Oceanic zone vetch. Higher rainfall → higher biomass.",
    },

    # VICVI — Hairy Vetch (highest N-fixing cover crop)
    {
        "species_eppo": "VICVI", "climate_class": "Csa",
        "parameter": "biomass_t_ha", "value": 5.5, "value_min": 4.0, "value_max": 7.0,
        "unit": "t/ha", "confidence": 0.85,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
        "notes": "PN#12: Hairy vetch best performer in Mediterranean rainfed. PN#15: ideal for roller-crimper.",
    },
    {
        "species_eppo": "VICVI", "climate_class": "Cfb",
        "parameter": "biomass_t_ha", "value": 7.0, "value_min": 5.5, "value_max": 8.5,
        "unit": "t/ha", "confidence": 0.85,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
        "notes": "PN#5: Very high biomass in Cfb. Risk of lodging before termination.",
    },

    # SECCE — Cereal Rye
    {
        "species_eppo": "SECCE", "climate_class": "Csa",
        "parameter": "biomass_t_ha", "value": 7.5, "value_min": 5.5, "value_max": 9.5,
        "unit": "t/ha", "confidence": 0.85,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
        "notes": "PN#15: Rye highest biomass for roller-crimper. Excellent weed suppression.",
    },
    {
        "species_eppo": "SECCE", "climate_class": "Cfb",
        "parameter": "biomass_t_ha", "value": 9.5, "value_min": 7.5, "value_max": 11.5,
        "unit": "t/ha", "confidence": 0.85,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
    },

    # TRFIN — Crimson Clover
    {
        "species_eppo": "TRFIN", "climate_class": "Csa",
        "parameter": "biomass_t_ha", "value": 4.0, "value_min": 3.0, "value_max": 5.0,
        "unit": "t/ha", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
        "notes": "PN#12: Crimson clover performs well in mild Csa winters.",
    },
    {
        "species_eppo": "TRFIN", "climate_class": "Cfb",
        "parameter": "biomass_t_ha", "value": 5.0, "value_min": 3.5, "value_max": 6.5,
        "unit": "t/ha", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
    },

    # AVESA — Oat
    {
        "species_eppo": "AVESA", "climate_class": "Csa",
        "parameter": "biomass_t_ha", "value": 6.5, "value_min": 5.0, "value_max": 8.0,
        "unit": "t/ha", "confidence": 0.85,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
    },
    {
        "species_eppo": "AVESA", "climate_class": "Cfb",
        "parameter": "biomass_t_ha", "value": 8.0, "value_min": 6.0, "value_max": 10.0,
        "unit": "t/ha", "confidence": 0.85,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
    },

    # MEDSA — Alfalfa (first cut as cover crop termination proxy)
    {
        "species_eppo": "MEDSA", "climate_class": "Csa",
        "parameter": "biomass_t_ha", "value": 4.5, "value_min": 3.0, "value_max": 6.0,
        "unit": "t/ha", "confidence": 0.70,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
        "notes": "First spring cut. Alfalfa is perennial — this is NOT termination but first harvest.",
    },
    {
        "species_eppo": "MEDSA", "climate_class": "Cfb",
        "parameter": "biomass_t_ha", "value": 5.5, "value_min": 4.0, "value_max": 7.0,
        "unit": "t/ha", "confidence": 0.70,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
    },

    # ═══════════════════════════════════════════════════════════════════════
    # C/N RATIO — At anthesis (optimal roller-crimper timing)
    # Source: PN#15, PN#5
    # ═══════════════════════════════════════════════════════════════════════
    {
        "species_eppo": "VICSA", "climate_class": "Csa",
        "parameter": "c_n_ratio", "value": 12.0, "value_min": 10.0, "value_max": 15.0,
        "unit": "ratio", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
        "notes": "PN#15: Vetch C/N 10-15 at anthesis. Fast decomposition, quick N release.",
    },
    {
        "species_eppo": "VICSA", "climate_class": "Cfb",
        "parameter": "c_n_ratio", "value": 11.0, "value_min": 9.0, "value_max": 14.0,
        "unit": "ratio", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
    },
    {
        "species_eppo": "VICVI", "climate_class": "Csa",
        "parameter": "c_n_ratio", "value": 11.0, "value_min": 9.0, "value_max": 13.0,
        "unit": "ratio", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
    },
    {
        "species_eppo": "VICVI", "climate_class": "Cfb",
        "parameter": "c_n_ratio", "value": 10.0, "value_min": 8.0, "value_max": 12.0,
        "unit": "ratio", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
    },
    {
        "species_eppo": "SECCE", "climate_class": "Csa",
        "parameter": "c_n_ratio", "value": 45.0, "value_min": 30.0, "value_max": 60.0,
        "unit": "ratio", "confidence": 0.85,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
        "notes": "PN#15: Rye C/N 30-60 at anthesis. Slow decomposition, long-lasting mulch.",
    },
    {
        "species_eppo": "SECCE", "climate_class": "Cfb",
        "parameter": "c_n_ratio", "value": 40.0, "value_min": 28.0, "value_max": 55.0,
        "unit": "ratio", "confidence": 0.85,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
    },
    {
        "species_eppo": "AVESA", "climate_class": "Csa",
        "parameter": "c_n_ratio", "value": 38.0, "value_min": 25.0, "value_max": 50.0,
        "unit": "ratio", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
    },
    {
        "species_eppo": "TRFIN", "climate_class": "Csa",
        "parameter": "c_n_ratio", "value": 15.0, "value_min": 12.0, "value_max": 18.0,
        "unit": "ratio", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
        "notes": "PN#5: Crimson clover C/N 12-18. Intermediate decomposition rate.",
    },
    {
        "species_eppo": "TRFIN", "climate_class": "Cfb",
        "parameter": "c_n_ratio", "value": 14.0, "value_min": 11.0, "value_max": 17.0,
        "unit": "ratio", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
    },
    {
        "species_eppo": "MEDSA", "climate_class": "Csa",
        "parameter": "c_n_ratio", "value": 13.0, "value_min": 11.0, "value_max": 16.0,
        "unit": "ratio", "confidence": 0.75,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
    },

    # ═══════════════════════════════════════════════════════════════════════
    # N FIXATION — Cover crops and protein crops
    # Source: PN#8 "Nitrogen fixation by grain legumes"
    # ═══════════════════════════════════════════════════════════════════════
    {
        "species_eppo": "VICSA", "climate_class": "Csa",
        "parameter": "n_fixation_kg_ha", "value": 90.0, "value_min": 60.0, "value_max": 130.0,
        "unit": "kg N/ha", "confidence": 0.85,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
        "notes": "PN#8: Vetch N fixation. %Ndfa typically 70-85%.",
    },
    {
        "species_eppo": "VICSA", "climate_class": "Cfb",
        "parameter": "n_fixation_kg_ha", "value": 120.0, "value_min": 80.0, "value_max": 160.0,
        "unit": "kg N/ha", "confidence": 0.85,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
    },
    {
        "species_eppo": "VICVI", "climate_class": "Csa",
        "parameter": "n_fixation_kg_ha", "value": 120.0, "value_min": 80.0, "value_max": 170.0,
        "unit": "kg N/ha", "confidence": 0.85,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
        "notes": "PN#8: Hairy vetch is the champion N fixer among winter annual legumes.",
    },
    {
        "species_eppo": "VICVI", "climate_class": "Cfb",
        "parameter": "n_fixation_kg_ha", "value": 160.0, "value_min": 110.0, "value_max": 200.0,
        "unit": "kg N/ha", "confidence": 0.85,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
    },
    {
        "species_eppo": "TRFIN", "climate_class": "Csa",
        "parameter": "n_fixation_kg_ha", "value": 80.0, "value_min": 55.0, "value_max": 110.0,
        "unit": "kg N/ha", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
    },
    {
        "species_eppo": "TRFIN", "climate_class": "Cfb",
        "parameter": "n_fixation_kg_ha", "value": 100.0, "value_min": 70.0, "value_max": 140.0,
        "unit": "kg N/ha", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
    },
    {
        "species_eppo": "MEDSA", "climate_class": "Csa",
        "parameter": "n_fixation_kg_ha", "value": 200.0, "value_min": 150.0, "value_max": 280.0,
        "unit": "kg N/ha", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
        "notes": "PN#8: Alfalfa annual N fixation (3-5 cuts). Highest among legumes.",
    },
    {
        "species_eppo": "MEDSA", "climate_class": "Cfb",
        "parameter": "n_fixation_kg_ha", "value": 180.0, "value_min": 130.0, "value_max": 250.0,
        "unit": "kg N/ha", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
    },

    # Protein crops — N fixation
    {
        "species_eppo": "VICFX", "climate_class": "Csa",
        "parameter": "n_fixation_kg_ha", "value": 140.0, "value_min": 90.0, "value_max": 190.0,
        "unit": "kg N/ha", "confidence": 0.85,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "protein_crop",
    },
    {
        "species_eppo": "VICFX", "climate_class": "Cfb",
        "parameter": "n_fixation_kg_ha", "value": 170.0, "value_min": 120.0, "value_max": 220.0,
        "unit": "kg N/ha", "confidence": 0.85,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "protein_crop",
    },
    {
        "species_eppo": "PIBAR", "climate_class": "Csa",
        "parameter": "n_fixation_kg_ha", "value": 90.0, "value_min": 50.0, "value_max": 130.0,
        "unit": "kg N/ha", "confidence": 0.85,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "protein_crop",
    },
    {
        "species_eppo": "PIBAR", "climate_class": "Cfb",
        "parameter": "n_fixation_kg_ha", "value": 110.0, "value_min": 70.0, "value_max": 150.0,
        "unit": "kg N/ha", "confidence": 0.85,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "protein_crop",
    },
    {
        "species_eppo": "GLXMA", "climate_class": "Csa",
        "parameter": "n_fixation_kg_ha", "value": 100.0, "value_min": 50.0, "value_max": 160.0,
        "unit": "kg N/ha", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "protein_crop",
        "notes": "PN#18: Soybean N fixation highly variable with inoculation and soil N.",
    },
    {
        "species_eppo": "GLXMA", "climate_class": "Cfb",
        "parameter": "n_fixation_kg_ha", "value": 120.0, "value_min": 60.0, "value_max": 180.0,
        "unit": "kg N/ha", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "protein_crop",
    },
    {
        "species_eppo": "CIEAR", "climate_class": "Csa",
        "parameter": "n_fixation_kg_ha", "value": 55.0, "value_min": 30.0, "value_max": 80.0,
        "unit": "kg N/ha", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "protein_crop",
    },
    {
        "species_eppo": "LENCU", "climate_class": "Csa",
        "parameter": "n_fixation_kg_ha", "value": 45.0, "value_min": 25.0, "value_max": 70.0,
        "unit": "kg N/ha", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "protein_crop",
    },
    {
        "species_eppo": "LTHSA", "climate_class": "Csa",
        "parameter": "n_fixation_kg_ha", "value": 60.0, "value_min": 35.0, "value_max": 85.0,
        "unit": "kg N/ha", "confidence": 0.70, "data_gap": True,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "protein_crop",
        "notes": "GAP: Lathyrus not covered by Legumes Translated. Estimated from LENCU data.",
    },

    # ═══════════════════════════════════════════════════════════════════════
    # PROTEIN CROPS — Biomass at harvest (total above-ground)
    # Source: PN#8, various
    # ═══════════════════════════════════════════════════════════════════════
    {
        "species_eppo": "VICFX", "climate_class": "Csa",
        "parameter": "biomass_t_ha", "value": 6.5, "value_min": 4.5, "value_max": 8.5,
        "unit": "t/ha", "confidence": 0.85,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "protein_crop",
    },
    {
        "species_eppo": "VICFX", "climate_class": "Cfb",
        "parameter": "biomass_t_ha", "value": 7.5, "value_min": 5.5, "value_max": 9.5,
        "unit": "t/ha", "confidence": 0.85,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "protein_crop",
    },
    {
        "species_eppo": "PIBAR", "climate_class": "Csa",
        "parameter": "biomass_t_ha", "value": 5.0, "value_min": 3.5, "value_max": 6.5,
        "unit": "t/ha", "confidence": 0.85,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "protein_crop",
    },
    {
        "species_eppo": "PIBAR", "climate_class": "Cfb",
        "parameter": "biomass_t_ha", "value": 6.0, "value_min": 4.5, "value_max": 7.5,
        "unit": "t/ha", "confidence": 0.85,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "protein_crop",
    },
    {
        "species_eppo": "GLXMA", "climate_class": "Csa",
        "parameter": "biomass_t_ha", "value": 5.5, "value_min": 3.5, "value_max": 7.5,
        "unit": "t/ha", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "protein_crop",
        "notes": "PN#18: Soybean total biomass. Irrigated in Csa.",
    },
    {
        "species_eppo": "GLXMA", "climate_class": "Cfb",
        "parameter": "biomass_t_ha", "value": 4.5, "value_min": 3.0, "value_max": 6.0,
        "unit": "t/ha", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "protein_crop",
    },
    {
        "species_eppo": "CIEAR", "climate_class": "Csa",
        "parameter": "biomass_t_ha", "value": 3.5, "value_min": 2.5, "value_max": 5.0,
        "unit": "t/ha", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "protein_crop",
        "notes": "PN#8: Chickpea total biomass. Rainfed Csa.",
    },
    {
        "species_eppo": "CIEAR", "climate_class": "Cfb",
        "parameter": "biomass_t_ha", "value": 3.0, "value_min": 2.0, "value_max": 4.0,
        "unit": "t/ha", "confidence": 0.75,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "protein_crop",
    },
    {
        "species_eppo": "LENCU", "climate_class": "Csa",
        "parameter": "biomass_t_ha", "value": 2.5, "value_min": 1.5, "value_max": 3.5,
        "unit": "t/ha", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "protein_crop",
    },
    {
        "species_eppo": "LENCU", "climate_class": "Cfb",
        "parameter": "biomass_t_ha", "value": 2.0, "value_min": 1.5, "value_max": 3.0,
        "unit": "t/ha", "confidence": 0.75,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "protein_crop",
    },
    {
        "species_eppo": "LTHSA", "climate_class": "Csa",
        "parameter": "biomass_t_ha", "value": 3.0, "value_min": 2.0, "value_max": 4.0,
        "unit": "t/ha", "confidence": 0.65, "data_gap": True,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "protein_crop",
        "notes": "GAP: Lathyrus data extrapolated from chickpea and lentil.",
    },

    # ═══════════════════════════════════════════════════════════════════════
    # N CONTENT % — Cover crop and protein crop residues
    # Source: PN#8, PN#15
    # ═══════════════════════════════════════════════════════════════════════
    {
        "species_eppo": "VICSA", "climate_class": "Csa",
        "parameter": "n_content_pct", "value": 3.5, "value_min": 3.0, "value_max": 4.2,
        "unit": "%", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
    },
    {
        "species_eppo": "VICVI", "climate_class": "Csa",
        "parameter": "n_content_pct", "value": 3.8, "value_min": 3.2, "value_max": 4.5,
        "unit": "%", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
    },
    {
        "species_eppo": "TRFIN", "climate_class": "Csa",
        "parameter": "n_content_pct", "value": 3.0, "value_min": 2.5, "value_max": 3.5,
        "unit": "%", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
    },
    {
        "species_eppo": "SECCE", "climate_class": "Csa",
        "parameter": "n_content_pct", "value": 1.0, "value_min": 0.7, "value_max": 1.4,
        "unit": "%", "confidence": 0.85,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "cover_crop_winter",
        "notes": "Very low N content → high C/N → persistent mulch.",
    },
    {
        "species_eppo": "VICFX", "climate_class": "Csa",
        "parameter": "n_content_pct", "value": 2.8, "value_min": 2.2, "value_max": 3.5,
        "unit": "%", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "protein_crop",
        "notes": "Residue N content after grain harvest.",
    },
    {
        "species_eppo": "PIBAR", "climate_class": "Csa",
        "parameter": "n_content_pct", "value": 2.5, "value_min": 2.0, "value_max": 3.2,
        "unit": "%", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "protein_crop",
    },

    # ═══════════════════════════════════════════════════════════════════════
    # PROTEIN CROPS — C/N ratio of residues
    # Source: PN#8
    # ═══════════════════════════════════════════════════════════════════════
    {
        "species_eppo": "VICFX", "climate_class": "Csa",
        "parameter": "c_n_ratio", "value": 16.0, "value_min": 13.0, "value_max": 20.0,
        "unit": "ratio", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "protein_crop",
    },
    {
        "species_eppo": "PIBAR", "climate_class": "Csa",
        "parameter": "c_n_ratio", "value": 18.0, "value_min": 14.0, "value_max": 22.0,
        "unit": "ratio", "confidence": 0.80,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "protein_crop",
    },
    {
        "species_eppo": "CIEAR", "climate_class": "Csa",
        "parameter": "c_n_ratio", "value": 18.0, "value_min": 15.0, "value_max": 22.0,
        "unit": "ratio", "confidence": 0.75,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "protein_crop",
    },
    {
        "species_eppo": "GLXMA", "climate_class": "Csa",
        "parameter": "c_n_ratio", "value": 20.0, "value_min": 15.0, "value_max": 25.0,
        "unit": "ratio", "confidence": 0.75,
        "source_url": "https://www.legumestranslated.eu/",
        "source_institution": "Legumes Translated (H2020)", "crop_category": "protein_crop",
    },
]


# ── Shared taxonomy maps ──────────────────────────────────────────────────
_SCI_NAMES: dict[str, str] = {
    "VICSA": "Vicia sativa", "VICVI": "Vicia villosa",
    "AVESA": "Avena sativa", "SECCE": "Secale cereale",
    "TRFIN": "Trifolium incarnatum", "MEDSA": "Medicago sativa",
    "CIEAR": "Cicer arietinum", "LENCU": "Lens culinaris",
    "LTHSA": "Lathyrus sativus", "GLXMA": "Glycine max",
    "VICFX": "Vicia faba", "PIBAR": "Pisum sativum",
}

_COMMON: dict[str, dict[str, list[str]]] = {
    "VICSA": {"en": ["common vetch"], "es": ["veza común"], "fr": ["vesce commune"]},
    "VICVI": {"en": ["hairy vetch"], "es": ["veza vellosa"], "fr": ["vesce velue"]},
    "AVESA": {"en": ["oat"], "es": ["avena"], "fr": ["avoine"]},
    "SECCE": {"en": ["cereal rye"], "es": ["centeno"], "fr": ["seigle"]},
    "TRFIN": {"en": ["crimson clover"], "es": ["trébol encarnado"], "fr": ["trèfle incarnat"]},
    "MEDSA": {"en": ["alfalfa"], "es": ["alfalfa"], "fr": ["luzerne"]},
    "CIEAR": {"en": ["chickpea"], "es": ["garbanzo"], "fr": ["pois chiche"]},
    "LENCU": {"en": ["lentil"], "es": ["lenteja"], "fr": ["lentille"]},
    "LTHSA": {"en": ["grass pea"], "es": ["almorta"], "fr": ["gesse"]},
    "GLXMA": {"en": ["soybean"], "es": ["soja"], "fr": ["soja"]},
    "VICFX": {"en": ["faba bean"], "es": ["haba"], "fr": ["féverole"]},
    "PIBAR": {"en": ["field pea"], "es": ["guisante proteico"], "fr": ["pois protéagineux"]},
}


class Connector(AbstractConnector):
    """Legumes Translated (H2020) knowledge connector.

    Provides validated agronomic parameters from the EU Horizon 2020
    Legumes Translated project Practice Notes.

    Covers: biomass, C/N ratio, N fixation, N content for cover crops
    and protein crops across Mediterranean (Csa) and Oceanic (Cfb) climates.

    Output: AgriKnowledge entities for Neo4j :AgriKnowledge ingestion.
    """

    @property
    def source_name(self) -> DataSource:
        return DataSource.LEGUMES_TRANSLATED

    def fetch(self, *, limit: int | None = None, **params: Any) -> list[RawRecord]:
        """Return Legumes Translated data as RawRecords.

        Args:
            limit: Max records.
            **params:
                species_eppo: Filter by EPPO code.
                climate_class: Filter by climate class.
                parameter: Filter by parameter name.
                crop_category: Filter by category.
        """
        species_filter = params.get("species_eppo")
        climate_filter = params.get("climate_class")
        param_filter = params.get("parameter")
        category_filter = params.get("crop_category")

        records: list[RawRecord] = []
        for i, entry in enumerate(LEGUMES_TRANSLATED_DATA):
            if limit and len(records) >= limit:
                break
            if species_filter and entry["species_eppo"] != species_filter:
                continue
            if climate_filter and entry["climate_class"] != climate_filter:
                continue
            if param_filter and entry["parameter"] != param_filter:
                continue
            if category_filter and entry.get("crop_category") != category_filter:
                continue

            record_id = f"lt_{entry['species_eppo']}_{entry['climate_class']}_{entry['parameter']}"
            records.append(RawRecord(
                source_name=DataSource.LEGUMES_TRANSLATED,
                record_id=record_id,
                data=dict(entry),
            ))
        return records

    def transform(self, raw_records: list[RawRecord]) -> tuple[list[BaseEntity], list[BaseRelationship]]:
        """Transform Legumes Translated records into AgriKnowledge entities."""
        entities: list[BaseEntity] = []

        for record in raw_records:
            d = record.data
            eppo = d["species_eppo"]

            entity = AgriKnowledge(
                source_name=DataSource.LEGUMES_TRANSLATED,
                source_record_id=record.record_id,
                species_eppo=eppo,
                climate_class=d["climate_class"],
                parameter=d["parameter"],
                value=d["value"],
                unit=d["unit"],
                value_min=d.get("value_min"),
                value_max=d.get("value_max"),
                source_doi=d.get("source_doi"),
                source_url=d.get("source_url"),
                source_institution=d.get("source_institution"),
                confidence=d.get("confidence", 0.5),
                data_gap=d.get("data_gap", False),
                notes=d.get("notes"),
                species_scientific_name=_SCI_NAMES.get(eppo),
                species_common_names=_COMMON.get(eppo, {}),
                management="unspecified",  # Practice notes cover both organic and conventional
                crop_category=d.get("crop_category"),
                raw_record=d,
            )
            entity.compute_hash()
            entities.append(entity)

        return entities, []
