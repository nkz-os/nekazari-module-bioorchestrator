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
    "biogas_production": {
        "canonical": "biogas_production",
        "agrovoc": None,
        "description": "Produccion de biogas / Biogas production",
        "sources": {"BSL": "biogasertrag"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "biogas_yield": {
        "canonical": "biogas_yield",
        "agrovoc": None,
        "description": "Rendimiento de biogas / Biogas yield",
        "sources": {"BSL": "biogasausbeute"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "cold_sensitivity": {
        "canonical": "cold_sensitivity",
        "agrovoc": "http://aims.fao.org/aos/agrovoc/c_1765",
        "description": "Sensibilidad al frio / Cold sensitivity",
        "sources": {"BSL": "kaelteempfindlichkeit"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "cold_sensitivity_juvenile": {
        "canonical": "cold_sensitivity_juvenile",
        "agrovoc": None,
        "description": "Sensibilidad al frio juvenil / Juvenile cold sensitivity",
        "sources": {"BSL": ["kaelteempfindlichkeit_jugend", "kaelteempfindlichkeit_i_d_jugend"]},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "color_tone": {
        "canonical": "color_tone",
        "agrovoc": None,
        "description": "Tono de color / Color tone",
        "sources": {"BSL": "farbton"},
        "scale": "categorical",
        "higherIs": None,
        "domain": None,
    },
    "cooking_potential": {
        "canonical": "cooking_potential",
        "agrovoc": None,
        "description": "Potencial de coccion / Cooking potential",
        "sources": {"BSL": "kochpotential"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "crude_protein_content": {
        "canonical": "crude_protein_content",
        "agrovoc": None,
        "description": "Contenido de proteina bruta / Crude protein content",
        "sources": {"BSL": "rohproteingehalt"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "crude_protein_yield": {
        "canonical": "crude_protein_yield",
        "agrovoc": None,
        "description": "Rendimiento de proteina bruta / Crude protein yield",
        "sources": {"BSL": "rohproteinertrag"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "dark_spots_susceptibility": {
        "canonical": "dark_spots_susceptibility",
        "agrovoc": None,
        "description": "Sensibilidad a manchas oscuras / Dark spots susceptibility",
        "sources": {"BSL": "neigung_zu_dunkelfleckigkeit"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "development_before_winter": {
        "canonical": "development_before_winter",
        "agrovoc": None,
        "description": "Desarrollo antes del invierno / Pre-winter development",
        "sources": {"BSL": "entwicklung_vor_winter"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "digestibility": {
        "canonical": "digestibility",
        "agrovoc": None,
        "description": "Digestibilidad / Digestibility",
        "sources": {"BSL": "verdaulichkeit"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "dough_elasticity": {
        "canonical": "dough_elasticity",
        "agrovoc": None,
        "description": "Elasticidad de la masa / Dough elasticity",
        "sources": {"BSL": "elastizitaet_des_teiges"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "dough_surface": {
        "canonical": "dough_surface",
        "agrovoc": None,
        "description": "Superficie de la masa / Dough surface quality",
        "sources": {"BSL": "oberflaechenbeschaffenheit_des_teiges"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "dry_matter_yield": {
        "canonical": "dry_matter_yield",
        "agrovoc": None,
        "description": "Rendimiento de materia seca / Dry matter yield",
        "sources": {"BSL": "trockenmasseertrag"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "dry_substance_at_harvest": {
        "canonical": "dry_substance_at_harvest",
        "agrovoc": None,
        "description": "Sustancia seca en cosecha / Dry substance at harvest",
        "sources": {"BSL": "trockensubstanzgehalt_bei_ernte"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "ear_breakage": {
        "canonical": "ear_breakage",
        "agrovoc": None,
        "description": "Rotura de espiga / Ear breakage",
        "sources": {"BSL": "aehrenknicken"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "earliness_of_flowering": {
        "canonical": "earliness_of_flowering",
        "agrovoc": None,
        "description": "Precocidad de floracion / Earliness of flowering",
        "sources": {"AHDB": "earliness_of_flowering"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "earliness_of_maturity": {
        "canonical": "earliness_of_maturity",
        "agrovoc": None,
        "description": "Precocidad de madurez / Earliness of maturity",
        "sources": {"AHDB": "earliness_of_maturity"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "early_growth": {
        "canonical": "early_growth",
        "agrovoc": None,
        "description": "Crecimiento temprano / Early growth",
        "sources": {"BSL": "fruehwuchs"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "early_vigor": {
        "canonical": "early_vigor",
        "agrovoc": None,
        "description": "Vigor temprano / Early vigor",
        "sources": {"BSL": "massebildung_in_der_jugend"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "ears_per_m2": {
        "canonical": "ears_per_m2",
        "agrovoc": None,
        "description": "Espigas por m2 / Ears per m2",
        "sources": {"BSL": "bestandesdichte_aehren_m2"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "falling_number": {
        "canonical": "falling_number",
        "agrovoc": None,
        "description": "Indice de caida / Falling number",
        "sources": {"BSL": "fallzahl"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "falling_number_stability": {
        "canonical": "falling_number_stability",
        "agrovoc": None,
        "description": "Estabilidad del indice de caida / Falling number stability",
        "sources": {"BSL": "fallzahlstabilitaet"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "flour_yield_t550": {
        "canonical": "flour_yield_t550",
        "agrovoc": None,
        "description": "Rendimiento harinero T550 / Flour yield T550",
        "sources": {"BSL": "mehlausbeute_t_550"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "flowering_begin": {
        "canonical": "flowering_begin",
        "agrovoc": None,
        "description": "Inicio de floracion / Flowering begin",
        "sources": {"BSL": "bluehbeginn"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "flowering_duration": {
        "canonical": "flowering_duration",
        "agrovoc": None,
        "description": "Duracion de floracion / Flowering duration",
        "sources": {"BSL": "bluehdauer"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "flowering_time_female": {
        "canonical": "flowering_time_female",
        "agrovoc": "http://aims.fao.org/aos/agrovoc/c_15954",
        "description": "Floracion femenina / Female flowering time",
        "sources": {"BSL": "zeitpunkt_weibliche_bluete"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "fresh_matter_total": {
        "canonical": "fresh_matter_total",
        "agrovoc": None,
        "description": "Materia verde total / Total fresh matter",
        "sources": {"BSL": "gesamtgruenmasse"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "frost_sensitivity_juvenile": {
        "canonical": "frost_sensitivity_juvenile",
        "agrovoc": None,
        "description": "Sensibilidad a heladas juvenil / Juvenile frost sensitivity",
        "sources": {"BSL": "kaelteempfindlichkeit_i_d_jugend"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "full_barley_portion": {
        "canonical": "full_barley_portion",
        "agrovoc": None,
        "description": "Porcion de cebada completa / Full barley portion",
        "sources": {"BSL": "vollgersteanteil"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "glucosinolate_content": {
        "canonical": "glucosinolate_content",
        "agrovoc": None,
        "description": "Contenido de glucosinolatos / Glucosinolate content",
        "sources": {"BSL": "glucosinolatgehalt"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "grain_color": {
        "canonical": "grain_color",
        "agrovoc": None,
        "description": "Color de espelta / Spelt color",
        "sources": {"BSL": "spelzenfarbe"},
        "scale": "categorical",
        "higherIs": None,
        "domain": None,
    },
    "grain_filling": {
        "canonical": "grain_filling",
        "agrovoc": None,
        "description": "Llenado de grano / Grain filling",
        "sources": {"BSL": "kornausbildung"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "grain_kernel_color": {
        "canonical": "grain_kernel_color",
        "agrovoc": None,
        "description": "Color de grano / Grain kernel color",
        "sources": {"BSL": "kornfarbe"},
        "scale": "categorical",
        "higherIs": None,
        "domain": None,
    },
    "grain_maturity_number": {
        "canonical": "grain_maturity_number",
        "agrovoc": None,
        "description": "Numero de madurez de grano / Grain maturity code",
        "sources": {"BSL": "koernerreifezahl"},
        "scale": "categorical",
        "higherIs": None,
        "domain": None,
    },
    "grain_vitreousness": {
        "canonical": "grain_vitreousness",
        "agrovoc": None,
        "description": "Vitrosidad del grano / Grain vitreousness",
        "sources": {"BSL": "glasigkeit"},
        "scale": "categorical",
        "higherIs": None,
        "domain": None,
    },
    "grain_yield": {
        "canonical": "grain_yield",
        "agrovoc": None,
        "description": "Rendimiento de grano / Grain yield",
        "sources": {"BSL": "kornertrag"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "heading_date": {
        "canonical": "heading_date",
        "agrovoc": None,
        "description": "Fecha de espigado / Heading date",
        "sources": {"BSL": "aehrenschieben"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "hectoliter_weight": {
        "canonical": "hectoliter_weight",
        "agrovoc": None,
        "description": "Peso hectolitrico / Hectoliter weight",
        "sources": {"BSL": "hektolitergewicht"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "kernel_dirt_content": {
        "canonical": "kernel_dirt_content",
        "agrovoc": None,
        "description": "Impurezas en grano / Kernel dirt content",
        "sources": {"BSL": "kornbesatz"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "kernel_retention": {
        "canonical": "kernel_retention",
        "agrovoc": None,
        "description": "Retencion de grano / Kernel retention",
        "sources": {"BSL": "kornplatzfestigkeit"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "kernel_shape": {
        "canonical": "kernel_shape",
        "agrovoc": None,
        "description": "Forma de grano / Kernel shape",
        "sources": {"BSL": "kornform"},
        "scale": "categorical",
        "higherIs": None,
        "domain": None,
    },
    "kernels_per_ear": {
        "canonical": "kernels_per_ear",
        "agrovoc": None,
        "description": "Granos por espiga / Kernels per ear",
        "sources": {"BSL": "kornzahl_aehre"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "kernels_per_panicle": {
        "canonical": "kernels_per_panicle",
        "agrovoc": None,
        "description": "Granos por panicula / Kernels per panicle",
        "sources": {"BSL": "kornzahl_rispe"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "leaf_length": {
        "canonical": "leaf_length",
        "agrovoc": None,
        "description": "Longitud de hoja / Leaf length",
        "sources": {"BSL": "blattlaenge"},
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
    "line_hybrid_type": {
        "canonical": "line_hybrid_type",
        "agrovoc": None,
        "description": "Tipo linea/hibrido / Line or hybrid type",
        "sources": {"BSL": "linie_hybride"},
        "scale": "categorical",
        "higherIs": None,
        "domain": None,
    },
    "lodging_incidence": {
        "canonical": "lodging_incidence",
        "agrovoc": None,
        "description": "Encamado / Lodging incidence",
        "sources": {"BSL": "lager"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "lodging_resistance": {
        "canonical": "lodging_resistance",
        "agrovoc": None,
        "description": "Resistencia al encamado / Lodging resistance",
        "sources": {"AHDB": "lodging_resistance"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "lodging_susceptibility": {
        "canonical": "lodging_susceptibility",
        "agrovoc": None,
        "description": "Propension al encamado / Lodging tendency",
        "sources": {"BSL": "neigung_zu_lager"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "loose_smut_grains": {
        "canonical": "loose_smut_grains",
        "agrovoc": None,
        "description": "Granos no descascarillados / Unhulled grains",
        "sources": {"BSL": "anteil_nicht_entspelzter_koerner"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "marketable_portion": {
        "canonical": "marketable_portion",
        "agrovoc": None,
        "description": "Porcion comercializable / Marketable portion",
        "sources": {"BSL": "marktwareanteil"},
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
    "maturity_number": {
        "canonical": "maturity_number",
        "agrovoc": None,
        "description": "Numero de madurez / Maturity number",
        "sources": {"BSL": "reifezahl"},
        "scale": "categorical",
        "higherIs": None,
        "domain": None,
    },
    "maturity_rating": {
        "canonical": "maturity_rating",
        "agrovoc": None,
        "description": "Madurez / Maturity rating",
        "sources": {"BSL": "reife"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "meal_grittiness": {
        "canonical": "meal_grittiness",
        "agrovoc": None,
        "description": "Grumosidad de harina / Meal grittiness",
        "sources": {"BSL": "griffigkeit"},
        "scale": "categorical",
        "higherIs": None,
        "domain": None,
    },
    "mineral_value_number": {
        "canonical": "mineral_value_number",
        "agrovoc": None,
        "description": "Indice de valor mineral / Mineral value number",
        "sources": {"BSL": "mineralstoffwertzahl"},
        "scale": "categorical",
        "higherIs": None,
        "domain": None,
    },
    "oil_content": {
        "canonical": "oil_content",
        "agrovoc": None,
        "description": "Contenido de aceite / Oil content",
        "sources": {"BSL": "oelgehalt"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "oil_yield": {
        "canonical": "oil_yield",
        "agrovoc": None,
        "description": "Rendimiento de aceite / Oil yield",
        "sources": {"BSL": "oelertrag"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "panicle_emergence": {
        "canonical": "panicle_emergence",
        "agrovoc": None,
        "description": "Emergencia de panicula / Panicle emergence",
        "sources": {"BSL": "rispenschieben"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "panicle_heading_time": {
        "canonical": "panicle_heading_time",
        "agrovoc": None,
        "description": "Fecha de panicula / Panicle heading time",
        "sources": {"BSL": "zeitpunkt_rispenschieben"},
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
    "plant_height_cm": {
        "canonical": "plant_height_cm",
        "agrovoc": None,
        "description": "Altura de planta cm / Plant height cm",
        "sources": {"BSL": "pflanzenlaenge_cm"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "pod_shatter_resistance": {
        "canonical": "pod_shatter_resistance",
        "agrovoc": None,
        "description": "Resistencia a dehiscencia / Pod shatter resistance",
        "sources": {"AHDB": "pod_shatter_resistance"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "protein_content": {
        "canonical": "protein_content",
        "agrovoc": None,
        "description": "Contenido de proteina / Protein content",
        "sources": {"BSL": "eiweissgehalt"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "ripening": {
        "canonical": "ripening",
        "agrovoc": None,
        "description": "Maduracion / Ripening",
        "sources": {"AHDB": "ripening"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "sedimentation_value": {
        "canonical": "sedimentation_value",
        "agrovoc": None,
        "description": "Valor de sedimentacion / Sedimentation value",
        "sources": {"BSL": "sedimentationswert"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "silage_maturity_number": {
        "canonical": "silage_maturity_number",
        "agrovoc": None,
        "description": "Numero de madurez para ensilado / Silage maturity code",
        "sources": {"BSL": "siloreifezahl"},
        "scale": "categorical",
        "higherIs": None,
        "domain": None,
    },
    "silage_maturity_rating": {
        "canonical": "silage_maturity_rating",
        "agrovoc": None,
        "description": "Madurez para ensilado / Silage maturity rating",
        "sources": {"BSL": "siloreifezahl"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "soil_cover_degree": {
        "canonical": "soil_cover_degree",
        "agrovoc": None,
        "description": "Grado de cobertura del suelo / Soil cover degree",
        "sources": {"BSL": "bodendeckungsgrad"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "soil_cover_pct": {
        "canonical": "soil_cover_pct",
        "agrovoc": None,
        "description": "Cobertura del suelo % / Soil cover pct",
        "sources": {"BSL": "bodendeckungsgrad_pct"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "sorting_2_0mm": {
        "canonical": "sorting_2_0mm",
        "agrovoc": None,
        "description": "Clasificacion >2.0mm / Sorting >2.0mm",
        "sources": {"BSL": "sortierung_2_0_mm"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "sorting_2_5mm": {
        "canonical": "sorting_2_5mm",
        "agrovoc": None,
        "description": "Clasificacion >2.5mm / Sorting >2.5mm",
        "sources": {"BSL": "sortierung_2_5_mm"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "sorting_2_8mm": {
        "canonical": "sorting_2_8mm",
        "agrovoc": None,
        "description": "Clasificacion >2.8mm / Sorting >2.8mm",
        "sources": {"BSL": "sortierung_2_8_mm"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "spelt_hull_content": {
        "canonical": "spelt_hull_content",
        "agrovoc": None,
        "description": "Contenido de cascarilla / Spelt hull content",
        "sources": {"BSL": "spelzenanteil"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "spring_development": {
        "canonical": "spring_development",
        "agrovoc": None,
        "description": "Desarrollo primaveral / Spring development",
        "sources": {"BSL": "fruehjahrsentwicklung"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "sprouting_resistance": {
        "canonical": "sprouting_resistance",
        "agrovoc": None,
        "description": "Resistencia al brotado / Sprouting resistance",
        "sources": {"AHDB": "sprouting_resistance"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "stand_density": {
        "canonical": "stand_density",
        "agrovoc": None,
        "description": "Densidad de poblacion / Stand density",
        "sources": {"BSL": "bestandesdichte"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "starch_content": {
        "canonical": "starch_content",
        "agrovoc": None,
        "description": "Contenido de almidon / Starch content",
        "sources": {"BSL": "staerkegehalt"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "stem_breakage": {
        "canonical": "stem_breakage",
        "agrovoc": None,
        "description": "Rotura de tallo / Stem breakage",
        "sources": {"BSL": "halmknicken"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "stem_stiffness": {
        "canonical": "stem_stiffness",
        "agrovoc": None,
        "description": "Rigidez de tallo / Stem stiffness",
        "sources": {"AHDB": "stem_stiffness"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "straw_height_cm": {
        "canonical": "straw_height_cm",
        "agrovoc": None,
        "description": "Altura de paja cm / Straw height cm",
        "sources": {"AHDB": "straw_height_cm"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "straw_ripening_delay": {
        "canonical": "straw_ripening_delay",
        "agrovoc": None,
        "description": "Retraso de madurez de paja / Straw ripening delay",
        "sources": {"BSL": "reifeverzoegerung_des_strohs"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "tannin_content": {
        "canonical": "tannin_content",
        "agrovoc": None,
        "description": "Contenido de taninos / Tannin content",
        "sources": {"BSL": "tanningehalt"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "thousand_kernel_mass": {
        "canonical": "thousand_kernel_mass",
        "agrovoc": None,
        "description": "Peso de mil granos / Thousand kernel mass",
        "sources": {"BSL": "tausendkornmasse"},
        "scale": "1-9_bsl",
        "higherIs": "better",
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
    "tkm_grams": {
        "canonical": "tkm_grams",
        "agrovoc": None,
        "description": "Peso de mil granos g / TKM grams",
        "sources": {"BSL": "tausendkornmasse_g"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "total_dry_matter": {
        "canonical": "total_dry_matter",
        "agrovoc": None,
        "description": "Materia seca total / Total dry matter",
        "sources": {"BSL": "gesamttrockenmasse"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "volume_yield": {
        "canonical": "volume_yield",
        "agrovoc": None,
        "description": "Rendimiento volumetrico / Volume yield",
        "sources": {"BSL": "volumenausbeute"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "water_absorption": {
        "canonical": "water_absorption",
        "agrovoc": None,
        "description": "Absorcion de agua / Water absorption",
        "sources": {"BSL": "wasseraufnahme"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "winter_hardiness": {
        "canonical": "winter_hardiness",
        "agrovoc": None,
        "description": "Resistencia invernal / Winter hardiness",
        "sources": {"BSL": "winterhaerte"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "winter_kill_susceptibility": {
        "canonical": "winter_kill_susceptibility",
        "agrovoc": None,
        "description": "Sensibilidad a mortalidad invernal / Winter kill susceptibility",
        "sources": {"BSL": "neigung_zu_auswinterung"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "winter_survival": {
        "canonical": "winter_survival",
        "agrovoc": None,
        "description": "Supervivencia invernal / Winter survival",
        "sources": {"BSL": "auswinterung"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "yellow_pigment_content": {
        "canonical": "yellow_pigment_content",
        "agrovoc": None,
        "description": "Contenido de pigmento amarillo / Yellow pigment content",
        "sources": {"BSL": "gelbpigmentgehalt"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Disease score mappings
# ═══════════════════════════════════════════════════════════════════════════════

DISEASE_REGISTRY: dict[str, dict] = {
    "ascochyta_resistance": {
        "canonical": "ascochyta_resistance",
        "description": "Resistencia a ascochyta / Ascochyta resistance",
        "sources": {"BSL": "ascochyta"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "barley_yellow_dwarf_resistance": {
        "canonical": "barley_yellow_dwarf_resistance",
        "description": "Resistencia a enanismo amarillo cebada / Barley yellow dwarf resistance",
        "sources": {"BSL": "gerstengelbverzwergung"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "barley_yellow_mosaic_resistance": {
        "canonical": "barley_yellow_mosaic_resistance",
        "description": "Resistencia a mosaico amarillo cebada / Barley yellow mosaic resistance",
        "sources": {"BSL": "gelbmosaikvirusresistenz"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "baymv1_resistance": {
        "canonical": "baymv1_resistance",
        "description": "Resistencia BaYMV-1 / BaYMV-1 resistance",
        "sources": {"BSL": "gelbmosaik_baymv_1_bammv"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "baymv2_resistance": {
        "canonical": "baymv2_resistance",
        "description": "Resistencia BaYMV-2 / BaYMV-2 resistance",
        "sources": {"BSL": "gelbmosaik_baymv_2"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "botrytis_resistance": {
        "canonical": "botrytis_resistance",
        "description": "Resistencia a botrytis / Botrytis resistance",
        "sources": {"BSL": "botrytis", "LEGACY": "botrytis"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "broken_plants": {
        "canonical": "broken_plants",
        "description": "Plantas rotas / Broken plants",
        "sources": {"GENVCE": "plantas_rotas_pct"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "broken_stalks": {
        "canonical": "broken_stalks",
        "description": "Tallos rotos / Broken stalks",
        "sources": {"NEBIH": "broken_stalks_pct"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "brown_rust_resistance": {
        "canonical": "brown_rust_resistance",
        "description": "Resistencia a roya parda / Brown rust resistance",
        "sources": {"AHDB": "brown_rust", "BSL": "braunrost", "GENVCE": "roya_parda", "INTIA-EXP": "roya_parda", "LFL-BAYERN": "braunrost"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "corn_smut_resistance": {
        "canonical": "corn_smut_resistance",
        "description": "Resistencia a carbon del maiz / Corn smut resistance",
        "sources": {"BSL": "beulenbrand"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "crop_loss_pct": {
        "canonical": "crop_loss_pct",
        "description": "Perdida de cultivo % / Crop loss pct",
        "sources": {"NEBIH": "loss_pct"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "crown_rust_resistance": {
        "canonical": "crown_rust_resistance",
        "description": "Resistencia a roya coronada / Crown rust resistance",
        "sources": {"BSL": ["kronenrost", "anfaelligkeit_fuer_kronenrost"]},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "days_to_silking": {
        "canonical": "days_to_silking",
        "description": "Dias a floracion femenina / Days to silking",
        "sources": {"NEBIH": "days_to_silking"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "dtr_resistance": {
        "canonical": "dtr_resistance",
        "description": "Resistencia a DTR / DTR resistance",
        "sources": {"BSL": "dtr"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "dwarf_rust_resistance": {
        "canonical": "dwarf_rust_resistance",
        "description": "Resistencia a roya enana / Dwarf rust resistance",
        "sources": {"BSL": "zwergrost", "GENVCE": "roya_nana", "INTIA-EXP": "roya_nana"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "ear_fusarium_resistance": {
        "canonical": "ear_fusarium_resistance",
        "description": "Resistencia a fusarium de espiga / Ear fusarium resistance",
        "sources": {"BSL": "aehrenfusarium", "LFL-BAYERN": "aehrenfusarium"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "ear_fusarium_rot_resistance": {
        "canonical": "ear_fusarium_rot_resistance",
        "description": "Resistencia a podredumbre de mazorca / Ear fusarium rot resistance",
        "sources": {"BSL": "kolbenfusarium"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "early_growth_vigor": {
        "canonical": "early_growth_vigor",
        "description": "Vigor temprano / Early growth vigor",
        "sources": {"NEBIH": "early_growth_vigor"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "ergot_resistance": {
        "canonical": "ergot_resistance",
        "description": "Resistencia a cornezuelo / Ergot resistance",
        "sources": {"BSL": "mutterkorn"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "eyespot_resistance": {
        "canonical": "eyespot_resistance",
        "description": "Resistencia a pseudocercosporella / Eyespot resistance",
        "sources": {"BSL": "pseudocercosporella"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "ff_resistance": {
        "canonical": "ff_resistance",
        "description": "Resistencia a Ff / Ff resistance",
        "sources": {"GENVCE": "Ff"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "fire_blight_resistance": {
        "canonical": "fire_blight_resistance",
        "description": "Resistencia a fuego bacteriano / Fire blight resistance",
        "sources": {"GENVCE": "feu_bacterien"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "fol_resistance": {
        "canonical": "fol_resistance",
        "description": "Resistencia a Fol / Fol resistance",
        "sources": {"GENVCE": "Fol"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "frost_resistance": {
        "canonical": "frost_resistance",
        "description": "Resistencia a heladas / Frost resistance",
        "sources": {"GENVCE": "froid"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "fusarium_ear_blight_resistance": {
        "canonical": "fusarium_ear_blight_resistance",
        "description": "Resistencia a fusarium de espiga / Fusarium ear blight resistance",
        "sources": {"AHDB": "fusarium_ear_blight"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "fusarium_resistance": {
        "canonical": "fusarium_resistance",
        "description": "Resistencia a fusarium / Fusarium resistance",
        "sources": {"BSL": "fusarium"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "glume_blotch_resistance": {
        "canonical": "glume_blotch_resistance",
        "description": "Resistencia a mancha de gluma / Glume blotch resistance",
        "sources": {"BSL": "spelzenbraeune"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "helminthosporium_resistance": {
        "canonical": "helminthosporium_resistance",
        "description": "Resistencia a helmintosporiosis / Helminthosporium resistance",
        "sources": {"BSL": "helminthosporium", "GENVCE": ["helmintosporiosis", "helmintosporium"], "INTIA-EXP": "helmintosporiosis"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "initial_development_score": {
        "canonical": "initial_development_score",
        "description": "Desarrollo inicial / Initial development score",
        "sources": {"NEBIH": "initial_development_score"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "leaf_blotch_resistance": {
        "canonical": "leaf_blotch_resistance",
        "description": "Resistencia a manchas foliares / Leaf blotch resistance",
        "sources": {"BSL": "blattflecken"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "leaf_septoria_resistance": {
        "canonical": "leaf_septoria_resistance",
        "description": "Resistencia a septoria foliar / Leaf septoria resistance",
        "sources": {"BSL": "blattseptoria"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "leaf_sheath_rot_resistance": {
        "canonical": "leaf_sheath_rot_resistance",
        "description": "Resistencia a podredumbre de vaina / Leaf sheath rot resistance",
        "sources": {"BSL": "blattscheidenfaeule"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "light_leaf_spot_resistance": {
        "canonical": "light_leaf_spot_resistance",
        "description": "Resistencia a mancha foliar clara / Light leaf spot resistance",
        "sources": {"AHDB": "light_leaf_spot"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "lodging_pct": {
        "canonical": "lodging_pct",
        "description": "Encamado % / Lodging pct",
        "sources": {"GENVCE": "encamado_pct", "NEBIH": "lodging_pct"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "lodging_score": {
        "canonical": "lodging_score",
        "description": "Encamado / Lodging score",
        "sources": {"NEBIH": "lodging_score"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "loose_smut_resistance": {
        "canonical": "loose_smut_resistance",
        "description": "Resistencia a carbon desnudo / Loose smut resistance",
        "sources": {"BSL": "flugbrand"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "ma_resistance": {
        "canonical": "ma_resistance",
        "description": "Resistencia a Ma / Ma resistance",
        "sources": {"GENVCE": "Ma"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "mi_resistance": {
        "canonical": "mi_resistance",
        "description": "Resistencia a Mi / Mi resistance",
        "sources": {"GENVCE": "Mi"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "mj_resistance": {
        "canonical": "mj_resistance",
        "description": "Resistencia a Mj / Mj resistance",
        "sources": {"GENVCE": "Mj"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "mrdv_resistance": {
        "canonical": "mrdv_resistance",
        "description": "Resistencia a MRDV / MRDV resistance",
        "sources": {"GENVCE": "mrdv_pct"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "net_blotch_resistance": {
        "canonical": "net_blotch_resistance",
        "description": "Resistencia a mancha en red / Net blotch resistance",
        "sources": {"BSL": "netzflecken"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "on_resistance": {
        "canonical": "on_resistance",
        "description": "Resistencia a On / On resistance",
        "sources": {"GENVCE": "On"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "pf_resistance": {
        "canonical": "pf_resistance",
        "description": "Resistencia a Pf / Pf resistance",
        "sources": {"GENVCE": "Pf"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "phoma_resistance": {
        "canonical": "phoma_resistance",
        "description": "Resistencia a phoma / Phoma resistance",
        "sources": {"BSL": "phoma"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "phytophthora_resistance": {
        "canonical": "phytophthora_resistance",
        "description": "Resistencia a phytophthora / Phytophthora resistance",
        "sources": {"GENVCE": "phytophthora"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "plant_loss": {
        "canonical": "plant_loss",
        "description": "Perdida de plantas / Plant loss",
        "sources": {"NEBIH": "plant_loss_score"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "powdery_mildew_genes": {
        "canonical": "powdery_mildew_genes",
        "description": "Genes de resistencia a oidio / Powdery mildew resistance genes",
        "sources": {"BSL": "mehltau_resistenzgene"},
        "scale": "categorical",
        "higherIs": "better",
        "domain": None,
    },
    "powdery_mildew_resistance": {
        "canonical": "powdery_mildew_resistance",
        "description": "Resistencia a oidio / Powdery mildew resistance",
        "sources": {"AHDB": "mildew", "BSL": ["mehltau", "anfaelligkeit_fuer_mehltau"], "GENVCE": "oidio", "INTIA-EXP": "oidio", "LFL-BAYERN": "mehltau"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "ramularia_resistance": {
        "canonical": "ramularia_resistance",
        "description": "Resistencia a ramularia / Ramularia resistance",
        "sources": {"BSL": "ramularia"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "rhynchosporium_resistance": {
        "canonical": "rhynchosporium_resistance",
        "description": "Resistencia a rincosporiosis / Rhynchosporium resistance",
        "sources": {"BSL": "rhynchosporium", "GENVCE": ["rincosporiosis", "rynchosporium", "rincosporium"], "INTIA-EXP": "rinchosporiosis"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "root_fusarium_resistance": {
        "canonical": "root_fusarium_resistance",
        "description": "Resistencia a fusarium de raiz / Root fusarium resistance",
        "sources": {"BSL": "wurzelfusarium"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "sclerotinia_resistance": {
        "canonical": "sclerotinia_resistance",
        "description": "Resistencia a sclerotinia / Sclerotinia resistance",
        "sources": {"BSL": "sclerotinia"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "septoria_resistance": {
        "canonical": "septoria_resistance",
        "description": "Resistencia a septoria / Septoria resistance",
        "sources": {"BSL": "septoria", "GENVCE": "septoria", "INTIA-EXP": "septoria", "LFL-BAYERN": "septoria"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "septoria_tritici_resistance": {
        "canonical": "septoria_tritici_resistance",
        "description": "Resistencia a septoria tritici / Septoria tritici resistance",
        "sources": {"AHDB": "septoria_tritici"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "sharka_resistance": {
        "canonical": "sharka_resistance",
        "description": "Resistencia a sharka / Sharka resistance",
        "sources": {"GENVCE": "sharka"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "shattering": {
        "canonical": "shattering",
        "description": "Dehiscencia / Shattering",
        "sources": {"LEGACY": "eclatement"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "shattering_resistance": {
        "canonical": "shattering_resistance",
        "description": "Resistencia a desgrane / Shattering resistance",
        "sources": {"NEBIH": "shattering_resistance"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "shattering_score": {
        "canonical": "shattering_score",
        "description": "Desgrane / Shattering score",
        "sources": {"NEBIH": "shattering_score"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "si_resistance": {
        "canonical": "si_resistance",
        "description": "Resistencia a Si / Si resistance",
        "sources": {"GENVCE": "Si"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "soil_fatigue_resistance": {
        "canonical": "soil_fatigue_resistance",
        "description": "Resistencia a fatiga del suelo / Soil fatigue resistance",
        "sources": {"GENVCE": "fatigue_sol"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "standing_ability": {
        "canonical": "standing_ability",
        "description": "Capacidad de mantenerse en pie / Standing ability",
        "sources": {"NEBIH": "standing_ability_score"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "stay_green": {
        "canonical": "stay_green",
        "description": "Persistencia verde / Stay green",
        "sources": {"GENVCE": "stay_green"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "stem_base_rot_resistance": {
        "canonical": "stem_base_rot_resistance",
        "description": "Resistencia a podredumbre base tallo / Stem base rot resistance",
        "sources": {"GENVCE": "podredumbres_base_tallo_pct"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "stem_canker_resistance": {
        "canonical": "stem_canker_resistance",
        "description": "Resistencia a chancro de tallo / Stem canker resistance",
        "sources": {"AHDB": "stem_canker"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "stem_rot_pct": {
        "canonical": "stem_rot_pct",
        "description": "Podredumbre tallo % / Stem rot pct",
        "sources": {"GENVCE": "stem_rot_pct"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "stem_rot_resistance": {
        "canonical": "stem_rot_resistance",
        "description": "Resistencia a podredumbre de tallo / Stem rot resistance",
        "sources": {"BSL": ["staengelfaeule", "anfaelligkeit_fuer_staengelfaeule", "anfaelligkeit_staengelfaeule"], "GENVCE": ["podredumbre_tallo_pct", "podredumbres_tallo_pct"]},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "stem_rust_resistance": {
        "canonical": "stem_rust_resistance",
        "description": "Resistencia a roya de tallo / Stem rust resistance",
        "sources": {"BSL": "rost", "GENVCE": "roya"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "stripe_disease_resistance": {
        "canonical": "stripe_disease_resistance",
        "description": "Resistencia a estriado / Stripe disease resistance",
        "sources": {"BSL": "streifenkrankheit"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "tan_spot_resistance": {
        "canonical": "tan_spot_resistance",
        "description": "Resistencia a mancha amarilla / Tan spot resistance",
        "sources": {"BSL": "drechslera_tritici_repentis"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "tmv_resistance": {
        "canonical": "tmv_resistance",
        "description": "Resistencia a TMV / TMV resistance",
        "sources": {"GENVCE": "TMV"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "tobrfv_resistance": {
        "canonical": "tobrfv_resistance",
        "description": "Resistencia a ToBRFV / ToBRFV resistance",
        "sources": {"GENVCE": "ToBRFV"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "tomv_resistance": {
        "canonical": "tomv_resistance",
        "description": "Resistencia a ToMV / ToMV resistance",
        "sources": {"GENVCE": "ToMV"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "va_resistance": {
        "canonical": "va_resistance",
        "description": "Resistencia a Va / Va resistance",
        "sources": {"GENVCE": "Va"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "vd_resistance": {
        "canonical": "vd_resistance",
        "description": "Resistencia a Vd / Vd resistance",
        "sources": {"GENVCE": "Vd"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "verticillium_resistance": {
        "canonical": "verticillium_resistance",
        "description": "Resistencia a verticillium / Verticillium resistance",
        "sources": {"AHDB": "verticillium"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "virus_mrdv": {
        "canonical": "virus_mrdv",
        "description": "Virus MRDV / MRDV virus",
        "sources": {"GENVCE": "virus_mrdv_pct"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "water_stress": {
        "canonical": "water_stress",
        "description": "Estres hidrico / Water stress",
        "sources": {"LEGACY": "stress_hydrique"},
        "scale": "1-9_bsl",
        "higherIs": "worse",
        "domain": [1, 9],
    },
    "woolly_aphid_resistance": {
        "canonical": "woolly_aphid_resistance",
        "description": "Resistencia a pulgon lanigero / Woolly aphid resistance",
        "sources": {"GENVCE": "puceron_lanigere"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
    "yellow_rust_resistance": {
        "canonical": "yellow_rust_resistance",
        "description": "Resistencia a roya amarilla / Yellow rust resistance",
        "sources": {"AHDB": "yellow_rust", "BSL": "gelbrost", "GENVCE": "roya_amarilla", "INTIA-EXP": "roya_amarilla", "LFL-BAYERN": "gelbrost"},
        "scale": "1-9_bsl",
        "higherIs": "better",
        "domain": [1, 9],
    },
}

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
SCALE_NORMALIZERS["categorical"] = lambda v, higher_is=None: v


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


def _fuzzy_match_key(source_key: str | list[str], raw_map: dict) -> str | None:
    """Find a source key in raw_map, trying exact match then common variant suffixes.

    Handles GENVCE-style suffixes (_pct, _escala, _escala_0_9, _0_9, _scale)
    and BSL umlaut/spelling variants so the registry doesn't need duplicate entries.
    """
    if not source_key or not raw_map:
        return None

    if isinstance(source_key, list):
        for sk in source_key:
            res = _fuzzy_match_key(sk, raw_map)
            if res is not None:
                return res
        return None

    # 1. Exact match
    if source_key in raw_map:
        return source_key

    # 2. Try common suffix variants for the source key
    suffixes = ["_pct", "_scale", "_escala", "_escala_0_9", "_0_9", "_0-9",
                "_score", "_percent", "_percentage"]
    for suffix in suffixes:
        variant = source_key + suffix
        if variant in raw_map:
            return variant

    # 3. Try matching raw keys against the source key by stripping suffixes
    for raw_key in raw_map:
        if raw_key == source_key:
            return raw_key
        # Check if raw_key is source_key + known suffix
        for suffix in suffixes:
            if raw_key == source_key + suffix:
                return raw_key
            # Also check if raw_key without suffix matches
            if raw_key.endswith(suffix):
                base = raw_key[:-len(suffix)]
                if base == source_key:
                    return raw_key

    # 5. Spanish/genvce plural variants: "podredumbres_tallo_pct" → "podredumbre_tallo_pct"
    spanish_plural_map = {"podredumbres": "podredumbre", "plantas": "planta"}
    for raw_key in raw_map:
        for plural, singular in spanish_plural_map.items():
            if plural in raw_key:
                singular_key = raw_key.replace(plural, singular)
                if singular_key == source_key:
                    return raw_key
    for prefix in ["anfaelligkeit_fuer_", "anfaelligkeit_"]:
        if source_key.startswith(prefix):
            base = source_key[len(prefix):]
            if base in raw_map:
                return base
        # Also try: raw_key without prefix matches source_key
        for raw_key in raw_map:
            if raw_key.startswith(prefix):
                base = raw_key[len(prefix):]
                if base == source_key:
                    return raw_key
    umlaut_map_source = {"ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss"}
    source_key_ascii = source_key
    for uml, ascii in umlaut_map_source.items():
        source_key_ascii = source_key_ascii.replace(uml, ascii)
    for raw_key in raw_map:
        raw_key_ascii = raw_key
        for uml, ascii in umlaut_map_source.items():
            raw_key_ascii = raw_key_ascii.replace(uml, ascii)
        if source_key_ascii == raw_key_ascii:
            return raw_key

    return None


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
        matched_key = _fuzzy_match_key(source_key, traits_map)
        if matched_key is not None:
            raw_value = traits_map[matched_key]
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
                "sourceKey": matched_key,
                "agrovoc": config.get("agrovoc"),
            }

    for canonical, config in DISEASE_REGISTRY.items():
        source_key = config["sources"].get(source_id)
        matched_key = _fuzzy_match_key(source_key, disease_map)
        if matched_key is not None:
            raw_value = disease_map[matched_key]
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
                "sourceKey": matched_key,
            }

    traits_json = json.dumps(unified_traits, ensure_ascii=False) if unified_traits else None
    disease_json = json.dumps(unified_disease, ensure_ascii=False) if unified_disease else None
    return traits_json, disease_json
