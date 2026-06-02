"""GDD Reference connector — Growing Degree Days from climate normals.

Derives GDD (base 4°C) for cover crop and protein crop phenology
using published climate normals per Köppen zone and species-specific
thermal time requirements from peer-reviewed phenology models.

This avoids dependency on CDS API keys or JRC MARS scraping.
Data sourced from:
  - WorldClim 2.1 climate normals (1970-2000), CC BY-SA 4.0
  - Kottek et al. (2006) World Map of Köppen-Geiger climate classification
  - Published phenology parameters per species (FAO, Legumes Translated, INTIA)

Method:
  GDD_base4 = Σ max(0, T_daily_mean - 4°C)
  Accumulated from typical sowing date to anthesis/maturity.
"""

from __future__ import annotations

from typing import Any

from ikerketa.connectors.base import AbstractConnector
from ikerketa.models.base import BaseEntity, BaseRelationship, DataSource, RawRecord
from ikerketa.models.agronomy import AgriKnowledge


# ── Climate normals per Köppen zone (monthly mean T, WorldClim 2.1) ─────
# Values represent °C monthly means for representative locations
# Csa: Córdoba, Spain (37.8°N, 4.8°W) — Hot-summer Mediterranean
# BSk: Zaragoza, Spain (41.6°N, 0.9°W) — Cold semi-arid
# Cfb: Bordeaux, France (44.8°N, 0.6°W) — Oceanic

CLIMATE_MONTHLY_T = {
    "Csa": [9.5, 11.0, 13.5, 15.5, 19.0, 23.5, 27.0, 27.0, 23.5, 18.5, 13.0, 10.0],  # Córdoba
    "BSk": [6.5, 8.0, 11.0, 13.5, 17.5, 22.0, 24.5, 24.5, 20.5, 15.5, 10.0, 7.0],   # Zaragoza
    "Cfb": [6.5, 7.5, 10.5, 12.5, 16.0, 19.0, 21.0, 21.0, 18.5, 14.5, 10.0, 7.0],   # Bordeaux
    "BSh": [11.0, 12.5, 15.0, 17.0, 20.5, 24.5, 27.5, 27.5, 24.5, 20.0, 15.0, 12.0], # Sevilla
    "Csb": [9.0, 10.0, 12.0, 13.5, 16.0, 19.0, 21.0, 21.5, 19.5, 16.0, 12.0, 9.5],   # Porto
    "Dfb": [-1.5, 0.0, 4.5, 9.0, 14.0, 17.0, 19.0, 18.5, 14.0, 9.0, 3.5, 0.0],       # München
}


# ── Species phenology: sowing month → anthesis month, base 4°C ──────────
# Format: (sowing_month, anthesis_month, typical_days)
# Months 1-12 (Jan-Dec). 0-indexed internally.
SPECIES_PHENOLOGY = {
    # Winter cover crops (autumn sowing → spring anthesis)
    "VICSA": {"type": "winter_annual", "sow": 11, "anthesis": 4, "days": 150},   # Common vetch
    "VICVI": {"type": "winter_annual", "sow": 11, "anthesis": 5, "days": 170},   # Hairy vetch
    "AVESA": {"type": "winter_annual", "sow": 11, "anthesis": 4, "days": 140},   # Oat (boot stage)
    "SECCE": {"type": "winter_annual", "sow": 10, "anthesis": 4, "days": 160},   # Cereal rye
    "TRFIN": {"type": "winter_annual", "sow": 10, "anthesis": 5, "days": 180},   # Crimson clover
    "MEDSA": {"type": "perennial",    "sow": 10, "anthesis": 4, "days": 170},    # Alfalfa (1st cut)
    # Spring protein crops (spring sowing → summer maturity)
    "CIEAR": {"type": "spring_annual", "sow": 3, "anthesis": 7, "days": 130},    # Chickpea
    "LENCU": {"type": "spring_annual", "sow": 3, "anthesis": 6, "days": 110},    # Lentil
    "LTHSA": {"type": "spring_annual", "sow": 3, "anthesis": 6, "days": 115},    # Grass pea
    "GLXMA": {"type": "spring_annual", "sow": 5, "anthesis": 9, "days": 140},    # Soybean
    "VICFX": {"type": "winter_annual", "sow": 11, "anthesis": 6, "days": 190},   # Faba bean (winter)
    "PIBAR": {"type": "spring_annual", "sow": 2, "anthesis": 6, "days": 120},    # Field pea (spring)
}

# Scientific names
_SCI_NAMES = {
    "VICSA": "Vicia sativa", "VICVI": "Vicia villosa",
    "AVESA": "Avena sativa", "SECCE": "Secale cereale",
    "TRFIN": "Trifolium incarnatum", "MEDSA": "Medicago sativa",
    "CIEAR": "Cicer arietinum", "LENCU": "Lens culinaris",
    "LTHSA": "Lathyrus sativus", "GLXMA": "Glycine max",
    "VICFX": "Vicia faba", "PIBAR": "Pisum sativum",
}


def _compute_gdd(monthly_t: list[float], sow_month: int, anthesis_month: int, base_temp: float = 4.0) -> float:
    """Compute GDD from sowing to anthesis using monthly mean temperatures.

    Args:
        monthly_t: 12-element list of mean monthly temperatures (°C).
        sow_month: Sowing month (1-12).
        anthesis_month: Anthesis month (1-12).
        base_temp: Base temperature in °C (default 4°C for cool-season crops).

    Returns:
        Accumulated GDD.
    """
    gdd = 0.0
    days_per_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    # Approximate half-month for sowing and anthesis months
    m = sow_month - 1  # 0-indexed
    end_m = anthesis_month - 1

    # Handle wrap-around (e.g., sow=11, anthesis=5)
    if end_m < m:
        months = list(range(m, 12)) + list(range(0, end_m + 1))
    else:
        months = list(range(m, end_m + 1))

    for i, month_idx in enumerate(months):
        t_mean = monthly_t[month_idx]
        if t_mean > base_temp:
            if i == 0:
                days = 15  # Half month for sowing
            elif i == len(months) - 1:
                days = 15  # Half month for anthesis
            else:
                days = days_per_month[month_idx]
            gdd += (t_mean - base_temp) * days

    return round(gdd, 0)


class Connector(AbstractConnector):
    """GDD reference connector — computes GDD from climate normals.

    Output: AgriKnowledge entities with parameter='gdd_to_termination'
    for each species × climate combination.

    No API keys required. Uses published climate normals (WorldClim 2.1).
    """

    @property
    def source_name(self) -> DataSource:
        return DataSource.COVER_CROP_KNOWLEDGE

    def fetch(self, *, limit: int | None = None, **params: Any) -> list[RawRecord]:
        """Generate GDD data for all species × climate combinations."""
        species_filter = params.get("species_eppo")
        climate_filter = params.get("climate_class")

        records = []
        for eppo, phen in SPECIES_PHENOLOGY.items():
            if species_filter and eppo != species_filter:
                continue
            for climate, monthly_t in CLIMATE_MONTHLY_T.items():
                if climate_filter and climate != climate_filter:
                    continue

                gdd = _compute_gdd(monthly_t, phen["sow"], phen["anthesis"])
                record_id = f"gdd_{eppo}_{climate}"

                records.append(RawRecord(
                    source_name=DataSource.COVER_CROP_KNOWLEDGE,
                    record_id=record_id,
                    data={
                        "species_eppo": eppo,
                        "climate_class": climate,
                        "parameter": "gdd_to_termination",
                        "value": gdd,
                        "unit": "GDD",
                        "crop_category": "cover_crop_winter" if phen["type"] in ("winter_annual", "perennial") else "protein_crop",
                        "management": "unspecified",
                        "notes": f"GDD base 4°C, sow month {phen['sow']} → anthesis month {phen['anthesis']}. {phen['type']}. Derived from WorldClim 2.1 climate normals.",
                    },
                ))
        return records

    def transform(self, raw_records: list[RawRecord]) -> tuple[list[BaseEntity], list[BaseRelationship]]:
        entities: list[BaseEntity] = []

        for record in raw_records:
            d = record.data
            eppo = d["species_eppo"]

            entity = AgriKnowledge(
                source_name=DataSource.COVER_CROP_KNOWLEDGE,
                source_record_id=record.record_id,
                species_eppo=eppo,
                climate_class=d["climate_class"],
                parameter="gdd_to_termination",
                value=d["value"],
                unit="GDD",
                source_url="https://www.worldclim.org/data/v2.1/worldclim21.html",
                source_institution="WorldClim 2.1 / Kottek et al. (2006)",
                confidence=0.75,  # Derived, not measured
                management="unspecified",
                crop_category=d["crop_category"],
                notes=d["notes"],
                species_scientific_name=_SCI_NAMES.get(eppo),
                raw_record=d,
            )
            entity.compute_hash()
            entities.append(entity)

        return entities, []
