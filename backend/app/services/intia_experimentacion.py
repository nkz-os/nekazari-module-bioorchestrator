"""INTIA Experimentación connector — reads extracted JSON from nkz-intia-scraper.

Consumes the output of the INTIA PDF table extractor (nkz-intia-scraper)
and transforms observations into AgriKnowledge entities for Neo4j ingestion.

Data sources: 6 PDFs (2020-2025) from https://www.intiasa.es/es/experimentacion
Extraction pipeline: nkz-intia-scraper/src/intia_scraper/extractor.py
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ikerketa.connectors.base import AbstractConnector
from ikerketa.models.base import BaseEntity, BaseRelationship, DataSource, RawRecord
from ikerketa.models.agronomy import AgriKnowledge


# ── Parameter mapping ──────────────────────────────────────────────────
PARAM_MAP = {
    "biomass_dry_kg_ha": "biomass_t_ha",
    "biomass_fresh_kg_ha": "biomass_t_ha",
    "grain_yield_kg_ha": "biomass_t_ha",
    "total_biomass_t_ha": "biomass_t_ha",
}

UNIT_MAP = {"kg/ha": "t/ha", "t/ha": "t/ha"}

_SCI_NAMES = {
    "vicia sativa": "Vicia sativa",
    "vicia villosa": "Vicia villosa",
    "avena": "Avena sativa",
    "centeno": "Secale cereale",
    "cebada": "Hordeum vulgare",
    "trigo": "Triticum aestivum",
    "guisante": "Pisum sativum",
    "habas": "Vicia faba",
    "lenteja": "Lens culinaris",
    "garbanzo": "Cicer arietinum",
    "trifolium incarnatum": "Trifolium incarnatum",
    "trifolium repens": "Trifolium repens",
    "medicago sativa": "Medicago sativa",
    "medicago scutellata": "Medicago scutellata",
    "onobrychis vicifolia": "Onobrychis viciifolia",
    "lotus corniculatus": "Lotus corniculatus",
    "sinapis alba": "Sinapis alba",
    "brassica juncea": "Brassica juncea",
}


def _kg_ha_to_t_ha(val: float) -> float:
    return val / 1000.0


def _resolve_climate(location: str) -> str:
    """Resolve Köppen class from INTIA station location."""
    loc = location.lower()
    if any(x in loc for x in ('sartaguda', 'cadreita', 'milagro', 'sesma', 'olite', 'tudela')):
        return 'BSk'
    if any(x in loc for x in ('ripodas', 'ilundain', 'torres de elorz', 'azpa')):
        return 'Cfb'
    return 'BSk'


class Connector(AbstractConnector):
    """INTIA Experimentación connector — reads extracted trial data.

    Output: AgriKnowledge entities for Neo4j :AgriKnowledge ingestion.
    """

    @property
    def source_name(self) -> DataSource:
        return DataSource.INTIA_COVER_CROPS

    def fetch(self, *, limit: int | None = None, **params: Any) -> list[RawRecord]:
        json_path = params.get("json_path", "")
        if not json_path:
            candidates = [
                "/home/g/Documents/nekazari/nkz-intia-scraper/data/output/intia_extracted.json",
                "data/output/intia_extracted.json",
            ]
            for c in candidates:
                if Path(c).exists():
                    json_path = c
                    break

        if not json_path or not Path(json_path).exists():
            return []

        with open(json_path) as f:
            raw_data = json.load(f)

        species_filter = params.get("species_eppo")
        param_filter = params.get("parameter")
        records = []
        for i, entry in enumerate(raw_data):
            if limit and len(records) >= limit:
                break
            if species_filter and entry.get("species_eppo") != species_filter:
                continue
            if param_filter and entry.get("parameter") != param_filter:
                continue
            records.append(RawRecord(
                source_name=DataSource.INTIA_COVER_CROPS,
                record_id=f"intia_exp_{i}",
                data=entry,
            ))
        return records

    def transform(self, raw_records: list[RawRecord]) -> tuple[list[BaseEntity], list[BaseRelationship]]:
        entities: list[BaseEntity] = []

        for record in raw_records:
            d = record.data
            eppo = d.get("species_eppo") or ""

            # Skip records without valid 5-char EPPO code
            if len(eppo) != 5:
                continue

            raw_param = d.get("parameter", "")
            agri_param = PARAM_MAP.get(raw_param)
            if agri_param is None:
                continue  # Skip non-mapped params (height, etc.)

            raw_value = d.get("value", 0.0)
            raw_unit = d.get("unit", "")

            # Cover crop biomass values from PDF are effectively t/ha already
            # (header says kg/ha but values match t/ha scale).
            # Grain yields ARE in kg/ha and need conversion.
            if raw_param == "grain_yield_kg_ha" and raw_unit == "kg/ha":
                value = _kg_ha_to_t_ha(raw_value)
                unit = "t/ha"
            elif raw_unit == "kg/ha":
                value = raw_value  # Keep as-is (effectively t/ha)
                unit = "t/ha"
            else:
                value = raw_value
                unit = UNIT_MAP.get(raw_unit, raw_unit)

            location = d.get("location", "")
            climate = _resolve_climate(location)
            species_name = d.get("species_name", "").lower()

            entity = AgriKnowledge(
                source_name=DataSource.INTIA_COVER_CROPS,
                source_record_id=record.record_id,
                species_eppo=eppo,
                climate_class=climate,
                parameter=agri_param,
                value=value,
                unit=unit,
                source_url="https://www.intiasa.es/es/experimentacion",
                source_institution="INTIA Navarra",
                confidence=0.80,
                data_gap=False,
                notes=f"[{d.get('campaign', '')}] {d.get('notes', '')}",
                species_scientific_name=_SCI_NAMES.get(species_name),
                species_common_names={},
                crop_category="cover_crop_winter",
                raw_record=d,
            )
            entity.compute_hash()
            entities.append(entity)

        return entities, []
