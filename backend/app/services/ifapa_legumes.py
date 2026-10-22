"""IFAPA Servifapa connector — reads extracted legume yield data from Andalusia.

Consumes nkz-ifapa-scraper output and produces AgriKnowledge entities.
Data: 133 observations across 9 species from IFAPA variety trials (2003-2024).
Climate: Csa (Mediterranean) and BSh (hot semi-arid) in Andalusia, Spain.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ikerketa.connectors.base import AbstractConnector
from ikerketa.models.agronomy import AgriKnowledge
from ikerketa.models.base import BaseEntity, BaseRelationship, DataSource, RawRecord


def _resolve_management(entry: dict) -> str:
    """Resolve management from observation field or filename fallback."""
    mgmt = entry.get("management", "")
    if mgmt and mgmt != "conventional":
        # Trust the extractor's management classification
        # (e.g., 'organic', 'low_input' from intercropping/inoculated systems)
        return mgmt
    source_pdf = entry.get("source_pdf", "")
    if any(k in source_pdf.lower() for k in ('ecologico', 'ecológica', 'ecológico')):
        return "organic"
    return "conventional"


class Connector(AbstractConnector):
    """IFAPA legume variety trial connector.

    Output: AgriKnowledge entities for Neo4j :AgriKnowledge ingestion.
    Management: conventional (default) or organic (for ecológico PDFs).
    """

    @property
    def source_name(self) -> DataSource:
        return DataSource.IFAPA

    def fetch(self, *, limit: int | None = None, **params: Any) -> list[RawRecord]:
        json_path = params.get("json_path", "")
        if not json_path:
            candidates = [
                "/home/g/Documents/nekazari/nkz-ifapa-scraper/data/output/ifapa_extracted.json",
                "data/output/ifapa_extracted.json",
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
        records = []
        for i, entry in enumerate(raw_data):
            if limit and len(records) >= limit:
                break
            eppo = entry.get("species_eppo", "")
            if not eppo or len(eppo) != 5:
                continue
            if species_filter and eppo != species_filter:
                continue
            records.append(RawRecord(
                source_name=DataSource.IFAPA,
                record_id=f"ifapa_{i}",
                data=entry,
            ))
        return records

    def transform(self, raw_records: list[RawRecord]) -> tuple[list[BaseEntity], list[BaseRelationship]]:
        from collections import defaultdict

        entities: list[BaseEntity] = []

        # Grain yield caps per species (filter straw + greenhouse):
        MAX_GRAIN_YIELD: dict[str, float] = {
            "CIEAR": 8000,
            "PIBAR": 8000,
            "LENCU": 2800,
            "VICFX": 8000,
            "LTHSA": 4000,
            "VICSA": 8000,
            "VICVI": 8000,
        }

        # Group by (species_eppo, climate_class) and compute mean yield
        groups: dict[tuple[str, str], list[float]] = defaultdict(list)
        details: dict[tuple[str, str], dict] = {}

        for record in raw_records:
            d = record.data
            eppo = d.get("species_eppo", "")
            climate = d.get("climate_class", "Csa")
            yld = d.get("yield_kg_ha", 0) or 0
            if yld <= 0:
                continue

            # Filter straw yields, greenhouse data, guides
            max_grain = MAX_GRAIN_YIELD.get(eppo, 8000)
            if yld > max_grain:
                continue
            notes = d.get("notes", "")
            source_pdf = d.get("source_pdf", "")
            if "guía" in notes.lower() or "guia" in source_pdf.lower():
                continue

            key = (eppo, climate)
            groups[key].append(yld)
            if key not in details:
                details[key] = {
                    "species": d.get("species", ""),
                    "location": d.get("location", ""),
                    "campaign": d.get("campaign", ""),
                    "management": _resolve_management(d),
                    "varieties": [],
                    "pdfs": set(),
                }
            details[key]["varieties"].append(d.get("variety", ""))
            details[key]["pdfs"].add(d.get("source_pdf", ""))

        for (eppo, climate), yields in groups.items():
            if len(yields) < 1:
                continue
            mean_yield = sum(yields) / len(yields)
            val_t_ha = mean_yield / 1000.0
            val_min = min(yields) / 1000.0
            val_max = max(yields) / 1000.0

            det = details[(eppo, climate)]
            n_varieties = len(set(det["varieties"]))

            entity = AgriKnowledge(
                source_name=DataSource.IFAPA,
                source_record_id=f"ifapa_{eppo}_{climate}",
                species_eppo=eppo,
                climate_class=climate,
                parameter="biomass_t_ha",
                value=round(val_t_ha, 2),
                unit="t/ha",
                value_min=round(val_min, 2),
                value_max=round(val_max, 2),
                source_url="https://www.juntadeandalucia.es/agriculturaypesca/ifapa/servifapa/",
                source_institution="IFAPA (Junta de Andalucía)",
                confidence=0.85,
                management=det["management"],
                crop_category="protein_crop",
                notes=f"{det['species']} yield. {len(yields)} trials, {n_varieties} varieties. {det['location']}, {det['campaign']}. PDFs: {', '.join(sorted(det['pdfs'])[:3])}",
                raw_record={"yields": yields, "n": len(yields)},
            )
            entity.compute_hash()
            entities.append(entity)

        return entities, []
