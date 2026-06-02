"""ITACyL legume trial connector — Castilla y León (BSk climate).

Consumes nkz-itacyl-scraper output and produces AgriKnowledge entities.
Climate: BSk (cold semi-arid continental) in Castilla y León, Spain.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ikerketa.connectors.base import AbstractConnector
from ikerketa.models.base import BaseEntity, BaseRelationship, DataSource, RawRecord
from ikerketa.models.agronomy import AgriKnowledge


def _resolve_management(entry: dict) -> str:
    """Resolve management from observation field."""
    mgmt = entry.get("management", "")
    if mgmt and mgmt != "conventional":
        return mgmt
    return "conventional"


class Connector(AbstractConnector):
    """ITACyL legume variety trial connector.

    Output: AgriKnowledge entities for Neo4j :AgriKnowledge ingestion.
    Management: conventional (official variety trials in Castilla y León).
    """

    @property
    def source_name(self) -> DataSource:
        return DataSource.ITACYL

    def fetch(self, *, limit: int | None = None, **params: Any) -> list[RawRecord]:
        json_path = params.get("json_path", "")
        if not json_path:
            candidates = [
                "/home/g/Documents/nekazari/nkz-itacyl-scraper/data/output/itacyl_extracted.json",
                "data/output/itacyl_extracted.json",
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
                source_name=DataSource.ITACYL,
                record_id=f"itacyl_{i}",
                data=entry,
            ))
        return records

    def transform(self, raw_records: list[RawRecord]) -> tuple[list[BaseEntity], list[BaseRelationship]]:
        from collections import defaultdict

        entities: list[BaseEntity] = []

        # Group by (species_eppo, climate_class, management) and compute stats
        groups: dict[tuple[str, str], list[float]] = defaultdict(list)
        details: dict[tuple[str, str], dict] = {}

        # Configure which yields to include (filter out straw/guide data)
        MAX_GRAIN_YIELD: dict[str, float] = {
            "CIEAR": 8000,
            "PIBAR": 8000,   # pea grain up to 6 t/ha in irrigated
            "LENCU": 2800,   # lentil grain max ~2.5 t/ha
            "VICFX": 8000,
            "LTHSA": 4000,
            "GLXMA": 8000,
            "VICSA": 8000,
            "VICVI": 8000,
        }

        for record in raw_records:
            d = record.data
            eppo = d.get("species_eppo", "")
            climate = d.get("climate_class", "BSk")
            mgmt = _resolve_management(d)

            yld = d.get("yield_kg_ha", 0) or 0
            if yld <= 0:
                continue

            # Filter straw yields and greenhouse data
            max_grain = MAX_GRAIN_YIELD.get(eppo, 8000)
            if yld > max_grain:
                continue

            # Skip cultivation guides
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
                    "management": mgmt,
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
                source_name=DataSource.ITACYL,
                source_record_id=f"itacyl_{eppo}_{climate}",
                species_eppo=eppo,
                climate_class=climate,
                parameter="biomass_t_ha",
                value=round(val_t_ha, 2),
                unit="t/ha",
                value_min=round(val_min, 2),
                value_max=round(val_max, 2),
                source_url="https://www.itacyl.es/investigacion-e-innovacion/i-i-agricola/resultados-de-ensayos",
                source_institution="ITACyL (Instituto Tecnológico Agrario de Castilla y León)",
                confidence=0.85,
                management=det["management"],
                crop_category="protein_crop",
                notes=(
                    f"{det['species']} grain yield. {len(yields)} trials, "
                    f"{n_varieties} varieties. "
                    f"Locations: {det['location']}. Campaigns: {det['campaign']}. "
                    f"PDFs: {', '.join(sorted(det['pdfs'])[:3])}"
                ),
                raw_record={"yields": yields, "n": len(yields)},
            )
            entity.compute_hash()
            entities.append(entity)

        return entities, []
