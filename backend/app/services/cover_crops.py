"""Protein legume catalogue: which cash crops are grain legumes.

Identity only (scientific name, common name, type). Per-climate parameters (biomass, C/N, GDD to
termination, frost tolerance, N fixation) are not kept: no source registered in
``backend/data/registries/sources.yaml`` publishes them per species and climate class, and a number
without a source that publishes it is not shown. Cover-crop agronomic data comes back only with
such a source.

Usage:
    from app.services.cover_crops import PROTEIN_CROPS
"""

from __future__ import annotations

PROTEIN_CROPS: dict[str, dict[str, str]] = {
    "VICFX": {"scientific": "Vicia faba L.", "common_name": "Faba bean", "type": "legume"},
    "PIBAR": {"scientific": "Pisum sativum L.", "common_name": "Field pea", "type": "legume"},
    "CIEAR": {"scientific": "Cicer arietinum L.", "common_name": "Chickpea", "type": "legume"},
    "LENCU": {"scientific": "Lens culinaris Medik.", "common_name": "Lentil", "type": "legume"},
    "LTHSA": {"scientific": "Lathyrus sativus L.", "common_name": "Grass pea / Almorta", "type": "legume"},
    "GLXMA": {"scientific": "Glycine max (L.) Merr.", "common_name": "Soybean", "type": "legume"},
    "VICSA": {"scientific": "Vicia sativa L.", "common_name": "Common vetch (grain)", "type": "legume"},
}
