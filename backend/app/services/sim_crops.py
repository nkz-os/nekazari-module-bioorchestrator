"""Species -> AquaCrop built-in crop. Anything not listed is refused, never defaulted."""
from __future__ import annotations

from dataclasses import dataclass

from app.services.sim_errors import SimulationError
from app.species_registry import resolve_species

# Thermal-time (GDD) variants: AquaCrop then paces phenology by degree-days, so
# the same crop works at any sowing date and latitude.
AQUACROP_CROPS: dict[str, str] = {
    "wheat": "WheatGDD",
    "durum_wheat": "WheatGDD",
    "barley": "BarleyGDD",
    "maize": "MaizeGDD",
    "rice": "PaddyRiceGDD",
    "soybean": "SoybeanGDD",
    "sunflower": "SunflowerGDD",
    "sugar_beet": "SugarBeetGDD",
    "potato": "PotatoGDD",
    "tomato": "TomatoGDD",
    "bean": "DryBeanGDD",
}

_NOTES: dict[str, str] = {
    "durum_wheat": (
        "AquaCrop has no durum wheat: simulated with its bread wheat (WheatGDD) "
        "calibration; durum-specific differences are not represented."
    ),
}


@dataclass(frozen=True)
class CropChoice:
    slug: str
    aquacrop_crop: str
    note: str | None


def resolve_aquacrop_crop(identifier: str) -> CropChoice:
    """Canonical species slug and its AquaCrop crop; ``unsupported_crop`` (422) otherwise."""
    slug = resolve_species(identifier)
    if slug is None:
        raise SimulationError("unsupported_crop", f"Unknown crop '{identifier}'.")
    crop = AQUACROP_CROPS.get(slug)
    if crop is None:
        raise SimulationError(
            "unsupported_crop",
            f"Crop '{slug}' cannot be simulated yet. Supported: {', '.join(sorted(AQUACROP_CROPS))}.")
    return CropChoice(slug, crop, _NOTES.get(slug))
