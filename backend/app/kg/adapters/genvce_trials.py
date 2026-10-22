"""GENVCE-TRIALS adapter: the per-locality sheet extractions of the GENVCE results map -> raw rows.

Input: ``<source folder>/data/results_map/extractions/<sheet id>.json``, one file per sheet of the map
(locality x crop x campaign), written by the deterministic XLSX/PDF parsers of the raw-data repository.
Each file carries the sheet header (title, executing entity, sowing and harvest dates, zone label,
footer statistics) and one row per variety (yield at standard moisture, productivity index, statistical
group, harvested yield when printed, and the traits of the other tables keyed by a fixed id).

What the source really is. One sheet = one trial at a located site, run by a regional entity for the
GENVCE network and published by GENVCE under its reuse notice. The campaign term of the map is the
harvest year (a winter cereal of ``any`` 2026 was sown in autumn 2025).

Decisions an adapter makes here, each one reported in the warnings it returns:
* ``irrigation``: only what the printed zone label states ("Regadíos …" / "Secanos …"); a label that
  states neither leaves the row without a regime (counted), never a guess.
* ``production_system``: ``ecológico`` only when the crop label says so; otherwise absent (the convention of
  the GENVCE adapter).
* ``cycle_class``: ``invierno`` / ``primavera`` when the crop label states it (winter cereals); the maize FAO
  group is NOT a per-row observation: it stays in the crop label, which the contract carries in the study
  scope.
* ``sheet.fungicide``: ``treated`` / ``untreated`` only when the map entry's title or slug states it
  ("tratado con fungicida", "no tratado con fungicida", "-t" / "-nt"); absent otherwise.
* a trait key the parsers never emit, or an extraction field this adapter does not know, is refused: a new
  column is decided on, never dropped.
"""
from __future__ import annotations

import datetime as dt
import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from . import AdapterError, AdapterResult, WarningLog, fingerprints, sorted_files

SOURCE_ID = "GENVCE-TRIALS"
EXTRACTIONS_DIR = Path("data/results_map/extractions")

_EXTRACTION_KEYS = frozenset({
    "any", "check_mean_kg_ha", "columns", "crop_label", "cv_pct", "design", "executing_entity", "harvest_date",
    "locality", "pdf_sha256", "post_title", "rows", "sd_kg_ha", "sheet_id", "slug", "source_format",
    "sowing_date", "title", "trial_mean_kg_ha", "url_pdf", "url_xlsx", "xlsx_sha256", "yield_moisture_pct",
    "zone_label", "warnings",
})
_VARIETY_KEYS = frozenset({
    "variety", "is_check", "is_pre_evaluation", "yield_std_kg_ha", "relative_yield_pct", "statistical_group",
    "yield_raw_kg_ha", "traits",
})
# the trait ids of the parsers' HEADER_MAP (raw-data repository, results_map/common.py); keep in step
_TRAIT_KEYS = frozenset({
    # maize
    "flowering_female_date", "grain_moisture_pct", "plant_density_m2", "plant_height_cm", "ear_height_cm",
    "stalk_breakage_pct", "stalk_rot_pct", "stay_green_score_0_5", "mrdv_virus_pct", "mdmv_virus_pct",
    "corn_borer_larvae_per_plant",
    # winter cereals
    "emergence_date", "emergence_score_1_5", "jointing_date", "heading_date", "maturity_date",
    "spike_density_m2", "thousand_kernel_weight_g", "lodging_pct", "cold_damage_pct",
    "powdery_mildew_pct", "powdery_mildew_score_0_9", "septoria_pct", "septoria_score_0_9",
    "brown_rust_pct", "brown_rust_score_0_9", "yellow_rust_pct", "yellow_rust_score_0_9", "stem_rust_pct",
    "crown_rust_pct", "crown_rust_score_0_9", "helminthosporium_pct", "helminthosporium_score_0_9",
    "scald_pct", "scald_score_0_9", "ramularia_score_0_9",
    # oilseeds
    "flowering_date", "flowering_start_date", "flowering_end_date", "seed_oil_pct", "broomrape_pct", "necrosis_pct",
    # grain and seed quality
    "test_weight_kg_hl", "grain_protein_pct", "vitreousness_pct", "falling_number_s", "semolina_yellow_index",
    "alveograph_w", "alveograph_p", "alveograph_l", "alveograph_pl",
})

_FILE = re.compile(r"^(?P<id>\d+)\.json$")
# the printed scale of the visual scores; a value outside it was printed under the wrong column head (some
# sheets print percentages under a "(0-9)" label) and is dropped, counted, never re-interpreted
_SCORE_DOMAINS: dict[str, tuple[float, float]] = {"emergence_score_1_5": (0.0, 5.0), "stay_green_score_0_5": (0.0, 5.0)}


def _score_domain(trait: str) -> tuple[float, float] | None:
    if trait in _SCORE_DOMAINS:
        return _SCORE_DOMAINS[trait]
    return (0.0, 9.0) if trait.endswith("_score_0_9") else None
_UNTREATED = re.compile(r"\b(no tratad[oa]|sin fungicida)\b", re.IGNORECASE)
_TREATED = re.compile(r"\b(tratad[oa]|con fungicida)\b", re.IGNORECASE)
_NT_TOKEN = re.compile(r"\bNT\b")
_T_TOKEN = re.compile(r"\bT\b")
_REGIME_WORD = re.compile(r"\b(secanos?|regad[ií]os?)\b", re.IGNORECASE)


def regime_from_zone(label: str | None) -> str | None:
    if not label:
        return None
    low = label.strip().lower()
    if low.startswith("regad"):
        return "regadío"
    if low.startswith("secano"):
        return "secano"
    return None


def cycle_from_label(crop_label: str) -> str | None:
    low = crop_label.lower()
    if " de invierno" in low:
        return "invierno"
    if " de primavera" in low:
        return "primavera"
    return None


def fungicide_variant(post_title: str, slug: str, title: str | None = None) -> str | None:
    """``treated`` / ``untreated`` when the map entry (its title or slug) or the printed sheet title states
    it: "tratado / no tratado con fungicida", "con / sin fungicida", or a bare "T" / "NT" tag; else None."""
    printed = (title or "").split(",", 1)[0]  # the crop part of "Ensayo <crop> [T|NT], <locality>, <year>"
    texts = [post_title, printed]
    if any(_UNTREATED.search(t) for t in texts):
        return "untreated"
    if any(_TREATED.search(t) for t in texts):
        return "treated"
    if any(_NT_TOKEN.search(t) for t in texts):
        return "untreated"
    if any(_T_TOKEN.search(t) for t in texts):
        return "treated"
    tokens = [t for t in re.sub(r"-\d+$", "", slug.lower()).split("-") if t]
    if "nt" in tokens:
        return "untreated"
    if "t" in tokens:
        return "treated"
    return None


def regime_from_title(post_title: str) -> str | None:
    """A regime the map entry's title states ("Colza Secano", "Colza Regadío"), else None."""
    match = _REGIME_WORD.search(post_title)
    if match is None:
        return None
    return "secano" if match.group(1).lower().startswith("secano") else "regadío"


def season_dates(sowing: str, harvest: str | None, harvest_year: int, log: WarningLog, where: str) -> tuple[str | None, str | None]:
    """The printed dates checked against the sheet's harvest year: an autumn sowing printed in the harvest
    year with a harvest before it is a year typo (repaired to the year before); a sowing outside the two
    seasons' years, a harvest outside the harvest year, or a harvest not after the sowing is dropped."""
    sown: dt.date | None = dt.date.fromisoformat(sowing)
    reaped: dt.date | None = dt.date.fromisoformat(harvest) if harvest else None
    assert sown is not None
    if sown.year == harvest_year and sown.month >= 8 and (reaped is None or reaped < sown):
        sown = sown.replace(year=harvest_year - 1)
        log.add("sowing_year_repaired", "an autumn sowing printed in the harvest year, after the harvest; the "
                "year before is used", where=where)
    if sown.year not in (harvest_year - 1, harvest_year):
        log.add("sowing_date_dropped", "the sowing year is neither the harvest year nor the one before; dropped",
                where=where)
        sown = None
    if reaped is not None and reaped.year != harvest_year:
        log.add("harvest_date_dropped", "the harvest year is not the sheet's campaign year; dropped", where=where)
        reaped = None
    if sown is not None and reaped is not None and reaped <= sown:
        log.add("harvest_not_after_sowing", "the printed harvest date is not after the sowing date; the harvest "
                "date is dropped", where=where)
        reaped = None
    return (sown.isoformat() if sown else None), (reaped.isoformat() if reaped else None)


def _check_keys(found: Mapping[str, Any], allowed: frozenset[str], what: str) -> None:
    unknown = sorted(set(found) - allowed)
    if unknown:
        raise AdapterError(f"{what}: unknown field(s) {unknown}; decide whether to map or ignore them")


def sheet_rows(extraction: Mapping[str, Any], file_name: str, log: WarningLog) -> list[dict[str, Any]]:
    """The raw rows of one sheet extraction (one per variety)."""
    _check_keys(extraction, _EXTRACTION_KEYS, file_name)
    sheet_id = int(extraction["sheet_id"])
    if file_name != f"{sheet_id}.json":
        raise AdapterError(f"{file_name}: sheet_id {sheet_id} does not match the file name")
    sheet: dict[str, Any] = {
        "id": sheet_id,
        "title": extraction["title"],
        "post_title": extraction["post_title"],
        "slug": extraction["slug"],
        "any": int(extraction["any"]),
        "locality": extraction["locality"],
        "crop_label": extraction["crop_label"],
        "zone_label": extraction.get("zone_label"),
        "executing_entity": extraction["executing_entity"],
        "sowing_date": extraction["sowing_date"],
        "harvest_date": extraction["harvest_date"],
        "design": extraction.get("design"),
        "trial_mean_kg_ha": extraction.get("trial_mean_kg_ha"),
        "check_mean_kg_ha": extraction.get("check_mean_kg_ha"),
        "sd_kg_ha": extraction.get("sd_kg_ha"),
        "cv_pct": extraction.get("cv_pct"),
        "url_pdf": extraction.get("url_pdf"),
        "url_xlsx": extraction.get("url_xlsx"),
        # inconsistencies of the sheet the parsers tolerated (text), kept with the row as provenance
        "warnings": list(extraction.get("warnings") or []),
    }
    sheet["sowing_date"], sheet["harvest_date"] = season_dates(
        extraction["sowing_date"], extraction.get("harvest_date"), int(extraction["any"]), log, file_name)
    variant = fungicide_variant(extraction["post_title"], extraction["slug"], extraction["title"])
    if variant is not None:
        sheet["fungicide"] = variant
    raw_sha = extraction.get("pdf_sha256") or extraction.get("xlsx_sha256")
    url_pdf = extraction.get("url_pdf") or ""
    doc = {
        "title": extraction["title"],
        "issue": str(sheet_id),
        "year": int(extraction["any"]),
        "file_name": url_pdf.rsplit("/", 1)[-1] if url_pdf else None,
        "raw_sha256": raw_sha,
    }
    irrigation = regime_from_zone(extraction.get("zone_label")) or regime_from_title(extraction["post_title"])
    if irrigation is None:
        log.add("regime_not_stated", "neither the zone label nor the entry title states rainfed or irrigated",
                where=file_name)
    cycle = cycle_from_label(extraction["crop_label"])
    organic = "ecológic" in extraction["crop_label"].lower()
    rows: list[dict[str, Any]] = []
    for position, entry in enumerate(extraction["rows"]):
        _check_keys(entry, _VARIETY_KEYS, f"{file_name} row {position}")
        _check_keys(entry["traits"], _TRAIT_KEYS, f"{file_name} row {position} traits")
        traits: dict[str, Any] = {}
        for trait, value in entry["traits"].items():
            domain = _score_domain(trait)
            if domain is not None and isinstance(value, (int, float)) and not domain[0] <= value <= domain[1]:
                log.add("value_outside_printed_scale",
                        "a visual score outside the scale its column head prints (another scale under the same "
                        "head); the value is dropped", where=f"{file_name}:{entry['variety']}:{trait}")
                continue
            traits[trait] = value
        row: dict[str, Any] = {
            "sheet": dict(sheet),
            "doc": dict(doc),
            "crop": extraction["crop_label"],
            "variety": entry["variety"],
            "is_check": bool(entry["is_check"]),
            "is_pre_evaluation": bool(entry["is_pre_evaluation"]),
            "site": extraction["locality"],
            "season": str(int(extraction["any"])),
            "traits": traits,
        }
        if entry.get("yield_std_kg_ha") is not None:
            row["yield_std_kg_ha"] = float(entry["yield_std_kg_ha"])
            row["yield_moisture_pct"] = float(extraction["yield_moisture_pct"])
        else:
            # the yield table prints no value for this entry: a unit without a yield (its traits stay)
            log.add("entry_without_yield", "a variety entry with no yield printed on the sheet",
                    where=f"{file_name}:{entry['variety']}")
        if irrigation is not None:
            row["irrigation"] = irrigation
        if cycle is not None:
            row["cycle_class"] = cycle
        if organic:
            row["production_system"] = "ecológico"
        if entry.get("yield_raw_kg_ha") is not None:
            row["yield_raw_kg_ha"] = float(entry["yield_raw_kg_ha"])
        if entry.get("relative_yield_pct") is not None:
            row["relative_yield_pct"] = float(entry["relative_yield_pct"])
        if entry.get("statistical_group"):
            row["statistical_group"] = str(entry["statistical_group"])
        if entry["is_pre_evaluation"]:
            log.add("pre_evaluation_entry", "a variety in GENVCE pre-evaluation (PG) is included as any other entry",
                    where=f"{file_name}:{entry['variety']}")
        rows.append(row)
    if not rows:
        raise AdapterError(f"{file_name}: a sheet without variety rows")
    return rows


def load(source_dir: str | Path) -> AdapterResult:
    """Read every sheet extraction of ``<source_dir>/data/results_map/extractions`` into raw rows."""
    root = Path(source_dir)
    directory = root / EXTRACTIONS_DIR
    files = sorted_files(directory, "*.json")
    if not files:
        raise AdapterError(f"no sheet extractions under {directory}")
    log = WarningLog()
    rows: list[dict[str, Any]] = []
    for path in files:
        if _FILE.match(path.name) is None:
            raise AdapterError(f"{path.name}: not a sheet extraction (<sheet id>.json)")
        try:
            extraction = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise AdapterError(f"{path.name}: cannot read: {exc}") from exc
        rows.extend(sheet_rows(extraction, path.name, log))
    return AdapterResult(rows=tuple(rows), warnings=log.result(), inputs=fingerprints(root, files))


__all__ = ["SOURCE_ID", "cycle_from_label", "fungicide_variant", "load", "regime_from_title", "regime_from_zone",
           "season_dates", "sheet_rows"]
