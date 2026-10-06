"""GENVCE adapter: the LLM extraction files of the GENVCE reports -> raw rows.

Input: ``<source folder>/data/extractions/*.json``, one file per campaign and crop group
(``metadata`` + ``variety_trials``). This is the extraction pass of 2026-06-15; the legacy
``enriched`` pass (older, 505 of 1105 shared keys with another yield) and the converter and
``adequate`` files derived from it are not read: the latter replace what the extraction printed with
invented values (see below).

What the source really is. A GENVCE report prints, per table, the mean of a group of trials: a
climatic zone, a yield stratum, a geographic group ("Norte", "Centro"), or all trials ("Nacional").
No trial location is printed (``trial_location`` is null in every extracted row, and a non-null
value is refused here). The legacy ``adequate`` step mapped these averages onto four reference
cities; this adapter never does. The observed zone, stratum or group label becomes the row's
``zone`` (the contract maps it to the unit's site) and a row without any label has none.

Decisions an adapter makes here, each one reported in the warnings it returns:

* ``zone``: ``yield_notes.zone`` (the table's own label, e.g. "Norte") wins over
  ``agroclimatic_zone``, which for those tables is the extraction's relabel (the 2020 maize tables
  are titled "zona Norte/Centro/Sur" and were relabelled to agroclimatic names).
* ``irrigation``: only what the observed zone label states ("Secanos ...", "Regadios ..."). The
  extraction filled ``irrigation_regime`` from assumptions ("se asume regimen de regadio", "se asume
  como secano") and the 15 maize reports never mention irrigation at all, so a regime that no label
  states is left out (and counted), never passed on as observed.
* ``season``: the report's year, or the multi-year period when the table prints one
  (``yield_notes.year_range`` / ``periodo``).
* disease scales: a bare ``oidio``, ``roya_parda``, ... is a 0-9 visual score in the reports of
  2005/06 to 2011/12, whose disease tables print "(Escala visual 0-9)" in every column; the rule is
  applied by campaign, never by the size of a value. Elsewhere the report prints % or a score per
  table and the extraction did not record which: the bare key is passed on unchanged (the contract
  ignores it).
* ``regime_not_derivable``: why no regime may ever be derived from the yield for the row's group (a label
  mixing "secano" and "regadio", or a yield stratum "Rendimiento/Productividad alto/medio/bajo": a
  stratum is defined by the yield, so a yield cutoff on it is circular). The contract names it.
* ``productivity_class``: only what the group label itself states (yield stratum, "alto potencial
  humedos", "aridos y semiaridos"); never inferred from a yield. Not part of any key.
* numbers printed as text ("-2") are numbers; ``None`` groups are dropped.
"""
from __future__ import annotations

import json
import re
import unicodedata
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from . import AdapterError, AdapterResult, WarningLog, fingerprints, sorted_files

SOURCE_ID = "GENVCE"
EXTRACTIONS_GLOB = "data/extractions/*.json"

# Keys of an extracted row; anything else is an error (a new extraction field must be decided on).
_ROW_KEYS = frozenset({
    "crop", "crop_scientific", "variety", "agroclimatic_zone", "year", "yield_kg_ha", "yield_relative_pct",
    "quality_params", "disease_scores", "agronomic_traits", "yield_notes", "special_status",
    "irrigation_regime", "trial_location", "page_in_issue", "table_number", "confidence",
})
_GROUPS = ("quality_params", "disease_scores", "agronomic_traits")
_METADATA_KEYS = ("article_title", "issue_period", "year", "article_topic")

# The extraction names the crop twice: the printed group label ("Cebada de ciclo largo") and the
# species. The label is the crop; the species is only checked against it.
_SPECIES_BY_LABEL_STEM = {
    "maíz": "Zea mays", "cebada": "Hordeum vulgare", "trigo": "Triticum aestivum", "colza": "Brassica napus",
}

# Campaigns whose disease tables print "(Escala visual 0-9)" in every disease column (checked in the
# reports of 2005/06, 2006/07, 2007/08, 2008/09, 2009/10, 2010/11 and 2011/12).
_SCORE_0_9_CAMPAIGNS = frozenset({
    "2005/2006", "2006/2007", "2007/2008", "2008/2009", "2009/2010", "2010/2011", "2011/2012",
})
_BARE_DISEASE_KEYS = ("oidio", "roya_parda", "roya_amarilla", "helmintosporiosis", "rincosporiosis")
# The 2013/14 report prints rincosporiosis as "(Escala visual 0-9)" while the extraction wrote the
# key "rincosporiosis_escala" without the range.
_SCORE_0_9_BY_KEY_AND_CAMPAIGN = {("rincosporiosis_escala", "2013/2014"): "rincosporiosis_escala_0_9"}

# The scale a key names, when it names one (``..._0_9``, ``..._escala_0_5``). A value outside it is an
# extraction defect (a percentage under a score's name), not a score; the engine refuses such a value,
# so it is left out here and reported.
_NAMED_SCALE = re.compile(r"_(?:escala_)?0_(?P<top>[59])$")

_NUMBER_TEXT = re.compile(r"^[+-]?\d+(?:[.,]\d+)?$")
_TABLE_PREFIX = re.compile(r"^\s*tabla\s*", re.IGNORECASE)


def _fold(text: str) -> str:
    return re.sub(r"\s+", " ", unicodedata.normalize("NFC", text).strip().casefold())


def _text(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise AdapterError(f"expected text, got {value!r}")
    stripped = unicodedata.normalize("NFC", value).strip()
    return stripped or None


def _irrigation_stated_by(zone: str | None) -> str | None:
    """The regime the observed zone or stratum label itself states, else None."""
    if zone is None:
        return None
    label = _fold(zone)
    rainfed, irrigated = "secano" in label, "regad" in label
    if rainfed == irrigated:  # neither, or "Secanos y regadios templados"
        return None
    return "secano" if rainfed else "regadío"


_STRATUM_LABEL = re.compile(r"^(rendimiento|productividad) (alt|medi|baj)[oa]$")
_STRATUM_LEVEL = {"alt": "high", "medi": "medium", "baj": "low"}


def _productivity_class(zone: str | None) -> str | None:
    """The productivity class the group label itself states, else None (never read from the yield)."""
    if zone is None:
        return None
    label = _fold(zone)
    stratum = _STRATUM_LABEL.match(label)
    if stratum:
        return f"yield_stratum_{_STRATUM_LEVEL[stratum.group(2)]}"
    if "secano" in label and "alto potencial" in label:
        return "rainfed_humid_high_potential"
    if "secano" in label and re.search(r"semi.?rid", label):
        return "rainfed_arid_semiarid"
    return None


def _regime_not_derivable(zone: str | None) -> str | None:
    """Why a group can never get a regime derived from its yield, else None.

    A group of mixed regimes has no single regime; a yield stratum is defined by the yield itself, so a
    yield cutoff on it would be circular.
    """
    if zone is None:
        return None
    label = _fold(zone)
    if "secano" in label and "regad" in label:
        return "group label mixes rainfed and irrigated"
    if _STRATUM_LABEL.match(label):
        return "group label is a yield stratum"
    return None


def _table_number(value: Any, where: str) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise AdapterError(f"{where}: table_number {value!r} is not a table number")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        number = _TABLE_PREFIX.sub("", value).strip()
        if number:
            return number
    raise AdapterError(f"{where}: table_number {value!r} is not a table number")


def _clean_group(group: Any, name: str, where: str, log: WarningLog) -> dict[str, Any]:
    """A trait group without its format noise: numbers printed as text are numbers."""
    if group is None:
        return {}
    if not isinstance(group, Mapping):
        raise AdapterError(f"{where}: {name} must be a mapping, got {type(group).__name__}")
    cleaned: dict[str, Any] = {}
    for key, value in group.items():
        if isinstance(value, str) and _NUMBER_TEXT.match(value.strip()):
            log.add("number_text_parsed", "a number printed as text was read as a number",
                    f"{where}.{name}.{key}")
            value = float(value.strip().replace(",", "."))
            value = int(value) if value.is_integer() else value
        cleaned[key] = value
    return cleaned


def _rename_bare_disease_keys(
    disease: dict[str, Any], campaign: str, where: str, log: WarningLog,
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in disease.items():
        new_key = key
        if key in _BARE_DISEASE_KEYS:
            if campaign in _SCORE_0_9_CAMPAIGNS:
                new_key = f"{key}_escala_0_9"
                log.add("disease_scale_resolved",
                        "bare disease key read as the 0-9 visual score its campaign's tables print",
                        f"{where}.disease_scores.{key}")
            else:
                log.add("disease_scale_unresolved",
                        "bare disease key passed on unchanged: the report prints % or a score per "
                        "table and the extraction did not record which",
                        f"{where}.disease_scores.{key}")
        elif (key, campaign) in _SCORE_0_9_BY_KEY_AND_CAMPAIGN:
            new_key = _SCORE_0_9_BY_KEY_AND_CAMPAIGN[(key, campaign)]
            log.add("disease_scale_resolved", "disease key read as the 0-9 visual score its report prints",
                    f"{where}.disease_scores.{key}")
        if new_key in out:
            raise AdapterError(f"{where}: disease_scores holds both {key!r} and {new_key!r} after renaming")
        out[new_key] = value
    return out


def _guard_named_scales(group: dict[str, Any], name: str, where: str, log: WarningLog) -> dict[str, Any]:
    """Leave out (and report) a value outside the 0-N scale its own key names."""
    guarded: dict[str, Any] = {}
    for key, value in group.items():
        scale = _NAMED_SCALE.search(key)
        if (scale and isinstance(value, (int, float)) and not isinstance(value, bool)
                and not 0 <= value <= int(scale.group("top"))):
            log.add("ordinal_value_out_of_scale",
                    "a value outside the 0-N scale its key names was left out (extraction defect)",
                    f"{where}.{name}.{key}={value}")
            value = None
        guarded[key] = value
    return guarded


def _check_species(label: str, species: Any, where: str) -> None:
    stem = _fold(label).split(" ")[0]
    expected = _SPECIES_BY_LABEL_STEM.get(stem)
    if expected is None:
        raise AdapterError(f"{where}: crop label {label!r} is not one the adapter knows")
    if species != expected:
        raise AdapterError(f"{where}: crop label {label!r} but species {species!r} (expected {expected!r})")


def rows_from_extraction(extraction: Mapping[str, Any], file_name: str, log: WarningLog) -> list[dict[str, Any]]:
    """The raw rows of one extraction file (a campaign and crop group), in file order."""
    if not isinstance(extraction, Mapping) or "metadata" not in extraction:
        raise AdapterError(f"{file_name}: not a GENVCE extraction (no metadata)")
    metadata = extraction["metadata"]
    missing = [key for key in _METADATA_KEYS if metadata.get(key) in (None, "")]
    if missing:
        raise AdapterError(f"{file_name}: metadata lacks {missing}")
    if metadata.get("source") not in (None, SOURCE_ID):
        raise AdapterError(f"{file_name}: metadata.source is {metadata.get('source')!r}")
    campaign = str(metadata["issue_period"]).replace("-", "/")
    document = {
        "title": metadata["article_title"], "issue": str(metadata["issue_period"]), "year": metadata["year"],
        "topic": metadata["article_topic"],
    }
    rows: list[dict[str, Any]] = []
    for index, trial in enumerate(extraction.get("variety_trials") or []):
        where = f"{file_name}[{index}]"
        unknown = sorted(set(trial) - _ROW_KEYS)
        if unknown:
            raise AdapterError(f"{where}: unknown extraction field(s) {unknown}: decide on them in the adapter")
        if trial.get("trial_location") not in (None, ""):
            raise AdapterError(f"{where}: trial_location {trial['trial_location']!r}: GENVCE rows carry no trial "
                               "location; a value would change the data model")
        label = _text(trial.get("crop"))
        if label is None:
            raise AdapterError(f"{where}: no crop label")
        _check_species(label, trial.get("crop_scientific"), where)

        notes = trial.get("yield_notes")
        if notes is not None and not isinstance(notes, Mapping):
            raise AdapterError(f"{where}: yield_notes must be a mapping or null")
        notes = dict(notes or {})
        table_zone = _text(notes.pop("zone", None))
        year_range = _text(notes.pop("year_range", None))
        period = _text(notes.pop("periodo", None))
        if year_range and period:
            raise AdapterError(f"{where}: both year_range and periodo in yield_notes")

        extracted_zone = _text(trial.get("agroclimatic_zone"))
        zone = table_zone or extracted_zone
        if table_zone and extracted_zone and _fold(table_zone) != _fold(extracted_zone):
            log.add("zone_label_from_table",
                    "the table's own zone label (yield_notes.zone) replaces the extraction's agroclimatic relabel",
                    where)

        stated = _irrigation_stated_by(zone)
        extracted_irrigation = _text(trial.get("irrigation_regime"))
        if extracted_irrigation is not None:
            if stated is None:
                log.add("irrigation_not_stated_by_source",
                        "irrigation_regime left out: no zone label states it (the extraction assumed it)", where)
            elif _fold(extracted_irrigation) != _fold(stated):
                log.add("irrigation_conflicts_with_zone_label",
                        "irrigation_regime contradicts the zone label; the label is used", where)
        irrigation = stated

        production_system = "ecológico" if "ecológico" in _fold(label) else None
        year = trial.get("year")
        if isinstance(year, bool) or not isinstance(year, int):
            raise AdapterError(f"{where}: year {year!r} is not a whole year")

        groups = {name: _clean_group(trial.get(name), name, where, log) for name in _GROUPS}
        groups["disease_scores"] = _rename_bare_disease_keys(groups["disease_scores"], campaign, where, log)
        groups = {name: _guard_named_scales(group, name, where, log) for name, group in groups.items()}

        row: dict[str, Any] = {
            "doc": dict(document),
            "table": {"number": _table_number(trial.get("table_number"), where), "page": trial.get("page_in_issue")},
            "crop": label,
            "variety": _text(trial.get("variety")),
            "zone": zone,
            "season": year_range or period or str(year),
            "irrigation": irrigation,
            "regime_not_derivable": _regime_not_derivable(zone),
            "productivity_class": _productivity_class(zone),
            "production_system": production_system,
            "yield_kg_ha": trial.get("yield_kg_ha"),
            "yield_relative_pct": trial.get("yield_relative_pct"),
            "special_status": _text(trial.get("special_status")),
            "confidence": trial.get("confidence"),
        }
        for name, group in groups.items():
            if group:
                row[name] = group
        if notes:
            row["yield_notes"] = notes
        rows.append(row)
    return rows


def load(source_dir: str | Path) -> AdapterResult:
    """Read every extraction file of ``<source_dir>/data/extractions`` into raw rows.

    ``source_dir`` is the GENVCE folder of the raw-data repository. Files are read in name order;
    row order is file order, so the result is a function of the files alone.
    """
    root = Path(source_dir)
    files = sorted_files(root, EXTRACTIONS_GLOB)
    # batch_stats.json sits next to the extractions and is not one of them
    files = [path for path in files if path.name != "batch_stats.json"]
    if not files:
        raise AdapterError(f"no extraction files under {root / 'data/extractions'}")
    log = WarningLog()
    rows: list[dict[str, Any]] = []
    for path in files:
        try:
            extraction = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise AdapterError(f"{path.name}: cannot read: {exc}") from exc
        rows.extend(rows_from_extraction(extraction, path.name, log))
    return AdapterResult(rows=tuple(rows), warnings=log.result(), inputs=fingerprints(root, files))
