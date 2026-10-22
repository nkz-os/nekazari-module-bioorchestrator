"""CREA adapter: the extraction files of the CREA maize variety booklets -> raw rows.

Input: ``<source folder>/data/extractions/``. The licensed data are the five booklets "Risultati reti
nazionali di confronto varietale mais" 2021 to 2025 (``crea_mais_<year>.json``, one file per
booklet): per year, the mean of the late grain hybrids (FAO 500-600-700) over the network's
locations ("Media N Località") and the trial of one field site. Two other files sit in the same
folder and are NOT CREA data; their rows are excluded one by one and each is reported:

* ``crea_mais_1993.json``: 19 rows of Romanian winter wheat from an unrelated 1993 paper (a
  fertilisation trial at one Romanian station), extracted under the CREA source and stored in the
  legacy graph as CREA maize. The paper is not licensed.
* ``grano_extractions.json``: 3 rows made by a test script from a dummy PDF.

Any other file in the folder is an error: a new file must be decided on, not read by accident.

The crop is the EPPO code the file declares (``eppo``), checked against the rows' printed label
("Mais") and species. The legacy graph also held every maize row under ``ZEAMA``; the crops registry
resolves that alias, so such a twin file collapses into the same units (counted by the engine).

``irrigation``: the booklets state a regime per location in the "Scheda Agronomica ... Irrigazioni (n°
e modalità)" sheet of 2023, 2024 and 2025 (every location of those years' tables shows at least one
irrigation, except Merlino in 2025, which is blank) and flag a dry location as "Asciutta" (Masi San
Giacomo in 2022). The extraction wrote "irrigato" on every row; it is passed on only where the booklet
supports it for the whole row: the field site of 2023, 2024 and 2025 and the network means of 2023 and
2024. Everywhere else it is left out and counted.

``yield_kg_ha``: the booklets print q/ha at 15.5 % moisture; the extraction wrote kg/ha (= q/ha x 100,
checked for 320 of 320 rows against the booklet text).
"""
from __future__ import annotations

import json
import re
import unicodedata
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from . import AdapterError, AdapterResult, WarningLog, fingerprints, sorted_files

SOURCE_ID = "CREA"
EXTRACTIONS_DIR = "data/extractions"
MANIFEST = "RAW_MANIFEST.sha256"

_BOOKLET_FILE = re.compile(r"^crea_mais_(?P<year>20\d{2})\.json$")
_BOOKLET_TITLE = "Risultati reti nazionali di confronto varietale mais {year}"  # the cover prints it in capitals
_BOOKLET_PDF = "data/pdfs/Fascicolo_risultati_Mais_{year}.pdf"

_OUT_OF_SCOPE = {
    "crea_mais_1993.json": (
        "mislabelled_rows_excluded",
        ("row excluded: Romanian winter wheat from an unrelated 1993 paper, extracted under CREA and stored "
         "in the legacy graph as CREA maize; not CREA data and not licensed")),
    "grano_extractions.json": (
        "placeholder_rows_excluded",
        "row excluded: made by a test script from a dummy PDF, not a trial report"),
}

_ROW_KEYS = frozenset({
    "crop", "crop_scientific", "variety", "agroclimatic_zone", "year", "yield_kg_ha", "yield_relative_pct",
    "quality_params", "disease_scores", "agronomic_traits", "yield_notes", "special_status",
    "irrigation_regime", "trial_location", "page_in_issue", "table_number", "confidence",
})
_GROUPS = ("quality_params", "disease_scores", "agronomic_traits", "yield_notes")

# (booklet year, location as printed) where the booklet supports "irrigated" for every location the row
# covers: the sheet "Scheda Agronomica campi Granella tardivi" lists at least one irrigation for each.
_IRRIGATED_IN_BOOKLET = frozenset({
    (2023, "Media 8 Località"), (2023, "Villafranca Piemonte (TO)"),
    (2024, "Media 13 Località"), (2024, "Villafranca Piemonte (TO)"),
    (2025, "Villafranca Piemonte (TO)"),
})
# Network means that include a location the booklet flags "Asciutta": mixed regimes, not one regime.
_MIXED_IN_BOOKLET = frozenset({(2022, "Media 10 Località")})


def _fold(text: str) -> str:
    return re.sub(r"\s+", " ", unicodedata.normalize("NFC", text).strip().casefold())


def _text(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise AdapterError(f"expected text, got {value!r}")
    stripped = unicodedata.normalize("NFC", value).strip()
    return stripped or None


def _group(value: Any, name: str, where: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise AdapterError(f"{where}: {name} must be a mapping, got {type(value).__name__}")
    return dict(value)


def _manifest_sha(source_dir: Path, relative: str) -> str | None:
    """The fingerprint the raw repository records for a PDF, or None when it keeps no manifest."""
    manifest = source_dir / MANIFEST
    if not manifest.exists():
        return None
    for line in manifest.read_text(encoding="utf-8").splitlines():
        parts = line.split()
        if len(parts) == 3 and parts[2] == relative and re.fullmatch(r"[0-9a-f]{64}", parts[0]):
            return parts[0]
    raise AdapterError(f"{MANIFEST} has no entry for {relative}")


def booklet_rows(
    extraction: Mapping[str, Any], file_name: str, log: WarningLog, *, pdf_sha256: str | None = None,
) -> list[dict[str, Any]]:
    """The raw rows of one booklet file, in file order."""
    year, eppo = extraction.get("year"), extraction.get("eppo")
    if isinstance(year, bool) or not isinstance(year, int) or not isinstance(eppo, str) or not eppo.strip():
        raise AdapterError(f"{file_name}: a booklet extraction names its year and the EPPO code of its crop")
    document = {
        "title": _BOOKLET_TITLE.format(year=year), "year": year,
        "file_name": _BOOKLET_PDF.format(year=year).rsplit("/", 1)[-1], "raw_sha256": pdf_sha256,
    }
    out: list[dict[str, Any]] = []
    for index, trial in enumerate(extraction.get("variety_trials") or []):
        where = f"{file_name}[{index}]"
        unknown = sorted(set(trial) - _ROW_KEYS)
        if unknown:
            raise AdapterError(f"{where}: unknown extraction field(s) {unknown}: decide on them in the adapter")
        if _fold(trial.get("crop") or "") != "mais" or trial.get("crop_scientific") != "Zea mays":
            raise AdapterError(f"{where}: crop {trial.get('crop')!r} / {trial.get('crop_scientific')!r} is not the "
                               "maize of a maize booklet")
        if trial.get("year") != year:
            raise AdapterError(f"{where}: row year {trial.get('year')!r} differs from the booklet year {year}")
        place = _text(trial.get("trial_location"))
        irrigation = None
        extracted = _text(trial.get("irrigation_regime"))
        if (year, place) in _MIXED_IN_BOOKLET:
            if extracted is not None:
                log.add("irrigation_mixed_regimes",
                        "irrigation_regime left out: the mean covers a location the booklet flags 'Asciutta' "
                        "next to irrigated ones", where)
        elif (year, place) in _IRRIGATED_IN_BOOKLET:
            irrigation = extracted
        elif extracted is not None:
            log.add("irrigation_not_stated_by_source",
                    "irrigation_regime left out: the booklet states no regime for every location of this row "
                    "(no agronomic sheet, or a location with a blank entry)", where)
        row: dict[str, Any] = {
            "doc": dict(document),
            "table": {"number": None if trial.get("table_number") is None else str(trial["table_number"]),
                      "page": trial.get("page_in_issue")},
            "crop": eppo.strip(),
            "variety": _text(trial.get("variety")),
            "site": place,
            "season": str(year),
            "irrigation": irrigation,
            "agroclimatic_zone": _text(trial.get("agroclimatic_zone")),
            "yield_kg_ha": trial.get("yield_kg_ha"),
            "yield_relative_pct": trial.get("yield_relative_pct"),
            "special_status": _text(trial.get("special_status")),
            "confidence": trial.get("confidence"),
        }
        for name in _GROUPS:
            group = _group(trial.get(name), name, where)
            if group:
                row[name] = group
        out.append(row)
    return out


def _excluded(path: Path, log: WarningLog) -> None:
    code, message = _OUT_OF_SCOPE[path.name]
    try:
        trials = json.loads(path.read_text(encoding="utf-8")).get("variety_trials") or []
    except (OSError, json.JSONDecodeError, AttributeError) as exc:
        raise AdapterError(f"{path.name}: cannot read: {exc}") from exc
    for index, trial in enumerate(trials):
        log.add(code, message, f"{path.name}[{index}] {trial.get('crop')} / {trial.get('variety')}", keep_all=True)


def load(source_dir: str | Path) -> AdapterResult:
    """Read the CREA booklet extractions of ``<source_dir>/data/extractions`` into raw rows.

    ``source_dir`` is the CREA folder of the raw-data repository. Files are read in name order; the
    rows of the two out-of-scope files are excluded and reported one by one.
    """
    root = Path(source_dir)
    directory = root / EXTRACTIONS_DIR
    files = sorted_files(directory, "*.json")
    if not files:
        raise AdapterError(f"no extraction files under {directory}")
    log = WarningLog()
    rows: list[dict[str, Any]] = []
    read: list[Path] = []
    for path in files:
        match = _BOOKLET_FILE.match(path.name)
        if match is None and path.name not in _OUT_OF_SCOPE:
            raise AdapterError(f"{path.name}: an extraction file the CREA adapter does not know; decide whether "
                               "it is CREA data before reading it")
        read.append(path)
        if match is None:
            _excluded(path, log)
            continue
        try:
            extraction = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise AdapterError(f"{path.name}: cannot read: {exc}") from exc
        sha = _manifest_sha(root, _BOOKLET_PDF.format(year=int(match.group("year"))))
        rows.extend(booklet_rows(extraction, path.name, log, pdf_sha256=sha))
    if not rows:
        raise AdapterError(f"{directory}: no CREA booklet extraction (crea_mais_<year>.json) found")
    return AdapterResult(rows=tuple(rows), warnings=log.result(), inputs=fingerprints(root, read))


__all__ = ["SOURCE_ID", "booklet_rows", "load"]
