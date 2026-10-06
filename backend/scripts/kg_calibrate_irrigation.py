"""Calibrate the per-crop irrigation yield cutoffs on the rows whose regime the source states.

Reads the raw extractions of GENVCE and CREA (the private raw-data repository), builds both bundles
through their adapters and contracts, takes the units whose regime the source itself states
(``raw_irrigation`` is set) and calibrates one cutoff pair per crop (see ``app.kg.irrigation_cutoff``).
It then reports how the cutoffs would judge the units whose source is silent, and how that compares
with the regime the extraction had assumed for them.

Nothing is written unless asked: ``--report FILE`` writes the markdown report (keep it outside the
repository), ``--write-registry`` rewrites the thresholds of ``data/registries/irrigation_thresholds.yaml``
(the file's header is kept; ``owner_approval`` is never filled by this script) and is refused (exit 2, nothing
written) for cutoffs that fail the leave-one-year-out guard (``MAX_HELDOUT_ERROR``, ``MIN_HELDOUT_YEARS``).

    NKZ_DATA_SOURCES_DIR=<raw repo> python scripts/kg_calibrate_irrigation.py --report /path/outside/repo.md
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import subprocess
import sys
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import yaml

from app.kg import identity
from app.kg.adapters import crea, genvce
from app.kg.contracts import Bundle, load_contract, run_contract
from app.kg.irrigation_cutoff import (
    EPSILON,
    MAX_DECIDED_ERROR,
    MAX_HELDOUT_ERROR,
    MIN_HELDOUT_YEARS,
    MIN_LABELLED,
    STEP_KG_HA,
    Calibration,
    calibrate,
    classify_yield,
    leave_one_year_out,
)
from app.kg.registries import (
    DEFAULT_REGISTRIES_PATH,
    Registries,
    load_registries,
)

CONTRACTS = Path(__file__).resolve().parents[1] / "data" / "sources"
THRESHOLDS_FILE = DEFAULT_REGISTRIES_PATH / "irrigation_thresholds.yaml"
CROP_ORDER = ("ZEAMX", "HORVX", "TRZAX", "BRSNN")
# what the extraction wrote, as a regime (it printed "secano", "regadío" or "irrigato", or nothing)
ASSUMED = {"secano": "rainfed", "regadío": "irrigated", "irrigato": "irrigated"}


def build_bundles(raw_dir: Path, registries: Registries) -> dict[str, tuple[Any, Bundle]]:
    out: dict[str, tuple[Any, Bundle]] = {}
    for source, adapter in (("GENVCE", genvce), ("CREA", crea)):
        result = adapter.load(raw_dir / source.lower())
        out[source] = (result, run_contract(load_contract(CONTRACTS / f"{source}.yaml"), registries, result.rows))
    return out


def labelled_yields(bundles: Iterable[Bundle], registries: Registries) -> dict[str, dict[str, Any]]:
    """Per crop: the yields (kg/ha) of the units whose source states the regime, and the sources they come from."""
    rainfed, irrigated = registries.vocab("irrigation", "rainfed"), registries.vocab("irrigation", "irrigated")
    out: dict[str, dict[str, Any]] = collections.defaultdict(
        lambda: {"rainfed": [], "irrigated": [], "sources": set()})
    for bundle in bundles:
        for unit in bundle.units:
            if unit.raw_irrigation is None or unit.yield_kg_ha is None:
                continue
            regime = {rainfed: "rainfed", irrigated: "irrigated"}.get(unit.irrigation_regime)
            if regime is not None:
                out[unit.crop_eppo][regime].append(unit.yield_kg_ha)
                out[unit.crop_eppo]["sources"].add(bundle.source_id)
    return out


def labelled_rows(bundles: Iterable[Bundle], registries: Registries) -> dict[str, list[tuple[int | None, float, str]]]:
    """Per crop: (year, yield kg/ha, regime) of the units whose source states the regime."""
    rainfed, irrigated = registries.vocab("irrigation", "rainfed"), registries.vocab("irrigation", "irrigated")
    out: dict[str, list[tuple[int | None, float, str]]] = collections.defaultdict(list)
    for bundle in bundles:
        for unit in bundle.units:
            regime = {rainfed: "rainfed", irrigated: "irrigated"}.get(unit.irrigation_regime)
            if unit.raw_irrigation is not None and unit.yield_kg_ha is not None and regime is not None:
                out[unit.crop_eppo].append((unit.year, unit.yield_kg_ha, regime))
    return out


def registry_refusals(
    calibrations: Sequence[Calibration], rows: dict[str, list[tuple[int | None, float, str]]],
) -> list[str]:
    """Why the calibrated crops may not be written (leave-one-year-out guard); empty when all hold out of sample."""
    out = []
    for c in calibrations:
        if not c.calibrated:
            continue
        why = leave_one_year_out(c.crop, rows.get(c.crop, [])).refusal()  # type: ignore[arg-type]
        if why:
            out.append(why)
    return out


def _raw_trials(source: str, raw_dir: Path) -> list[dict[str, Any]]:
    """The extraction's trials in the order the adapter emits its rows (to read the dropped regime)."""
    folder = raw_dir / source.lower() / "data" / "extractions"
    trials: list[dict[str, Any]] = []
    for path in sorted(folder.glob("*.json"), key=lambda p: p.name):
        if source == "GENVCE" and path.name == "batch_stats.json":
            continue
        if source == "CREA" and not path.name.startswith("crea_mais_20"):
            continue
        trials.extend(json.loads(path.read_text(encoding="utf-8")).get("variety_trials") or [])
    return trials


def assumed_regimes(source: str, raw_dir: Path, result: Any, bundle: Bundle) -> dict[str, str | None]:
    """Unit key -> the regime the extraction assumed for it ('rainfed', 'irrigated' or None)."""
    trials = _raw_trials(source, raw_dir)
    if len(trials) != len(result.rows):
        raise SystemExit(f"{source}: {len(trials)} extracted trials but {len(result.rows)} adapter rows")
    documents = {identity.document_key(d): d for d in bundle.documents}
    by_unit: dict[tuple, str] = {}
    for unit in bundle.units:
        doc = documents[unit.document_key]
        group = unit.factor_levels[0].level if unit.factor_levels else None
        by_unit[(doc.title, doc.issue, group, unit.raw_variety, unit.raw_site, unit.raw_season,
                 unit.row_discriminator)] = identity.unit_key(unit)
    found: dict[str, str | None] = {}
    for row, trial in zip(result.rows, trials, strict=True):
        doc = row["doc"]
        key = (doc["title"], doc.get("issue"), row["crop"] if source == "GENVCE" else None, row["variety"],
               row.get("zone") if source == "GENVCE" else row.get("site"), row["season"],
               row["table"]["number"] if source == "GENVCE" else None)
        unit_key = by_unit.get(key)
        if unit_key is None:
            raise SystemExit(f"{source}: no unit for row {key}")
        found[unit_key] = ASSUMED.get(trial.get("irrigation_regime") or "")
    if len(found) != len(bundle.units):
        raise SystemExit(f"{source}: {len(found)} rows matched {len(bundle.units)} units")
    return found


def silent_judgement(
    source: str, raw_dir: Path, result: Any, bundle: Bundle, calibrations: dict[str, Calibration],
) -> dict[str, collections.Counter]:
    """Per crop: how the cutoffs judge the silent units, against what the extraction had assumed."""
    assumed = assumed_regimes(source, raw_dir, result, bundle)
    out: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    for unit in bundle.units:
        if unit.raw_irrigation is not None:
            continue
        calibration = calibrations.get(unit.crop_eppo)
        if unit.yield_kg_ha is None:
            band = "no yield"
        elif calibration is None or not calibration.calibrated:
            band = "no cutoff"
        else:
            band = classify_yield(unit.yield_kg_ha, calibration.low_kg_ha, calibration.high_kg_ha) or "ambiguous"
        out[unit.crop_eppo][(band, assumed[identity.unit_key(unit)] or "none")] += 1
    return out


def render_thresholds(
    calibrations: Sequence[Calibration], raw_commit: str | None, sources: dict[str, set[str]] | None = None,
) -> str:
    """The ``thresholds:`` list of the registry file for these calibrations."""
    entries: list[dict[str, Any]] = []
    for c in calibrations:
        evidence: dict[str, Any] = {
            "method": "per-class tail error, rounded outward", "epsilon": EPSILON,
            "labelled_sources": sorted((sources or {}).get(c.crop, ())),
            "n_rainfed": c.n_rainfed, "n_irrigated": c.n_irrigated,
            "rainfed_quantiles": c.rainfed_quantiles, "irrigated_quantiles": c.irrigated_quantiles,
        }
        if c.separation is not None:
            evidence["separation"] = round(c.separation, 3)
        if c.calibrated:
            evidence.update(
                misclassified_rainfed=c.rainfed_judged_irrigated, misclassified_irrigated=c.irrigated_judged_rainfed,
                n_ambiguous=c.n_ambiguous, error_rate=round(c.error_rate, 4),
                decided_error_rate=round(c.decided_error_rate, 4), max_decided_error=MAX_DECIDED_ERROR)
        else:
            evidence["reason"] = c.reason
        if raw_commit:
            evidence["raw_data_commit"] = raw_commit
        entry: dict[str, Any] = {"crop": c.crop, "status": "assumption" if c.calibrated else "not_calibrated"}
        if c.calibrated:
            entry.update(low_kg_ha=c.low_kg_ha, high_kg_ha=c.high_kg_ha)
        entry.update(owner_approval=None, evidence=evidence)
        entries.append(entry)
    return yaml.safe_dump({"thresholds": entries}, allow_unicode=True, sort_keys=False, width=110).split(
        "thresholds:\n", 1)[1]


def write_registry(text: str) -> None:
    current = THRESHOLDS_FILE.read_text(encoding="utf-8")
    head = current.split("\nthresholds:\n", 1)[0]
    THRESHOLDS_FILE.write_text(head + "\nthresholds:\n" + text, encoding="utf-8")


def _fmt(value: float | None) -> str:
    return "-" if value is None else f"{value:,.0f}".replace(",", " ")


def render_report(
    calibrations: Sequence[Calibration], judgement: dict[str, collections.Counter], raw_commit: str | None,
) -> str:
    lines = [
        (f"parameters: epsilon {EPSILON:.0%} per class, step {STEP_KG_HA:g} kg/ha, minimum {MIN_LABELLED} labelled "
         f"rows per regime, maximum error among decided rows {MAX_DECIDED_ERROR:.0%}; "
         f"raw data {raw_commit or 'unknown'}"),
        "",
        ("| crop | labelled rainfed | labelled irrigated | P(irrigated > rainfed) | low | high | wrong / labelled "
         "| error | decided error | ambiguous labelled | status |"),
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for c in calibrations:
        rate = "-" if c.error_rate is None else f"{c.error_rate:.1%}"
        decided = "-" if c.decided_error_rate is None else f"{c.decided_error_rate:.1%}"
        wrong = "-" if c.n_wrong is None else f"{c.n_wrong} / {c.n_labelled}"
        ambiguous = "-" if c.n_ambiguous is None else str(c.n_ambiguous)
        sep = "-" if c.separation is None else f"{c.separation:.2f}"
        lines.append(f"| {c.crop} | {c.n_rainfed} | {c.n_irrigated} | {sep} | {_fmt(c.low_kg_ha)} | {_fmt(c.high_kg_ha)} "
                     f"| {wrong} | {rate} | {decided} | {ambiguous} | {'assumption' if c.calibrated else 'no cutoff'} |")
    lines += ["", "quantiles of the labelled yields (kg/ha)", "",
              "| crop | regime | n | p05 | p25 | p50 | p75 | p95 |", "|---|---|---:|---:|---:|---:|---:|---:|"]
    for c in calibrations:
        for regime, n, quant in (("rainfed", c.n_rainfed, c.rainfed_quantiles),
                                 ("irrigated", c.n_irrigated, c.irrigated_quantiles)):
            cells = " | ".join(_fmt(quant.get(level)) for level in ("p05", "p25", "p50", "p75", "p95"))
            lines.append(f"| {c.crop} | {regime} | {n} | {cells} |")
    lines += ["", "silent units (the source states no regime) by band, against the regime the extraction assumed", "",
              "| crop | band | extraction: secano | extraction: regadío / irrigato | extraction: none | total |",
              "|---|---|---:|---:|---:|---:|"]
    for crop in CROP_ORDER:
        counter = judgement.get(crop, collections.Counter())
        for band in ("rainfed", "irrigated", "ambiguous", "no cutoff", "no yield"):
            cells = [sum(n for (b, a), n in counter.items() if b == band and a == regime)
                     for regime in ("rainfed", "irrigated", "none")]
            if sum(cells):
                lines.append(f"| {crop} | {band} | {cells[0]} | {cells[1]} | {cells[2]} | {sum(cells)} |")
    return "\n".join(lines) + "\n"


def git_commit(raw_dir: Path) -> str | None:
    try:
        head = subprocess.run(["git", "-C", str(raw_dir), "rev-parse", "HEAD"], capture_output=True, text=True,
                              check=True).stdout.strip()
        dirty = subprocess.run(["git", "-C", str(raw_dir), "status", "--porcelain"], capture_output=True,
                               text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None
    return head + ("+dirty" if dirty else "")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--raw-dir", default=os.environ.get("NKZ_DATA_SOURCES_DIR", ""))
    parser.add_argument("--report", help="write the markdown report here (outside the repository)")
    parser.add_argument("--write-registry", action="store_true", help="rewrite the thresholds of the registry file")
    args = parser.parse_args(argv)
    if not args.raw_dir:
        parser.error("give --raw-dir or set NKZ_DATA_SOURCES_DIR")
    raw_dir = Path(args.raw_dir)
    registries = load_registries()
    built = build_bundles(raw_dir, registries)
    labelled = labelled_yields((bundle for _, bundle in built.values()), registries)
    calibrations = [calibrate(crop, labelled[crop]["rainfed"], labelled[crop]["irrigated"]) for crop in CROP_ORDER]
    by_crop = {c.crop: c for c in calibrations}
    judgement: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    for source, (result, bundle) in built.items():
        for crop, counter in silent_judgement(source, raw_dir, result, bundle, by_crop).items():
            judgement[crop].update(counter)
    commit = git_commit(raw_dir)
    report = render_report(calibrations, judgement, commit)
    print(report)
    if args.report:
        Path(args.report).write_text(report, encoding="utf-8")
    if args.write_registry:
        refusals = registry_refusals(calibrations, labelled_rows((bundle for _, bundle in built.values()), registries))
        if refusals:
            print(f"refused to write {THRESHOLDS_FILE.name}: the cutoffs do not hold on years they were not fitted "
                  f"on (limit {MAX_HELDOUT_ERROR:.0%} held-out error, {MIN_HELDOUT_YEARS}+ held-out years per "
                  "regime):", file=sys.stderr)
            for why in refusals:
                print(f"  - {why}", file=sys.stderr)
            return 2
        write_registry(render_thresholds(calibrations, commit, {c: set(d["sources"]) for c, d in labelled.items()}))
        print(f"rewrote {THRESHOLDS_FILE.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
