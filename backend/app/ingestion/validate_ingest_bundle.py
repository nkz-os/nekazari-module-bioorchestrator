"""Pre-ingestion validation gate for JSON-LD trial bundles.

A pure, DB-free validator: takes a JSON-LD bundle (scraper output or a review
candidate) and produces a structured pass/fail report — per-node issues plus a
bundle summary. It is a **read-only gate** that mutates nothing and never
touches Neo4j.

Why this exists: the knowledge-graph advisor is data-bound. A large
mass-ingestion effort is queued and ingesting it blindly would poison the graph
we just made honest. This gate runs before every `BaseIngester.merge()` to
catch structural defects (missing EPPO, orphan trials that won't link) and yield
dishonesty (impossible metrics, cumulative leaks, `note*1000` fabrication)
before they reach staging.

It mirrors the linking rule of `base_ingester._merge_relationships` exactly:
a trial links iff a TrialSite in the bundle shares its `source_id` AND matches
its `trial_location` (case-insensitive) against the site's `name` or
`municipality`. Rules 5 (name) and 14 (source_id) together encode that pair.

CLI:
    python -m app.ingestion.validate_ingest_bundle <bundle.jsonld>
    → prints report; exit 0 if no ERROR, 1 otherwise.
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

from app.common.source_registry import get_source
from app.ingestion.base_ingester import BaseIngester

# ── Metric plausibility (table-driven; unknown crop → skip) ─────────────────
# Upper bound for annual `yield_kg_ha` per EPPO. A kernel yield above the almond
# bound is almost certainly in-shell (wrong `yield_metric`); an olive value
# above its bound is likely a multi-year cumulative leaking into the annual
# field. Extend as more crops enter the ingestion pipeline.
PLAUSIBLE_MAX: dict[str, float] = {
    "PRNDU": 2500.0,   # almond, kernel basis
    "OLVEU": 3000.0,   # olive, fruit basis
}

# quality_params keys that flag a non-annual value leaking alongside yield_kg_ha
_CUMULATIVE_KEY = re.compile(r"cumulative|_(?:19|20)\d\d_(?:19|20)\d\d", re.IGNORECASE)


@dataclass
class Issue:
    severity: str      # "ERROR" | "WARNING" | "INFO"
    code: str          # e.g. "missing_eppo", "orphan_trial", "implausible_yield"
    node_id: str | None
    message: str


@dataclass
class ValidationReport:
    ok: bool                                     # False if any ERROR
    stats: dict                                  # counts (see rule 16)
    issues: list[Issue] = field(default_factory=list)

    def errors(self) -> list[Issue]:
        return [i for i in self.issues if i.severity == "ERROR"]

    def warnings(self) -> list[Issue]:
        return [i for i in self.issues if i.severity == "WARNING"]


# ── Helpers ─────────────────────────────────────────────────────────────────

def _norm(value) -> str:
    """Case-insensitive, trimmed key for name/location matching."""
    return str(value).strip().lower() if value else ""


def _node_id(node: dict) -> str | None:
    return node.get("@id") or node.get("mergeKey")


def _load_bundle(bundle: dict | str) -> dict:
    if isinstance(bundle, str):
        with open(bundle, encoding="utf-8") as f:
            return json.load(f)
    return bundle


def _graph(bundle: dict) -> list[dict]:
    graph = bundle.get("@graph")
    if not isinstance(graph, list):
        raise ValueError("bundle missing @graph list")
    return graph


def validate_bundle(bundle: dict | str) -> ValidationReport:
    """Validate a JSON-LD bundle. Accepts a parsed dict or a path.

    Returns a `ValidationReport`; `ok` is False if any ERROR issue was raised.
    Pure and read-only — never mutates the bundle or touches the database.
    """
    data = _load_bundle(bundle)
    graph = _graph(data)

    sites = [n for n in graph if n.get("@type") == "TrialSite"]
    trials = [n for n in graph if n.get("@type") == "VarietyTrial"]

    issues: list[Issue] = []

    # Site lookup by name/municipality — source-agnostic (matches _merge_relationships).
    sites_by_key: dict[str, list[dict]] = {}
    names_all: set[str] = set()
    for s in sites:
        for field_name in ("name", "municipality"):
            key = _norm(s.get(field_name))
            if key:
                sites_by_key.setdefault(key, []).append(s)
                names_all.add(key)

    for site in sites:
        issues.extend(_check_site(site))

    for trial in trials:
        issues.extend(_check_trial(trial, sites_by_key, names_all))

    issues.extend(_check_registered_sources(trials, sites))

    stats = _build_stats(trials, sites, issues)
    ok = not any(i.severity == "ERROR" for i in issues)
    return ValidationReport(ok=ok, stats=stats, issues=issues)


def _check_site(site: dict) -> list[Issue]:
    out: list[Issue] = []
    nid = _node_id(site)

    if not str(site.get("name") or "").strip():
        out.append(Issue("ERROR", "site_no_name", nid, "TrialSite missing name"))

    if not str(site.get("climate_class") or "").strip():
        out.append(Issue("WARNING", "site_no_climate", nid,
                         f"TrialSite '{site.get('name')}' missing climate_class"))

    if site.get("latitude") is None or site.get("longitude") is None:
        out.append(Issue("WARNING", "site_no_coords", nid,
                         f"TrialSite '{site.get('name')}' missing latitude/longitude"))
    return out


def _check_trial(trial: dict, sites_by_key: dict, names_all: set[str]) -> list[Issue]:
    out: list[Issue] = []
    nid = _node_id(trial)

    # Rule 1 — crop_eppo present + normalizes to a 5-char EPPO
    raw_eppo = trial.get("crop_eppo")
    if not raw_eppo:
        out.append(Issue("ERROR", "missing_eppo", nid, "VarietyTrial missing crop_eppo"))
    elif BaseIngester._normalize_eppo(raw_eppo) is None:
        out.append(Issue("ERROR", "bad_eppo", nid,
                         f"crop_eppo '{raw_eppo}' does not normalize to a 5-char EPPO"))

    # Rule 2 — variety present, non-empty
    if not str(trial.get("variety") or "").strip():
        out.append(Issue("ERROR", "missing_variety", nid, "VarietyTrial missing variety"))

    # Rule 3 — year present, int, > 1900
    year = trial.get("year")
    if not isinstance(year, int) or isinstance(year, bool) or year <= 1900:
        out.append(Issue("ERROR", "bad_year", nid, f"invalid year: {year!r}"))

    # Rule 4 — trial_location present
    location = str(trial.get("trial_location") or "").strip()
    if not location:
        out.append(Issue("ERROR", "missing_location", nid, "VarietyTrial missing trial_location"))

    # Rules 5 + 14 — linkage by name/municipality only (source-agnostic).
    if location:
        loc_key = _norm(location)
        if loc_key not in names_all:
            out.append(Issue("ERROR", "orphan_trial", nid,
                             f"trial_location '{location}' matches no TrialSite name/municipality"))

    # Rule 6 — yield_note_s1, if present, is a scalar
    note = trial.get("yield_note_s1")
    if note is not None and isinstance(note, (dict, list)):
        out.append(Issue("ERROR", "note_is_object", nid,
                         "yield_note_s1 must be a scalar (BSL 1-9), never an object"))

    # Rule 7 — no note*1000 fabrication
    yield_kg = trial.get("yield_kg_ha")
    if (isinstance(yield_kg, (int, float)) and not isinstance(yield_kg, bool)
            and isinstance(note, (int, float)) and not isinstance(note, bool)
            and yield_kg == note * 1000):
        out.append(Issue("ERROR", "fabricated_yield", nid,
                         "yield_kg_ha == yield_note_s1 * 1000 — fabricated, not measured"))

    # Rule 8 — metric plausibility (per-crop upper bound)
    eppo = BaseIngester._normalize_eppo(raw_eppo)
    bound = PLAUSIBLE_MAX.get(eppo) if eppo else None
    if (bound is not None and isinstance(yield_kg, (int, float))
            and not isinstance(yield_kg, bool) and yield_kg > bound):
        out.append(Issue("WARNING", "implausible_yield", nid,
                         f"yield_kg_ha {yield_kg} exceeds plausible max {bound} for {eppo} — "
                         f"likely wrong yield_metric ({trial.get('yield_metric')})"))

    # Rule 9 — cumulative leak
    if isinstance(yield_kg, (int, float)) and not isinstance(yield_kg, bool):
        qp = trial.get("quality_params") or {}
        leaked = [k for k in qp if _CUMULATIVE_KEY.search(str(k))]
        if leaked:
            out.append(Issue("WARNING", "cumulative_leak", nid,
                             f"cumulative quality_params {leaked} present alongside annual yield_kg_ha"))

    # Rule 10 — mergeKey present (ingestion generates it if absent;
    # the real gate items are EPPO/variety/year/location above).
    if not str(trial.get("mergeKey") or "").strip():
        out.append(Issue("WARNING", "missing_mergekey", nid,
                         "VarietyTrial missing mergeKey — will be auto-generated"))

    return out


def _build_stats(trials: list[dict], sites: list[dict], issues: list[Issue]) -> dict:
    from collections import Counter

    error_nodes = {i.node_id for i in issues if i.severity == "ERROR"}

    by_type = Counter()
    for n in trials:
        by_type["VarietyTrial"] += 1
    by_type["TrialSite"] = len(sites)

    trials_by_source = Counter(t.get("source_id") for t in trials)
    with_yield = sum(1 for t in trials if isinstance(t.get("yield_kg_ha"), (int, float))
                     and not isinstance(t.get("yield_kg_ha"), bool))
    orphan = sum(1 for i in issues if i.code in ("orphan_trial", "source_id_mismatch"))
    missing_eppo = sum(1 for i in issues if i.code == "missing_eppo")

    ranking_ready = 0
    for t in trials:
        nid = _node_id(t)
        has_yield = (isinstance(t.get("yield_kg_ha"), (int, float))
                     and not isinstance(t.get("yield_kg_ha"), bool))
        if (has_yield and nid not in error_nodes
                and t.get("ranking_eligible") is not False):
            ranking_ready += 1

    return {
        "by_type": dict(by_type),
        "variety_trials_by_source_id": dict(trials_by_source),
        "with_yield_kg_ha": with_yield,
        "without_yield_kg_ha": len(trials) - with_yield,
        "orphan_trials": orphan,
        "missing_eppo": missing_eppo,
        "ranking_ready": ranking_ready,
    }


# ── source_id ↔ registry alignment (bundle-level, rule 15) ──────────────────

def _check_registered_sources(trials: list[dict], sites: list[dict]) -> list[Issue]:
    out: list[Issue] = []
    seen: set[str] = set()
    for n in trials + sites:
        sid = n.get("source_id")
        if not sid or sid in seen:
            continue
        seen.add(sid)
        try:
            get_source(sid)
        except KeyError:
            out.append(Issue("WARNING", "source_not_registered", None,
                             f"source_id '{sid}' is not in sources_registry.json — "
                             f"BaseIngester.__init__ would raise KeyError at ingest"))
    return out


# ── CLI ─────────────────────────────────────────────────────────────────────

def _format_report(report: ValidationReport) -> str:
    lines = ["── Validation report ──"]
    for key, val in report.stats.items():
        lines.append(f"  {key}: {val}")
    lines.append("")
    for issue in report.issues:
        icon = {"ERROR": "❌", "WARNING": "⚠️ ", "INFO": "ℹ️ "}.get(issue.severity, "  ")
        node = f" [{issue.node_id}]" if issue.node_id else ""
        lines.append(f"  {icon} {issue.code}{node}: {issue.message}")
    lines.append("")
    n_err = len(report.errors())
    n_warn = len(report.warnings())
    if n_err:
        lines.append(f"  {n_err} error(s) — BLOCKS ingestion")
    if n_warn:
        lines.append(f"  {n_warn} warning(s) — human review recommended")
    lines.append("  RESULT: " + ("FAIL" if not report.ok else "PASS"))
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if len(argv) != 1:
        print("usage: python -m app.ingestion.validate_ingest_bundle <bundle.jsonld>",
              file=sys.stderr)
        return 2
    path = argv[0]
    if not Path(path).exists():
        print(f"file not found: {path}", file=sys.stderr)
        return 2
    report = validate_bundle(path)
    print(_format_report(report))
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
