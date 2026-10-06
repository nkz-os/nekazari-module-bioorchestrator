"""Quality gate: one case per rule, the licence bypass, and the carried-forward rulings.

Bundles come from the real contract engine run on small synthetic rows against the real registries
(or against a copy with one registry value changed, in a temporary directory), so the gate sees
exactly what a build produces. A defect is then injected into the frozen bundle with ``model_copy``
(which skips validation), because the row models refuse most of the defects the gate exists to catch.

``CASES`` maps every rule in ``gate.RULES`` to a case that must trigger it, and a test checks the two
have the same keys: a rule added without a case fails the suite. A last test runs the whole GENVCE and
CREA bundles of Task 5 where the private raw-data repository is available.
"""
from __future__ import annotations

import copy
import json
import os
import shutil
from collections.abc import Callable
from pathlib import Path

import pytest
import yaml

from app.kg import identity
from app.kg.adapters import AdapterWarning, crea, genvce
from app.kg.contracts import Contract, load_contract, run_contract
from app.kg.gate import (
    EXIT_ERRORS,
    EXIT_OK,
    RULES,
    GateReport,
    exit_code,
    run_gate,
    unit_content_key,
    write_report,
    write_review_queue,
)
from app.kg.registries import DEFAULT_REGISTRIES_PATH, load_registries

REGISTRIES = load_registries()
JUSTIFIED = "synthetic: the report states it"
AGGREGATE = REGISTRIES.site("ES-GENVCE-NACIONAL").name  # a registered aggregate
FIELD = REGISTRIES.site("ES-VALLADOLID").name  # a registered field site (no published coordinates)


# ═════════════════════════════════════════════════════════════════════════════
# synthetic bundles
# ═════════════════════════════════════════════════════════════════════════════

def base_contract() -> dict:
    return {
        "source_id": "GENVCE",
        "raw": {"repo": "synthetic-raw", "paths": ["rows/*.json"], "extraction_version": "t1"},
        "adapter": "app.kg.adapters.synthetic",
        "document": {
            "title": {"from": "doc.title"},
            "issue": {"from": "doc.issue"},
            "year": {"from": "doc.year"},
            "locator": [{"label": "page", "from": "doc.page"}, {"label": "table", "from": "doc.table"}],
        },
        "study": {"type": "variety", "group_by": ["network"]},
        "unit": {
            "fields": {
                "crop": {"from": "crop"},
                "variety": {"from": "variety"},
                "site": {"from": "zone"},
                "season": {"from": "year"},
                "irrigation": {"from": "irrigation", "vocab": "irrigation"},
            },
            "purpose": {"default": "grain", "justification": JUSTIFIED},
            "yield": {
                "from": "yield",
                "unit": "kg/ha",
                "metric": {"default": "grain", "justification": JUSTIFIED},
                "basis": {"default": "standard_moisture", "justification": JUSTIFIED},
                "moisture_pct": {"default": 13, "justification": JUSTIFIED},
            },
        },
        "observations": [
            {"from": "quality.protein", "variable": "grain_protein_content", "unit": "%"},
            {"from": "score", "variable": "emergence_score"},
        ],
        "ignore": [{"field": "_validation", "reason": "extraction metadata"}],
        "sites": {"aggregate_patterns": []},
        "expected": {"units": 1, "observations": 2, "sites": 0},
    }


def row(**over) -> dict:
    values = {
        "doc": {"title": "Informe sintetico 2021", "issue": "n1", "year": 2021, "page": 12, "table": "3"},
        "crop": "Cebada",
        "variety": "AGUEDA",  # registered (a candidate of the varieties registry), so no review queue
        "zone": AGGREGATE,
        "year": 2021,
        "irrigation": "secano",
        "network": "red-a",
        "yield": 6200,
        "quality": {"protein": 11.5},
        "score": 2,
        "_validation": {"ok": True},
    }
    values.update(over)
    return values


def build(rows, *, sites=1, registries=REGISTRIES):
    """Run the synthetic contract; every row gives a unit, a yield, a protein and a score."""
    data = base_contract()
    data["expected"] = {"units": len(rows), "observations": 3 * len(rows), "sites": sites}
    return run_contract(Contract.model_validate(data), registries, rows)


def gate(bundle, target="production", registries=REGISTRIES, **kw) -> GateReport:
    return run_gate(bundle, registries, target, **kw)


def finding(report: GateReport, rule: str):
    return next((f for f in (*report.errors, *report.warnings) if f.rule == rule), None)


def swap_unit(bundle, index=0, **update):
    units = list(bundle.units)
    units[index] = units[index].model_copy(update=update)
    return bundle.model_copy(update={"units": tuple(units)})


def yield_obs(bundle):
    return next(o for o in bundle.observations if o.variable_id == "crop_yield")


def swap_obs(bundle, old, **update):
    new = old.model_copy(update=update)
    return bundle.model_copy(update={"observations": tuple(new if o is old else o for o in bundle.observations)})


def swap_yield(bundle, **update):
    return swap_obs(bundle, yield_obs(bundle), **update)


def swap_report(bundle, **update):
    return bundle.model_copy(update={"report": bundle.report.model_copy(update=update)})


class FakeGraph:
    """A read-only existing graph that records every call made to it."""

    def __init__(self, pairs):
        self.pairs = list(pairs)
        self.calls: list[tuple[str, str]] = []

    def unit_identities(self, source_id):
        self.calls.append(("unit_identities", source_id))
        return list(self.pairs)


# ═════════════════════════════════════════════════════════════════════════════
# registry variants (one value changed, in a temporary copy)
# ═════════════════════════════════════════════════════════════════════════════

def _variant(root: Path, name: str, edit: Callable[[Path], None]):
    target = root / name
    shutil.copytree(DEFAULT_REGISTRIES_PATH, target)
    edit(target)
    return load_registries(target)


def _edit_yaml(file: str, change: Callable[[dict], None]) -> Callable[[Path], None]:
    def edit(root: Path) -> None:
        path = root / file
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        change(data)
        path.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return edit


def _licence(source_id: str, verdict: str, **extra) -> Callable[[Path], None]:
    def change(data: dict) -> None:
        for source in data["sources"]:
            if source["source_id"] == source_id:
                source["licence"]["commercial_use"] = verdict
                source["licence"].update(extra)
    return _edit_yaml("sources.yaml", change)


def _review_all_ranges(data: dict) -> None:
    for rng in data["ranges"]:
        rng.update(status="reviewed", reviewer="Test Agronomist", evidence="synthetic evidence")


@pytest.fixture(scope="module")
def variants(tmp_path_factory):
    root = tmp_path_factory.mktemp("registries")
    return {
        "denied": _variant(root, "denied", _licence("GENVCE", "denied")),
        "unknown": _variant(root, "unknown", _licence("GENVCE", "unknown")),
        "granted": _variant(root, "granted", _licence("GENVCE", "permission_granted", permission_ref="synthetic-ref")),
        "reviewed": _variant(root, "reviewed", _edit_yaml("ranges.yaml", _review_all_ranges)),
        "stale": _variant(root, "stale", lambda r: (r / "crops.yaml").write_text(
            (r / "crops.yaml").read_text(encoding="utf-8") + "\n# edited after the build\n", encoding="utf-8")),
    }


# ═════════════════════════════════════════════════════════════════════════════
# one case per rule
# ═════════════════════════════════════════════════════════════════════════════

def _outlier_rows():
    yields = [6000, 6050, 6100, 6150, 6200, 6250, 6300, 6350, 6400, 9000]
    return [row(variety=f"V{i}", **{"yield": y}) for i, y in enumerate(yields)]


def _case_site_kind(kind):
    def case(v):
        bundle = build([row()])
        site = bundle.sites[0].model_copy(update={"site_kind": kind})
        return gate(bundle.model_copy(update={"sites": (site,)}))
    return case


CASES: dict[str, Callable[[dict], GateReport]] = {
    "registries_mismatch": lambda v: gate(build([row()]), registries=v["stale"]),
    "source_mismatch": lambda v: gate(swap_report(build([row()]), source_id="CREA")),
    "foreign_source_row": lambda v: gate(swap_unit(build([row()]), source_id="CREA")),
    "unregistered_source": lambda v: gate(build([row()]).model_copy(update={"source_id": "NOPE"})),
    "licence_not_permitted": lambda v: gate(build([row()], registries=v["denied"]), registries=v["denied"]),
    "licence_not_publishable": lambda v: gate(build([row()], registries=v["denied"]), "local-test",
                                              registries=v["denied"]),
    "duplicate_key": lambda v: (b := build([row()])) and gate(b.model_copy(update={"units": (*b.units, b.units[0])})),
    "key_conflict": lambda v: (b := build([row()])) and gate(b.model_copy(update={"units": (
        *b.units, b.units[0].model_copy(update={"yield_kg_ha": 1.0}))})),
    "orphan_observation": lambda v: gate(swap_yield(build([row()]), unit_key="0" * 64)),
    "dangling_reference": lambda v: gate(swap_unit(build([row()]), document_key="f" * 64)),
    "unresolved_eppo": lambda v: gate(swap_unit(build([row()]), crop_eppo="ZZZZZ")),
    "unregistered_variable": lambda v: gate(swap_yield(build([row()]), variable_id="no_such_variable")),
    "unregistered_unit": lambda v: gate(swap_yield(build([row()]), unit="furlong")),
    "unit_mismatch": lambda v: gate(swap_yield(build([row()]), unit="t/ha")),
    "vocabulary_unregistered": lambda v: gate(swap_unit(build([row()]), irrigation_regime="secano")),
    "unresolved_vocab": lambda v: gate(build([row(irrigation="regado")])),
    "yield_without_metric": lambda v: gate(swap_yield(build([row()]), metric=None)),
    "yield_without_purpose": lambda v: gate(swap_yield(build([row()]), purpose=None)),
    "yield_purpose_mismatch": lambda v: gate(swap_yield(build([row()]), purpose="forage")),
    "unknown_basis": lambda v: gate(swap_yield(build([row()]), basis="unknown", moisture_pct=None)),
    "fabricated_yield": lambda v: gate(build([row(**{"yield": 5000, "score": 5})])),
    "cumulative_yield_source": lambda v: gate(swap_yield(build([row()]), raw_key="yield_2019_2020")),
    "unresolved_site": lambda v: gate(build([row(zone="Nowhere")], sites=0)),
    "site_kind_invalid": _case_site_kind(None),
    "site_kind_mismatch": _case_site_kind("region"),
    "no_observed_site": lambda v: gate(build([row(zone=None)], sites=0)),
    "treatment_code_in_site_name": lambda v: gate(build([row(zone="Juansenea (Doneztebe) - C1N0")], sites=0)),
    "field_site_without_coordinates": lambda v: gate(build([row(zone=FIELD)])),
    "missing_variety": lambda v: gate(build([row(variety=None)])),
    "bad_year": lambda v: gate(swap_unit(build([row()]), year=1066)),
    "missing_year": lambda v: gate(swap_unit(build([row()]), year=None)),
    "value_out_of_reviewed_range": lambda v: gate(
        build([row(**{"yield": 99999})], registries=v["reviewed"]), registries=v["reviewed"]),
    "value_out_of_assumption_range": lambda v: gate(build([row(**{"yield": 99999})])),
    "range_checks_skipped": lambda v: gate(build([row()])),
    "outlier_in_study": lambda v: gate(build(_outlier_rows())),
    "identical_values_across_varieties": lambda v: gate(build([row(variety=f"V{i}") for i in range(3)])),
    "unregistered_variety": lambda v: gate(build([row(variety="Brand New")])),
    "content_duplicate_in_graph": lambda v: (b := build([row()])) and gate(b, existing=FakeGraph(
        [("an-older-key", unit_content_key(b.units[0]))])),
    "engine_warning": lambda v: gate(swap_report(build([row()]), warnings=("a contract warning",))),
    "adapter_warning": lambda v: gate(build([row()]), adapter_warnings=(
        AdapterWarning(code="disease_scale_resolved", message="scale resolved by campaign", count=7),)),
}


def test_every_rule_has_a_case_and_every_case_a_rule():
    assert set(CASES) == set(RULES)


@pytest.mark.parametrize("rule", sorted(RULES))
def test_each_rule_fires_with_its_severity_and_counts(rule, variants):
    report = CASES[rule](variants)
    hit = finding(report, rule)
    assert hit is not None, f"{rule} did not fire; errors={report.error_counts} warnings={report.warning_counts}"
    assert hit.severity == RULES[rule].severity
    assert hit.count >= 1 and hit.message == RULES[rule].message
    bucket = report.errors if hit.severity == "error" else report.warnings
    assert hit in bucket
    assert (report.error_counts if hit.severity == "error" else report.warning_counts)[rule] == hit.count
    assert (report.status == "fail") == bool(report.errors)


# ═════════════════════════════════════════════════════════════════════════════
# the report, the exit code, the targets
# ═════════════════════════════════════════════════════════════════════════════

def test_a_clean_bundle_passes_for_production_and_is_publishable():
    report = gate(build([row()]))
    assert report.errors == () and report.error_counts == {}
    assert (report.status, report.publishable, report.not_publishable_reasons) == ("pass", True, ())
    assert exit_code(report) == EXIT_OK == 0
    assert report.rows["units"] == 1 and report.rows["observations"] == 3
    assert report.content_duplicate_check == "not_run"


def test_the_report_serialises_to_deterministic_json_and_the_exit_code_follows_the_errors():
    bundle = build([row(), row(variety="V2", zone="Nowhere")], sites=1)
    first, second = gate(bundle), gate(bundle)
    assert first.to_json() == second.to_json()
    parsed = json.loads(first.to_json())
    assert parsed["status"] == "fail" and parsed["target"] == "production"
    assert parsed["error_counts"]["unresolved_site"] == 1
    assert exit_code(first) == EXIT_ERRORS == 1
    assert GateReport.model_validate(parsed) == first  # nothing is lost in the round trip


def test_the_row_order_of_the_input_never_changes_the_report():
    rows = _outlier_rows()
    forward, backward = gate(build(rows)), gate(build(list(reversed(rows))))
    assert forward.to_json() == backward.to_json()


def test_an_unknown_target_is_refused():
    with pytest.raises(ValueError, match="unknown gate target"):
        gate(build([row()]), target="staging")


# ── licence ──────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("verdict", ["denied", "unknown"])
def test_a_denied_or_unknown_licence_is_an_error_for_production_even_with_a_valid_contract(variants, verdict):
    registries = variants[verdict]
    bundle = build([row()], registries=registries)  # the contract is valid and the bundle builds
    report = gate(bundle, "production", registries)
    assert report.error_counts == {"licence_not_permitted": 1}
    assert report.errors[0].groups == {f"GENVCE ({verdict})": 1}
    assert (report.status, report.publishable) == ("fail", False)
    assert exit_code(report) == EXIT_ERRORS


@pytest.mark.parametrize("verdict", ["denied", "unknown"])
def test_local_test_allows_the_licence_but_marks_the_result_not_publishable(variants, verdict):
    registries = variants[verdict]
    report = gate(build([row()], registries=registries), "local-test", registries)
    assert report.errors == () and report.status == "pass" and exit_code(report) == EXIT_OK
    assert report.publishable is False
    assert report.not_publishable_reasons == (f"GENVCE: commercial use is {verdict}",)
    assert report.warning_counts["licence_not_publishable"] == 1


def test_a_licensed_source_is_publishable_in_both_targets_and_a_written_permission_counts(variants):
    for registries in (REGISTRIES, variants["granted"]):
        bundle = build([row()], registries=registries)
        for target in ("production", "local-test"):
            report = gate(bundle, target, registries)
            assert report.errors == () and report.publishable is True, (target, report.error_counts)


def test_relabelling_rows_of_a_denied_source_as_a_licensed_bundle_does_not_get_past_the_gate():
    bundle = swap_unit(build([row()]), source_id="NAVARRA-AGRARIA")  # denied in the real registry
    report = gate(bundle)
    assert report.error_counts["foreign_source_row"] == 1
    assert report.errors and finding(report, "licence_not_permitted").groups == {"NAVARRA-AGRARIA (denied)": 1}
    relabelled = gate(swap_report(build([row()]), source_id="NAVARRA-AGRARIA"))
    assert "licence_not_permitted" in relabelled.error_counts and "source_mismatch" in relabelled.error_counts


# ═════════════════════════════════════════════════════════════════════════════
# carried-forward rulings
# ═════════════════════════════════════════════════════════════════════════════

def test_every_yield_row_must_have_a_purpose_unit_copy_and_observation_alike():
    bundle = swap_unit(swap_yield(build([row()]), purpose=None), purpose=None)
    report = gate(bundle)
    assert finding(report, "yield_without_purpose").groups == {"observation": 1, "unit": 1}
    assert report.status == "fail"


def test_a_unit_without_yield_does_not_need_a_purpose_but_a_yield_without_a_metric_is_an_error():
    bundle = swap_unit(build([row()]), yield_metric=None)
    assert finding(gate(bundle), "yield_without_metric").groups == {"unit": 1}


def test_skipped_range_checks_are_counted_in_the_report_by_reason():
    bundle = build([row(), row(variety="V2", **{"yield": 99999})])
    report = gate(bundle)
    engine = bundle.report.range_checks
    summary = report.range_checks
    assert summary.skipped == engine.skipped > 0
    assert summary.skipped_by_reason == engine.skipped_by_reason
    assert (summary.evaluated, summary.in_range, summary.not_applicable) == (
        engine.evaluated, engine.in_range, engine.not_applicable)
    skipped = finding(report, "range_checks_skipped")
    assert skipped.count == engine.skipped and skipped.groups == engine.skipped_by_reason
    assert (summary.skipped, summary.evaluated, summary.not_applicable) == (2, 2, 2)  # 2 proteins, 2 yields, 2 scores
    assert summary.out_of_range_assumption == 1 and summary.out_of_range_reviewed == 0


def test_a_run_with_every_range_check_made_has_no_skipped_warning():
    bundle = build([row()])
    bundle = bundle.model_copy(update={"observations": tuple(
        o for o in bundle.observations if o.variable_id == "crop_yield")})
    report = gate(swap_report(bundle, range_checks=bundle.report.range_checks.model_copy(
        update={"skipped": 0, "skipped_by_reason": {}})))
    assert finding(report, "range_checks_skipped") is None and report.range_checks.skipped == 0


def test_the_same_unit_key_or_observation_key_with_different_values_is_a_conflict_not_a_duplicate():
    bundle = build([row()])
    conflicting_unit = bundle.units[0].model_copy(update={"yield_kg_ha": 1.0})
    report = gate(bundle.model_copy(update={"units": (*bundle.units, conflicting_unit)}))
    assert finding(report, "key_conflict").groups == {"unit": 1} and finding(report, "duplicate_key") is None

    obs = yield_obs(bundle)
    conflicting_obs = obs.model_copy(update={"value": 1.0})
    report = gate(bundle.model_copy(update={"observations": (*bundle.observations, conflicting_obs)}))
    assert finding(report, "key_conflict").groups == {"observation": 1}

    identical = gate(bundle.model_copy(update={"observations": (*bundle.observations, obs)}))
    assert finding(identical, "duplicate_key").groups == {"observation": 1} and finding(identical, "key_conflict") is None


@pytest.mark.parametrize("field, value", [
    ("irrigation_regime", "secano"),  # an alias, not the stored AGROVOC value
    ("production_system", "bio"),
    ("purpose", "grano"),
    ("yield_basis", "humedad estándar"),
])
def test_unit_vocabulary_values_must_be_registered_ones(field, value):
    report = gate(swap_unit(build([row()]), **{field: value}))
    hit = finding(report, "vocabulary_unregistered")
    assert hit is not None and hit.severity == "error" and any(value in group for group in hit.groups)


def test_study_and_observation_vocabulary_values_are_checked_too_and_registered_ones_pass():
    bundle = build([row()])
    study = bundle.studies[0].model_copy(update={"study_type": "ensayo"})
    assert finding(gate(bundle.model_copy(update={"studies": (study,)})), "vocabulary_unregistered") is not None
    assert finding(gate(swap_yield(bundle, basis="13 %")), "vocabulary_unregistered") is not None
    assert finding(gate(swap_yield(bundle, metric="grano")), "vocabulary_unregistered") is not None
    assert finding(gate(bundle), "vocabulary_unregistered") is None  # id and stored forms are accepted


# ── sites ────────────────────────────────────────────────────────────────────

def test_a_unit_with_no_observed_site_is_a_counted_warning_listed_by_source_never_an_error():
    rows = [row(zone=None, variety=f"V{i}") for i in range(3)] + [row(variety="V9")]
    report = gate(build(rows, sites=1))
    hit = finding(report, "no_observed_site")
    assert (hit.severity, hit.count, hit.groups) == ("warning", 3, {"GENVCE": 3})
    assert report.errors == () and report.status == "pass"


def test_an_observed_label_that_does_not_resolve_is_an_error_with_the_label_listed():
    report = gate(build([row(zone="Nowhere"), row(zone="Nowhere", variety="V2")], sites=0))
    hit = finding(report, "unresolved_site")
    assert (hit.severity, hit.count, hit.groups) == ("error", 2, {"Nowhere": 2})
    assert finding(report, "no_observed_site") is None


def test_an_aggregate_site_without_a_site_kind_is_an_error_and_a_wrong_kind_is_another():
    bundle = build([row()])
    site = bundle.sites[0]
    assert site.site_kind == "aggregate"
    missing = gate(bundle.model_copy(update={"sites": (site.model_copy(update={"site_kind": None}),)}))
    assert missing.error_counts == {"site_kind_invalid": 1}
    blank = gate(bundle.model_copy(update={"sites": (site.model_copy(update={"site_kind": ""}),)}))
    assert blank.error_counts == {"site_kind_invalid": 1}
    wrong = gate(bundle.model_copy(update={"sites": (site.model_copy(update={"site_kind": "field"}),)}))
    assert "site_kind_mismatch" in wrong.error_counts


def test_a_site_row_the_registry_does_not_know_is_an_unresolved_site():
    bundle = build([row()])
    stranger = bundle.sites[0].model_copy(update={"site_id": "ES-NOWHERE"})
    report = gate(bundle.model_copy(update={"sites": (stranger,)}))
    assert finding(report, "unresolved_site").groups == {"ES-NOWHERE": 1}


def test_treatment_codes_are_flagged_in_a_site_row_name_and_in_an_observed_label():
    bundle = build([row()])
    named = bundle.sites[0].model_copy(update={"name": "Juansenea - C1N0"})
    assert finding(gate(bundle.model_copy(update={"sites": (named,)})), "treatment_code_in_site_name") is not None
    assert finding(gate(build([row(zone="Zona 1")], sites=0)), "treatment_code_in_site_name") is None
    assert finding(gate(build([row(zone="Cadreita (N120)")], sites=0)), "treatment_code_in_site_name") is None


# ── units and observations ───────────────────────────────────────────────────

def test_a_unit_of_a_variety_study_needs_a_variety_and_a_year_inside_the_bounds():
    bundle = build([row()])
    assert finding(gate(swap_unit(bundle, raw_variety=None)), "missing_variety").count == 1
    assert finding(gate(swap_unit(bundle, year=1900)), "bad_year") is not None
    assert finding(gate(swap_unit(bundle, year=2101)), "bad_year") is not None
    assert finding(gate(swap_unit(bundle, year=2021)), "bad_year") is None


def test_a_yield_that_is_an_ordinal_score_times_1000_is_fabricated_but_a_zero_score_is_not():
    assert finding(gate(build([row(**{"yield": 5000, "score": 5})])), "fabricated_yield").count == 1
    assert finding(gate(build([row(**{"yield": 6200, "score": 5})])), "fabricated_yield") is None
    assert finding(gate(build([row(**{"yield": 6200, "score": 0})])), "fabricated_yield") is None


def test_a_cumulative_looking_source_field_is_only_judged_on_the_yield_observation():
    bundle = build([row()])
    protein = next(o for o in bundle.observations if o.variable_id == "grain_protein_content")
    assert finding(gate(swap_obs(bundle, protein, raw_key="numero_ensayos_2023_2024")),
                   "cumulative_yield_source") is None
    assert finding(gate(swap_yield(bundle, raw_key="cumulative_yield")), "cumulative_yield_source") is not None


# ── ranges ───────────────────────────────────────────────────────────────────

def test_only_a_reviewed_range_makes_an_error_and_an_assumption_range_a_warning(variants):
    rows = [row(**{"yield": 99999})]
    assumption = gate(build(rows))
    assert assumption.errors == () and assumption.warning_counts["value_out_of_assumption_range"] == 1
    reviewed = gate(build(rows, registries=variants["reviewed"]), registries=variants["reviewed"])
    assert reviewed.error_counts == {"value_out_of_reviewed_range": 1}
    assert finding(reviewed, "value_out_of_reviewed_range").groups == {"crop_yield.HORVX.grain.rainfed": 1}
    assert reviewed.range_checks.out_of_range_reviewed == 1


# ── statistics ───────────────────────────────────────────────────────────────

def test_a_robust_outlier_is_a_warning_and_the_report_says_how_many_groups_could_be_judged():
    report = gate(build(_outlier_rows()))
    hit = finding(report, "outlier_in_study")
    assert (hit.severity, hit.count, hit.groups) == ("warning", 1, {"crop_yield": 1})
    assert "value=9000" in hit.examples[0]
    out = report.outliers
    # two comparable groups (the score is ordinal): the yield is judged, the constant protein has a zero MAD
    assert (out.groups, out.evaluated, out.mad_zero, out.too_small, out.flagged) == (2, 1, 1, 0, 1)
    assert report.errors == ()


def test_a_small_study_is_counted_as_not_judged_and_a_constant_group_as_zero_mad():
    small = gate(build(_outlier_rows()[:5]))
    assert finding(small, "outlier_in_study") is None
    assert (small.outliers.too_small, small.outliers.evaluated) == (2, 0)
    constant = gate(build([row(variety=f"V{i}") for i in range(10)]))
    assert finding(constant, "outlier_in_study") is None and constant.outliers.mad_zero == 2


def test_identical_yields_need_three_varieties_in_the_same_study_and_slot():
    assert finding(gate(build([row(variety=f"V{i}") for i in range(3)])), "identical_values_across_varieties").count == 1
    assert finding(gate(build([row(variety=f"V{i}") for i in range(2)])), "identical_values_across_varieties") is None
    different = [row(variety=f"V{i}", **{"yield": 6000 + i}) for i in range(3)]
    assert finding(gate(build(different)), "identical_values_across_varieties") is None
    other_sites = [row(variety=f"V{i}", zone=z) for i, z in enumerate([AGGREGATE, FIELD, "Nowhere"])]
    assert finding(gate(build(other_sites, sites=2)), "identical_values_across_varieties") is None


# ── review queue and files ───────────────────────────────────────────────────

def test_an_unregistered_variety_is_a_warning_and_a_review_queue_file_never_a_merge(tmp_path):
    report = gate(build([row(variety="Brand New"), row(variety="Brand New", zone=FIELD), row()], sites=2))
    assert finding(report, "unregistered_variety").groups == {"HORVX": 2}
    assert [(i.crop_eppo, i.name, i.units) for i in report.review_queue] == [("HORVX", "Brand New", 2)]
    assert report.errors == ()
    path = write_review_queue(report, tmp_path / "out")
    assert path.name == "review-queue-GENVCE.json"
    assert json.loads(path.read_text(encoding="utf-8"))["items"] == [{"crop_eppo": "HORVX", "name": "Brand New", "units": 2}]


def test_the_review_queue_file_is_written_empty_when_nothing_is_queued_and_the_report_file_too(tmp_path):
    report = gate(build([row()]))
    queue = write_review_queue(report, tmp_path)
    assert json.loads(queue.read_text(encoding="utf-8"))["items"] == []
    written = write_report(report, tmp_path)
    assert written.name == "gate-GENVCE-production.json" and written.read_text(encoding="utf-8") == report.to_json()
    assert not list(tmp_path.glob("*.tmp"))


# ── the optional, read-only content-duplicate check ──────────────────────────

def test_the_content_duplicate_check_only_reads_and_a_same_key_unit_is_not_a_duplicate():
    bundle = build([row()])
    unit = bundle.units[0]
    key, content = identity.unit_key(unit), unit_content_key(unit)
    before = copy.deepcopy(bundle)

    graph = FakeGraph([("another-key", content)])
    report = gate(bundle, existing=graph)
    assert graph.calls == [("unit_identities", "GENVCE")]  # nothing but the one read, for this source
    assert finding(report, "content_duplicate_in_graph").count == 1 and report.content_duplicate_check == "run"
    assert report.errors == () and bundle == before

    rerun = gate(bundle, existing=FakeGraph([(key, content)]))  # the same unit already loaded: idempotent
    assert finding(rerun, "content_duplicate_in_graph") is None
    unrelated = gate(bundle, existing=FakeGraph([("x", "y")]))
    assert finding(unrelated, "content_duplicate_in_graph") is None


def test_the_content_key_ignores_where_a_row_was_printed_but_not_what_it_says():
    unit = build([row()]).units[0]
    assert unit_content_key(unit) == unit_content_key(unit.model_copy(update={
        "document_key": "a" * 64, "row_discriminator": "7", "locator": "p. 9", "source_id": "CREA"}))
    assert unit_content_key(unit) != unit_content_key(unit.model_copy(update={"yield_kg_ha": 6201.0}))
    assert unit_content_key(unit) != unit_content_key(unit.model_copy(update={"raw_variety": "MAYA"}))
    assert unit_content_key(unit) == unit_content_key(unit.model_copy(update={"raw_variety": " agueda "}))


# ── pass-through ─────────────────────────────────────────────────────────────

def test_adapter_and_engine_warnings_are_carried_into_the_report_with_their_counts():
    warnings = (AdapterWarning(code="a_code", message="what happened", count=5),
                AdapterWarning(code="b_code", message="other", count=2))
    report = gate(swap_report(build([row()]), warnings=("engine said so",)), adapter_warnings=warnings)
    assert finding(report, "adapter_warning").count == 7
    assert finding(report, "adapter_warning").groups == {"a_code": 5, "b_code": 2}
    assert finding(report, "engine_warning").examples == ("engine said so",)
    assert report.errors == ()


# ═════════════════════════════════════════════════════════════════════════════
# the real GENVCE and CREA bundles (only where the private raw-data repository is available)
# ═════════════════════════════════════════════════════════════════════════════

RAW_REPO = os.environ.get("NKZ_DATA_SOURCES_DIR", "")
SOURCES = Path(__file__).resolve().parents[2] / "data" / "sources"


@pytest.mark.skipif(not RAW_REPO, reason="set NKZ_DATA_SOURCES_DIR to the raw-data repository to run")
@pytest.mark.parametrize("source_id, adapter", [("GENVCE", genvce), ("CREA", crea)])
def test_the_real_bundles_pass_the_gate_for_production_with_zero_errors(source_id, adapter):
    result = adapter.load(Path(RAW_REPO) / source_id.lower())
    bundle = run_contract(load_contract(SOURCES / f"{source_id}.yaml"), REGISTRIES, result.rows)
    report = run_gate(bundle, REGISTRIES, "production", adapter_warnings=result.warnings)

    assert report.errors == () and report.error_counts == {}, report.error_counts
    assert (report.status, report.publishable, exit_code(report)) == ("pass", True, 0)
    # the skipped range checks are the engine's, reported and not hidden
    assert report.range_checks.skipped == bundle.report.range_checks.skipped > 0
    assert report.warning_counts["range_checks_skipped"] == report.range_checks.skipped
    assert report.warning_counts["adapter_warning"] == sum(w.count for w in result.warnings)
    assert report.review_queue == ()
    if source_id == "GENVCE":
        # the units whose tables print no zone label: an explicit gap, never an error
        assert report.warning_counts["no_observed_site"] == 1758
        assert finding(report, "no_observed_site").groups == {"GENVCE": 1758}
    else:
        assert "no_observed_site" not in report.warning_counts
    print(f"\n{source_id} warnings per rule: {json.dumps(report.warning_counts, sort_keys=True)}")
