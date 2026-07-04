"""Pre-ingestion validation gate for JSON-LD bundles.

`validate_ingest_bundle` is the mandatory read-only gate every bundle passes
before `BaseIngester.merge()`. It catches the three defects that poisoned the
graph in the last two days: missing `crop_eppo` (98% of legacy trials), orphan
trials (name/source_id that won't produce a TRIAL_AT link), and yield
dishonesty (impossible kernel yields, cumulative leaks, `note*1000` fabrication).

The validator mutates nothing and never touches Neo4j. It mirrors the linking
rule of `base_ingester._merge_relationships` exactly, so a bundle that passes
must not orphan trials at merge.
"""

from __future__ import annotations

from app.ingestion.validate_ingest_bundle import (
    Issue,
    ValidationReport,
    validate_bundle,
)


def _site(name, source_id="BSL", **extra):
    node = {
        "@type": "TrialSite",
        "name": name,
        "source_id": source_id,
        "climate_class": "Csa",
        "latitude": 42.5,
        "longitude": -1.6,
    }
    node.update(extra)
    return node


def _trial(location, source_id="BSL", **extra):
    node = {
        "@type": "VarietyTrial",
        "source_id": source_id,
        "variety": "Guara",
        "crop_eppo": "eppo:PRNDU",
        "year": 2021,
        "trial_location": location,
        "yield_kg_ha": 1327.36,
        "mergeKey": f"{source_id}|eppo:PRNDU|guara|{location.lower()}|secano|2021",
    }
    node.update(extra)
    return node


def _bundle(*nodes):
    return {"@graph": list(nodes)}


# ── Rule set 1: a good bundle passes ────────────────────────────────────────

def test_good_minimal_bundle_passes():
    bundle = _bundle(_site("Dicastillo"), _trial("Dicastillo"))
    report = validate_bundle(bundle)
    assert isinstance(report, ValidationReport)
    assert report.ok is True
    assert report.errors() == []


def _codes(report, severity=None):
    return {i.code for i in report.issues if severity is None or i.severity == severity}


# ── Rule set 2: ERROR rules in isolation ────────────────────────────────────

def test_missing_eppo_is_error():
    trial = _trial("Dicastillo")
    del trial["crop_eppo"]
    report = validate_bundle(_bundle(_site("Dicastillo"), trial))
    assert report.ok is False
    assert "missing_eppo" in _codes(report, "ERROR")


def test_bad_eppo_is_error():
    report = validate_bundle(_bundle(_site("Dicastillo"), _trial("Dicastillo", crop_eppo="eppo:TOOLONG")))
    assert "bad_eppo" in _codes(report, "ERROR")


def test_missing_variety_is_error():
    report = validate_bundle(_bundle(_site("Dicastillo"), _trial("Dicastillo", variety="")))
    assert "missing_variety" in _codes(report, "ERROR")


def test_bad_year_is_error():
    report = validate_bundle(_bundle(_site("Dicastillo"), _trial("Dicastillo", year=1800)))
    assert "bad_year" in _codes(report, "ERROR")


def test_string_year_is_error():
    report = validate_bundle(_bundle(_site("Dicastillo"), _trial("Dicastillo", year="2021")))
    assert "bad_year" in _codes(report, "ERROR")


def test_missing_location_is_error():
    trial = _trial("Dicastillo")
    trial["trial_location"] = ""
    report = validate_bundle(_bundle(_site("Dicastillo"), trial))
    assert "missing_location" in _codes(report, "ERROR")


def test_orphan_trial_no_matching_site_is_error():
    report = validate_bundle(_bundle(_site("Dicastillo"), _trial("Sartaguda")))
    assert report.ok is False
    assert "orphan_trial" in _codes(report, "ERROR")


def test_matches_by_municipality_is_ok():
    site = _site("Estación Norte", municipality="Sartaguda")
    report = validate_bundle(_bundle(site, _trial("sartaguda")))
    assert "orphan_trial" not in _codes(report)
    assert report.ok is True


def test_source_id_mismatch_is_error():
    # name matches but the site belongs to a different source_id -> would orphan at merge
    report = validate_bundle(_bundle(
        _site("Dicastillo", source_id="NAVARRA-AGRARIA"),
        _trial("Dicastillo", source_id="BSL"),
    ))
    assert report.ok is False
    assert "source_id_mismatch" in _codes(report, "ERROR")
    assert "orphan_trial" not in _codes(report)


def test_note_as_object_is_error():
    report = validate_bundle(_bundle(_site("Dicastillo"),
                                     _trial("Dicastillo", yield_note_s1={"raw": 5})))
    assert "note_is_object" in _codes(report, "ERROR")


def test_fabricated_note_times_1000_is_error():
    report = validate_bundle(_bundle(
        _site("Dicastillo"),
        _trial("Dicastillo", yield_note_s1=7, yield_kg_ha=7000),
    ))
    assert "fabricated_yield" in _codes(report, "ERROR")


def test_missing_mergekey_is_error():
    trial = _trial("Dicastillo")
    del trial["mergeKey"]
    report = validate_bundle(_bundle(_site("Dicastillo"), trial))
    assert "missing_mergekey" in _codes(report, "ERROR")


def test_site_missing_name_is_error():
    report = validate_bundle(_bundle(_site(""), _trial("Dicastillo")))
    assert "site_no_name" in _codes(report, "ERROR")


# ── Rule set 3: WARNING rules (do not hard-block) ───────────────────────────

def test_implausible_kernel_yield_is_warning():
    # 5997 kernel_kg_ha for almond is impossible (in-shell was mislabelled)
    report = validate_bundle(_bundle(
        _site("Dicastillo"),
        _trial("Dicastillo", yield_kg_ha=5997, yield_metric="kernel_kg_ha"),
    ))
    assert "implausible_yield" in _codes(report, "WARNING")
    assert report.ok is True  # WARNING alone does not block


def test_plausible_kernel_yield_no_warning():
    report = validate_bundle(_bundle(_site("Dicastillo"),
                                     _trial("Dicastillo", yield_kg_ha=1500)))
    assert "implausible_yield" not in _codes(report)


def test_unknown_crop_skips_plausibility():
    report = validate_bundle(_bundle(_site("Dicastillo"),
                                     _trial("Dicastillo", crop_eppo="eppo:TRZAX", yield_kg_ha=99999)))
    assert "implausible_yield" not in _codes(report)


def test_cumulative_leak_is_warning():
    report = validate_bundle(_bundle(
        _site("Dicastillo"),
        _trial("Dicastillo", yield_kg_ha=1200,
               quality_params={"cumulative_oil_kg_ha_1998_2009": 42000}),
    ))
    assert "cumulative_leak" in _codes(report, "WARNING")
    assert report.ok is True


def test_site_missing_climate_is_warning():
    site = _site("Dicastillo")
    del site["climate_class"]
    report = validate_bundle(_bundle(site, _trial("Dicastillo")))
    assert "site_no_climate" in _codes(report, "WARNING")


def test_site_missing_coords_is_warning():
    site = _site("Dicastillo")
    del site["latitude"]
    report = validate_bundle(_bundle(site, _trial("Dicastillo")))
    assert "site_no_coords" in _codes(report, "WARNING")


def test_unregistered_source_is_warning():
    report = validate_bundle(_bundle(
        _site("Dicastillo", source_id="MADE-UP-SOURCE"),
        _trial("Dicastillo", source_id="MADE-UP-SOURCE"),
    ))
    assert "source_not_registered" in _codes(report, "WARNING")
    assert report.ok is True  # WARNING, not ERROR


def test_registered_source_no_warning():
    report = validate_bundle(_bundle(_site("Dicastillo", source_id="BSL"),
                                     _trial("Dicastillo", source_id="BSL")))
    assert "source_not_registered" not in _codes(report)


# ── Rule set 4: stats ───────────────────────────────────────────────────────

def test_stats_on_mixed_bundle():
    orphan = _trial("Nowhere")              # ERROR orphan, excluded from ranking-ready
    no_yield = _trial("Dicastillo")
    del no_yield["yield_kg_ha"]
    ineligible = _trial("Dicastillo", ranking_eligible=False)  # has yield but flagged out
    good = _trial("Dicastillo")
    report = validate_bundle(_bundle(_site("Dicastillo"), orphan, no_yield, ineligible, good))
    s = report.stats
    assert s["by_type"] == {"VarietyTrial": 4, "TrialSite": 1}
    assert s["variety_trials_by_source_id"]["BSL"] == 4
    assert s["with_yield_kg_ha"] == 3
    assert s["without_yield_kg_ha"] == 1
    assert s["orphan_trials"] == 1
    assert s["ranking_ready"] == 1          # only `good`
    assert all(isinstance(i, Issue) for i in report.issues)


def test_path_input_is_accepted(tmp_path):
    import json as _json
    p = tmp_path / "b.jsonld"
    p.write_text(_json.dumps(_bundle(_site("Dicastillo"), _trial("Dicastillo"))))
    report = validate_bundle(str(p))
    assert report.ok is True


# ── Rule set 5: CLI ─────────────────────────────────────────────────────────

def test_cli_exit_zero_on_clean(tmp_path, capsys):
    from app.ingestion.validate_ingest_bundle import main
    import json as _json
    p = tmp_path / "ok.jsonld"
    p.write_text(_json.dumps(_bundle(_site("Dicastillo"), _trial("Dicastillo"))))
    rc = main([str(p)])
    assert rc == 0
    assert "PASS" in capsys.readouterr().out


def test_cli_exit_one_on_error(tmp_path, capsys):
    from app.ingestion.validate_ingest_bundle import main
    import json as _json
    trial = _trial("Dicastillo")
    del trial["crop_eppo"]
    p = tmp_path / "bad.jsonld"
    p.write_text(_json.dumps(_bundle(_site("Dicastillo"), trial)))
    rc = main([str(p)])
    assert rc == 1
    assert "FAIL" in capsys.readouterr().out


def test_cli_missing_file_returns_two(capsys):
    from app.ingestion.validate_ingest_bundle import main
    rc = main(["/no/such/bundle.jsonld"])
    assert rc == 2
