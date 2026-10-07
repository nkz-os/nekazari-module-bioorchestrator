"""Readers on a MIXED graph (T12 option b): legacy trials (no siteKind / unitKey / zone) next to F1 units.

One container walks through three states of the same database:

* L  the restored copy as the served graph is today: legacy sites and trials only;
* M  after ``mark-target`` -> ``migrate-restored`` -> ``build`` of the fixture slice (legacy + F1);
* F  the F1 graph alone (the legacy nodes deleted from M).

Requirements: for crops and tiers the legacy data alone answers, M answers exactly as L; for what only F1
answers, M answers exactly as F; where both exist, the field tier is the legacy rows (identical to L) and the
regional tier is the F1 rows (identical to F), with the new semantics (tier, declared aggregate, country match).
"""
from __future__ import annotations

import json
import shutil

import pytest

from app.api.v1 import recommend as rec_api
from app.graph import dao as dao_mod
from app.graph.dao import GraphDAO
from tests.kg import equivalence_harness as h
from tests.kg.test_cli import (  # noqa: F401  (fixture_mode is a pytest fixture, imported to be reused)
    PASSWORD,
    _cfg,
    _loop,
    _q,
    _run,
    fixture_mode,
    graph,
)

needs_docker = pytest.mark.skipif(shutil.which("docker") is None, reason="docker unavailable")

# Legacy graph: shapes the old ingesters wrote. No siteKind, no unitKey, no country on the second site.
_SITES = [
    {"name": "Legacy Campo Csa", "siteKey": "legacy-csa", "country": "ES", "climateClass": "Csa",
     "annualRainfallMm": 500.0, "annualET0Mm": 1000.0, "frostDaysPerYear": 10.0, "elevationM": 300.0},
    {"name": "Legacy Campo Cfb", "siteKey": "legacy-cfb", "climateClass": "Cfb", "annualRainfallMm": 800.0},
    {"name": "Zona sin tipo", "siteKey": "legacy-zone", "climateClass": "Csa"},  # reads as a field site
]
# (crop, variety, kg/ha, site, year): SOLTU only legacy; HORVX legacy and F1 (GENVCE ES aggregates)
_TRIALS = [
    ("SOLTU", "Ariane", 41000.0, "Legacy Campo Csa", 2019), ("SOLTU", "Bintje", 38000.0, "Legacy Campo Csa", 2020),
    ("SOLTU", "Agria", 35000.0, "Legacy Campo Cfb", 2020),
    ("HORVX", "Hispanic", 6100.0, "Legacy Campo Csa", 2019), ("HORVX", "Pewter", 5400.0, "Legacy Campo Csa", 2020),
    ("HORVX", "Zebra", 4900.0, "Zona sin tipo", 2020),
]
F1_ONLY_CROPS = ("BRSNN", "TRZAX")
LEGACY_ONLY_CROP = "SOLTU"
BOTH_CROP = "HORVX"
PARCELS = (("Csa", "ES"), ("Cfb", "ES"), ("Cfb", "FR"))


def _run_async(coro):
    return _loop.run_until_complete(coro)


async def _seed_legacy(driver) -> None:
    async with driver.session() as s:
        await (await s.run("UNWIND $sites AS s CREATE (t:TrialSite) SET t = s", sites=[dict(x) for x in _SITES])).consume()
        await (await s.run(
            """UNWIND $rows AS r MATCH (ts:TrialSite {name: r.site})
            CREATE (vt:VarietyTrial {cropEppo: r.crop, varietyNormalized: r.variety, variety: r.variety,
                                     yieldKgHa: r.kg, year: r.year, aggregationScope: 'site',
                                     source_id: 'NAVARRA-AGRARIA', dataSource: 'NAVARRA-AGRARIA',
                                     management: 'conventional', mergeKey: r.crop + r.variety + toString(r.year)})
            CREATE (vt)-[:TRIAL_AT]->(ts)""",
            rows=[{"crop": c, "variety": v, "kg": k, "site": s_, "year": y} for c, v, k, s_, y in _TRIALS])).consume()


async def _answers(driver) -> dict:
    """The reader outputs that depend on the trial graph, for the parcels and crops of this test."""
    dao = GraphDAO(driver)
    out: dict = {}
    out["crops"] = sorted(c["eppo"] if "eppo" in c else str(c) for c in await dao.get_available_crops())
    for climate, country in PARCELS:
        pk = f"{country}-{climate}"
        for crop in (LEGACY_ONLY_CROP, BOTH_CROP, *F1_ONLY_CROPS):
            dao_mod._RECOMMEND_CACHE.clear()
            out[f"variety-trials|{pk}|{crop}"] = await dao.get_variety_trials(crop=crop, climate_class=climate, limit=50)
            out[f"extrapolate|{pk}|{crop}"] = await dao.extrapolate_varieties(
                crop=crop, climate_class=climate, top_n=10)
            for tier in ("field", "regional"):
                out[f"evidence|{pk}|{crop}|{tier}"] = await rec_api.recommend_evidence(
                    driver=driver, cond=h._Cond(climate, None, "main"), crop=crop, variety=None, page=1,
                    page_size=50, similarity="koppen", tier=tier, country=country, zone=None)
            dao_mod._RECOMMEND_CACHE.clear()
            out[f"recommend|{pk}|{crop}"] = await dao.recommend_for_conditions({
                "climate_class": climate, "country": country, "irrigation_regime": None, "management": "any",
                "season": "all", "purpose": "main", "crops": [crop], "top_n": 10})
    return json.loads(json.dumps(out, default=str, sort_keys=True))


@pytest.fixture(scope="module")
def states(graph, tmp_path_factory):  # noqa: F811
    n, d = graph
    tmp_path = tmp_path_factory.mktemp("mixed")
    env = {"NEO4J_PASSWORD": PASSWORD, "NKZ_KG_ALLOWED_TARGET_HOSTS": "localhost"}
    url = n.get_connection_url()
    from app.kg import cli

    # fixture_mode is function-scoped: apply the same patches by hand for this module-scoped fixture
    mp = pytest.MonkeyPatch()
    try:
        _patch_fixture_slice(mp, cli)
        _q(d, "MATCH (x) DETACH DELETE x")
        for row in _q(d, "SHOW CONSTRAINTS YIELD name RETURN name"):
            _q(d, f"DROP CONSTRAINT `{row['name']}` IF EXISTS")
        for row in _q(d, "SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP' RETURN name"):
            _q(d, f"DROP INDEX `{row['name']}` IF EXISTS")
        label = "local-mixed"
        assert cli.main(["mark-target", "--target", url, "--target-label", label, "--execute"], env=env) == 0
        # the restored copy: the pre-F1 schema and the legacy data
        from app.kg.migrations import MIGRATIONS_DIR, apply_migrations
        pre = tmp_path / "pre-f1"
        pre.mkdir()
        for f in sorted(MIGRATIONS_DIR.glob("*.cypher")):
            if f.name < "010":
                shutil.copy(f, pre / f.name)
        _run_async(apply_migrations(d, pre))
        _run_async(_seed_legacy(d))
        legacy = _run_async(_answers(d))

        assert cli.main(["migrate-restored", "--target", url, "--target-label", label, "--execute"], env=env) == 0
        built = _run(_cfg(tmp_path, target=url, target_label=label, execute=True, allow_existing=True), env)
        assert built.status == "ok", built.summary
        mixed = _run_async(_answers(d))

        _q(d, "MATCH (v:VarietyTrial) WHERE v.unitKey IS NULL DETACH DELETE v")
        _q(d, "MATCH (t:TrialSite) WHERE t.siteKey STARTS WITH 'legacy-' DETACH DELETE t")
        f1 = _run_async(_answers(d))
    finally:
        mp.undo()
    return legacy, mixed, f1


def _patch_fixture_slice(mp, cli) -> None:
    """Same patches as the ``fixture_mode`` fixture of test_cli (the build reads the fixture slice)."""
    from pathlib import Path
    from unittest import mock

    from app.kg.adapters import crea, genvce
    from app.kg.contracts import Expected, load_contract, run_contract
    from app.kg.registries import load_registries
    from tests.kg.test_cli import FIXTURES

    regs = load_registries()
    contracts = {s: load_contract(cli.SOURCES_DIR / f"{s}.yaml") for s in ("GENVCE", "CREA")}
    rows = {"GENVCE": genvce.load(FIXTURES / "genvce").rows, "CREA": crea.load(FIXTURES / "crea").rows}
    sliced = {}
    for s, c in contracts.items():
        with mock.patch("app.kg.contracts._check_expected"):
            b = run_contract(c, regs, rows[s])
        sliced[s] = c.model_copy(update={"expected": Expected(units=len(b.units), observations=len(b.observations),
                                                              sites=len(b.sites))})
    mp.setattr(cli, "load_contract", lambda path: sliced[Path(path).stem])
    mp.setattr(cli, "repo_state", lambda path: {"sha": "fixture", "dirty": False})
    mp.setattr(cli, "_check_raw_manifest", lambda path: {"listed": 0, "present_locally": 0, "absent_locally": 0})
    mp.setattr(cli, "SOURCES", {
        "GENVCE": ("genvce", lambda _p: genvce.load(FIXTURES / "genvce")),
        "CREA": ("crea", lambda _p: crea.load(FIXTURES / "crea"))})


def _without_site_list(answer):
    """extrapolate also names the climate-matched sites, and a mixed graph has more of them (the legacy ones);
    that list is not an answer about the crop. What is compared is the ranking and the trial counts."""
    if isinstance(answer, dict) and "ranked_varieties" in answer:
        quality = answer.get("data_quality", {})
        return {"ranked_varieties": answer["ranked_varieties"],
                "trials": quality.get("total_trials_analyzed"), "varieties": quality.get("unique_varieties")}
    return answer


def _keys(answers: dict, crop: str, kind: str | None = None) -> list[str]:
    return sorted(k for k in answers if f"|{crop}" in k and (kind is None or k.startswith(kind)))


@needs_docker
def test_the_states_differ_where_they_should(states):
    legacy, mixed, f1 = states
    assert legacy != mixed and mixed != f1
    assert not [k for s in states for k, v in s.items() if isinstance(v, dict) and "__error__" in v]


@needs_docker
def test_a_crop_only_the_legacy_data_answers_is_answered_exactly_as_before(states):
    legacy, mixed, _f1 = states
    keys = _keys(legacy, LEGACY_ONLY_CROP)
    assert keys and all(mixed[k] == legacy[k] for k in keys)
    assert any(legacy[k] for k in keys if k.startswith("variety-trials|ES-Csa"))  # not vacuous
    assert legacy[f"evidence|ES-Csa|{LEGACY_ONLY_CROP}|field"]["items"]


@needs_docker
def test_a_crop_only_f1_answers_is_answered_as_the_f1_graph_alone(states):
    _legacy, mixed, f1 = states
    for crop in F1_ONLY_CROPS:
        keys = _keys(f1, crop)
        assert keys
        for k in keys:
            assert _without_site_list(mixed[k]) == _without_site_list(f1[k]), k
    regional = f1["evidence|ES-Csa|BRSNN|regional"]
    assert regional["items"] and {i["tier"] for i in regional["items"]} == {"regional"}


@needs_docker
def test_where_both_exist_the_field_tier_is_the_legacy_rows_and_the_regional_tier_the_f1_rows(states):
    legacy, mixed, f1 = states
    for climate, country in PARCELS:
        pk = f"{country}-{climate}"
        assert mixed[f"evidence|{pk}|{BOTH_CROP}|field"] == legacy[f"evidence|{pk}|{BOTH_CROP}|field"], pk
        assert mixed[f"evidence|{pk}|{BOTH_CROP}|regional"] == f1[f"evidence|{pk}|{BOTH_CROP}|regional"], pk
    field = mixed[f"evidence|ES-Csa|{BOTH_CROP}|field"]
    assert sorted(i["variety"] for i in field["items"]) == ["Hispanic", "Pewter", "Zebra"]
    regional = mixed[f"evidence|ES-Csa|{BOTH_CROP}|regional"]
    assert regional["items"] and all(i["tier"] == "regional" for i in regional["items"])


@needs_docker
def test_the_listing_endpoints_keep_every_legacy_row_and_add_the_f1_ones(states):
    legacy, mixed, f1 = states
    for climate, country in PARCELS:
        pk = f"{country}-{climate}"
        key = f"variety-trials|{pk}|{BOTH_CROP}"
        before, after = legacy[key], mixed[key]
        rows = lambda r: r if isinstance(r, list) else r.get("items", r.get("trials", []))
        legacy_rows = [r for r in rows(after) if r.get("source_id") == "NAVARRA-AGRARIA"
                       or r.get("sourceId") == "NAVARRA-AGRARIA" or r.get("dataSource") == "NAVARRA-AGRARIA"]
        assert legacy_rows == [r for r in rows(before)], pk
        assert len(rows(after)) >= len(rows(before))
    assert f1[f"variety-trials|ES-Csa|{BOTH_CROP}"] != legacy[f"variety-trials|ES-Csa|{BOTH_CROP}"]


@needs_docker
def test_recommend_combines_both_without_losing_the_legacy_trials(states):
    legacy, mixed, f1 = states
    key = f"recommend|ES-Csa|{BOTH_CROP}"

    def count(response):
        recs = response.get("recommendations") or []
        return sum((r["evidence"].get("trial_count") or 0) for r in recs)

    assert count(legacy[key]) > 0 and count(f1[key]) > 0
    assert count(mixed[key]) >= count(legacy[key])
