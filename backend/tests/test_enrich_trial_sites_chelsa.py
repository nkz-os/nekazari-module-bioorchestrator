import json
import re
from unittest.mock import AsyncMock, MagicMock

import pytest

from scripts import enrich_trial_sites_chelsa as en

CLIMATE = {
    "koppen": "Cfb", "annual_temp_c": 11.2, "annual_rainfall_mm": 640.0,
    "annual_et0_mm": 800.0, "coldest_month_min_c": -2.5,
}


def _site(sid, name, lat, lon, **extra):
    props = {"name": name, "latitude": lat, "longitude": lon, "climateClass": "Csa", **extra}
    return {"id": sid, "props": props}


def _driver():
    session = MagicMock()
    res = MagicMock()
    res.consume = AsyncMock(return_value=MagicMock())
    res.single = AsyncMock(return_value={"n": 1})
    session.run = AsyncMock(return_value=res)
    session.res = res
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    driver = MagicMock()
    driver.session = MagicMock(return_value=session)
    return driver, session


def _dao(result=CLIMATE):
    dao = MagicMock()
    dao.parcel_climate = AsyncMock(return_value=result)
    dao.save_climate_cell = AsyncMock()
    return dao


SITES = [_site("4:a:1", "Olite", 42.48, -1.65), _site("4:a:2", "Tudela", 42.06, -1.60)]


@pytest.fixture
def read_cell(monkeypatch):
    mock = AsyncMock(return_value=CLIMATE)
    monkeypatch.setattr(en, "read_cell", mock)
    return mock


async def test_dry_run_writes_nothing_and_prints_csv(capsys, read_cell):
    driver, session = _driver()
    dao = _dao()
    counts = await en.enrich(driver, dao, SITES, execute=False, backup=None)
    dao.parcel_climate.assert_not_awaited()
    dao.save_climate_cell.assert_not_awaited()
    session.run.assert_not_awaited()
    out = capsys.readouterr().out.splitlines()
    assert out[0] == "name,stored,chelsa,changed"
    assert out[1] == "Olite,Csa,Cfb,True"
    assert counts["computed"] == 2 and counts["written"] == 0


async def test_dry_run_reads_cell_directly_with_timeout(read_cell):
    await en.enrich(_driver()[0], _dao(), SITES[:1], execute=False, backup=None)
    assert read_cell.await_args.args == (42.48, -1.65)
    assert read_cell.await_args.kwargs == {"timeout_s": 90}


async def test_parcel_climate_called_with_wait_true(tmp_path):
    dao = _dao()
    await en.enrich(_driver()[0], dao, SITES[:1], execute=True, backup=str(tmp_path / "b.json"))
    assert dao.parcel_climate.await_args.args == (42.48, -1.65)
    assert dao.parcel_climate.await_args.kwargs == {"wait": True, "timeout_s": 90}


def test_execute_without_backup_exits_nonzero():
    with pytest.raises(SystemExit) as exc:
        en.parse_args(["--execute"])
    assert exc.value.code != 0


def test_default_is_dry_run():
    args = en.parse_args([])
    assert args.execute is False


async def test_execute_writes_only_new_properties_after_backup(tmp_path):
    driver, session = _driver()
    backup = tmp_path / "bk.json"
    order = []
    real_backup = en.write_backup

    def spy(path, sites):
        order.append("backup")
        real_backup(path, sites)

    async def _run(*a, **k):
        order.append("write")
        return session.res

    session.run.side_effect = _run
    en.write_backup = spy
    try:
        counts = await en.enrich(driver, _dao(), SITES, execute=True, backup=str(backup))
    finally:
        en.write_backup = real_backup
    assert order == ["backup", "write", "write"]
    assert counts["written"] == 2
    dump = json.loads(backup.read_text())
    assert [d["id"] for d in dump] == ["4:a:1", "4:a:2"]
    assert dump[0]["properties"]["climateClass"] == "Csa"
    query = session.run.await_args_list[0].args[0]
    assert not re.search(r"climateClass(?!Chelsa)", query)
    for legacy in ("annualRainfallMm", "annualET0Mm", "frostDaysPerYear", "latitude", "name"):
        assert not re.search(rf"ts\.{legacy}(?!Chelsa)", query.split("SET", 1)[1])
    params = session.run.await_args_list[0].kwargs
    assert params["climateClassChelsa"] == "Cfb"
    assert params["annualRainfallMmChelsa"] == 640.0
    assert params["climateChelsaCellKey"] == en.cell_key(42.48, -1.65)
    assert params["climateChelsaComputedAt"]


async def test_backup_failure_writes_nothing(tmp_path):
    driver, session = _driver()
    bad = tmp_path / "missing-dir" / "bk.json"
    with pytest.raises(OSError):
        await en.enrich(driver, _dao(), SITES, execute=True, backup=str(bad))
    session.run.assert_not_awaited()


async def test_existing_backup_is_not_overwritten(tmp_path):
    driver, session = _driver()
    bk = tmp_path / "bk.json"
    bk.write_text("keep")
    with pytest.raises(OSError):
        await en.enrich(driver, _dao(), SITES, execute=True, backup=str(bk))
    assert bk.read_text() == "keep"
    session.run.assert_not_awaited()


async def test_idempotent_skip_when_cell_key_matches(read_cell):
    done = _site("4:a:1", "Olite", 42.48, -1.65,
                 climateChelsaCellKey=en.cell_key(42.48, -1.65))
    dao = _dao()
    counts = await en.enrich(_driver()[0], dao, [done, SITES[1]], execute=False, backup=None)
    assert read_cell.await_count == 1
    assert counts["skipped"] == 1 and counts["computed"] == 1


async def test_failed_cell_is_not_written(tmp_path):
    driver, session = _driver()
    dao = _dao(result=None)
    counts = await en.enrich(driver, dao, SITES[:1], execute=True, backup=str(tmp_path / "b.json"))
    session.run.assert_not_awaited()
    assert counts["failed"] == 1 and counts["written"] == 0


async def test_site_without_coordinates_is_skipped(read_cell):
    bad = {"id": "x", "props": {"name": "N", "latitude": None, "longitude": None}}
    dao = _dao()
    counts = await en.enrich(_driver()[0], dao, [bad], execute=False, backup=None)
    dao.parcel_climate.assert_not_awaited()
    assert counts["no_coords"] == 1


async def test_driver_error_propagates_and_is_not_counted(tmp_path):
    driver, session = _driver()
    session.run.side_effect = RuntimeError("neo4j down")
    bk = tmp_path / "b.json"
    with pytest.raises(RuntimeError):
        await en.enrich(driver, _dao(), SITES, execute=True, backup=str(bk))
    assert bk.exists()
    assert json.loads(bk.read_text())[0]["id"] == "4:a:1"


async def test_zero_matched_nodes_counts_as_failed(tmp_path, caplog):
    driver, session = _driver()
    session.res.single.return_value = {"n": 0}
    counts = await en.enrich(driver, _dao(), SITES[:1], execute=True, backup=str(tmp_path / "b.json"))
    assert counts["written"] == 0 and counts["failed"] == 1
    assert "Olite" in caplog.text


async def test_write_query_guards_on_coordinates_and_counts():
    assert "ts.latitude = $lat AND ts.longitude = $lon" in en.WRITE_QUERY
    assert "RETURN count(ts) AS n" in en.WRITE_QUERY


@pytest.mark.parametrize("missing", ["koppen", "annual_rainfall_mm", "annual_et0_mm"])
async def test_incomplete_cell_is_all_or_nothing(tmp_path, missing):  # failed or incomplete
    driver, session = _driver()
    data = {**CLIMATE, missing: None}
    counts = await en.enrich(driver, _dao(result=data), SITES[:1], execute=True,
                             backup=str(tmp_path / "b.json"))
    session.run.assert_not_awaited()
    assert counts["failed"] + counts["incomplete"] == 1 and counts["written"] == 0


def test_backup_without_execute_warns(caplog):
    en.parse_args(["--backup", "x.json"])
    assert "no effect without --execute" in caplog.text


def test_backup_records_identity_fields(tmp_path):
    en.write_backup(str(tmp_path / "b.json"), SITES[:1])
    rec = json.loads((tmp_path / "b.json").read_text())[0]
    assert rec["name"] == "Olite" and rec["latitude"] == 42.48 and rec["longitude"] == -1.65


async def test_coastal_cell_without_et0_is_incomplete_not_failed(tmp_path, caplog):
    driver, session = _driver()
    data = {**CLIMATE, "annual_et0_mm": None}
    counts = await en.enrich(driver, _dao(result=data), SITES[:1], execute=True,
                             backup=str(tmp_path / "b.json"))
    session.run.assert_not_awaited()
    assert counts["incomplete"] == 1 and counts["failed"] == 0 and counts["written"] == 0
    assert "Olite" in caplog.text
    assert en.exit_code(counts) == 0


@pytest.mark.parametrize("missing", ["koppen", "annual_rainfall_mm"])
async def test_missing_koppen_or_rain_stays_failed(tmp_path, missing):
    counts = await en.enrich(_driver()[0], _dao(result={**CLIMATE, missing: None}), SITES[:1],
                             execute=True, backup=str(tmp_path / "b.json"))
    assert counts["failed"] == 1 and counts["incomplete"] == 0
    assert en.exit_code(counts) == 1


def test_exit_code_zero_only_without_failures():
    assert en.exit_code({"failed": 0, "incomplete": 3}) == 0
    assert en.exit_code({"failed": 1, "incomplete": 0}) == 1
