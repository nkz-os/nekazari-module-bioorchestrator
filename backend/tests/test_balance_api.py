"""Public balance reference endpoints (DAO patched; attributions from the real registry)."""
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.api.v1 import balance as balance_api
from app.auth import SKIP_AUTH_PREFIXES
from app.core.dependencies import get_neo4j_driver
from app.graph.balance_dao import BalanceDao
from app.main import app
from app.services.crop_balance import to_node_props

CROP = {
    "crop_type": "cereal", "harvest_product": "grain", "residue_product": "straw",
    "n_fixing": False,
    "values": {
        "n_product_pct": {"value": 2.0, "source_id": "ludemann_cc0", "ref": "Tier 1, wheat",
                          "raw": "2.0 % DM", "transformation": "none"},
        "p_product_pct": {"value": 0.356, "source_id": "panagos_2022", "ref": "Table 1",
                          "raw": "0.356 % DM", "transformation": "none"},
    },
}
NODE = to_node_props("TRZAX", CROP, "2026-10-11.1")
YIELDS = [
    {"key": f"apro_cpshr|ES22|C1110|{y}", "dataset": "apro_cpshr",
     "sourceId": "eurostat_apro_cpshr", "nuts": "ES22", "nutsLevel": 2,
     "nutsVersion": "as published", "eurostatCrop": "C1110", "eppo": "TRZAX", "year": y,
     "areaHa": a, "productionT": p, "humidityBasis": "eu_standard", "humidityPct": 14.0,
     "yieldTHa": p / a, "yieldMethod": "production_over_area", "flags": [],
     "datasetUpdated": "2026-09-08T23:00:00+0200", "extractedAt": "2026-10-11T09:29:00+00:00"}
    for y, a, p in [(2022, 79000.0, 316000.0), (2023, 81360.0, 331950.0),
                    (2024, 76600.0, 431910.0)]
]


@pytest.fixture(scope="module")
def client():
    mp = pytest.MonkeyPatch()
    mp.setenv("AUTH_DISABLED", "true")
    app.dependency_overrides[get_neo4j_driver] = lambda: MagicMock()
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.pop(get_neo4j_driver, None)
        mp.undo()


def test_routes_are_public():
    assert "/api/graph/agriculture/" in SKIP_AUTH_PREFIXES


def test_coefficients_with_provenance_and_attributions(client):
    with patch.object(BalanceDao, "get_crop_coefficients", AsyncMock(return_value=NODE)) as m:
        body = client.get("/api/graph/agriculture/crop-balance-coefficients?eppo=trzax").json()
    m.assert_awaited_once_with("TRZAX")
    assert body["values"]["p_product_pct"]["source_id"] == "panagos_2022"
    assert body["values"]["n_product_pct"]["unit"] == "% DM"
    assert "balance_harvest_index_pct" in body["missing"]
    assert [a["source_id"] for a in body["attributions"]] == ["ludemann_cc0", "panagos_2022"]


def test_coefficients_unknown_crop_is_404(client):
    with patch.object(BalanceDao, "get_crop_coefficients", AsyncMock(return_value=None)):
        resp = client.get("/api/graph/agriculture/crop-balance-coefficients?eppo=ABCDE")
    assert resp.status_code == 404


def test_coefficients_bad_eppo_is_422(client):
    assert client.get("/api/graph/agriculture/crop-balance-coefficients?eppo=a%21").status_code \
        == 422


def test_regional_yields_series_median_and_attribution(client):
    with patch.object(BalanceDao, "get_regional_yields", AsyncMock(return_value=YIELDS)) as m:
        body = client.get("/api/graph/agriculture/regional-yields?eppo=TRZAX&nuts=es22").json()
    m.assert_awaited_once_with("TRZAX", "ES22")
    assert body["humidity_basis"] == "eu_standard" and body["recent_years"] == [2022, 2023, 2024]
    assert body["years_used"] == 3 and body["latest_year"] == 2024
    assert body["recent_median_t_ha"] == pytest.approx(331950 / 81360)
    assert [a["source_id"] for a in body["attributions"]] == ["eurostat_apro_cpshr"]


def test_regional_yields_empty_is_404(client):
    with patch.object(BalanceDao, "get_regional_yields", AsyncMock(return_value=[])):
        resp = client.get("/api/graph/agriculture/regional-yields?eppo=TRZAX&nuts=ES22")
    assert resp.status_code == 404


def test_coefficients_maize_registry_code_resolves_to_the_stored_code(client):
    with patch.object(BalanceDao, "get_crop_coefficients", AsyncMock(return_value=NODE)) as m:
        resp = client.get("/api/graph/agriculture/crop-balance-coefficients?eppo=zeama")
    assert resp.status_code == 200
    m.assert_awaited_once_with("ZEAMX")


def test_regional_yields_maize_registry_code_resolves_to_the_stored_code(client):
    with patch.object(BalanceDao, "get_regional_yields", AsyncMock(return_value=YIELDS)) as m:
        resp = client.get("/api/graph/agriculture/regional-yields?eppo=ZEAMA&nuts=es22")
    assert resp.status_code == 200
    m.assert_awaited_once_with("ZEAMX", "ES22")


# ── plausibility bounds from the crop_yield ranges ───────────────────────────

def _with_outlier(rows, eppo="TRZAX"):
    # one extra year at 15 t/ha (above the TRZAX maximum of 14)
    outlier = dict(rows[-1], year=2025, areaHa=1000.0, productionT=15000.0, yieldTHa=15.0)
    return [dict(r, eppo=eppo) for r in [*rows, outlier]]


def test_regional_yields_wheat_has_its_range_and_excludes_an_implausible_year(client):
    rows = _with_outlier(YIELDS)
    with patch.object(BalanceDao, "get_regional_yields", AsyncMock(return_value=rows)):
        body = client.get("/api/graph/agriculture/regional-yields?eppo=TRZAX&nuts=es22").json()
    assert body["plausible_range"] == {
        "min_t_ha": 0.5, "max_t_ha": 14.0, "range_id": "crop_yield.TRZAX.grain",
        "status": "assumption", "excludes": "above_max"}
    assert [s["range_check"] for s in body["series"]] == ["ok", "ok", "ok", "above_max"]
    assert body["series"][-1]["year"] == 2025 and body["series"][-1]["yield_t_ha"] == 15.0
    assert body["recent_years"] == [2022, 2023, 2024] and body["latest_year"] == 2024
    assert body["recent_median_t_ha"] == pytest.approx(331950 / 81360)


def test_regional_yields_maize_range_comes_from_the_stored_code(client):
    with patch.object(BalanceDao, "get_regional_yields", AsyncMock(return_value=YIELDS)):
        body = client.get("/api/graph/agriculture/regional-yields?eppo=ZEAMA&nuts=es22").json()
    assert body["plausible_range"]["range_id"] == "crop_yield.ZEAMX.grain"
    assert (body["plausible_range"]["min_t_ha"], body["plausible_range"]["max_t_ha"]) == (3.0, 24.0)


def test_regional_yields_crop_without_a_range_is_not_checked(client):
    rows = _with_outlier(YIELDS, eppo="BEAVX")
    with patch.object(BalanceDao, "get_regional_yields", AsyncMock(return_value=rows)):
        body = client.get("/api/graph/agriculture/regional-yields?eppo=BEAVX&nuts=es22").json()
    assert body["plausible_range"] is None
    assert [s["range_check"] for s in body["series"]] == [None] * 4
    assert body["recent_years"] == [2022, 2023, 2024, 2025]   # the 15 t/ha year stays


def test_plausible_range_converts_kg_ha_to_t_ha():
    got = balance_api._plausible_range("TRZAX")
    assert (got.min_t_ha, got.max_t_ha, got.range_id, got.status) == (
        0.5, 14.0, "crop_yield.TRZAX.grain", "assumption")
    assert balance_api._plausible_range("BEAVX") is None


def test_plausible_range_in_another_unit_is_not_guessed(caplog):
    fake_range = SimpleNamespace(id="crop_yield.XXXXX.grain", min=1.0, max=9.0, unit="t/ha",
                                 status="assumption")
    fake = SimpleNamespace(range_for=lambda *_a, **_k: fake_range)
    with (patch.object(balance_api, "_registries", lambda: fake),
          caplog.at_level(logging.WARNING, logger=balance_api.logger.name)):
        assert balance_api._plausible_range("XXXXX") is None
    assert any("crop_yield.XXXXX.grain" in r.getMessage() and r.levelno == logging.WARNING
               for r in caplog.records)


def test_registries_are_loaded_once():
    assert balance_api._registries() is balance_api._registries()


def test_nuts_region_for_a_spanish_municipality(client):
    body = client.get("/api/graph/agriculture/nuts-region?municipality=01059").json()
    assert body == {"municipality": "01059", "country": "ES", "nuts": "ES21",
                    "name": "País Vasco", "nuts_version": 2024, "method": "ine_province"}


def test_nuts_region_bad_and_unknown_codes(client):
    assert client.get("/api/graph/agriculture/nuts-region?municipality=1059").status_code == 422
    resp = client.get("/api/graph/agriculture/nuts-region?municipality=99001")
    assert resp.status_code == 404 and resp.json()["detail"] == "Unknown province"


def test_nuts_regions_lists_named_regions_with_data(client):
    rows = [{"nuts": "ES22", "eppos": ["HORVX", "TRZAX"], "latest_year": 2024},
            {"nuts": "ESZZ", "eppos": ["TRZAX"], "latest_year": 2024}]
    with patch.object(BalanceDao, "list_yield_regions", AsyncMock(return_value=rows)) as m:
        body = client.get("/api/graph/agriculture/nuts-regions?country=es").json()
    m.assert_awaited_once_with("ES")
    assert body["regions"] == [
        {"nuts": "ES22", "name": "Comunidad Foral de Navarra", "eppos": ["HORVX", "TRZAX"],
         "latest_year": 2024}]
    assert [a["source_id"] for a in body["attributions"]] == ["eurostat_apro_cpshr"]


def test_nuts_regions_none_is_404_and_bad_country_422(client):
    with patch.object(BalanceDao, "list_yield_regions", AsyncMock(return_value=[])):
        resp = client.get("/api/graph/agriculture/nuts-regions?country=XX")
    assert resp.status_code == 404 and resp.json()["detail"] == "No regions with yields"
    assert client.get("/api/graph/agriculture/nuts-regions?country=E5").status_code == 422


@pytest.fixture(autouse=True)
def _fresh_region_cache():
    from app.api.v1 import balance
    balance._REGION_CACHE.clear()
    yield
    balance._REGION_CACHE.clear()


def test_nuts_regions_drops_superseded_and_passes_latest_year(client, _fresh_region_cache):
    rows = [{"nuts": "ITD5", "eppos": ["TRZAX"], "latest_year": 2014},
            {"nuts": "ITH5", "eppos": ["TRZAX"], "latest_year": 2024}]
    with patch.object(BalanceDao, "list_yield_regions", AsyncMock(return_value=rows)):
        body = client.get("/api/graph/agriculture/nuts-regions?country=IT").json()
    assert [(r["nuts"], r["latest_year"]) for r in body["regions"]] == [("ITH5", 2024)]


def test_nuts_regions_cached_within_ttl(client, _fresh_region_cache):
    rows = [{"nuts": "ES22", "eppos": ["TRZAX"], "latest_year": 2024}]
    clock = [1000.0]
    with patch("app.api.v1.balance._monotonic", lambda: clock[0]), \
            patch.object(BalanceDao, "list_yield_regions", AsyncMock(return_value=rows)) as m:
        assert client.get("/api/graph/agriculture/nuts-regions?country=ES").status_code == 200
        clock[0] += 599
        assert client.get("/api/graph/agriculture/nuts-regions?country=es").status_code == 200
        assert m.await_count == 1
        clock[0] += 2
        assert client.get("/api/graph/agriculture/nuts-regions?country=ES").status_code == 200
        assert m.await_count == 2


def test_nuts_region_survives_names_failure(client):
    with patch("app.api.v1.balance.load_region_names", side_effect=OSError("boom")):
        resp = client.get("/api/graph/agriculture/nuts-region?municipality=31201")
    assert resp.status_code == 200
    assert resp.json()["nuts"] == "ES22" and resp.json()["name"] is None


def test_warm_nuts_reference_logs_error_and_does_not_raise(caplog):
    from app.api.v1 import balance
    with patch("app.api.v1.balance.load_region_names", side_effect=OSError("boom")), \
            caplog.at_level("ERROR"):
        balance.warm_nuts_reference()
    assert any("boom" in r.getMessage() for r in caplog.records)
