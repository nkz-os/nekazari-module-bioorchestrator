"""Public balance reference endpoints (DAO patched; attributions from the real registry)."""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

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
