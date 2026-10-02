"""Which requests under public prefixes still need a verified identity."""
import pytest

from app.auth_policy import requires_identity

URN = "urn:ngsi-ld:AgriParcel:tenant-a:p1"


@pytest.mark.parametrize("path,method,query", [
    ("/api/graph/agriculture/parcel-environment", "GET", {"parcel_id": URN}),
    ("/api/graph/agriculture/suggest-crops", "GET", {"parcel_id": URN}),
    ("/api/graph/agriculture/crop-context", "GET", {"parcel_id": URN}),
    ("/api/graph/agriculture/extrapolate", "GET", {"crop": "TRZAX", "parcel_id": URN}),
    ("/api/graph/agriculture/yield-potential", "GET", {"parcel_id": URN}),
    ("/api/graph/agriculture/assign-crop", "POST", {}),
    ("/api/graph/agriculture/crop-plan", "POST", {}),
    ("/api/graph/agriculture/rotation-optimize", "POST", {}),
    ("/api/graph/agriculture/wofost-simulation", "POST", {"parcel_id": URN}),
    (f"/api/graph/agriculture/crop-plan/{URN}/segments/1/advance", "POST", {}),
    ("/api/graph/agriculture/crop-plan", "GET", {"parcel_id": URN}),
])
def test_tenant_requests_require_identity(path, method, query):
    assert requires_identity(path, method, query) is True


@pytest.mark.parametrize("path,method,query", [
    ("/api/graph/agriculture/extrapolate", "GET", {"crop": "TRZAX", "climate_class": "Csa"}),
    ("/api/graph/agriculture/trial-sites", "GET", {}),
    ("/api/graph/agriculture/crop-name", "GET", {"eppo": "TRZAX"}),
    ("/api/graph/agriculture/nutrient-profile", "GET", {"crop": "TRZAX"}),
    ("/api/graph/agriculture/variety-trials", "GET", {"crop": "TRZAX"}),
    ("/api/graph/agriculture/yield-potential", "GET", {"crop": "TRZAX"}),
    ("/api/graph/phenology-stages", "GET", {"species": "Zea mays"}),
    ("/api/ngsi-ld/notify", "POST", {}),
    ("/api/graph/internal/phenology-update", "POST", {}),
])
def test_reference_requests_stay_public(path, method, query):
    assert requires_identity(path, method, query) is False


def test_empty_parcel_id_still_requires_identity():
    # An explicitly present but empty parcel_id is still a tenant-shaped request.
    assert requires_identity("/api/graph/agriculture/crop-context", "GET", {"parcel_id": ""}) is True
