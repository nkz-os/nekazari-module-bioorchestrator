"""The tenant must never be taken from the request itself."""
from types import SimpleNamespace

from app.api.v1.graph import _get_tenant_id


def _req(state_tenant="", headers=None, query=None):
    return SimpleNamespace(
        state=SimpleNamespace(tenant_id=state_tenant) if state_tenant else SimpleNamespace(),
        headers=headers or {},
        query_params=query or {},
    )


def test_state_tenant_is_used():
    assert _get_tenant_id(_req("tenant-a")) == "tenant-a"


def test_header_is_ignored():
    assert _get_tenant_id(_req(headers={"X-Tenant-ID": "tenant-b"})) == ""


def test_query_param_is_ignored():
    assert _get_tenant_id(_req(query={"tenant_id": "tenant-b"})) == ""


def test_parcel_urn_is_ignored():
    assert _get_tenant_id(_req(query={"parcel_id": "urn:ngsi-ld:AgriParcel:tenant-b:p1"})) == ""
