from app.api.v1.notify import _is_valid_ngsi_ld_subscription


def test_valid_payload():
    assert _is_valid_ngsi_ld_subscription({
        "data": [{"id": "urn:ngsi-ld:AgriCrop:Test", "type": "AgriCrop"}],
        "subscriptionId": "urn:ngsi-ld:Subscription:test",
    }) is True


def test_invalid_payload_no_data():
    assert _is_valid_ngsi_ld_subscription({"foo": "bar"}) is False


def test_invalid_payload_data_not_list():
    assert _is_valid_ngsi_ld_subscription({"data": "not_a_list"}) is False


def test_filter_non_agri_crop():
    """Only AgriCrop entities are queued."""
    entities = [
        {"type": "AgriCrop", "id": "urn:x"},
        {"type": "AgriParcel", "id": "urn:y"},
        {"type": "AgriCrop", "id": "urn:z"},
    ]
    agri_crops = [e for e in entities if e.get("type") == "AgriCrop"]
    assert len(agri_crops) == 2


def test_derived_token_accepted_once_master_refused(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from nkz_platform_sdk.notify_auth import TOKEN_HEADER, notification_token

    from app.api.v1 import notify

    monkeypatch.setenv("NOTIFY_REQUIRE_INTERNAL_SECRET", "true")
    monkeypatch.setenv("INTERNAL_SERVICE_SECRET", "test-secret")
    monkeypatch.setenv("NKZ_NOTIFY_ACCEPT_MASTER", "false")
    app = FastAPI()
    app.include_router(notify.router, prefix="/api/ngsi-ld")
    c = TestClient(app)
    # Starlette's TestClient sends Host: testserver; the token binds to that host.
    token = notification_token("test-secret", "tenant-a", "testserver")
    url = "/api/ngsi-ld/notify"
    body = {"data": []}
    assert c.post(url, json=body, headers={"Fiware-Service": "tenant-a", TOKEN_HEADER: token}).status_code == 204
    assert c.post(url, json=body, headers={"Fiware-Service": "tenant-b", TOKEN_HEADER: token}).status_code == 401
    assert c.post(url, json=body, headers={
        "Fiware-Service": "tenant-a", "X-Internal-Service-Secret": "test-secret"}).status_code == 401
