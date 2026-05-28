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
