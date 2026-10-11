import pytest

from app.planning.service import InvalidParcelId, parcel_urn

REAL = "urn:ngsi-ld:AgriParcel:12d68d51-35f5-4957-9222-caa961f9a427"
TENANT_STYLE = "urn:ngsi-ld:AgriParcel:t:p1"


@pytest.mark.parametrize("raw, expected", [
    (REAL, REAL),
    (TENANT_STYLE, TENANT_STYLE),
    ("p1", "urn:ngsi-ld:AgriParcel:p1"),
    ("AgriParcel:p1", "urn:ngsi-ld:AgriParcel:p1"),
])
def test_accepts_real_tenant_style_and_short_ids(raw, expected):
    assert parcel_urn(raw) == expected


@pytest.mark.parametrize("raw", [
    'x?x="', "p1?x=1", "p1#frag", "#frag", "a/../../version", "../version", 'p"1', "", "p 1", "p1\n",
    "urn:ngsi-ld:AgriCrop:c1", "urn:ngsi-ld:AgriParcel:", "p1;q", "parcéla",
])
def test_rejects_anything_that_could_rewrite_a_query_or_path(raw):
    with pytest.raises(InvalidParcelId):
        parcel_urn(raw)


def test_invalid_parcel_id_is_a_value_error():
    assert issubclass(InvalidParcelId, ValueError)
