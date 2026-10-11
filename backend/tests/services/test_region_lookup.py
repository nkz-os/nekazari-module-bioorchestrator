"""Point -> Spanish province (INE code) over the bundled Natural Earth admin-1 boundaries."""
import json

import pytest

from app.services import region_lookup as rl


@pytest.mark.parametrize("lat,lon,code,name", [
    (42.8125, -1.6458, "31", "Navarra"),                  # Pamplona
    (42.8467, -2.6716, "01", "Araba/Álava"),              # Vitoria-Gasteiz
    (39.8628, -4.0273, "45", "Toledo"),                   # Toledo
    (38.9848, -3.9274, "13", "Ciudad Real"),              # Ciudad Real
    (28.4636, -16.2518, "38", "Santa Cruz de Tenerife"),  # Santa Cruz de Tenerife
    (41.6176, 0.6200, "25", "Lleida"),                    # Lleida
])
def test_provinces(lat, lon, code, name):
    assert rl.es_province_at(lat, lon) == {"scheme": "es_province", "code": code, "name": name}


@pytest.mark.parametrize("lat,lon", [(38.7223, -9.1393), (45.5, -5.0), (43.2965, 5.3698)])  # Lisbon, Biscay, Marseille
def test_outside_spain(lat, lon):
    assert rl.es_province_at(lat, lon) is None


@pytest.mark.parametrize("lat,lon", [(None, -3.7), (40.4, None)])
def test_none_inputs(lat, lon):
    assert rl.es_province_at(lat, lon) is None


def test_data_file_has_every_province_once():
    data = json.loads(rl.DEFAULT_PATH.read_text(encoding="utf-8"))
    codes = sorted(f["properties"]["ine_code"] for f in data["features"])
    assert codes == [f"{i:02d}" for i in range(1, 53)]
    assert all(f["properties"]["name"] for f in data["features"])
