import pytest
from app.graph.dao import GraphDAO


def test_extract_value_standard():
    entity = {"kcIni": {"type": "Property", "value": 0.55}}
    assert GraphDAO._extract_value(entity, "kcIni") == 0.55


def test_extract_value_missing():
    entity = {"name": {"type": "Property", "value": "Olive"}}
    assert GraphDAO._extract_value(entity, "kcIni") is None


def test_extract_value_raw_scalar():
    """If someone passes a raw int (legacy), treat as value."""
    entity = {"kcIni": 0.55}
    assert GraphDAO._extract_value(entity, "kcIni") == 0.55
