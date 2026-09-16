from app.core.config import settings


def test_catalog_tenant_defaults_to_shared():
    assert settings.catalog_tenant == "shared"
