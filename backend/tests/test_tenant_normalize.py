"""Regression: normalize_tenant_id must be hyphen-canonical.

The platform canonical tenant format is hyphenated (K8s-native; see the
authoritative nkz/services/common/tenant_utils.py). The old copy here
replaced hyphens with underscores, so auth.py stored an underscored tenant
in request.state.tenant_id and every Orion request went to a phantom tenant
(e.g. asociacion-allotarra -> asociacion_allotarra).
"""

import pytest

from app.common.tenant_utils import normalize_tenant_id


def test_hyphen_is_preserved():
    assert normalize_tenant_id("asociacion-allotarra") == "asociacion-allotarra"


def test_already_canonical_is_idempotent():
    assert normalize_tenant_id("test-tenant-1") == "test-tenant-1"


def test_lowercases_and_collapses_invalid_chars_to_hyphen():
    assert normalize_tenant_id("My Tenant@123") == "my-tenant-123"


def test_strips_leading_trailing_separators():
    assert normalize_tenant_id("  -Foo--Bar-  ") == "foo-bar"


def test_no_underscore_introduced():
    assert "_" not in normalize_tenant_id("a-b-c-d")


def test_empty_raises():
    with pytest.raises(ValueError):
        normalize_tenant_id("")
