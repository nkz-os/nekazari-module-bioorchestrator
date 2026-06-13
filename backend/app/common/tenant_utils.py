#!/usr/bin/env python3
# =============================================================================
# Tenant Utilities - Common Functions for Tenant ID Normalization
# =============================================================================
# Provides consistent tenant ID normalization across all services
# Ensures compatibility with PostgreSQL, MongoDB, and other services

import re
import logging

logger = logging.getLogger(__name__)

# Tenant ID constraints
MIN_TENANT_ID_LENGTH = 3
MAX_TENANT_ID_LENGTH = 63  # MongoDB database name limit
ALLOWED_CHARS_PATTERN = re.compile(r'^[a-z0-9]+(?:-[a-z0-9]+)*$')


def normalize_tenant_id(tenant_id: str) -> str:
    """
    Normalize tenant ID to the platform canonical (hyphen) format.

    The canonical tenant format is hyphenated and K8s-native — see the
    authoritative services/common/tenant_utils.py
    (`^[a-z0-9]+(?:-[a-z0-9]+)*$`). Hyphens MUST be preserved: underscoring
    them here routed every Orion request to a phantom underscore tenant.

    Rules:
    - Convert to lowercase
    - Collapse any run of non-alphanumeric characters into a single hyphen
    - Strip leading/trailing hyphens
    - Ensure minimum and maximum length

    Args:
        tenant_id: Raw tenant ID string

    Returns:
        Normalized, hyphen-canonical tenant ID string

    Examples:
        >>> normalize_tenant_id("TESTTENANT")
        'testtenant'
        >>> normalize_tenant_id("Test-Tenant-1")
        'test-tenant-1'
        >>> normalize_tenant_id("My Tenant@123")
        'my-tenant-123'
    """
    if not tenant_id:
        raise ValueError("Tenant ID cannot be empty")

    # Convert to lowercase
    normalized = tenant_id.lower().strip()

    # Collapse anything that is not [a-z0-9] into a single hyphen (canonical).
    normalized = re.sub(r'[^a-z0-9]+', '-', normalized)

    # Remove leading/trailing hyphens
    normalized = normalized.strip('-')
    
    # Validate length
    if len(normalized) < MIN_TENANT_ID_LENGTH:
        raise ValueError(
            f"Tenant ID must be at least {MIN_TENANT_ID_LENGTH} characters after normalization. "
            f"Got: '{normalized}' (from '{tenant_id}')"
        )
    
    if len(normalized) > MAX_TENANT_ID_LENGTH:
        raise ValueError(
            f"Tenant ID must be at most {MAX_TENANT_ID_LENGTH} characters after normalization. "
            f"Got: '{normalized}' (from '{tenant_id}')"
        )
    
    # Final validation: should only contain allowed characters
    if not ALLOWED_CHARS_PATTERN.match(normalized):
        raise ValueError(
            f"Tenant ID contains invalid characters after normalization. "
            f"Only lowercase letters, numbers, and hyphens are allowed. "
            f"Got: '{normalized}' (from '{tenant_id}')"
        )
    
    return normalized

