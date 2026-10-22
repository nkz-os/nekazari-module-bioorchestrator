"""Bearer tokens for gateway-signed test requests.

The middleware trusts a gateway request only through its HMAC, then reads `sub`
and roles from the token's claims (it does not re-verify the JWT signature), so
these tokens are signed with a throwaway key and the signature is irrelevant.
"""
from __future__ import annotations

from collections.abc import Iterable

import jwt

_KEY = "gateway-test-key-0123456789abcdef0123456789"


def gateway_token(
    sub: str = "u1", tenant: str = "tenant-a", roles: Iterable[str] = ()
) -> str:
    claims = {"sub": sub, "tenant_id": tenant, "realm_access": {"roles": list(roles)}}
    return jwt.encode(claims, _KEY, algorithm="HS256")


USER_TOKEN = gateway_token()
