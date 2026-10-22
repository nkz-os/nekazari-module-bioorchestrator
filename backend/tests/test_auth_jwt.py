"""JWT validation must pin the issuer and never fall back to a built-in JWKS URL."""
from __future__ import annotations

import time
from unittest.mock import MagicMock

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from app import auth
from app.auth import NKZAuthMiddleware

ISS = "https://idp.example/realms/r"
_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _token(iss: str = ISS, exp_offset: int = 300) -> str:
    return jwt.encode(
        {"iss": iss, "sub": "u1", "tenant_id": "tenant-a", "exp": int(time.time()) + exp_offset},
        _KEY, algorithm="RS256",
    )


@pytest.fixture
def strict(monkeypatch):
    monkeypatch.setenv("AUTH_STRICT", "true")
    monkeypatch.setenv("KEYCLOAK_JWKS_URL", "https://idp.example/certs")
    monkeypatch.setenv("JWT_ISSUERS", ISS)
    fake = MagicMock()
    fake.get_signing_key_from_jwt.return_value = MagicMock(key=_KEY.public_key())
    monkeypatch.setattr(auth, "_jwks_client", lambda url: fake)
    return NKZAuthMiddleware(app=MagicMock())


async def test_valid_token_accepted(strict):
    payload = await strict._validate_token(_token())
    assert payload["tenant_id"] == "tenant-a"


async def test_wrong_issuer_rejected(strict):
    with pytest.raises(jwt.InvalidIssuerError):
        await strict._validate_token(_token(iss="https://idp.example/realms/other"))


async def test_expired_token_rejected(strict):
    with pytest.raises(jwt.ExpiredSignatureError):
        await strict._validate_token(_token(exp_offset=-10))


async def test_missing_jwks_url_fails_closed(strict, monkeypatch):
    monkeypatch.delenv("KEYCLOAK_JWKS_URL")
    with pytest.raises(RuntimeError):
        await strict._validate_token(_token())


async def test_missing_issuers_fails_closed(strict, monkeypatch):
    monkeypatch.setenv("JWT_ISSUERS", "  ")
    with pytest.raises(RuntimeError):
        await strict._validate_token(_token())


async def test_tampered_signature_rejected(strict):
    header, payload, sig = _token().split(".")
    tampered = f"{header}.{payload}.{sig[:-4]}{'AAAA' if sig[-4:] != 'AAAA' else 'BBBB'}"
    with pytest.raises(jwt.InvalidSignatureError):
        await strict._validate_token(tampered)


async def test_hs256_token_rejected(strict):
    forged = jwt.encode(
        {"iss": ISS, "sub": "u1", "tenant_id": "tenant-a", "exp": int(time.time()) + 300},
        "attacker-chosen-secret-of-sufficient-length", algorithm="HS256",
    )
    with pytest.raises(jwt.PyJWTError):
        await strict._validate_token(forged)
