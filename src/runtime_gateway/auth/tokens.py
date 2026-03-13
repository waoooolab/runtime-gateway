"""Minimal HMAC token utilities for runtime-gateway P1 baseline."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
import uuid
from typing import Any


class TokenError(ValueError):
    """Token verification or parsing error."""


_DEFAULT_TOKEN_SECRET = "dev-insecure-secret"


def _env_truthy(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _secret() -> bytes:
    value = os.environ.get("RUNTIME_GATEWAY_TOKEN_SECRET", _DEFAULT_TOKEN_SECRET)
    if _env_truthy("WAOOOOLAB_STRICT_TOKEN_SECRET", default=False) and value == _DEFAULT_TOKEN_SECRET:
        raise TokenError(
            "insecure default token secret is forbidden when WAOOOOLAB_STRICT_TOKEN_SECRET=true"
        )
    return value.encode("utf-8")


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _b64url_decode(raw: str) -> bytes:
    pad = "=" * (-len(raw) % 4)
    return base64.urlsafe_b64decode(raw + pad)


def _sign(payload_part: str) -> str:
    digest = hmac.new(_secret(), payload_part.encode("utf-8"), hashlib.sha256).digest()
    return _b64url_encode(digest)


def issue_token(claims: dict[str, Any], ttl_seconds: int = 300) -> str:
    """Issue a signed token from claims."""
    now = int(time.time())
    merged = dict(claims)
    merged.setdefault("iat", now)
    merged.setdefault("exp", now + ttl_seconds)
    merged.setdefault("jti", str(uuid.uuid4()))
    payload_bytes = json.dumps(
        merged, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    payload_part = _b64url_encode(payload_bytes)
    signature_part = _sign(payload_part)
    return f"{payload_part}.{signature_part}"


def verify_token(token: str, audience: str | None = None) -> dict[str, Any]:
    """Verify token signature, expiry and optional audience."""
    try:
        payload_part, signature_part = token.split(".", 1)
    except ValueError as exc:
        raise TokenError("invalid token format") from exc

    expected = _sign(payload_part)
    if not hmac.compare_digest(signature_part, expected):
        raise TokenError("invalid token signature")

    try:
        payload = json.loads(_b64url_decode(payload_part).decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
        raise TokenError("invalid token payload") from exc

    exp = payload.get("exp")
    if not isinstance(exp, int) or exp <= int(time.time()):
        raise TokenError("token expired")

    if audience:
        aud = payload.get("aud")
        if isinstance(aud, str):
            ok = aud == audience
        elif isinstance(aud, list):
            ok = audience in aud
        else:
            ok = False
        if not ok:
            raise TokenError("token audience mismatch")

    return payload
