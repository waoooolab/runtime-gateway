"""Token exchange core logic decoupled from web framework."""

from __future__ import annotations

from typing import Any

from .tokens import TokenError, issue_token, verify_token

ALLOWED_TOKEN_USES = {"service", "device", "exchange"}


class ExchangeError(ValueError):
    """Error raised by token exchange validation."""

    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


def _validate_requested_scope(requested_scope: list[str], parent_scope: list[str]) -> None:
    if not requested_scope:
        raise ExchangeError(422, "scope must not be empty")
    if parent_scope and not set(requested_scope).issubset(set(parent_scope)):
        raise ExchangeError(403, "requested scope exceeds subject scope")


def exchange_subject_token(
    *,
    subject_token: str,
    requested_token_use: str,
    audience: str,
    scope: list[str],
    requested_ttl_seconds: int,
    tenant_id: str | None = None,
    app_id: str | None = None,
    session_key: str | None = None,
    run_id: str | None = None,
    task_id: str | None = None,
    trace_id: str | None = None,
) -> dict[str, Any]:
    """Exchange parent token into a delegated short-lived token."""
    if requested_ttl_seconds < 30 or requested_ttl_seconds > 3600:
        raise ExchangeError(422, "requested_ttl_seconds must be in [30, 3600]")
    if requested_token_use not in ALLOWED_TOKEN_USES:
        raise ExchangeError(422, "unsupported requested_token_use")
    if not audience:
        raise ExchangeError(422, "audience must not be empty")

    try:
        parent_claims = verify_token(subject_token)
    except TokenError as exc:
        raise ExchangeError(401, f"invalid subject token: {exc}") from exc

    parent_scope = parent_claims.get("scope", [])
    if not isinstance(parent_scope, list):
        parent_scope = []
    _validate_requested_scope(scope, parent_scope)

    delegated_claims: dict[str, Any] = {
        "iss": "runtime-gateway",
        "sub": str(parent_claims.get("sub", "unknown")),
        "aud": audience,
        "tenant_id": tenant_id or str(parent_claims.get("tenant_id", "unknown")),
        "app_id": app_id or str(parent_claims.get("app_id", "unknown")),
        "scope": scope,
        "token_use": requested_token_use,
        "trace_id": trace_id or str(parent_claims.get("trace_id", "unknown")),
    }
    if session_key:
        delegated_claims["session_key"] = session_key
    if run_id:
        delegated_claims["run_id"] = run_id
    if task_id:
        delegated_claims["task_id"] = task_id

    delegated_token = issue_token(delegated_claims, ttl_seconds=requested_ttl_seconds)
    issued_claims = verify_token(delegated_token, audience=audience)

    if requested_token_use == "device":
        issued_token_type = "urn:waoooolab:token-type:device_token"
    else:
        issued_token_type = "urn:waoooolab:token-type:service_token"

    return {
        "issued_token_type": issued_token_type,
        "access_token": delegated_token,
        "expires_in": requested_ttl_seconds,
        "scope": scope,
        "audience": audience,
        "token_use": requested_token_use,
        "jti": str(issued_claims["jti"]),
    }
