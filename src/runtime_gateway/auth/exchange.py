"""Token exchange core logic decoupled from web framework."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .tokens import TokenError, issue_token, verify_token

ALLOWED_TOKEN_USES = {"service", "device", "exchange"}
ALLOWED_SUBJECT_TOKEN_USES = {"access", "service", "exchange"}


class ExchangeError(ValueError):
    """Error raised by token exchange validation."""

    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


@dataclass(frozen=True)
class ExchangeSubjectParams:
    subject_token: str
    requested_token_use: str
    audience: str
    scope: list[str]
    requested_ttl_seconds: int
    tenant_id: str | None
    app_id: str | None
    session_key: str | None
    run_id: str | None
    task_id: str | None
    trace_id: str | None


def _validate_requested_scope(requested_scope: list[str], parent_scope: list[str]) -> None:
    if not requested_scope:
        raise ExchangeError(422, "scope must not be empty")
    if parent_scope and not set(requested_scope).issubset(set(parent_scope)):
        raise ExchangeError(403, "requested scope exceeds subject scope")


def _validate_exchange_inputs(
    *, requested_ttl_seconds: int, requested_token_use: str, audience: str
) -> None:
    if requested_ttl_seconds < 30 or requested_ttl_seconds > 3600:
        raise ExchangeError(422, "requested_ttl_seconds must be in [30, 3600]")
    if requested_token_use not in ALLOWED_TOKEN_USES:
        raise ExchangeError(422, "unsupported requested_token_use")
    if not audience:
        raise ExchangeError(422, "audience must not be empty")


def _verify_parent_claims(subject_token: str) -> dict[str, Any]:
    try:
        return verify_token(subject_token)
    except TokenError as exc:
        raise ExchangeError(401, f"invalid subject token: {exc}") from exc


def _validate_parent_claims(parent_claims: dict[str, Any]) -> None:
    for field in ("tenant_id", "app_id", "trace_id"):
        value = parent_claims.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ExchangeError(401, f"invalid subject token claims: missing {field}")
    token_use = parent_claims.get("token_use")
    if not isinstance(token_use, str) or not token_use.strip():
        raise ExchangeError(401, "invalid subject token claims: missing token_use")
    if token_use.strip().lower() not in ALLOWED_SUBJECT_TOKEN_USES:
        raise ExchangeError(
            401,
            f"invalid subject token claims: unsupported token_use '{token_use}'",
        )


def _delegated_claims(
    *,
    parent_claims: dict[str, Any],
    audience: str,
    requested_token_use: str,
    scope: list[str],
    tenant_id: str | None,
    app_id: str | None,
    session_key: str | None,
    run_id: str | None,
    task_id: str | None,
    trace_id: str | None,
) -> dict[str, Any]:
    claims: dict[str, Any] = {
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
        claims["session_key"] = session_key
    if run_id:
        claims["run_id"] = run_id
    if task_id:
        claims["task_id"] = task_id
    return claims


def _issued_token_type(requested_token_use: str) -> str:
    if requested_token_use == "device":
        return "urn:waoooolab:token-type:device_token"
    return "urn:waoooolab:token-type:service_token"


def _exchange_subject_token(params: ExchangeSubjectParams) -> dict[str, Any]:
    _validate_exchange_inputs(
        requested_ttl_seconds=params.requested_ttl_seconds,
        requested_token_use=params.requested_token_use,
        audience=params.audience,
    )
    parent_claims = _verify_parent_claims(params.subject_token)
    _validate_parent_claims(parent_claims)
    parent_scope = parent_claims.get("scope", [])
    if not isinstance(parent_scope, list):
        parent_scope = []
    _validate_requested_scope(params.scope, parent_scope)

    delegated_claims = _delegated_claims(
        parent_claims=parent_claims,
        audience=params.audience,
        requested_token_use=params.requested_token_use,
        scope=params.scope,
        tenant_id=params.tenant_id,
        app_id=params.app_id,
        session_key=params.session_key,
        run_id=params.run_id,
        task_id=params.task_id,
        trace_id=params.trace_id,
    )
    delegated_token = issue_token(delegated_claims, ttl_seconds=params.requested_ttl_seconds)
    issued_claims = verify_token(delegated_token, audience=params.audience)
    return {
        "issued_token_type": _issued_token_type(params.requested_token_use),
        "access_token": delegated_token,
        "expires_in": params.requested_ttl_seconds,
        "scope": params.scope,
        "audience": params.audience,
        "token_use": params.requested_token_use,
        "jti": str(issued_claims["jti"]),
    }


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
    return _exchange_subject_token(
        ExchangeSubjectParams(
            subject_token=subject_token,
            requested_token_use=requested_token_use,
            audience=audience,
            scope=scope,
            requested_ttl_seconds=requested_ttl_seconds,
            tenant_id=tenant_id,
            app_id=app_id,
            session_key=session_key,
            run_id=run_id,
            task_id=task_id,
            trace_id=trace_id,
        )
    )
