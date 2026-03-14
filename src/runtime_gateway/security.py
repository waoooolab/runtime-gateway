"""Auth context and scope guards for runtime-gateway endpoints."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fastapi import Header, HTTPException

from .audit.emitter import emit_audit_event
from .auth.tokens import TokenError, verify_token

EVENT_SCOPE_WRITE = {"events:write", "runs:write", "devices:write", "capabilities:write"}
EVENT_SCOPE_READ = {
    "events:read",
    "runs:read",
    "runs:write",
    "devices:read",
    "capabilities:read",
    "capabilities:write",
    "capabilities:invoke",
}
EVENT_TYPE_PREFIX_ALLOWLIST = (
    "runtime.run.",
    "runtime.task.",
    "runtime.route.",
    "device.route.",
    "device.lease.",
    "app.capability.",
)
ALLOWED_GATEWAY_TOKEN_USES = {"access", "service"}


@dataclass(frozen=True)
class AuthContext:
    claims: dict[str, Any]
    subject_token: str


def scope_contains(claims: dict[str, Any], required: set[str]) -> bool:
    scope = claims.get("scope")
    if not isinstance(scope, list):
        return False
    granted = {str(item) for item in scope}
    return len(granted.intersection(required)) > 0


def allowed_event_type(event_type: str) -> bool:
    return any(event_type.startswith(prefix) for prefix in EVENT_TYPE_PREFIX_ALLOWLIST)


def _extract_bearer_token(authorization: str | None, *, action: str) -> str:
    if authorization and authorization.startswith("Bearer "):
        return authorization.split(" ", 1)[1].strip()
    emit_audit_event(
        action=action,
        decision="deny",
        actor_id="unknown",
        metadata={"reason": "missing bearer token"},
    )
    raise HTTPException(status_code=401, detail="missing bearer token")


def _verify_gateway_token(token: str, *, action: str) -> dict[str, Any]:
    try:
        claims = verify_token(token, audience="runtime-gateway")
        validate_required_claims(claims)
        return claims
    except TokenError as exc:
        emit_audit_event(
            action=action,
            decision="deny",
            actor_id="unknown",
            metadata={"reason": str(exc)},
        )
        raise HTTPException(status_code=401, detail=str(exc)) from exc


def validate_required_claims(claims: dict[str, Any]) -> None:
    for field in ("tenant_id", "app_id", "trace_id"):
        value = claims.get(field)
        if not isinstance(value, str) or not value.strip():
            raise TokenError(f"invalid token claims: missing {field}")
    token_use = claims.get("token_use")
    if not isinstance(token_use, str) or not token_use.strip():
        raise TokenError("invalid token claims: missing token_use")
    normalized = token_use.strip().lower()
    if normalized not in ALLOWED_GATEWAY_TOKEN_USES:
        raise TokenError(
            f"invalid token claims: unsupported token_use '{token_use}'"
        )


def require_runs_write_context(authorization: str | None = Header(default=None)) -> AuthContext:
    action = "runs.create"
    token = _extract_bearer_token(authorization, action=action)
    claims = _verify_gateway_token(token, action=action)

    actor_id = str(claims.get("sub", "unknown"))
    scope = claims.get("scope", [])
    if not isinstance(scope, list) or "runs:write" not in scope:
        emit_audit_event(
            action=action,
            decision="deny",
            actor_id=actor_id,
            trace_id=str(claims.get("trace_id", "")),
            metadata={"reason": "missing required scope", "required_scope": "runs:write"},
        )
        raise HTTPException(status_code=403, detail="missing required scope: runs:write")

    emit_audit_event(
        action=action,
        decision="allow",
        actor_id=actor_id,
        trace_id=str(claims.get("trace_id", "")),
        metadata={"scope": scope},
    )
    return AuthContext(claims=claims, subject_token=token)


def require_runs_read_context(authorization: str | None = Header(default=None)) -> AuthContext:
    action = "runs.read"
    token = _extract_bearer_token(authorization, action=action)
    claims = _verify_gateway_token(token, action=action)

    actor_id = str(claims.get("sub", "unknown"))
    scope = claims.get("scope", [])
    if not isinstance(scope, list) or "runs:read" not in scope:
        emit_audit_event(
            action=action,
            decision="deny",
            actor_id=actor_id,
            trace_id=str(claims.get("trace_id", "")),
            metadata={"reason": "missing required scope", "required_scope": "runs:read"},
        )
        raise HTTPException(status_code=403, detail="missing required scope: runs:read")

    emit_audit_event(
        action=action,
        decision="allow",
        actor_id=actor_id,
        trace_id=str(claims.get("trace_id", "")),
        metadata={"scope": scope},
    )
    return AuthContext(claims=claims, subject_token=token)


def require_capabilities_write_context(authorization: str | None = Header(default=None)) -> AuthContext:
    action = "capabilities.write"
    token = _extract_bearer_token(authorization, action=action)
    claims = _verify_gateway_token(token, action=action)

    actor_id = str(claims.get("sub", "unknown"))
    scope = claims.get("scope", [])
    if not isinstance(scope, list) or "capabilities:write" not in scope:
        emit_audit_event(
            action=action,
            decision="deny",
            actor_id=actor_id,
            trace_id=str(claims.get("trace_id", "")),
            metadata={"reason": "missing required scope", "required_scope": "capabilities:write"},
        )
        raise HTTPException(status_code=403, detail="missing required scope: capabilities:write")

    emit_audit_event(
        action=action,
        decision="allow",
        actor_id=actor_id,
        trace_id=str(claims.get("trace_id", "")),
        metadata={"scope": scope},
    )
    return AuthContext(claims=claims, subject_token=token)


def require_capabilities_read_context(authorization: str | None = Header(default=None)) -> AuthContext:
    action = "capabilities.read"
    token = _extract_bearer_token(authorization, action=action)
    claims = _verify_gateway_token(token, action=action)

    actor_id = str(claims.get("sub", "unknown"))
    scope = claims.get("scope", [])
    if not isinstance(scope, list) or "capabilities:read" not in scope:
        emit_audit_event(
            action=action,
            decision="deny",
            actor_id=actor_id,
            trace_id=str(claims.get("trace_id", "")),
            metadata={"reason": "missing required scope", "required_scope": "capabilities:read"},
        )
        raise HTTPException(status_code=403, detail="missing required scope: capabilities:read")

    emit_audit_event(
        action=action,
        decision="allow",
        actor_id=actor_id,
        trace_id=str(claims.get("trace_id", "")),
        metadata={"scope": scope},
    )
    return AuthContext(claims=claims, subject_token=token)


def require_capabilities_invoke_context(authorization: str | None = Header(default=None)) -> AuthContext:
    action = "capabilities.invoke"
    token = _extract_bearer_token(authorization, action=action)
    claims = _verify_gateway_token(token, action=action)

    actor_id = str(claims.get("sub", "unknown"))
    scope = claims.get("scope", [])
    if not isinstance(scope, list) or "capabilities:invoke" not in scope:
        emit_audit_event(
            action=action,
            decision="deny",
            actor_id=actor_id,
            trace_id=str(claims.get("trace_id", "")),
            metadata={"reason": "missing required scope", "required_scope": "capabilities:invoke"},
        )
        raise HTTPException(status_code=403, detail="missing required scope: capabilities:invoke")

    emit_audit_event(
        action=action,
        decision="allow",
        actor_id=actor_id,
        trace_id=str(claims.get("trace_id", "")),
        metadata={"scope": scope},
    )
    return AuthContext(claims=claims, subject_token=token)


def require_events_write_context(authorization: str | None = Header(default=None)) -> AuthContext:
    action = "events.publish"
    token = _extract_bearer_token(authorization, action=action)
    claims = _verify_gateway_token(token, action=action)
    if not scope_contains(claims, EVENT_SCOPE_WRITE):
        emit_audit_event(
            action=action,
            decision="deny",
            actor_id=str(claims.get("sub", "unknown")),
            trace_id=str(claims.get("trace_id", "")),
            metadata={
                "reason": "missing event publish scope",
                "required_scope_one_of": sorted(EVENT_SCOPE_WRITE),
            },
        )
        raise HTTPException(status_code=403, detail="missing required scope for event publish")
    return AuthContext(claims=claims, subject_token=token)


def require_events_read_context(authorization: str | None = Header(default=None)) -> AuthContext:
    action = "events.read"
    token = _extract_bearer_token(authorization, action=action)
    claims = _verify_gateway_token(token, action=action)
    if not scope_contains(claims, EVENT_SCOPE_READ):
        emit_audit_event(
            action=action,
            decision="deny",
            actor_id=str(claims.get("sub", "unknown")),
            trace_id=str(claims.get("trace_id", "")),
            metadata={
                "reason": "missing event read scope",
                "required_scope_one_of": sorted(EVENT_SCOPE_READ),
            },
        )
        raise HTTPException(status_code=403, detail="missing required scope for event read")
    return AuthContext(claims=claims, subject_token=token)
