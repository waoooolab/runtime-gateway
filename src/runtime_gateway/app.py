"""FastAPI app for runtime-gateway."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException

from .api.schemas import (
    CreateRunRequest,
    CreateRunResponse,
    TokenExchangeRequest,
    TokenExchangeResponse,
)
from .audit.emitter import emit_audit_event, get_audit_events, read_audit_log
from .auth.exchange import ExchangeError, exchange_subject_token
from .auth.tokens import TokenError, verify_token
from .contracts.validation import (
    ContractValidationError,
    validate_command_envelope_contract,
    validate_token_exchange_contract,
)
from .events.validation import validate_event_envelope
from .integration import RuntimeExecutionClient, RuntimeExecutionClientError


@dataclass(frozen=True)
class AuthContext:
    claims: dict[str, Any]
    subject_token: str


app = FastAPI(title="runtime-gateway", version="0.1.0")
_execution_client = RuntimeExecutionClient()


def _build_execution_command(req: CreateRunRequest, trace_id: str) -> dict[str, Any]:
    return {
        "command_id": str(uuid.uuid4()),
        "command_type": "run.start",
        "tenant_id": req.tenant_id,
        "app_id": req.app_id,
        "session_key": req.session_key,
        "trace_id": trace_id,
        "idempotency_key": str(uuid.uuid4()),
        "retry_policy": {
            "max_attempts": 3,
            "backoff_ms": 250,
            "strategy": "fixed",
        },
        "ts": datetime.now(timezone.utc).isoformat(),
        "payload": req.payload,
    }


def _require_runs_write_context(
    authorization: str | None = Header(default=None),
) -> AuthContext:
    action = "runs.create"
    if not authorization or not authorization.startswith("Bearer "):
        emit_audit_event(
            action=action,
            decision="deny",
            actor_id="unknown",
            metadata={"reason": "missing bearer token"},
        )
        raise HTTPException(status_code=401, detail="missing bearer token")

    token = authorization.split(" ", 1)[1].strip()
    try:
        claims = verify_token(token, audience="runtime-gateway")
    except TokenError as exc:
        emit_audit_event(
            action=action,
            decision="deny",
            actor_id="unknown",
            metadata={"reason": str(exc)},
        )
        raise HTTPException(status_code=401, detail=str(exc)) from exc

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


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok", "service": "runtime-gateway"}


@app.get("/v1/audit/events")
def list_audit_events(limit: int = 50, source: str = "memory") -> dict:
    if source == "durable":
        return {"items": read_audit_log(limit)}
    return {"items": get_audit_events(limit)}


@app.post("/v1/auth/token/exchange", response_model=TokenExchangeResponse)
def token_exchange(req: TokenExchangeRequest) -> TokenExchangeResponse:
    try:
        validate_token_exchange_contract(req.model_dump())
    except ContractValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    actor_id = "unknown"
    try:
        claims = verify_token(req.subject_token)
        actor_id = str(claims.get("sub", "unknown"))
    except TokenError:
        actor_id = "unknown"

    try:
        exchange_result = exchange_subject_token(
            subject_token=req.subject_token,
            requested_token_use=req.requested_token_use,
            audience=req.audience,
            scope=req.scope,
            requested_ttl_seconds=req.requested_ttl_seconds,
            tenant_id=req.tenant_id,
            app_id=req.app_id,
            session_key=req.session_key,
            run_id=req.run_id,
            task_id=req.task_id,
            trace_id=req.trace_id or str(uuid.uuid4()),
        )
    except ExchangeError as exc:
        emit_audit_event(
            action="auth.token_exchange",
            decision="deny",
            actor_id=actor_id,
            trace_id=req.trace_id,
            metadata={"reason": exc.detail, "audience": req.audience},
        )
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc

    response = TokenExchangeResponse(**exchange_result)
    try:
        validate_token_exchange_contract(response.model_dump())
    except ContractValidationError as exc:
        raise HTTPException(status_code=500, detail=f"token exchange response invalid: {exc}") from exc

    emit_audit_event(
        action="auth.token_exchange",
        decision="allow",
        actor_id=actor_id,
        trace_id=req.trace_id,
        metadata={
            "audience": req.audience,
            "token_use": req.requested_token_use,
            "scope": req.scope,
        },
    )
    return response


@app.post("/v1/runs", response_model=CreateRunResponse)
def create_run(
    req: CreateRunRequest,
    auth_context: AuthContext = Depends(_require_runs_write_context),
) -> CreateRunResponse:
    claims = auth_context.claims
    trace_id = str(claims.get("trace_id", "")).strip() or str(uuid.uuid4())

    command = _build_execution_command(req, trace_id)
    try:
        validate_command_envelope_contract(command)
    except ContractValidationError as exc:
        raise HTTPException(status_code=500, detail=f"invalid command envelope: {exc}") from exc

    actor_id = str(claims.get("sub", "unknown"))
    try:
        delegated = exchange_subject_token(
            subject_token=auth_context.subject_token,
            requested_token_use="service",
            audience="runtime-execution",
            scope=["runs:write"],
            requested_ttl_seconds=300,
            tenant_id=req.tenant_id,
            app_id=req.app_id,
            session_key=req.session_key,
            trace_id=trace_id,
        )
    except ExchangeError as exc:
        emit_audit_event(
            action="runs.dispatch",
            decision="deny",
            actor_id=actor_id,
            trace_id=trace_id,
            metadata={"reason": exc.detail, "audience": "runtime-execution"},
        )
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc

    try:
        execution_event = _execution_client.submit_command(
            envelope=command,
            auth_token=str(delegated["access_token"]),
        )
    except RuntimeExecutionClientError as exc:
        emit_audit_event(
            action="runs.dispatch",
            decision="deny",
            actor_id=actor_id,
            trace_id=trace_id,
            metadata={"reason": str(exc)},
        )
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    try:
        validate_event_envelope(execution_event)
    except ValueError as exc:
        emit_audit_event(
            action="runs.dispatch",
            decision="deny",
            actor_id=actor_id,
            trace_id=trace_id,
            metadata={"reason": f"invalid execution event envelope: {exc}"},
        )
        raise HTTPException(status_code=502, detail=f"invalid execution event envelope: {exc}") from exc

    payload = execution_event.get("payload")
    if not isinstance(payload, dict):
        raise HTTPException(status_code=502, detail="invalid execution response payload")

    run_id = payload.get("run_id")
    status = payload.get("status")
    if not isinstance(run_id, str) or not run_id:
        raise HTTPException(status_code=502, detail="execution response missing run_id")
    if not isinstance(status, str) or not status:
        raise HTTPException(status_code=502, detail="execution response missing status")

    emit_audit_event(
        action="runs.dispatch",
        decision="allow",
        actor_id=actor_id,
        trace_id=trace_id,
        metadata={
            "run_id": run_id,
            "status": status,
            "downstream_event_type": execution_event.get("event_type"),
        },
    )
    return CreateRunResponse(run_id=run_id, status=status)
