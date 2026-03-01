"""FastAPI app for runtime-gateway."""

from __future__ import annotations

import uuid

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
from .contracts.validation import ContractValidationError, validate_token_exchange_contract
from .events.envelope import build_event_envelope
from .events.validation import validate_event_envelope

# In-process runtime-execution adapter for P0 baseline.
try:
    from runtime_execution.service import RuntimeExecutionService
except ModuleNotFoundError:  # pragma: no cover - exercised in integration environments
    RuntimeExecutionService = None  # type: ignore[assignment]

app = FastAPI(title="runtime-gateway", version="0.1.0")
_service = RuntimeExecutionService() if RuntimeExecutionService is not None else None


def _runtime_service() -> "RuntimeExecutionService":
    if _service is None:
        raise HTTPException(
            status_code=503,
            detail="runtime-execution adapter not available in current environment",
        )
    return _service


def _require_runs_write_claims(
    authorization: str | None = Header(default=None),
) -> dict:
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
    return claims


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
    req: CreateRunRequest, _claims: dict = Depends(_require_runs_write_claims)
) -> CreateRunResponse:
    trace_id = str(_claims.get("trace_id", "")).strip() or str(uuid.uuid4())
    run = _runtime_service().submit_run(
        session_key=req.session_key,
        payload=req.payload,
        trace_id=trace_id,
    )

    event = build_event_envelope(
        event_type="run.requested",
        tenant_id=req.tenant_id,
        app_id=req.app_id,
        session_key=req.session_key,
        payload={"run_id": run.run_id},
        trace_id=getattr(run, "trace_id", trace_id),
        correlation_id=trace_id,
    )
    try:
        validate_event_envelope(event)
    except ValueError as exc:
        emit_audit_event(
            action="runs.create",
            decision="deny",
            actor_id=str(_claims.get("sub", "unknown")),
            trace_id=str(_claims.get("trace_id", "")),
            metadata={"reason": f"invalid event envelope: {exc}"},
        )
        raise HTTPException(status_code=500, detail=f"event envelope invalid: {exc}") from exc

    return CreateRunResponse(run_id=run.run_id, status=run.status.value)
