"""FastAPI app for runtime-gateway."""

from __future__ import annotations

from typing import Any

from fastapi import Depends, FastAPI, HTTPException, WebSocket

from .api.schemas import (
    CreateRunRequest,
    CreateRunResponse,
    TokenExchangeRequest,
    TokenExchangeResponse,
)
from .audit.emitter import emit_audit_event, get_audit_events, read_audit_log
from .contracts import ContractValidationError, validate_executor_profile_catalog_contract
from .events.bus import InMemoryEventBus
from .events.validation import validate_event_envelope
from .executor_profiles import list_executor_profiles
from .integration import RuntimeExecutionClient
from .run_approval import dispatch_approve_run, dispatch_reject_run
from .run_dispatch import dispatch_create_run
from .security import (
    AuthContext,
    allowed_event_type,
    require_events_read_context,
    require_events_write_context,
    require_runs_read_context,
    require_runs_write_context,
)
from .token_exchange_api import token_exchange_response
from .ws_events import handle_websocket_events

app = FastAPI(title="runtime-gateway", version="0.1.0")
_execution_client = RuntimeExecutionClient()
_event_bus = InMemoryEventBus()


def _publish_gateway_event(event: dict[str, Any]) -> int | None:
    event_type = str(event.get("event_type", ""))
    if not allowed_event_type(event_type):
        return None
    return _event_bus.publish(event)


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok", "service": "runtime-gateway"}


@app.get("/v1/audit/events")
def list_audit_events(limit: int = 50, source: str = "memory") -> dict:
    if source == "durable":
        return {"items": read_audit_log(limit)}
    return {"items": get_audit_events(limit)}


@app.get("/v1/events/recent")
def list_recent_events(
    limit: int = 50,
    tenant_id: str | None = None,
    app_id: str | None = None,
    event_types: str | None = None,
    auth_context: AuthContext = Depends(require_events_read_context),
) -> dict:
    claims = auth_context.claims
    effective_tenant = tenant_id or str(claims.get("tenant_id", ""))
    effective_app = app_id or str(claims.get("app_id", ""))
    parsed_types = (
        {item.strip() for item in event_types.split(",") if item.strip()}
        if event_types is not None
        else None
    )
    return {
        "items": _event_bus.recent(
            limit=limit,
            tenant_id=effective_tenant or None,
            app_id=effective_app or None,
            event_types=parsed_types,
        ),
        "stats": _event_bus.stats(),
    }


@app.post("/v1/events/publish")
def publish_event(
    envelope: dict[str, Any],
    auth_context: AuthContext = Depends(require_events_write_context),
) -> dict[str, Any]:
    try:
        validate_event_envelope(envelope)
    except ValueError as exc:
        emit_audit_event(
            action="events.publish",
            decision="deny",
            actor_id=str(auth_context.claims.get("sub", "unknown")),
            trace_id=str(auth_context.claims.get("trace_id", "")),
            metadata={"reason": f"invalid event envelope: {exc}"},
        )
        raise HTTPException(status_code=422, detail=f"invalid event envelope: {exc}") from exc

    event_type = str(envelope.get("event_type", ""))
    if not allowed_event_type(event_type):
        raise HTTPException(status_code=422, detail=f"event type not publishable: {event_type}")

    bus_seq = _publish_gateway_event(envelope)
    emit_audit_event(
        action="events.publish",
        decision="allow",
        actor_id=str(auth_context.claims.get("sub", "unknown")),
        trace_id=str(auth_context.claims.get("trace_id", "")),
        metadata={
            "event_type": event_type,
            "event_id": envelope.get("event_id"),
            "bus_seq": bus_seq,
        },
    )
    return {
        "accepted": True,
        "bus_seq": bus_seq,
        "event_id": envelope.get("event_id"),
        "event_type": event_type,
    }


@app.websocket("/v1/ws/events")
async def websocket_events(websocket: WebSocket) -> None:
    await handle_websocket_events(websocket, _event_bus)


@app.post("/v1/auth/token/exchange", response_model=TokenExchangeResponse)
def token_exchange(req: TokenExchangeRequest) -> TokenExchangeResponse:
    return token_exchange_response(req)


@app.post("/v1/runs", response_model=CreateRunResponse)
def create_run(
    req: CreateRunRequest,
    auth_context: AuthContext = Depends(require_runs_write_context),
) -> CreateRunResponse:
    return dispatch_create_run(
        req=req,
        claims=auth_context.claims,
        subject_token=auth_context.subject_token,
        execution_client=_execution_client,
        publish_gateway_event=_publish_gateway_event,
    )


@app.post("/v1/runs/{run_id}:approve")
def approve_run(
    run_id: str,
    auth_context: AuthContext = Depends(require_runs_write_context),
) -> dict[str, Any]:
    return dispatch_approve_run(
        run_id=run_id,
        claims=auth_context.claims,
        subject_token=auth_context.subject_token,
        execution_client=_execution_client,
        publish_gateway_event=_publish_gateway_event,
    )


@app.post("/v1/runs/{run_id}:reject")
def reject_run(
    run_id: str,
    auth_context: AuthContext = Depends(require_runs_write_context),
) -> dict[str, Any]:
    return dispatch_reject_run(
        run_id=run_id,
        claims=auth_context.claims,
        subject_token=auth_context.subject_token,
        execution_client=_execution_client,
        publish_gateway_event=_publish_gateway_event,
    )


@app.get("/v1/executors/profiles")
def get_executor_profiles(
    auth_context: AuthContext = Depends(require_runs_read_context),
) -> dict[str, Any]:
    _ = auth_context
    payload = {
        "items": list_executor_profiles(),
    }
    try:
        validate_executor_profile_catalog_contract(payload)
    except ContractValidationError as exc:
        raise HTTPException(status_code=500, detail=f"invalid executor profile catalog: {exc}") from exc
    return payload
