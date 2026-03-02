"""Run dispatch orchestration for runtime-gateway."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

from fastapi import HTTPException

from .api.schemas import CreateRunRequest, CreateRunResponse
from .audit.emitter import emit_audit_event
from .auth.exchange import ExchangeError, exchange_subject_token
from .contracts.validation import ContractValidationError, validate_command_envelope_contract
from .events.validation import validate_event_envelope
from .integration import RuntimeExecutionClient, RuntimeExecutionClientError


def _resolve_trace_id(claims: Mapping[str, Any]) -> str:
    return str(claims.get("trace_id", "")).strip() or str(uuid.uuid4())


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


def _build_validated_command(req: CreateRunRequest, trace_id: str) -> dict[str, Any]:
    command = _build_execution_command(req, trace_id)
    try:
        validate_command_envelope_contract(command)
    except ContractValidationError as exc:
        raise HTTPException(status_code=500, detail=f"invalid command envelope: {exc}") from exc
    return command


def _exchange_runtime_execution_token(
    *,
    req: CreateRunRequest,
    subject_token: str,
    actor_id: str,
    trace_id: str,
) -> dict[str, Any]:
    try:
        return exchange_subject_token(
            subject_token=subject_token,
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


def _submit_command(
    *,
    execution_client: RuntimeExecutionClient,
    command: dict[str, Any],
    delegated_token: str,
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
    actor_id: str,
    trace_id: str,
) -> dict[str, Any]:
    try:
        return execution_client.submit_command(envelope=command, auth_token=delegated_token)
    except RuntimeExecutionClientError as exc:
        downstream_event_type = None
        bus_seq = None
        if isinstance(exc.response_body, dict):
            try:
                validate_event_envelope(exc.response_body)
                downstream_event_type = str(exc.response_body.get("event_type", ""))
                bus_seq = publish_gateway_event(exc.response_body)
            except ValueError:
                downstream_event_type = None
                bus_seq = None

        emit_audit_event(
            action="runs.dispatch",
            decision="deny",
            actor_id=actor_id,
            trace_id=trace_id,
            metadata={
                "reason": str(exc),
                "status_code": exc.status_code,
                "downstream_event_type": downstream_event_type,
                "bus_seq": bus_seq,
            },
        )
        raise HTTPException(status_code=exc.status_code or 502, detail=str(exc)) from exc


def _extract_run_result(execution_event: dict[str, Any]) -> tuple[str, str]:
    payload = execution_event.get("payload")
    if not isinstance(payload, dict):
        raise HTTPException(status_code=502, detail="invalid execution response payload")

    run_id = payload.get("run_id")
    status = payload.get("status")
    if not isinstance(run_id, str) or not run_id:
        raise HTTPException(status_code=502, detail="execution response missing run_id")
    if not isinstance(status, str) or not status:
        raise HTTPException(status_code=502, detail="execution response missing status")
    return run_id, status


def _validate_execution_event(
    execution_event: dict[str, Any],
    *,
    actor_id: str,
    trace_id: str,
) -> None:
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


def _build_dispatch_response(
    *,
    execution_event: dict[str, Any],
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
    actor_id: str,
    trace_id: str,
) -> CreateRunResponse:
    bus_seq = publish_gateway_event(execution_event)
    run_id, status = _extract_run_result(execution_event)
    emit_audit_event(
        action="runs.dispatch",
        decision="allow",
        actor_id=actor_id,
        trace_id=trace_id,
        metadata={
            "run_id": run_id,
            "status": status,
            "downstream_event_type": execution_event.get("event_type"),
            "bus_seq": bus_seq,
        },
    )
    return CreateRunResponse(run_id=run_id, status=status)


def dispatch_create_run(
    *,
    req: CreateRunRequest,
    claims: Mapping[str, Any],
    subject_token: str,
    execution_client: RuntimeExecutionClient,
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
) -> CreateRunResponse:
    trace_id = _resolve_trace_id(claims)
    command = _build_validated_command(req, trace_id)
    actor_id = str(claims.get("sub", "unknown"))
    delegated = _exchange_runtime_execution_token(
        req=req,
        subject_token=subject_token,
        actor_id=actor_id,
        trace_id=trace_id,
    )
    execution_event = _submit_command(
        execution_client=execution_client,
        command=command,
        delegated_token=str(delegated["access_token"]),
        publish_gateway_event=publish_gateway_event,
        actor_id=actor_id,
        trace_id=trace_id,
    )
    _validate_execution_event(
        execution_event,
        actor_id=actor_id,
        trace_id=trace_id,
    )
    return _build_dispatch_response(
        execution_event=execution_event,
        publish_gateway_event=publish_gateway_event,
        actor_id=actor_id,
        trace_id=trace_id,
    )
