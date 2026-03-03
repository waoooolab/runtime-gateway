"""Worker loop dispatch orchestration for runtime-gateway."""

from __future__ import annotations

from typing import Any, Mapping

from fastapi import HTTPException

from .audit.emitter import emit_audit_event
from .auth.exchange import ExchangeError, exchange_subject_token
from .integration import RuntimeExecutionClient, RuntimeExecutionClientError


def _exchange_runtime_execution_token(
    *,
    action: str,
    claims: Mapping[str, Any],
    subject_token: str,
) -> str:
    trace_id = str(claims.get("trace_id", ""))
    actor_id = str(claims.get("sub", "unknown"))
    try:
        delegated = exchange_subject_token(
            subject_token=subject_token,
            requested_token_use="service",
            audience="runtime-execution",
            scope=["runs:write"],
            requested_ttl_seconds=300,
            trace_id=trace_id,
        )
    except ExchangeError as exc:
        emit_audit_event(
            action=action,
            decision="deny",
            actor_id=actor_id,
            trace_id=trace_id,
            metadata={"reason": exc.detail},
        )
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
    return str(delegated["access_token"])


def dispatch_worker_tick(
    *,
    claims: Mapping[str, Any],
    subject_token: str,
    execution_client: RuntimeExecutionClient,
    fair: bool = True,
    auto_start: bool = True,
) -> dict[str, Any]:
    action = "orchestration.worker_tick"
    trace_id = str(claims.get("trace_id", ""))
    actor_id = str(claims.get("sub", "unknown"))
    token = _exchange_runtime_execution_token(
        action=action,
        claims=claims,
        subject_token=subject_token,
    )
    try:
        result = execution_client.worker_tick(
            auth_token=token,
            fair=fair,
            auto_start=auto_start,
        )
    except RuntimeExecutionClientError as exc:
        emit_audit_event(
            action=action,
            decision="deny",
            actor_id=actor_id,
            trace_id=trace_id,
            metadata={"reason": str(exc), "status_code": exc.status_code},
        )
        raise HTTPException(status_code=exc.status_code or 502, detail=str(exc)) from exc
    emit_audit_event(
        action=action,
        decision="allow",
        actor_id=actor_id,
        trace_id=trace_id,
        metadata={"result": result},
    )
    return result


def dispatch_worker_drain(
    *,
    claims: Mapping[str, Any],
    subject_token: str,
    execution_client: RuntimeExecutionClient,
    max_items: int = 16,
    fair: bool = True,
    auto_start: bool = True,
) -> dict[str, Any]:
    action = "orchestration.worker_drain"
    trace_id = str(claims.get("trace_id", ""))
    actor_id = str(claims.get("sub", "unknown"))
    token = _exchange_runtime_execution_token(
        action=action,
        claims=claims,
        subject_token=subject_token,
    )
    try:
        result = execution_client.worker_drain(
            auth_token=token,
            max_items=max_items,
            fair=fair,
            auto_start=auto_start,
        )
    except RuntimeExecutionClientError as exc:
        emit_audit_event(
            action=action,
            decision="deny",
            actor_id=actor_id,
            trace_id=trace_id,
            metadata={"reason": str(exc), "status_code": exc.status_code},
        )
        raise HTTPException(status_code=exc.status_code or 502, detail=str(exc)) from exc
    emit_audit_event(
        action=action,
        decision="allow",
        actor_id=actor_id,
        trace_id=trace_id,
        metadata={"result": result},
    )
    return result
