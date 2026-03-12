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
    scope: list[str],
) -> str:
    trace_id = str(claims.get("trace_id", ""))
    actor_id = str(claims.get("sub", "unknown"))
    try:
        delegated = exchange_subject_token(
            subject_token=subject_token,
            requested_token_use="service",
            audience="runtime-execution",
            scope=scope,
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


def _build_worker_error_detail(
    *,
    message: str,
    status_code: int,
    response_body: dict[str, Any],
) -> dict[str, Any]:
    detail: dict[str, Any] = {
        "message": message,
        "status_code": status_code,
        "downstream_response": response_body,
    }
    downstream_detail = response_body.get("detail")
    if isinstance(downstream_detail, str) and downstream_detail.strip():
        detail["downstream_detail"] = downstream_detail
    for key in (
        "health_state",
        "is_stalled",
        "stalled_signal",
        "anomaly_ratio",
        "queue_depth",
        "processed",
        "remaining",
        "recommended_poll_after_ms",
    ):
        value = response_body.get(key)
        if value is not None:
            detail[key] = value
    for key in (
        "outcome_counts",
        "anomaly_counts",
        "scheduling_signal",
    ):
        value = response_body.get(key)
        if isinstance(value, dict):
            detail[key] = value
    return detail


def _build_worker_error_audit_metadata(
    *,
    reason: str,
    status_code: int | None,
    response_body: dict[str, Any] | None,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "reason": reason,
        "status_code": status_code,
    }
    if not isinstance(response_body, dict):
        return metadata
    downstream_detail = response_body.get("detail")
    if isinstance(downstream_detail, str) and downstream_detail.strip():
        metadata["downstream_detail"] = downstream_detail
    for key in (
        "health_state",
        "is_stalled",
        "stalled_signal",
        "anomaly_ratio",
        "queue_depth",
        "processed",
        "remaining",
        "recommended_poll_after_ms",
    ):
        value = response_body.get(key)
        if value is not None:
            metadata[key] = value
    for key in (
        "outcome_counts",
        "anomaly_counts",
        "scheduling_signal",
    ):
        value = response_body.get(key)
        if isinstance(value, dict):
            metadata[key] = value
    return metadata


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
        scope=["runs:write"],
    )
    try:
        result = execution_client.worker_tick(
            auth_token=token,
            fair=fair,
            auto_start=auto_start,
        )
    except RuntimeExecutionClientError as exc:
        detail: str | dict[str, Any] = str(exc)
        if isinstance(exc.response_body, dict):
            detail = _build_worker_error_detail(
                message=str(exc),
                status_code=exc.status_code or 502,
                response_body=exc.response_body,
            )
        emit_audit_event(
            action=action,
            decision="deny",
            actor_id=actor_id,
            trace_id=trace_id,
            metadata=_build_worker_error_audit_metadata(
                reason=str(exc),
                status_code=exc.status_code,
                response_body=exc.response_body if isinstance(exc.response_body, dict) else None,
            ),
        )
        raise HTTPException(status_code=exc.status_code or 502, detail=detail) from exc
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
        scope=["runs:write"],
    )
    try:
        result = execution_client.worker_drain(
            auth_token=token,
            max_items=max_items,
            fair=fair,
            auto_start=auto_start,
        )
    except RuntimeExecutionClientError as exc:
        detail: str | dict[str, Any] = str(exc)
        if isinstance(exc.response_body, dict):
            detail = _build_worker_error_detail(
                message=str(exc),
                status_code=exc.status_code or 502,
                response_body=exc.response_body,
            )
        emit_audit_event(
            action=action,
            decision="deny",
            actor_id=actor_id,
            trace_id=trace_id,
            metadata=_build_worker_error_audit_metadata(
                reason=str(exc),
                status_code=exc.status_code,
                response_body=exc.response_body if isinstance(exc.response_body, dict) else None,
            ),
        )
        raise HTTPException(status_code=exc.status_code or 502, detail=detail) from exc
    emit_audit_event(
        action=action,
        decision="allow",
        actor_id=actor_id,
        trace_id=trace_id,
        metadata={"result": result},
    )
    return result


def dispatch_worker_health(
    *,
    claims: Mapping[str, Any],
    subject_token: str,
    execution_client: RuntimeExecutionClient,
) -> dict[str, Any]:
    action = "orchestration.worker_health"
    trace_id = str(claims.get("trace_id", ""))
    actor_id = str(claims.get("sub", "unknown"))
    token = _exchange_runtime_execution_token(
        action=action,
        claims=claims,
        subject_token=subject_token,
        scope=["runs:read"],
    )
    try:
        result = execution_client.worker_health(auth_token=token)
    except RuntimeExecutionClientError as exc:
        detail: str | dict[str, Any] = str(exc)
        if isinstance(exc.response_body, dict):
            detail = _build_worker_error_detail(
                message=str(exc),
                status_code=exc.status_code or 502,
                response_body=exc.response_body,
            )
        emit_audit_event(
            action=action,
            decision="deny",
            actor_id=actor_id,
            trace_id=trace_id,
            metadata=_build_worker_error_audit_metadata(
                reason=str(exc),
                status_code=exc.status_code,
                response_body=exc.response_body if isinstance(exc.response_body, dict) else None,
            ),
        )
        raise HTTPException(status_code=exc.status_code or 502, detail=detail) from exc
    emit_audit_event(
        action=action,
        decision="allow",
        actor_id=actor_id,
        trace_id=trace_id,
        metadata={"result": result},
    )
    return result
