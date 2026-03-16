"""Run approval dispatch orchestration for runtime-gateway."""

from __future__ import annotations

from typing import Any, Callable, Mapping

from fastapi import HTTPException

from .audit.emitter import emit_audit_event
from .auth.exchange import ExchangeError, exchange_subject_token
from .events.validation import validate_event_envelope
from .integration import RuntimeExecutionClient, RuntimeExecutionClientError
from .upstream_error import (
    build_upstream_error_detail,
    extract_upstream_failure_classification,
    resolve_upstream_error_class,
    resolve_upstream_retryable,
    resolve_upstream_status_code,
)


def _extract_downstream_status(event: dict[str, Any]) -> str | None:
    payload = event.get("payload")
    if isinstance(payload, dict):
        status = payload.get("status")
        if isinstance(status, str) and status.strip():
            return status.strip()
    status = event.get("status")
    if isinstance(status, str) and status.strip():
        return status.strip()
    return None


def _build_downstream_error_detail(
    *,
    message: str,
    status_code: int,
    downstream_event: dict[str, Any],
    downstream_event_type: str,
    bus_seq: int | None,
    upstream_error_class: str,
) -> dict[str, Any]:
    detail: dict[str, Any] = {
        "message": message,
        "status_code": status_code,
        "downstream_event_type": downstream_event_type,
        "upstream_error_class": upstream_error_class,
    }
    event_id = downstream_event.get("event_id")
    if isinstance(event_id, str) and event_id.strip():
        detail["downstream_event_id"] = event_id
    correlation_id = downstream_event.get("correlation_id")
    if isinstance(correlation_id, str) and correlation_id.strip():
        detail["correlation_id"] = correlation_id
    payload = downstream_event.get("payload")
    if isinstance(payload, dict):
        run_id = payload.get("run_id")
        if isinstance(run_id, str) and run_id.strip():
            detail["run_id"] = run_id
        status = payload.get("status")
        if isinstance(status, str) and status.strip():
            detail["status"] = status
        failure = payload.get("failure")
        if isinstance(failure, dict):
            detail["failure"] = failure
        decision = payload.get("decision")
        if isinstance(decision, dict):
            detail["decision"] = decision
    if bus_seq is not None:
        detail["bus_seq"] = bus_seq
    return detail


def _exchange_runtime_execution_token(
    *,
    action: str,
    run_id: str,
    subject_token: str,
    actor_id: str,
    trace_id: str,
) -> dict[str, Any]:
    """Exchange subject token for runtime-execution run-write scope."""
    try:
        return exchange_subject_token(
            subject_token=subject_token,
            requested_token_use="service",
            audience="runtime-execution",
            scope=["runs:write"],
            requested_ttl_seconds=300,
            trace_id=trace_id,
            run_id=run_id,
        )
    except ExchangeError as exc:
        emit_audit_event(
            action=action,
            decision="deny",
            actor_id=actor_id,
            trace_id=trace_id,
            metadata={"reason": exc.detail, "run_id": run_id},
        )
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


def _submit_approval_action(
    *,
    action: str,
    run_id: str,
    claims: Mapping[str, Any],
    subject_token: str,
    execution_client: RuntimeExecutionClient,
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
    submitter: Callable[[str, str], dict[str, Any]],
) -> dict[str, Any]:
    trace_id = str(claims.get("trace_id", ""))
    actor_id = str(claims.get("sub", "unknown"))

    delegated = _exchange_runtime_execution_token(
        action=action,
        run_id=run_id,
        subject_token=subject_token,
        actor_id=actor_id,
        trace_id=trace_id,
    )

    delegated_token = str(delegated["access_token"])
    try:
        result = submitter(run_id, delegated_token)
        validate_event_envelope(result)
        bus_seq = publish_gateway_event(result)
    except RuntimeExecutionClientError as exc:
        resolved_status = resolve_upstream_status_code(
            status_code=exc.status_code,
            retryable=exc.retryable,
            message=str(exc),
        )
        normalized_retryable = resolve_upstream_retryable(
            status_code=exc.status_code,
            retryable=exc.retryable,
            message=str(exc),
            detail=exc.detail,
        )
        failure_classification = extract_upstream_failure_classification(
            message=str(exc),
            detail=exc.detail,
        )
        upstream_error_class = resolve_upstream_error_class(
            message=str(exc),
            detail=exc.detail,
            status_code=resolved_status,
            retryable=normalized_retryable,
            failure_classification=failure_classification,
        )
        downstream_event_type = None
        downstream_status = None
        bus_seq = None
        detail: str | dict[str, Any] = build_upstream_error_detail(
            message=str(exc),
            status_code=resolved_status,
            retryable=normalized_retryable,
            failure_classification=failure_classification,
            detail=exc.detail,
            upstream_error_class=upstream_error_class,
        )
        if isinstance(exc.response_body, dict):
            try:
                validate_event_envelope(exc.response_body)
                downstream_event_type = str(exc.response_body.get("event_type", ""))
                downstream_status = _extract_downstream_status(exc.response_body)
                bus_seq = publish_gateway_event(exc.response_body)
                detail = _build_downstream_error_detail(
                    message=str(exc),
                    status_code=resolved_status,
                    downstream_event=exc.response_body,
                    downstream_event_type=downstream_event_type,
                    bus_seq=bus_seq,
                    upstream_error_class=upstream_error_class,
                )
            except ValueError:
                downstream_event_type = None
                downstream_status = None
                bus_seq = None
        emit_audit_event(
            action=action,
            decision="deny",
            actor_id=actor_id,
            trace_id=trace_id,
            metadata={
                "reason": str(exc),
                "status_code": resolved_status,
                "run_id": run_id,
                "downstream_event_type": downstream_event_type,
                "downstream_status": downstream_status,
                "bus_seq": bus_seq,
                "retryable": normalized_retryable,
                "failure_classification": failure_classification,
                "upstream_error_class": upstream_error_class,
            },
        )
        raise HTTPException(status_code=resolved_status, detail=detail) from exc
    except ValueError as exc:
        emit_audit_event(
            action=action,
            decision="deny",
            actor_id=actor_id,
            trace_id=trace_id,
            metadata={"reason": f"invalid execution event envelope: {exc}", "run_id": run_id},
        )
        raise HTTPException(status_code=502, detail=f"invalid execution event envelope: {exc}") from exc

    emit_audit_event(
        action=action,
        decision="allow",
        actor_id=actor_id,
        trace_id=trace_id,
        metadata={
            "run_id": run_id,
            "downstream_event_type": result.get("event_type"),
            "downstream_status": _extract_downstream_status(result),
            "bus_seq": bus_seq,
        },
    )
    return result


def dispatch_approve_run(
    *,
    run_id: str,
    claims: Mapping[str, Any],
    subject_token: str,
    execution_client: RuntimeExecutionClient,
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
) -> dict[str, Any]:
    """Dispatch run approval to runtime-execution."""
    return _submit_approval_action(
        action="runs.approve",
        run_id=run_id,
        claims=claims,
        subject_token=subject_token,
        execution_client=execution_client,
        publish_gateway_event=publish_gateway_event,
        submitter=lambda target_run_id, auth_token: execution_client.approve_run(
            run_id=target_run_id,
            auth_token=auth_token,
        ),
    )


def dispatch_reject_run(
    *,
    run_id: str,
    claims: Mapping[str, Any],
    subject_token: str,
    execution_client: RuntimeExecutionClient,
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
) -> dict[str, Any]:
    """Dispatch run rejection to runtime-execution."""
    return _submit_approval_action(
        action="runs.reject",
        run_id=run_id,
        claims=claims,
        subject_token=subject_token,
        execution_client=execution_client,
        publish_gateway_event=publish_gateway_event,
        submitter=lambda target_run_id, auth_token: execution_client.reject_run(
            run_id=target_run_id,
            auth_token=auth_token,
        ),
    )
