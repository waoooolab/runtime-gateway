"""Run approval dispatch orchestration for runtime-gateway."""

from __future__ import annotations

from typing import Any, Callable, Mapping

from fastapi import HTTPException

from .audit.emitter import emit_audit_event
from .auth.exchange import ExchangeError, exchange_subject_token
from .events.validation import validate_event_envelope
from .integration import RuntimeExecutionClient, RuntimeExecutionClientError


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
            action=action,
            decision="deny",
            actor_id=actor_id,
            trace_id=trace_id,
            metadata={
                "reason": str(exc),
                "status_code": exc.status_code,
                "run_id": run_id,
                "downstream_event_type": downstream_event_type,
                "bus_seq": bus_seq,
            },
        )
        raise HTTPException(status_code=exc.status_code or 502, detail=str(exc)) from exc
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
