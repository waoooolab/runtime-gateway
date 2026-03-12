"""Run status lookup dispatch orchestration for runtime-gateway."""

from __future__ import annotations

from typing import Any, Mapping

from fastapi import HTTPException

from .audit.emitter import emit_audit_event
from .auth.exchange import ExchangeError, exchange_subject_token
from .events.validation import validate_event_envelope
from .integration import RuntimeExecutionClient, RuntimeExecutionClientError

_TERMINAL_RUN_STATUSES = {"succeeded", "failed", "canceled", "timed_out", "rejected"}


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


def _extract_downstream_run_id(event: dict[str, Any]) -> str | None:
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return None
    run_id = payload.get("run_id")
    if not isinstance(run_id, str):
        return None
    normalized = run_id.strip()
    return normalized or None


def _recommended_poll_after_ms_for_run_status(status: str | None) -> int:
    normalized = (status or "").strip().lower()
    if normalized in _TERMINAL_RUN_STATUSES:
        return 10000
    if normalized in {"running", "dispatching", "retrying"}:
        return 1000
    if normalized in {"requested", "queued"}:
        return 1500
    return 3000


def _ensure_downstream_run_id_matches(
    *,
    run_id: str,
    result: dict[str, Any],
    action: str,
    actor_id: str,
    trace_id: str,
) -> None:
    downstream_run_id = _extract_downstream_run_id(result)
    if downstream_run_id == run_id:
        return None
    emit_audit_event(
        action=action,
        decision="deny",
        actor_id=actor_id,
        trace_id=trace_id,
        metadata={
            "reason": "run status response mismatches requested run_id",
            "run_id": run_id,
            "downstream_run_id": downstream_run_id,
        },
    )
    raise HTTPException(status_code=502, detail="invalid execution event envelope: run_id mismatch")


def dispatch_get_run_status(
    *,
    run_id: str,
    claims: Mapping[str, Any],
    subject_token: str,
    execution_client: RuntimeExecutionClient,
) -> dict[str, Any]:
    action = "runs.read"
    trace_id = str(claims.get("trace_id", ""))
    actor_id = str(claims.get("sub", "unknown"))
    try:
        delegated = exchange_subject_token(
            subject_token=subject_token,
            requested_token_use="service",
            audience="runtime-execution",
            scope=["runs:read"],
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

    delegated_token = str(delegated["access_token"])
    try:
        result = execution_client.get_run_status(
            run_id=run_id,
            auth_token=delegated_token,
        )
        validate_event_envelope(result)
    except RuntimeExecutionClientError as exc:
        emit_audit_event(
            action=action,
            decision="deny",
            actor_id=actor_id,
            trace_id=trace_id,
            metadata={
                "reason": str(exc),
                "status_code": exc.status_code,
                "run_id": run_id,
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

    _ensure_downstream_run_id_matches(
        run_id=run_id,
        result=result,
        action=action,
        actor_id=actor_id,
        trace_id=trace_id,
    )
    downstream_status = _extract_downstream_status(result)
    recommended_poll_after_ms = _recommended_poll_after_ms_for_run_status(downstream_status)
    response_payload = dict(result)
    response_payload["recommended_poll_after_ms"] = recommended_poll_after_ms

    emit_audit_event(
        action=action,
        decision="allow",
        actor_id=actor_id,
        trace_id=trace_id,
        metadata={
            "run_id": run_id,
            "downstream_event_type": result.get("event_type"),
            "downstream_status": downstream_status,
            "recommended_poll_after_ms": recommended_poll_after_ms,
        },
    )
    return response_payload
