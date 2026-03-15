"""Run status lookup dispatch orchestration for runtime-gateway."""

from __future__ import annotations

from typing import Any, Mapping

from fastapi import HTTPException

from .audit.emitter import emit_audit_event
from .auth.exchange import ExchangeError, exchange_subject_token
from .code_terms import normalize_optional_code_term
from .events.validation import validate_event_envelope
from .integration import RuntimeExecutionClient, RuntimeExecutionClientError
from .run_status_terms import recommended_poll_after_ms_for_run_status
from .upstream_error import (
    build_upstream_error_detail,
    extract_upstream_failure_classification,
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


def _extract_downstream_run_id(event: dict[str, Any]) -> str | None:
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return None
    run_id = payload.get("run_id")
    if not isinstance(run_id, str):
        return None
    normalized = run_id.strip()
    return normalized or None


def _extract_downstream_failure_reason_code(event: dict[str, Any]) -> str | None:
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return None
    orchestration = payload.get("orchestration")
    if isinstance(orchestration, dict):
        failure_reason_code = normalize_optional_code_term(orchestration.get("failure_reason_code"))
        if failure_reason_code is not None:
            return failure_reason_code
    # Fallback for flattened payload shapes.
    return normalize_optional_code_term(payload.get("failure_reason_code"))


def _extract_downstream_route_metadata(event: dict[str, Any]) -> dict[str, Any]:
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return {}
    route = payload.get("route")
    if not isinstance(route, dict):
        return {}

    metadata: dict[str, Any] = {}
    scalar_fields: tuple[tuple[str, str], ...] = (
        ("event_type", "downstream_route_event_type"),
        ("execution_mode", "downstream_execution_mode"),
        ("route_target", "downstream_route_target"),
        ("placement_event_type", "downstream_placement_event_type"),
        ("placement_reason_code", "downstream_placement_reason_code"),
    )
    for source, target in scalar_fields:
        raw_value = route.get(source)
        if isinstance(raw_value, str) and raw_value.strip():
            if source == "placement_reason_code":
                normalized_code = normalize_optional_code_term(raw_value)
                if normalized_code is not None:
                    metadata[target] = normalized_code
                continue
            metadata[target] = raw_value.strip()
    placement_resource_snapshot = route.get("placement_resource_snapshot")
    if isinstance(placement_resource_snapshot, dict) and placement_resource_snapshot:
        metadata["downstream_placement_resource_snapshot"] = dict(placement_resource_snapshot)
    return metadata


def _build_downstream_error_detail(
    *,
    message: str,
    status_code: int,
    requested_run_id: str,
    response_body: dict[str, Any],
) -> dict[str, Any]:
    detail: dict[str, Any] = {
        "message": message,
        "status_code": status_code,
        "requested_run_id": requested_run_id,
        "downstream_response": response_body,
    }
    downstream_event_type = response_body.get("event_type")
    if isinstance(downstream_event_type, str) and downstream_event_type.strip():
        detail["downstream_event_type"] = downstream_event_type
    downstream_status = _extract_downstream_status(response_body)
    if downstream_status is not None:
        detail["downstream_status"] = downstream_status
    downstream_run_id = _extract_downstream_run_id(response_body)
    if downstream_run_id is not None:
        detail["downstream_run_id"] = downstream_run_id
    downstream_failure_reason_code = _extract_downstream_failure_reason_code(response_body)
    if downstream_failure_reason_code is not None:
        detail["downstream_failure_reason_code"] = downstream_failure_reason_code
    detail.update(_extract_downstream_route_metadata(response_body))
    return detail


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
        detail: str | dict[str, Any] = build_upstream_error_detail(
            message=str(exc),
            status_code=resolved_status,
            retryable=normalized_retryable,
            failure_classification=failure_classification,
            detail=exc.detail,
        )
        downstream_event_type = None
        downstream_status = None
        downstream_run_id = None
        downstream_failure_reason_code = None
        downstream_route_metadata: dict[str, Any] = {}
        if isinstance(exc.response_body, dict):
            downstream_event_type_raw = exc.response_body.get("event_type")
            if isinstance(downstream_event_type_raw, str) and downstream_event_type_raw.strip():
                downstream_event_type = downstream_event_type_raw
            downstream_status = _extract_downstream_status(exc.response_body)
            downstream_run_id = _extract_downstream_run_id(exc.response_body)
            downstream_failure_reason_code = _extract_downstream_failure_reason_code(exc.response_body)
            downstream_route_metadata = _extract_downstream_route_metadata(exc.response_body)
            detail = _build_downstream_error_detail(
                message=str(exc),
                status_code=resolved_status,
                requested_run_id=run_id,
                response_body=exc.response_body,
            )
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
                "downstream_run_id": downstream_run_id,
                "downstream_failure_reason_code": downstream_failure_reason_code,
                **downstream_route_metadata,
                "retryable": normalized_retryable,
                "failure_classification": failure_classification,
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

    _ensure_downstream_run_id_matches(
        run_id=run_id,
        result=result,
        action=action,
        actor_id=actor_id,
        trace_id=trace_id,
    )
    downstream_status = _extract_downstream_status(result)
    downstream_failure_reason_code = _extract_downstream_failure_reason_code(result)
    downstream_route_metadata = _extract_downstream_route_metadata(result)
    recommended_poll_after_ms = recommended_poll_after_ms_for_run_status(downstream_status)
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
            "downstream_failure_reason_code": downstream_failure_reason_code,
            **downstream_route_metadata,
            "recommended_poll_after_ms": recommended_poll_after_ms,
        },
    )
    return response_payload
