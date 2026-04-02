"""Run control dispatch orchestration for runtime-gateway."""

from __future__ import annotations

from typing import Any, Callable, Mapping

from fastapi import HTTPException

from .audit.emitter import emit_audit_event
from .identity_adapter import ExchangeError, exchange_subject_token
from .code_terms import normalize_optional_code_term
from .events.validation import validate_event_envelope
from .integration import RuntimeExecutionClient, RuntimeExecutionClientError
from .upstream_error import (
    build_upstream_error_detail,
    extract_upstream_failure_classification,
    resolve_upstream_error_class,
    resolve_upstream_retryable,
    resolve_upstream_status_code,
)


def _exchange_runtime_execution_token(
    *,
    action: str,
    run_id: str,
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


def _require_object_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    if payload is None:
        return {}
    if not isinstance(payload, dict):
        raise HTTPException(status_code=422, detail="request body must be object")
    return payload


def _parse_optional_str(payload: dict[str, Any], *, key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise HTTPException(status_code=422, detail=f"{key} must be non-empty string when present")
    return value.strip()


def _parse_optional_bool(payload: dict[str, Any], *, key: str) -> bool | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, bool):
        raise HTTPException(status_code=422, detail=f"{key} must be boolean when present")
    return value


def _parse_required_bool(payload: dict[str, Any], *, key: str) -> bool:
    value = payload.get(key)
    if not isinstance(value, bool):
        raise HTTPException(status_code=422, detail=f"{key} must be boolean")
    return value


def _parse_optional_int(payload: dict[str, Any], *, key: str) -> int | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise HTTPException(status_code=422, detail=f"{key} must be integer when present")
    return value


def _parse_requested_by_run_id(payload: dict[str, Any], *, primary_key: str) -> str | None:
    primary = _parse_optional_str(payload, key=primary_key)
    shared = _parse_optional_str(payload, key="requested_by_run_id")
    if primary is not None and shared is not None and primary != shared:
        raise HTTPException(
            status_code=422,
            detail=f"{primary_key} and requested_by_run_id must match when both are present",
        )
    return primary if primary is not None else shared


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


def _extract_downstream_route_metadata(event: dict[str, Any]) -> dict[str, str]:
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return {}
    route = payload.get("route")
    if not isinstance(route, dict):
        return {}
    metadata: dict[str, str] = {}
    scalar_fields: tuple[tuple[str, str], ...] = (
        ("event_type", "downstream_route_event_type"),
        ("execution_mode", "downstream_execution_mode"),
        ("route_target", "downstream_route_target"),
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
    return metadata


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
    downstream_failure_reason_code = _extract_downstream_failure_reason_code(downstream_event)
    if downstream_failure_reason_code is not None:
        detail["downstream_failure_reason_code"] = downstream_failure_reason_code
    detail.update(_extract_downstream_route_metadata(downstream_event))
    if bus_seq is not None:
        detail["bus_seq"] = bus_seq
    return detail


def _submit_control_action(
    *,
    action: str,
    run_id: str,
    claims: Mapping[str, Any],
    subject_token: str,
    execution_client: RuntimeExecutionClient,
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
    submitter: Callable[[str, str], dict[str, Any]],
    audit_metadata: dict[str, Any] | None = None,
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
        downstream_failure_reason_code = None
        downstream_route_metadata: dict[str, str] = {}
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
                downstream_failure_reason_code = _extract_downstream_failure_reason_code(exc.response_body)
                downstream_route_metadata = _extract_downstream_route_metadata(exc.response_body)
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
                downstream_failure_reason_code = None
                downstream_route_metadata = {}
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
                "downstream_failure_reason_code": downstream_failure_reason_code,
                **downstream_route_metadata,
                "bus_seq": bus_seq,
                "retryable": normalized_retryable,
                "failure_classification": failure_classification,
                "upstream_error_class": upstream_error_class,
                **(audit_metadata or {}),
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
            "downstream_failure_reason_code": _extract_downstream_failure_reason_code(result),
            **_extract_downstream_route_metadata(result),
            "bus_seq": bus_seq,
            **(audit_metadata or {}),
        },
    )
    return result


def dispatch_cancel_run(
    *,
    run_id: str,
    body: dict[str, Any] | None,
    claims: Mapping[str, Any],
    subject_token: str,
    execution_client: RuntimeExecutionClient,
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
) -> dict[str, Any]:
    payload = _require_object_payload(body)
    reason = _parse_optional_str(payload, key="reason")
    cascade_children = _parse_optional_bool(payload, key="cascade_children")
    canceled_by_run_id = _parse_requested_by_run_id(
        payload,
        primary_key="canceled_by_run_id",
    )
    return _submit_control_action(
        action="runs.cancel",
        run_id=run_id,
        claims=claims,
        subject_token=subject_token,
        execution_client=execution_client,
        publish_gateway_event=publish_gateway_event,
        submitter=lambda target_run_id, auth_token: execution_client.cancel_run(
            run_id=target_run_id,
            auth_token=auth_token,
            reason=reason,
            cascade_children=cascade_children,
            canceled_by_run_id=canceled_by_run_id,
        ),
    )


def dispatch_timeout_run(
    *,
    run_id: str,
    body: dict[str, Any] | None,
    claims: Mapping[str, Any],
    subject_token: str,
    execution_client: RuntimeExecutionClient,
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
) -> dict[str, Any]:
    payload = _require_object_payload(body)
    reason = _parse_optional_str(payload, key="reason")
    cascade_children = _parse_optional_bool(payload, key="cascade_children")
    timed_out_by_run_id = _parse_requested_by_run_id(
        payload,
        primary_key="timed_out_by_run_id",
    )
    return _submit_control_action(
        action="runs.timeout",
        run_id=run_id,
        claims=claims,
        subject_token=subject_token,
        execution_client=execution_client,
        publish_gateway_event=publish_gateway_event,
        submitter=lambda target_run_id, auth_token: execution_client.timeout_run(
            run_id=target_run_id,
            auth_token=auth_token,
            reason=reason,
            cascade_children=cascade_children,
            timed_out_by_run_id=timed_out_by_run_id,
        ),
    )


def dispatch_preempt_run(
    *,
    run_id: str,
    body: dict[str, Any] | None,
    claims: Mapping[str, Any],
    subject_token: str,
    execution_client: RuntimeExecutionClient,
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
) -> dict[str, Any]:
    payload = _require_object_payload(body)
    reason = _parse_optional_str(payload, key="reason")
    cascade_children = _parse_optional_bool(payload, key="cascade_children")
    preempted_by_run_id = _parse_requested_by_run_id(
        payload,
        primary_key="preempted_by_run_id",
    )
    return _submit_control_action(
        action="runs.preempt",
        run_id=run_id,
        claims=claims,
        subject_token=subject_token,
        execution_client=execution_client,
        publish_gateway_event=publish_gateway_event,
        submitter=lambda target_run_id, auth_token: execution_client.preempt_run(
            run_id=target_run_id,
            auth_token=auth_token,
            reason=reason,
            cascade_children=cascade_children,
            preempted_by_run_id=preempted_by_run_id,
        ),
    )


def dispatch_complete_run(
    *,
    run_id: str,
    body: dict[str, Any] | None,
    claims: Mapping[str, Any],
    subject_token: str,
    execution_client: RuntimeExecutionClient,
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
) -> dict[str, Any]:
    payload = _require_object_payload(body)
    success = _parse_required_bool(payload, key="success")
    raw_failure_reason_code = _parse_optional_str(payload, key="failure_reason_code")
    failure_reason_code = normalize_optional_code_term(raw_failure_reason_code)
    if success and raw_failure_reason_code is not None:
        raise HTTPException(status_code=422, detail="failure_reason_code is only allowed when success=false")
    if raw_failure_reason_code is not None and failure_reason_code is None:
        raise HTTPException(
            status_code=422,
            detail="failure_reason_code must be snake_case code term when present",
        )
    return _submit_control_action(
        action="runs.complete",
        run_id=run_id,
        claims=claims,
        subject_token=subject_token,
        execution_client=execution_client,
        publish_gateway_event=publish_gateway_event,
        submitter=lambda target_run_id, auth_token: execution_client.complete_run(
            run_id=target_run_id,
            auth_token=auth_token,
            success=success,
            failure_reason_code=failure_reason_code,
        ),
        audit_metadata={
            "requested_success": success,
            **(
                {"requested_failure_reason_code": failure_reason_code}
                if failure_reason_code is not None
                else {}
            ),
        },
    )


def dispatch_renew_run_lease(
    *,
    run_id: str,
    body: dict[str, Any] | None,
    claims: Mapping[str, Any],
    subject_token: str,
    execution_client: RuntimeExecutionClient,
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
) -> dict[str, Any]:
    payload = _require_object_payload(body)
    lease_ttl_seconds = _parse_optional_int(payload, key="lease_ttl_seconds")
    if lease_ttl_seconds is not None and (lease_ttl_seconds < 30 or lease_ttl_seconds > 3600):
        raise HTTPException(status_code=422, detail="lease_ttl_seconds must be integer in [30, 3600]")
    return _submit_control_action(
        action="runs.lease_renew",
        run_id=run_id,
        claims=claims,
        subject_token=subject_token,
        execution_client=execution_client,
        publish_gateway_event=publish_gateway_event,
        submitter=lambda target_run_id, auth_token: execution_client.renew_run_lease(
            run_id=target_run_id,
            auth_token=auth_token,
            lease_ttl_seconds=lease_ttl_seconds,
        ),
        audit_metadata={
            **(
                {"requested_lease_ttl_seconds": lease_ttl_seconds}
                if lease_ttl_seconds is not None
                else {}
            ),
        },
    )
