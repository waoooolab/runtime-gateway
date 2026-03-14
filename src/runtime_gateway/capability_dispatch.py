"""Capability API forwarding dispatch for runtime-gateway."""

from __future__ import annotations

from typing import Any, Callable, Mapping

from fastapi import HTTPException

from .audit.emitter import emit_audit_event
from .auth.exchange import ExchangeError, exchange_subject_token
from .events.validation import validate_event_envelope
from .integration import RuntimeExecutionClient, RuntimeExecutionClientError
from .upstream_error import resolve_upstream_status_code


def _require_object_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    if payload is None:
        raise HTTPException(status_code=422, detail="request body must be object")
    if not isinstance(payload, dict):
        raise HTTPException(status_code=422, detail="request body must be object")
    return payload


def _extract_downstream_status(event_or_payload: dict[str, Any]) -> str | None:
    payload = event_or_payload.get("payload")
    if isinstance(payload, dict):
        status = payload.get("status")
        if isinstance(status, str) and status.strip():
            return status.strip()
    status = event_or_payload.get("status")
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
) -> dict[str, Any]:
    detail: dict[str, Any] = {
        "message": message,
        "status_code": status_code,
        "downstream_event_type": downstream_event_type,
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
        capability_id = payload.get("capability_id")
        if isinstance(capability_id, str) and capability_id.strip():
            detail["capability_id"] = capability_id
        status = payload.get("status")
        if isinstance(status, str) and status.strip():
            detail["status"] = status
        failure = payload.get("failure")
        if isinstance(failure, dict):
            detail["failure"] = failure
    if bus_seq is not None:
        detail["bus_seq"] = bus_seq
    return detail


def _exchange_runtime_execution_token(
    *,
    action: str,
    scope: str,
    subject_token: str,
    actor_id: str,
    trace_id: str,
) -> dict[str, Any]:
    try:
        return exchange_subject_token(
            subject_token=subject_token,
            requested_token_use="service",
            audience="runtime-execution",
            scope=[scope],
            requested_ttl_seconds=300,
            trace_id=trace_id,
        )
    except ExchangeError as exc:
        emit_audit_event(
            action=action,
            decision="deny",
            actor_id=actor_id,
            trace_id=trace_id,
            metadata={"reason": exc.detail, "delegated_scope": scope},
        )
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


def _submit_capability_action(
    *,
    action: str,
    delegated_scope: str,
    claims: Mapping[str, Any],
    subject_token: str,
    execution_client: RuntimeExecutionClient,
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
    submitter: Callable[[str], dict[str, Any]],
    expect_event: bool,
    audit_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    trace_id = str(claims.get("trace_id", ""))
    actor_id = str(claims.get("sub", "unknown"))

    delegated = _exchange_runtime_execution_token(
        action=action,
        scope=delegated_scope,
        subject_token=subject_token,
        actor_id=actor_id,
        trace_id=trace_id,
    )
    delegated_token = str(delegated["access_token"])

    try:
        result = submitter(delegated_token)
        bus_seq = None
        if expect_event:
            validate_event_envelope(result)
            bus_seq = publish_gateway_event(result)
    except RuntimeExecutionClientError as exc:
        resolved_status = resolve_upstream_status_code(
            status_code=exc.status_code,
            retryable=exc.retryable,
            message=str(exc),
        )
        downstream_event_type = None
        downstream_status = None
        bus_seq = None
        detail: str | dict[str, Any] = str(exc)
        if expect_event and isinstance(exc.response_body, dict):
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
                "downstream_event_type": downstream_event_type,
                "downstream_status": downstream_status,
                "bus_seq": bus_seq,
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
            metadata={
                "reason": f"invalid execution event envelope: {exc}",
                **(audit_metadata or {}),
            },
        )
        raise HTTPException(status_code=502, detail=f"invalid execution event envelope: {exc}") from exc

    emit_audit_event(
        action=action,
        decision="allow",
        actor_id=actor_id,
        trace_id=trace_id,
        metadata={
            "delegated_scope": delegated_scope,
            "downstream_event_type": result.get("event_type") if expect_event else None,
            "downstream_status": _extract_downstream_status(result),
            **(audit_metadata or {}),
        },
    )
    return result


def dispatch_register_capability(
    *,
    body: dict[str, Any] | None,
    claims: Mapping[str, Any],
    subject_token: str,
    execution_client: RuntimeExecutionClient,
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
) -> dict[str, Any]:
    payload = _require_object_payload(body)
    return _submit_capability_action(
        action="capabilities.register",
        delegated_scope="capabilities:write",
        claims=claims,
        subject_token=subject_token,
        execution_client=execution_client,
        publish_gateway_event=publish_gateway_event,
        submitter=lambda auth_token: execution_client.register_capability(
            auth_token=auth_token,
            payload=payload,
        ),
        expect_event=False,
    )


def dispatch_list_capabilities(
    *,
    claims: Mapping[str, Any],
    subject_token: str,
    execution_client: RuntimeExecutionClient,
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
) -> dict[str, Any]:
    return _submit_capability_action(
        action="capabilities.list",
        delegated_scope="capabilities:read",
        claims=claims,
        subject_token=subject_token,
        execution_client=execution_client,
        publish_gateway_event=publish_gateway_event,
        submitter=lambda auth_token: execution_client.list_capabilities(auth_token=auth_token),
        expect_event=False,
    )


def dispatch_get_capability(
    *,
    capability_id: str,
    claims: Mapping[str, Any],
    subject_token: str,
    execution_client: RuntimeExecutionClient,
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
) -> dict[str, Any]:
    return _submit_capability_action(
        action="capabilities.get",
        delegated_scope="capabilities:read",
        claims=claims,
        subject_token=subject_token,
        execution_client=execution_client,
        publish_gateway_event=publish_gateway_event,
        submitter=lambda auth_token: execution_client.get_capability(
            capability_id=capability_id,
            auth_token=auth_token,
        ),
        expect_event=False,
        audit_metadata={"capability_id": capability_id},
    )


def dispatch_resolve_capability(
    *,
    body: dict[str, Any] | None,
    claims: Mapping[str, Any],
    subject_token: str,
    execution_client: RuntimeExecutionClient,
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
) -> dict[str, Any]:
    payload = _require_object_payload(body)
    return _submit_capability_action(
        action="capabilities.resolve",
        delegated_scope="capabilities:read",
        claims=claims,
        subject_token=subject_token,
        execution_client=execution_client,
        publish_gateway_event=publish_gateway_event,
        submitter=lambda auth_token: execution_client.resolve_capability(
            auth_token=auth_token,
            payload=payload,
        ),
        expect_event=False,
    )


def dispatch_compile_capability(
    *,
    body: dict[str, Any] | None,
    claims: Mapping[str, Any],
    subject_token: str,
    execution_client: RuntimeExecutionClient,
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
) -> dict[str, Any]:
    payload = _require_object_payload(body)
    return _submit_capability_action(
        action="capabilities.compile",
        delegated_scope="capabilities:write",
        claims=claims,
        subject_token=subject_token,
        execution_client=execution_client,
        publish_gateway_event=publish_gateway_event,
        submitter=lambda auth_token: execution_client.compile_capability(
            auth_token=auth_token,
            payload=payload,
        ),
        expect_event=True,
    )


def dispatch_publish_capability(
    *,
    body: dict[str, Any] | None,
    claims: Mapping[str, Any],
    subject_token: str,
    execution_client: RuntimeExecutionClient,
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
) -> dict[str, Any]:
    payload = _require_object_payload(body)
    return _submit_capability_action(
        action="capabilities.publish",
        delegated_scope="capabilities:write",
        claims=claims,
        subject_token=subject_token,
        execution_client=execution_client,
        publish_gateway_event=publish_gateway_event,
        submitter=lambda auth_token: execution_client.publish_capability(
            auth_token=auth_token,
            payload=payload,
        ),
        expect_event=True,
    )


def dispatch_invoke_capability(
    *,
    capability_id: str,
    body: dict[str, Any] | None,
    claims: Mapping[str, Any],
    subject_token: str,
    execution_client: RuntimeExecutionClient,
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
) -> dict[str, Any]:
    payload = _require_object_payload(body)
    return _submit_capability_action(
        action="capabilities.invoke",
        delegated_scope="capabilities:invoke",
        claims=claims,
        subject_token=subject_token,
        execution_client=execution_client,
        publish_gateway_event=publish_gateway_event,
        submitter=lambda auth_token: execution_client.invoke_capability(
            capability_id=capability_id,
            auth_token=auth_token,
            payload=payload,
        ),
        expect_event=True,
        audit_metadata={"capability_id": capability_id},
    )
