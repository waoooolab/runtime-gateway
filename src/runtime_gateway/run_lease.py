"""Run lease lookup dispatch orchestration for runtime-gateway."""

from __future__ import annotations

from typing import Any, Mapping

from fastapi import HTTPException

from .audit.emitter import emit_audit_event
from .auth.exchange import ExchangeError, exchange_subject_token
from .code_terms import normalize_optional_code_term
from .contracts import ContractValidationError, validate_runtime_run_lease_contract
from .integration import RuntimeExecutionClient, RuntimeExecutionClientError
from .upstream_error import resolve_upstream_status_code


def _recommended_poll_after_ms_for_lease_state(lease_state: str | None) -> int:
    normalized = (lease_state or "").strip().lower()
    if normalized in {"released", "expired"}:
        return 10000
    if normalized == "active":
        return 2000
    if normalized:
        return 4000
    return 5000


def _extract_lease_state(payload: dict[str, Any]) -> str | None:
    lease = payload.get("lease")
    if not isinstance(lease, dict):
        return None
    state = lease.get("state")
    if isinstance(state, str) and state.strip():
        return state.strip()
    return None


def _extract_device_hub_status(payload: dict[str, Any]) -> str | None:
    device_hub = payload.get("device_hub")
    if not isinstance(device_hub, dict):
        return None
    status = device_hub.get("status")
    if isinstance(status, str) and status.strip():
        return status.strip()
    return None


def _extract_device_hub_expire_reason_code(payload: dict[str, Any]) -> str | None:
    device_hub = payload.get("device_hub")
    if not isinstance(device_hub, dict):
        return None
    snapshot = device_hub.get("snapshot")
    if not isinstance(snapshot, dict):
        return None
    return normalize_optional_code_term(snapshot.get("expire_reason_code"))


def _extract_run_id(payload: dict[str, Any]) -> str | None:
    run_id = payload.get("run_id")
    if not isinstance(run_id, str):
        return None
    normalized = run_id.strip()
    return normalized or None


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
    downstream_run_id = _extract_run_id(response_body)
    if downstream_run_id is not None:
        detail["downstream_run_id"] = downstream_run_id
    lease_state = _extract_lease_state(response_body)
    if lease_state is not None:
        detail["lease_state"] = lease_state
    device_hub_status = _extract_device_hub_status(response_body)
    if device_hub_status is not None:
        detail["device_hub_status"] = device_hub_status
    expire_reason_code = _extract_device_hub_expire_reason_code(response_body)
    if expire_reason_code is not None:
        detail["expire_reason_code"] = expire_reason_code
    return detail


def _validate_run_lease_response_or_raise(
    *,
    result: dict[str, Any],
    requested_run_id: str,
    action: str,
    actor_id: str,
    trace_id: str,
) -> None:
    try:
        validate_runtime_run_lease_contract(result)
    except ContractValidationError as exc:
        emit_audit_event(
            action=action,
            decision="deny",
            actor_id=actor_id,
            trace_id=trace_id,
            metadata={
                "reason": str(exc),
                "run_id": requested_run_id,
                "validation_schema": "runtime/runtime-run-lease.v1.json",
            },
        )
        raise HTTPException(status_code=502, detail=f"invalid run lease response: {exc}") from exc

    downstream_run_id = _extract_run_id(result)
    if downstream_run_id == requested_run_id:
        return None
    emit_audit_event(
        action=action,
        decision="deny",
        actor_id=actor_id,
        trace_id=trace_id,
        metadata={
            "reason": "run lease response mismatches requested run_id",
            "run_id": requested_run_id,
            "downstream_run_id": downstream_run_id,
        },
    )
    raise HTTPException(status_code=502, detail="invalid run lease response: run_id mismatch")


def dispatch_get_run_lease(
    *,
    run_id: str,
    claims: Mapping[str, Any],
    subject_token: str,
    execution_client: RuntimeExecutionClient,
) -> dict[str, Any]:
    action = "runs.lease"
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
        result = execution_client.get_run_lease(
            run_id=run_id,
            auth_token=delegated_token,
        )
    except RuntimeExecutionClientError as exc:
        resolved_status = resolve_upstream_status_code(
            status_code=exc.status_code,
            retryable=exc.retryable,
            message=str(exc),
        )
        detail: str | dict[str, Any] = str(exc)
        downstream_run_id = None
        lease_state = None
        device_hub_status = None
        expire_reason_code = None
        if isinstance(exc.response_body, dict):
            downstream_run_id = _extract_run_id(exc.response_body)
            lease_state = _extract_lease_state(exc.response_body)
            device_hub_status = _extract_device_hub_status(exc.response_body)
            expire_reason_code = _extract_device_hub_expire_reason_code(exc.response_body)
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
                "downstream_run_id": downstream_run_id,
                "lease_state": lease_state,
                "device_hub_status": device_hub_status,
                "expire_reason_code": expire_reason_code,
            },
        )
        raise HTTPException(status_code=resolved_status, detail=detail) from exc

    _validate_run_lease_response_or_raise(
        result=result,
        requested_run_id=run_id,
        action=action,
        actor_id=actor_id,
        trace_id=trace_id,
    )
    lease_state = _extract_lease_state(result)
    recommended_poll_after_ms = _recommended_poll_after_ms_for_lease_state(lease_state)
    response_payload = dict(result)
    response_payload["recommended_poll_after_ms"] = recommended_poll_after_ms

    emit_audit_event(
        action=action,
        decision="allow",
        actor_id=actor_id,
        trace_id=trace_id,
        metadata={
            "run_id": run_id,
            "lease_state": lease_state,
            "device_hub_status": _extract_device_hub_status(result),
            "expire_reason_code": _extract_device_hub_expire_reason_code(result),
            "recommended_poll_after_ms": recommended_poll_after_ms,
        },
    )
    return response_payload
