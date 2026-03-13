"""Run dispatch orchestration for runtime-gateway."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

from fastapi import HTTPException

from .api.schemas import CreateRunRequest, CreateRunResponse
from .audit.emitter import emit_audit_event
from .auth.exchange import ExchangeError, exchange_subject_token
from .contracts.validation import (
    ContractValidationError,
    validate_command_envelope_contract,
    validate_execution_context_contract,
    validate_orchestration_hints_contract,
)
from .executor_profiles import validate_executor_profile
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


def _validate_execution_context_payload(req: CreateRunRequest) -> None:
    raw_context = req.payload.get("execution_context")
    if raw_context is None:
        return
    if not isinstance(raw_context, dict):
        raise HTTPException(status_code=422, detail="execution_context must be an object")
    try:
        validate_execution_context_contract(raw_context)
    except ContractValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    executor = raw_context.get("executor")
    if isinstance(executor, dict):
        try:
            validate_executor_profile(
                family=str(executor.get("family", "")).strip(),
                engine=str(executor.get("engine", "")).strip(),
                adapter=str(executor.get("adapter", "")).strip(),
            )
        except ContractValidationError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    if str(raw_context.get("task_plane")) != "runtime_workload":
        return
    runtime = raw_context.get("runtime")
    if not isinstance(runtime, dict):
        return

    profile = req.payload.get("execution_profile")
    if isinstance(profile, dict):
        profile_mode = profile.get("execution_mode")
    else:
        profile_mode = "control"
    if runtime.get("execution_mode") != profile_mode:
        raise HTTPException(
            status_code=422,
            detail=(
                "execution_context.runtime.execution_mode must match "
                "execution_profile.execution_mode for runtime_workload"
            ),
        )


def _validate_orchestration_payload(req: CreateRunRequest) -> None:
    raw_orchestration = req.payload.get("orchestration")
    if raw_orchestration is None:
        return
    if not isinstance(raw_orchestration, dict):
        raise HTTPException(status_code=422, detail="orchestration must be an object")
    try:
        validate_orchestration_hints_contract(raw_orchestration)
    except ContractValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _build_validated_command(req: CreateRunRequest, trace_id: str) -> dict[str, Any]:
    _validate_execution_context_payload(req)
    _validate_orchestration_payload(req)
    command = _build_execution_command(req, trace_id)
    try:
        validate_command_envelope_contract(command)
    except ContractValidationError as exc:
        raise HTTPException(status_code=500, detail=f"invalid command envelope: {exc}") from exc
    return command


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
        task_id = payload.get("task_id")
        if isinstance(task_id, str) and task_id.strip():
            detail["task_id"] = task_id
        failure = payload.get("failure")
        if isinstance(failure, dict):
            detail["failure"] = failure
        decision = payload.get("decision")
        if isinstance(decision, dict):
            detail["decision"] = decision
    detail.update(_extract_route_failure_metadata(downstream_event))

    if bus_seq is not None:
        detail["bus_seq"] = bus_seq
    return detail


def _extract_route_failure_metadata(downstream_event: Mapping[str, Any]) -> dict[str, Any]:
    payload = downstream_event.get("payload")
    if not isinstance(payload, dict):
        return {}

    metadata: dict[str, Any] = {}
    failure = payload.get("failure")
    if isinstance(failure, dict):
        failure_code = failure.get("code")
        if isinstance(failure_code, str) and failure_code.strip():
            metadata["failure_code"] = failure_code
        failure_classification = failure.get("classification")
        if isinstance(failure_classification, str) and failure_classification.strip():
            metadata["failure_classification"] = failure_classification
        failure_message = failure.get("message")
        if isinstance(failure_message, str) and failure_message.strip():
            metadata["failure_message"] = failure_message

    scheduling_signal = payload.get("scheduling_signal")
    if isinstance(scheduling_signal, dict):
        recommended_poll_after_ms = scheduling_signal.get("recommended_poll_after_ms")
        if isinstance(recommended_poll_after_ms, int) and recommended_poll_after_ms >= 0:
            metadata["recommended_poll_after_ms"] = recommended_poll_after_ms

    return metadata


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
        route_failure_metadata: dict[str, Any] = {}
        detail: str | dict[str, Any] = str(exc)
        if isinstance(exc.response_body, dict):
            try:
                validate_event_envelope(exc.response_body)
                downstream_event_type = str(exc.response_body.get("event_type", ""))
                bus_seq = publish_gateway_event(exc.response_body)
                route_failure_metadata = _extract_route_failure_metadata(exc.response_body)
                detail = _build_downstream_error_detail(
                    message=str(exc),
                    status_code=exc.status_code or 502,
                    downstream_event=exc.response_body,
                    downstream_event_type=downstream_event_type,
                    bus_seq=bus_seq,
                )
            except ValueError:
                downstream_event_type = None
                bus_seq = None

        audit_metadata: dict[str, Any] = {
            "reason": str(exc),
            "status_code": exc.status_code,
            "downstream_event_type": downstream_event_type,
            "bus_seq": bus_seq,
        }
        audit_metadata.update(route_failure_metadata)
        emit_audit_event(
            action="runs.dispatch",
            decision="deny",
            actor_id=actor_id,
            trace_id=trace_id,
            metadata=audit_metadata,
        )
        raise HTTPException(status_code=exc.status_code or 502, detail=detail) from exc


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
