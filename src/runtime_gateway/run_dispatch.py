"""Run dispatch orchestration for runtime-gateway."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

from fastapi import HTTPException

from .api.schemas import CreateRunRequest, CreateRunResponse
from .audit.emitter import emit_audit_event
from .auth.exchange import ExchangeError, exchange_subject_token
from .code_terms import normalize_optional_code_term
from .contracts.validation import (
    ContractValidationError,
    validate_command_envelope_contract,
    validate_execution_context_contract,
    validate_orchestration_hints_contract,
)
from .executor_profiles import validate_executor_profile
from .events.validation import validate_event_envelope
from .integration import RuntimeExecutionClient, RuntimeExecutionClientError
from .run_status_terms import is_terminal_run_status
from .upstream_error import (
    build_upstream_error_detail,
    extract_upstream_failure_classification,
    resolve_upstream_status_code,
)


def _resolve_trace_id(claims: Mapping[str, Any]) -> str:
    return str(claims.get("trace_id", "")).strip() or str(uuid.uuid4())


def _build_execution_command(req: CreateRunRequest, trace_id: str) -> dict[str, Any]:
    retry_policy = (
        req.retry_policy.model_dump()
        if req.retry_policy is not None
        else {
            "max_attempts": 3,
            "backoff_ms": 250,
            "strategy": "fixed",
        }
    )
    return {
        "command_id": str(uuid.uuid4()),
        "command_type": "run.start",
        "tenant_id": req.tenant_id,
        "app_id": req.app_id,
        "session_key": req.session_key,
        "trace_id": trace_id,
        "idempotency_key": str(uuid.uuid4()),
        "retry_policy": retry_policy,
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
    retryable: bool,
    retry_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    detail: dict[str, Any] = {
        "message": message,
        "status_code": status_code,
        "downstream_event_type": downstream_event_type,
        "retryable": retryable,
    }
    if isinstance(retry_policy, dict) and retry_policy:
        detail["retry_policy"] = dict(retry_policy)
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


def _extract_retry_policy_metadata(command: Mapping[str, Any]) -> dict[str, Any]:
    retry_policy = command.get("retry_policy")
    if not isinstance(retry_policy, dict):
        return {}

    max_attempts = retry_policy.get("max_attempts")
    backoff_ms = retry_policy.get("backoff_ms")
    strategy = retry_policy.get("strategy")
    if not isinstance(max_attempts, int) or max_attempts < 1:
        return {}
    if not isinstance(backoff_ms, int) or backoff_ms < 0:
        return {}
    if not isinstance(strategy, str) or not strategy.strip():
        return {}
    return {
        "retry_policy": {
            "max_attempts": max_attempts,
            "backoff_ms": backoff_ms,
            "strategy": strategy,
        }
    }


def _resolve_effective_retryable(
    *,
    fallback_retryable: bool,
    downstream_event: Mapping[str, Any] | None,
) -> bool:
    if not isinstance(downstream_event, Mapping):
        return fallback_retryable
    payload = downstream_event.get("payload")
    if not isinstance(payload, Mapping):
        return fallback_retryable
    run_status = payload.get("status")
    if not isinstance(run_status, str):
        return fallback_retryable
    normalized = run_status.strip()
    if is_terminal_run_status(normalized):
        return False
    return fallback_retryable


def _extract_route_failure_metadata(downstream_event: Mapping[str, Any]) -> dict[str, Any]:
    payload = downstream_event.get("payload")
    if not isinstance(payload, dict):
        return {}

    metadata: dict[str, Any] = {}
    run_status = payload.get("status")
    if isinstance(run_status, str) and run_status.strip():
        metadata["run_status"] = run_status

    failure = payload.get("failure")
    if isinstance(failure, dict):
        failure_code = normalize_optional_code_term(failure.get("code"))
        if failure_code is not None:
            metadata["failure_code"] = failure_code
        failure_classification = normalize_optional_code_term(failure.get("classification"))
        if failure_classification is not None:
            metadata["failure_classification"] = failure_classification
        failure_message = failure.get("message")
        if isinstance(failure_message, str) and failure_message.strip():
            metadata["failure_message"] = failure_message

    scheduling_signal = payload.get("scheduling_signal")
    if isinstance(scheduling_signal, dict):
        recommended_poll_after_ms = scheduling_signal.get("recommended_poll_after_ms")
        if isinstance(recommended_poll_after_ms, int) and recommended_poll_after_ms >= 0:
            metadata["recommended_poll_after_ms"] = recommended_poll_after_ms

    decision = payload.get("decision")
    if isinstance(decision, dict):
        placement_reason_code = normalize_optional_code_term(decision.get("reason_code"))
        if placement_reason_code is not None:
            metadata["placement_reason_code"] = placement_reason_code
        placement_event_type = decision.get("placement_event_type")
        if isinstance(placement_event_type, str) and placement_event_type.strip():
            metadata["placement_event_type"] = placement_event_type
        snapshot = _filter_resource_snapshot(decision.get("resource_snapshot"))
        if isinstance(snapshot, dict) and snapshot:
            metadata["placement_resource_snapshot"] = snapshot

    return metadata


def _filter_resource_snapshot(raw_snapshot: Any) -> dict[str, Any] | None:
    if not isinstance(raw_snapshot, dict):
        return None
    snapshot: dict[str, Any] = {}
    for key in (
        "queue_depth",
        "eligible_devices",
        "active_leases",
        "available_slots",
        "tenant_active_leases",
        "tenant_limit",
    ):
        value = raw_snapshot.get(key)
        if isinstance(value, int) and value >= 0:
            snapshot[key] = value
    tenant_id_raw = raw_snapshot.get("tenant_id")
    if isinstance(tenant_id_raw, str) and tenant_id_raw.strip():
        snapshot["tenant_id"] = tenant_id_raw
    if not snapshot:
        return None
    return snapshot


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
    retry_policy_metadata = _extract_retry_policy_metadata(command)
    try:
        return execution_client.submit_command(envelope=command, auth_token=delegated_token)
    except RuntimeExecutionClientError as exc:
        downstream_event_type = None
        bus_seq = None
        route_failure_metadata: dict[str, Any] = {}
        failure_classification = extract_upstream_failure_classification(
            message=str(exc),
            detail=exc.detail,
        )
        effective_retryable = exc.retryable
        effective_status_code = resolve_upstream_status_code(
            status_code=exc.status_code,
            retryable=effective_retryable,
            message=str(exc),
        )
        detail: str | dict[str, Any] = build_upstream_error_detail(
            message=str(exc),
            status_code=effective_status_code,
            retryable=effective_retryable,
            failure_classification=failure_classification,
            detail=exc.detail,
            retry_policy=retry_policy_metadata.get("retry_policy")
            if isinstance(retry_policy_metadata.get("retry_policy"), dict)
            else None,
        )
        if isinstance(exc.response_body, dict):
            try:
                validate_event_envelope(exc.response_body)
                downstream_event_type = str(exc.response_body.get("event_type", ""))
                bus_seq = publish_gateway_event(exc.response_body)
                route_failure_metadata = _extract_route_failure_metadata(exc.response_body)
                effective_retryable = _resolve_effective_retryable(
                    fallback_retryable=exc.retryable,
                    downstream_event=exc.response_body,
                )
                detail = _build_downstream_error_detail(
                    message=str(exc),
                    status_code=effective_status_code,
                    downstream_event=exc.response_body,
                    downstream_event_type=downstream_event_type,
                    bus_seq=bus_seq,
                    retryable=effective_retryable,
                    retry_policy=retry_policy_metadata.get("retry_policy")
                    if isinstance(retry_policy_metadata.get("retry_policy"), dict)
                    else None,
                )
            except ValueError:
                downstream_event_type = None
                bus_seq = None

        audit_metadata: dict[str, Any] = {
            "reason": str(exc),
            "status_code": effective_status_code,
            "downstream_event_type": downstream_event_type,
            "bus_seq": bus_seq,
            "retryable": effective_retryable,
            "failure_classification": failure_classification,
        }
        audit_metadata.update(retry_policy_metadata)
        audit_metadata.update(route_failure_metadata)
        emit_audit_event(
            action="runs.dispatch",
            decision="deny",
            actor_id=actor_id,
            trace_id=trace_id,
            metadata=audit_metadata,
        )
        raise HTTPException(status_code=effective_status_code, detail=detail) from exc


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
    command: Mapping[str, Any],
    execution_event: dict[str, Any],
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
    actor_id: str,
    trace_id: str,
) -> CreateRunResponse:
    bus_seq = publish_gateway_event(execution_event)
    run_id, status = _extract_run_result(execution_event)
    route_success_metadata = _extract_route_success_metadata(execution_event)
    audit_metadata: dict[str, Any] = {
        "run_id": run_id,
        "status": status,
        "downstream_event_type": execution_event.get("event_type"),
        "bus_seq": bus_seq,
    }
    audit_metadata.update(_extract_retry_policy_metadata(command))
    audit_metadata.update(route_success_metadata)
    emit_audit_event(
        action="runs.dispatch",
        decision="allow",
        actor_id=actor_id,
        trace_id=trace_id,
        metadata=audit_metadata,
    )
    return CreateRunResponse(run_id=run_id, status=status)


def _extract_route_success_metadata(execution_event: Mapping[str, Any]) -> dict[str, Any]:
    payload = execution_event.get("payload")
    if not isinstance(payload, dict):
        return {}
    route = payload.get("route")
    if not isinstance(route, dict):
        return {}

    metadata: dict[str, Any] = {}
    for key in (
        "execution_mode",
        "route_target",
        "placement_event_type",
        "placement_reason_code",
        "placement_reason",
    ):
        value = route.get(key)
        if key == "placement_reason_code":
            normalized_reason_code = normalize_optional_code_term(value)
            if normalized_reason_code is not None:
                metadata[key] = normalized_reason_code
            continue
        if isinstance(value, str) and value.strip():
            metadata[key] = value

    placement_score = route.get("placement_score")
    if isinstance(placement_score, (int, float)) and not isinstance(placement_score, bool):
        metadata["placement_score"] = float(placement_score)

    queue_depth = route.get("placement_queue_depth")
    if isinstance(queue_depth, int) and queue_depth >= 0:
        metadata["placement_queue_depth"] = queue_depth

    snapshot = _filter_resource_snapshot(route.get("placement_resource_snapshot"))
    if isinstance(snapshot, dict) and snapshot:
        metadata["placement_resource_snapshot"] = snapshot
    return metadata


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
        command=command,
        execution_event=execution_event,
        publish_gateway_event=publish_gateway_event,
        actor_id=actor_id,
        trace_id=trace_id,
    )
