"""Run dispatch orchestration for runtime-gateway."""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

from fastapi import HTTPException

from .api.schemas import CreateRunRequest, CreateRunResponse
from .audit.emitter import emit_audit_event
from .identity_adapter import ExchangeError, exchange_subject_token
from .code_terms import normalize_optional_code_term
from .contracts.validation import (
    ContractValidationError,
    validate_command_envelope_contract,
    validate_execution_context_contract,
    validate_orchestration_hints_contract,
    validate_template_capability_binding_contract,
)
from .execution_ingress_contract import execution_ingress_contract_payload
from .executor_profiles import validate_executor_profile
from .events.validation import validate_event_envelope
from .integration import RuntimeExecutionClient, RuntimeExecutionClientError
from .run_status_terms import is_terminal_run_status
from .upstream_error import (
    build_upstream_error_detail,
    extract_upstream_failure_classification,
    resolve_upstream_error_class,
    resolve_upstream_status_code,
)

_TASK_CONTRACT_VERSION_ENV = "OWA_TASK_CONTRACT_VERSION"
_AGENT_CONTRACT_VERSION_ENV = "OWA_AGENT_CONTRACT_VERSION"
_EVENT_SCHEMA_VERSION_ENV = "OWA_EVENT_SCHEMA_VERSION"
_ACTIVE_TASK_CONTRACT_VERSIONS_ENV = "OWA_ACTIVE_TASK_CONTRACT_VERSIONS"
_ACTIVE_AGENT_CONTRACT_VERSIONS_ENV = "OWA_ACTIVE_AGENT_CONTRACT_VERSIONS"
_ACTIVE_EVENT_SCHEMA_VERSIONS_ENV = "OWA_ACTIVE_EVENT_SCHEMA_VERSIONS"
_DEFAULT_TASK_CONTRACT_VERSION = "task-envelope.v1"
_DEFAULT_AGENT_CONTRACT_VERSION = "assistant-decision.v1"
_DEFAULT_EVENT_SCHEMA_VERSION = "event-envelope.v1"
_DEFAULT_SCOPE_TYPE = "session"
_DEFAULT_ACTIVE_TASK_CONTRACT_VERSIONS = ("task-envelope.v1", "task-envelope.v2")
_DEFAULT_ACTIVE_AGENT_CONTRACT_VERSIONS = ("assistant-decision.v1", "assistant-decision.v2")
_DEFAULT_ACTIVE_EVENT_SCHEMA_VERSIONS = ("event-envelope.v1", "event-envelope.v2")
_ALLOWED_INGRESS_MODES = frozenset({"assistant", "workflow", "tools", "mixed"})
_RUNTIME_GATEWAY_SERVICE_NAME_ENV = "RUNTIME_GATEWAY_SERVICE_NAME"
_RUNTIME_GATEWAY_DEPLOYMENT_ENV_ENV = "RUNTIME_GATEWAY_DEPLOYMENT_ENV"
_OWA_DEPLOY_ENV_ENV = "OWA_DEPLOY_ENV"


def _resolve_trace_id(claims: Mapping[str, Any]) -> str:
    return str(claims.get("trace_id", "")).strip() or str(uuid.uuid4())


def _resolve_scope_axis(req: CreateRunRequest) -> dict[str, str]:
    scope_id = str(req.scope_id or "").strip() or str(req.session_key).strip()
    if not scope_id:
        raise HTTPException(status_code=422, detail="scope_id cannot be empty")
    scope_type = str(req.scope_type or "").strip() or _DEFAULT_SCOPE_TYPE
    return {
        "scope_id": scope_id,
        "scope_type": scope_type,
    }


def _resolve_bound_contract_versions(req: CreateRunRequest) -> dict[str, str]:
    configured = req.contract_versions.model_dump(exclude_none=True) if req.contract_versions is not None else {}
    task_contract_version = str(
        configured.get("task_contract_version")
        or os.environ.get(_TASK_CONTRACT_VERSION_ENV, _DEFAULT_TASK_CONTRACT_VERSION)
    ).strip()
    agent_contract_version = str(
        configured.get("agent_contract_version")
        or os.environ.get(_AGENT_CONTRACT_VERSION_ENV, _DEFAULT_AGENT_CONTRACT_VERSION)
    ).strip()
    event_schema_version = str(
        configured.get("event_schema_version")
        or os.environ.get(_EVENT_SCHEMA_VERSION_ENV, _DEFAULT_EVENT_SCHEMA_VERSION)
    ).strip()
    return {
        "task_contract_version": task_contract_version,
        "agent_contract_version": agent_contract_version,
        "event_schema_version": event_schema_version,
    }


def _normalize_optional_str(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if not normalized:
        return None
    return normalized


def _normalize_ingress_mode(value: Any) -> str | None:
    normalized = _normalize_optional_str(value)
    if normalized is None:
        return None
    lowered = normalized.lower()
    if lowered not in _ALLOWED_INGRESS_MODES:
        return None
    return lowered


def _extract_payload_ingress_mode(payload: Mapping[str, Any]) -> str | None:
    control_ingress = payload.get("control_ingress")
    if isinstance(control_ingress, Mapping):
        mode = _normalize_ingress_mode(control_ingress.get("entry_mode"))
        if mode is not None:
            return mode
        mode = _normalize_ingress_mode(control_ingress.get("mode"))
        if mode is not None:
            return mode
    mode = _normalize_ingress_mode(payload.get("ingress_mode"))
    if mode is not None:
        return mode
    return _normalize_ingress_mode(payload.get("entry_mode"))


def _payload_metadata(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    raw = payload.get("metadata")
    if isinstance(raw, Mapping):
        return raw
    return {}


def _build_control_ingress_contract(
    *,
    req: CreateRunRequest,
    payload: Mapping[str, Any],
    claims: Mapping[str, Any],
    scope_axis: Mapping[str, str],
    trace_id: str,
    command_id: str,
) -> dict[str, Any]:
    normalized_requested_trace_id = _normalize_optional_str(req.ingress_trace_id)
    if normalized_requested_trace_id is not None and normalized_requested_trace_id != trace_id:
        raise HTTPException(
            status_code=422,
            detail="ingress_trace_id must match authenticated trace_id",
        )

    existing_control_ingress_raw = payload.get("control_ingress")
    existing_control_ingress = (
        dict(existing_control_ingress_raw)
        if isinstance(existing_control_ingress_raw, Mapping)
        else {}
    )
    entry_mode = _normalize_ingress_mode(req.ingress_mode)
    if entry_mode is None:
        entry_mode = _extract_payload_ingress_mode(payload) or "assistant"
    lifecycle_id = _normalize_optional_str(req.ingress_lifecycle_id)
    if lifecycle_id is None:
        lifecycle_id = _normalize_optional_str(existing_control_ingress.get("lifecycle_id"))
    payload_metadata = _payload_metadata(payload)

    control_ingress = dict(existing_control_ingress)
    control_ingress["entry_mode"] = entry_mode
    control_ingress["trace_id"] = trace_id
    control_ingress["tenant_id"] = req.tenant_id
    control_ingress["app_id"] = req.app_id
    control_ingress["session_key"] = req.session_key
    control_ingress["scope_id"] = str(scope_axis["scope_id"])
    control_ingress["scope_type"] = str(scope_axis["scope_type"])
    if lifecycle_id is not None:
        control_ingress["lifecycle_id"] = lifecycle_id

    metadata_values = {
        "team_id": (
            _normalize_optional_str(existing_control_ingress.get("team_id"))
            or _normalize_optional_str(payload.get("team_id"))
            or _normalize_optional_str(payload_metadata.get("team_id"))
            or _normalize_optional_str(claims.get("team_id"))
        ),
        "service": (
            _normalize_optional_str(existing_control_ingress.get("service"))
            or _normalize_optional_str(payload.get("service"))
            or _normalize_optional_str(payload_metadata.get("service"))
            or _normalize_optional_str(os.environ.get(_RUNTIME_GATEWAY_SERVICE_NAME_ENV))
            or "runtime-gateway"
        ),
        "env": (
            _normalize_optional_str(existing_control_ingress.get("env"))
            or _normalize_optional_str(payload.get("env"))
            or _normalize_optional_str(payload_metadata.get("env"))
            or _normalize_optional_str(os.environ.get(_RUNTIME_GATEWAY_DEPLOYMENT_ENV_ENV))
            or _normalize_optional_str(os.environ.get(_OWA_DEPLOY_ENV_ENV))
        ),
        "request_id": (
            _normalize_optional_str(existing_control_ingress.get("request_id"))
            or _normalize_optional_str(payload.get("request_id"))
            or _normalize_optional_str(payload_metadata.get("request_id"))
            or _normalize_optional_str(command_id)
            or _normalize_optional_str(trace_id)
        ),
        "cost_center": (
            _normalize_optional_str(existing_control_ingress.get("cost_center"))
            or _normalize_optional_str(payload.get("cost_center"))
            or _normalize_optional_str(payload_metadata.get("cost_center"))
            or _normalize_optional_str(claims.get("cost_center"))
        ),
    }
    for key, value in metadata_values.items():
        if value is not None:
            control_ingress[key] = value
    return control_ingress


def _build_payload(
    *,
    req: CreateRunRequest,
    claims: Mapping[str, Any],
    command_id: str,
    trace_id: str,
    scope_axis: Mapping[str, str],
) -> dict[str, Any]:
    payload: dict[str, Any] = dict(req.payload)
    payload["control_ingress"] = _build_control_ingress_contract(
        req=req,
        payload=payload,
        claims=claims,
        scope_axis=scope_axis,
        trace_id=trace_id,
        command_id=command_id,
    )
    return payload


def _parse_active_version_pool(env_name: str, defaults: tuple[str, ...]) -> set[str]:
    raw = os.environ.get(env_name)
    if raw is None:
        return {value for value in defaults if isinstance(value, str) and value.strip()}
    values = {item.strip() for item in raw.split(",") if isinstance(item, str) and item.strip()}
    if values:
        return values
    return {value for value in defaults if isinstance(value, str) and value.strip()}


def _validate_bound_versions_in_active_pool(bound_versions: Mapping[str, str]) -> None:
    active_pools = {
        "task_contract_version": _parse_active_version_pool(
            _ACTIVE_TASK_CONTRACT_VERSIONS_ENV,
            _DEFAULT_ACTIVE_TASK_CONTRACT_VERSIONS,
        ),
        "agent_contract_version": _parse_active_version_pool(
            _ACTIVE_AGENT_CONTRACT_VERSIONS_ENV,
            _DEFAULT_ACTIVE_AGENT_CONTRACT_VERSIONS,
        ),
        "event_schema_version": _parse_active_version_pool(
            _ACTIVE_EVENT_SCHEMA_VERSIONS_ENV,
            _DEFAULT_ACTIVE_EVENT_SCHEMA_VERSIONS,
        ),
    }
    for key, value in bound_versions.items():
        if value not in active_pools[key]:
            allowed = ",".join(sorted(active_pools[key]))
            raise HTTPException(
                status_code=422,
                detail=f"{key}={value} is not in active version pool ({allowed})",
            )


def _bound_contract_versions_from_command(command: Mapping[str, Any]) -> dict[str, str]:
    versions: dict[str, str] = {}
    for key in ("task_contract_version", "agent_contract_version", "event_schema_version"):
        raw = command.get(key)
        if not isinstance(raw, str) or not raw.strip():
            raise ValueError(f"missing bound contract version on command: {key}")
        versions[key] = raw.strip()
    return versions


def _enforce_event_contract_versions(
    *,
    event: dict[str, Any],
    bound_versions: Mapping[str, str],
) -> None:
    for key, expected in bound_versions.items():
        raw = event.get(key)
        if raw is None:
            event[key] = expected
            continue
        if not isinstance(raw, str) or not raw.strip():
            raise ValueError(f"invalid contract version field in downstream event: {key}")
        if raw.strip() != expected:
            raise ValueError(
                f"contract version drift detected: {key} expected={expected} got={raw.strip()}"
            )


def _build_execution_command(
    req: CreateRunRequest,
    trace_id: str,
    claims: Mapping[str, Any],
) -> dict[str, Any]:
    retry_policy = (
        req.retry_policy.model_dump()
        if req.retry_policy is not None
        else {
            "max_attempts": 3,
            "backoff_ms": 250,
            "strategy": "fixed",
        }
    )
    scope_axis = _resolve_scope_axis(req)
    command_id = str(uuid.uuid4())
    idempotency_key = str(uuid.uuid4())
    command: dict[str, Any] = {
        "command_id": command_id,
        "command_type": "run.start",
        "tenant_id": req.tenant_id,
        "app_id": req.app_id,
        "session_key": req.session_key,
        "trace_id": trace_id,
        "idempotency_key": idempotency_key,
        "retry_policy": retry_policy,
        "ts": datetime.now(timezone.utc).isoformat(),
        "payload": _build_payload(
            req=req,
            claims=claims,
            command_id=command_id,
            trace_id=trace_id,
            scope_axis=scope_axis,
        ),
    }
    command.update(_resolve_scope_axis(req))
    command.update(_resolve_bound_contract_versions(req))
    return command


def _project_scope_axis_onto_event(
    *,
    event: dict[str, Any],
    command: Mapping[str, Any],
) -> None:
    scope_id = str(command.get("scope_id", "")).strip() or str(command.get("session_key", "")).strip()
    scope_type = str(command.get("scope_type", "")).strip() or _DEFAULT_SCOPE_TYPE
    if scope_id and not str(event.get("scope_id", "")).strip():
        event["scope_id"] = scope_id
    if scope_type and not str(event.get("scope_type", "")).strip():
        event["scope_type"] = scope_type


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


def _validate_template_capability_binding_payload(req: CreateRunRequest) -> None:
    raw_binding = req.payload.get("template_capability_binding")
    if raw_binding is None:
        return
    if not isinstance(raw_binding, dict):
        raise HTTPException(
            status_code=422,
            detail="template_capability_binding must be an object",
        )
    try:
        validate_template_capability_binding_contract(raw_binding)
    except ContractValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _build_validated_command(
    req: CreateRunRequest,
    trace_id: str,
    claims: Mapping[str, Any],
) -> dict[str, Any]:
    _validate_execution_context_payload(req)
    _validate_orchestration_payload(req)
    _validate_template_capability_binding_payload(req)
    command = _build_execution_command(req, trace_id, claims)
    _validate_bound_versions_in_active_pool(_bound_contract_versions_from_command(command))
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
    upstream_error_class: str,
    retry_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    detail: dict[str, Any] = {
        "message": message,
        "status_code": status_code,
        "downstream_event_type": downstream_event_type,
        "retryable": retryable,
        "upstream_error_class": upstream_error_class,
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

    # Fallback for flattened payloads that do not carry nested `failure` object.
    if "failure_code" not in metadata:
        failure_code = normalize_optional_code_term(payload.get("failure_code"))
        if failure_code is not None:
            metadata["failure_code"] = failure_code
    if "failure_classification" not in metadata:
        failure_classification = normalize_optional_code_term(payload.get("failure_classification"))
        if failure_classification is not None:
            metadata["failure_classification"] = failure_classification
    if "failure_message" not in metadata:
        failure_message = payload.get("failure_message")
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
        placement_audit = _filter_placement_audit(decision.get("placement_audit"))
        if isinstance(placement_audit, dict) and placement_audit:
            metadata["placement_audit"] = placement_audit

    # Fallback for flattened placement metadata when `decision` object is absent.
    if "placement_reason_code" not in metadata:
        placement_reason_code = normalize_optional_code_term(payload.get("placement_reason_code"))
        if placement_reason_code is not None:
            metadata["placement_reason_code"] = placement_reason_code
    if "placement_event_type" not in metadata:
        placement_event_type = payload.get("placement_event_type")
        if isinstance(placement_event_type, str) and placement_event_type.strip():
            metadata["placement_event_type"] = placement_event_type
    if "placement_resource_snapshot" not in metadata:
        snapshot = _filter_resource_snapshot(payload.get("placement_resource_snapshot"))
        if isinstance(snapshot, dict) and snapshot:
            metadata["placement_resource_snapshot"] = snapshot
    if "placement_audit" not in metadata:
        placement_audit = _filter_placement_audit(payload.get("placement_audit"))
        if isinstance(placement_audit, dict) and placement_audit:
            metadata["placement_audit"] = placement_audit

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


def _filter_placement_audit(raw_audit: Any) -> dict[str, Any] | None:
    if not isinstance(raw_audit, dict):
        return None
    audit: dict[str, Any] = {}
    candidate_device_count = raw_audit.get("candidate_device_count")
    if isinstance(candidate_device_count, int) and candidate_device_count >= 0:
        audit["candidate_device_count"] = candidate_device_count
    candidate_execution_sites = raw_audit.get("candidate_execution_sites")
    if isinstance(candidate_execution_sites, list):
        normalized_sites = [
            item.strip()
            for item in candidate_execution_sites
            if isinstance(item, str) and item.strip()
        ]
        if normalized_sites:
            audit["candidate_execution_sites"] = normalized_sites
    for key in (
        "selected_device_id",
        "selected_execution_site",
        "selected_region",
        "selected_node_pool",
    ):
        value = raw_audit.get(key)
        if isinstance(value, str) and value.strip():
            audit[key] = value.strip()
    fallback_applied = raw_audit.get("fallback_applied")
    if isinstance(fallback_applied, bool):
        audit["fallback_applied"] = fallback_applied
    fallback_reason_code = normalize_optional_code_term(raw_audit.get("fallback_reason_code"))
    if fallback_reason_code is not None:
        audit["fallback_reason_code"] = fallback_reason_code
    failure_domain = normalize_optional_code_term(raw_audit.get("failure_domain"))
    if failure_domain is not None:
        audit["failure_domain"] = failure_domain
    if not audit:
        return None
    return audit


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
    bound_contract_versions = _bound_contract_versions_from_command(command)
    try:
        return execution_client.submit_command(envelope=command, auth_token=delegated_token)
    except RuntimeExecutionClientError as exc:
        downstream_event_type = None
        bus_seq = None
        contract_version_violation = None
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
        effective_failure_classification = failure_classification
        upstream_error_class = resolve_upstream_error_class(
            message=str(exc),
            detail=exc.detail,
            status_code=effective_status_code,
            retryable=effective_retryable,
            failure_classification=effective_failure_classification,
        )
        detail: str | dict[str, Any] = build_upstream_error_detail(
            message=str(exc),
            status_code=effective_status_code,
            retryable=effective_retryable,
            failure_classification=failure_classification,
            detail=exc.detail,
            upstream_error_class=upstream_error_class,
            retry_policy=retry_policy_metadata.get("retry_policy")
            if isinstance(retry_policy_metadata.get("retry_policy"), dict)
            else None,
        )
        if isinstance(exc.response_body, dict) and _is_event_envelope_candidate(exc.response_body):
            try:
                _project_scope_axis_onto_event(
                    event=exc.response_body,
                    command=command,
                )
                _enforce_event_contract_versions(
                    event=exc.response_body,
                    bound_versions=bound_contract_versions,
                )
                validate_event_envelope(exc.response_body)
                downstream_event_type = str(exc.response_body.get("event_type", ""))
                bus_seq = publish_gateway_event(exc.response_body)
                route_failure_metadata = _extract_route_failure_metadata(exc.response_body)
                effective_retryable = _resolve_effective_retryable(
                    fallback_retryable=exc.retryable,
                    downstream_event=exc.response_body,
                )
                route_failure_classification = normalize_optional_code_term(
                    route_failure_metadata.get("failure_classification")
                )
                if route_failure_classification is not None:
                    effective_failure_classification = route_failure_classification
                upstream_error_class = resolve_upstream_error_class(
                    message=str(exc),
                    detail=exc.detail,
                    status_code=effective_status_code,
                    retryable=effective_retryable,
                    failure_classification=effective_failure_classification,
                )
                detail = _build_downstream_error_detail(
                    message=str(exc),
                    status_code=effective_status_code,
                    downstream_event=exc.response_body,
                    downstream_event_type=downstream_event_type,
                    bus_seq=bus_seq,
                    retryable=effective_retryable,
                    upstream_error_class=upstream_error_class,
                    retry_policy=retry_policy_metadata.get("retry_policy")
                    if isinstance(retry_policy_metadata.get("retry_policy"), dict)
                    else None,
                )
            except ValueError as version_exc:
                contract_version_violation = str(version_exc)
                downstream_event_type = None
                bus_seq = None
                effective_status_code = 502
                effective_retryable = False
                effective_failure_classification = "validation"
                upstream_error_class = "contract_drift"
                detail = {
                    "message": "downstream event contract-version drift",
                    "status_code": effective_status_code,
                    "retryable": effective_retryable,
                    "upstream_error_class": upstream_error_class,
                    "reason": contract_version_violation,
                }

        audit_metadata: dict[str, Any] = {
            "reason": str(exc),
            "status_code": effective_status_code,
            "downstream_event_type": downstream_event_type,
            "bus_seq": bus_seq,
            "retryable": effective_retryable,
            "failure_classification": effective_failure_classification,
            "upstream_error_class": upstream_error_class,
        }
        audit_metadata.update(retry_policy_metadata)
        audit_metadata.update(route_failure_metadata)
        if isinstance(contract_version_violation, str) and contract_version_violation:
            audit_metadata["contract_version_violation"] = contract_version_violation
        emit_audit_event(
            action="runs.dispatch",
            decision="deny",
            actor_id=actor_id,
            trace_id=trace_id,
            metadata=audit_metadata,
        )
        raise HTTPException(status_code=effective_status_code, detail=detail) from exc


def _is_event_envelope_candidate(payload: Mapping[str, Any]) -> bool:
    event_type = payload.get("event_type")
    event_payload = payload.get("payload")
    if not isinstance(event_type, str) or not event_type.strip():
        return False
    if not isinstance(event_payload, Mapping):
        return False
    return True


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
    command: Mapping[str, Any],
    *,
    actor_id: str,
    trace_id: str,
) -> None:
    try:
        _project_scope_axis_onto_event(
            event=execution_event,
            command=command,
        )
        _enforce_event_contract_versions(
            event=execution_event,
            bound_versions=_bound_contract_versions_from_command(command),
        )
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
    return CreateRunResponse(
        run_id=run_id,
        status=status,
        execution_ingress=execution_ingress_contract_payload(),
    )


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
    placement_audit = _filter_placement_audit(route.get("placement_audit"))
    if isinstance(placement_audit, dict) and placement_audit:
        metadata["placement_audit"] = placement_audit
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
    command = _build_validated_command(req, trace_id, claims)
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
        command,
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
