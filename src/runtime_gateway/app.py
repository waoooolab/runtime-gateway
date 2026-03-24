"""FastAPI app for runtime-gateway."""

from __future__ import annotations

import asyncio
import json
import os
import re
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, AsyncIterator

from fastapi import Depends, FastAPI, HTTPException, Header, Query, Request, WebSocket
from fastapi.responses import JSONResponse, StreamingResponse

from .api.schemas import TokenExchangeRequest, TokenExchangeResponse
from .auth.tokens import TokenError, issue_token, verify_token
from .audit.emitter import emit_audit_event, get_audit_events, read_audit_log
from .contracts import (
    ContractValidationError,
    validate_executor_profile_catalog_contract,
    validate_runtime_events_page_contract,
)
from .events.bus import InMemoryEventBus
from .events.durable import append_event_record, read_event_page
from .events.validation import validate_event_envelope
from .executor_profiles import list_executor_profiles
from .integration import RuntimeExecutionClient, RuntimeExecutionClientError
from .routes_capabilities import register_capability_routes
from .routes_runs import register_run_routes
from .security import (
    EVENT_SCOPE_READ,
    AuthContext,
    allowed_event_type,
    require_events_read_context,
    require_events_write_context,
    require_runs_read_context,
    scope_contains,
    validate_required_claims,
)
from .token_exchange_api import token_exchange_response
from .ws_events import handle_websocket_events

app = FastAPI(title="runtime-gateway", version="0.1.0")
_execution_client = RuntimeExecutionClient()
_event_bus = InMemoryEventBus()
_VALID_CONTROL_OUTCOMES = {"dispatch", "queue", "defer", "reject"}
_DIRECT_COMPLETION_ALIAS_EVENT_TYPE = "direct_completion.v1"
_DIRECT_AUDIT_ALIAS_EVENT_TYPE = "direct_audit.v1"
_DIRECT_COMPLETION_RUNTIME_EVENT_TYPE = "runtime.run.direct.completion"
_DIRECT_AUDIT_RUNTIME_EVENT_PREFIX = "runtime.run.direct.audit."
_DIRECT_AUDIT_EVENT_SUFFIX_SANITIZE = re.compile(r"[^a-z0-9_.-]+")
_PROBE_APP_ID_DEFAULT = "owa-runtime"
_PROBE_APP_ID_LEGACY_ALIASES = {
    "waoooolab-runtime": _PROBE_APP_ID_DEFAULT,
}


def _publish_gateway_event(event: dict[str, Any]) -> int | None:
    event_type = str(event.get("event_type", ""))
    if not allowed_event_type(event_type):
        return None
    bus_seq = _event_bus.publish(event)
    append_event_record(bus_seq=bus_seq, event=event)
    return bus_seq


def _sanitize_direct_audit_suffix(raw_suffix: str) -> str:
    normalized = _DIRECT_AUDIT_EVENT_SUFFIX_SANITIZE.sub("_", raw_suffix.strip().lower()).strip("._-")
    if not normalized:
        return "unknown"
    return normalized


def _normalize_direct_alias_envelope(
    envelope: dict[str, Any],
) -> tuple[dict[str, Any], str | None]:
    source_event_type = str(envelope.get("event_type", "")).strip()
    if not source_event_type or allowed_event_type(source_event_type):
        return envelope, None

    normalized_event_type = ""
    if source_event_type == _DIRECT_COMPLETION_ALIAS_EVENT_TYPE:
        normalized_event_type = _DIRECT_COMPLETION_RUNTIME_EVENT_TYPE
    elif source_event_type == _DIRECT_AUDIT_ALIAS_EVENT_TYPE:
        payload = envelope.get("payload")
        runtime_event_hint = ""
        payload_event_type = ""
        if isinstance(payload, dict):
            runtime_event_hint = str(payload.get("runtime_event_type", "")).strip()
            payload_event_type = str(payload.get("event_type", "")).strip()
        if runtime_event_hint.startswith(_DIRECT_AUDIT_RUNTIME_EVENT_PREFIX):
            normalized_event_type = runtime_event_hint
        else:
            normalized_event_type = (
                f"{_DIRECT_AUDIT_RUNTIME_EVENT_PREFIX}{_sanitize_direct_audit_suffix(payload_event_type)}"
            )
    if not normalized_event_type:
        return envelope, None

    normalized_envelope = dict(envelope)
    normalized_envelope["event_type"] = normalized_event_type
    payload = normalized_envelope.get("payload")
    if isinstance(payload, dict):
        normalized_payload = dict(payload)
        normalized_payload.setdefault("source_event_type", source_event_type)
        normalized_envelope["payload"] = normalized_payload
    return normalized_envelope, source_event_type


def _scope_filter_or_forbid(
    *,
    field: str,
    query_value: str | None,
    claim_value: str,
) -> str:
    raw_query = (query_value or "").strip()
    raw_claim = claim_value.strip()
    if not raw_claim:
        raise HTTPException(status_code=403, detail=f"{field} claim is required")
    if raw_query and raw_query != raw_claim:
        raise HTTPException(
            status_code=403,
            detail=f"{field} query must match token claim",
        )
    return raw_claim


def _optional_scope_filter_or_forbid(
    *,
    field: str,
    query_value: str | None,
    claim_value: str,
) -> str | None:
    raw_query = (query_value or "").strip()
    raw_claim = claim_value.strip()
    if raw_query and raw_claim and raw_query != raw_claim:
        raise HTTPException(
            status_code=403,
            detail=f"{field} query must match token claim",
        )
    if raw_query:
        return raw_query
    return None


def _claim_or_forbid(*, field: str, claims: dict[str, Any]) -> str:
    value = str(claims.get(field, "")).strip()
    if not value:
        raise HTTPException(status_code=403, detail=f"{field} claim is required")
    return value


def _recommended_poll_after_ms_for_recent_events(*, has_more: bool, item_count: int) -> int:
    if has_more:
        return 250
    if item_count > 0:
        return 1500
    return 5000


def _normalize_next_cursor(*, requested_cursor: int | None, resolved_next_cursor: int) -> int:
    normalized = max(0, int(resolved_next_cursor))
    if requested_cursor is None:
        return normalized
    return max(int(requested_cursor), normalized)


def _parse_optional_ts_or_raise(raw_ts: str | None, *, field_name: str) -> datetime | None:
    if raw_ts is None:
        return None
    normalized = raw_ts.strip()
    if not normalized:
        return None
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail=f"{field_name} must be valid ISO-8601 date-time",
        ) from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _parse_event_types_or_none(raw_event_types: str | None) -> set[str] | None:
    if raw_event_types is None:
        return None
    parsed = {item.strip() for item in raw_event_types.split(",") if item.strip()}
    if not parsed:
        return None
    return parsed


def _parse_source_or_raise(raw_source: str) -> str:
    normalized = raw_source.strip().lower()
    if normalized not in {"memory", "durable"}:
        raise HTTPException(status_code=422, detail="source must be one of: memory, durable")
    return normalized


def _resolve_events_read_auth_context(
    *,
    authorization: str | None,
    access_token: str | None,
) -> AuthContext:
    raw_access_token = (access_token or "").strip()
    if not raw_access_token:
        return require_events_read_context(authorization=authorization)
    try:
        claims = verify_token(raw_access_token, audience="runtime-gateway")
        validate_required_claims(claims)
    except TokenError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    if not scope_contains(claims, EVENT_SCOPE_READ):
        raise HTTPException(status_code=403, detail="missing required scope for event read")
    return AuthContext(claims=claims, subject_token=raw_access_token)


def _resolve_event_read_filters(
    *,
    auth_context: AuthContext,
    source: str,
    tenant_id: str | None,
    app_id: str | None,
    session_key: str | None,
    event_types: str | None,
    run_id: str | None,
    since_ts: str | None,
    until_ts: str | None,
) -> tuple[
    str,
    str,
    str | None,
    set[str] | None,
    str | None,
    str,
    datetime | None,
    datetime | None,
]:
    claims = auth_context.claims
    effective_tenant = _scope_filter_or_forbid(
        field="tenant_id",
        query_value=tenant_id,
        claim_value=str(claims.get("tenant_id", "")),
    )
    effective_app = _scope_filter_or_forbid(
        field="app_id",
        query_value=app_id,
        claim_value=str(claims.get("app_id", "")),
    )
    effective_session_key = _optional_scope_filter_or_forbid(
        field="session_key",
        query_value=session_key,
        claim_value=str(claims.get("session_key", "")),
    )
    parsed_types = _parse_event_types_or_none(event_types)
    source_value = _parse_source_or_raise(source)
    parsed_since_ts = _parse_optional_ts_or_raise(since_ts, field_name="since_ts")
    parsed_until_ts = _parse_optional_ts_or_raise(until_ts, field_name="until_ts")
    if (
        parsed_since_ts is not None
        and parsed_until_ts is not None
        and parsed_since_ts > parsed_until_ts
    ):
        raise HTTPException(status_code=422, detail="since_ts must be <= until_ts")
    run_filter = (run_id or "").strip() or None
    return (
        effective_tenant,
        effective_app,
        effective_session_key,
        parsed_types,
        run_filter,
        source_value,
        parsed_since_ts,
        parsed_until_ts,
    )


def _read_event_chunk(
    *,
    source: str,
    limit: int,
    tenant_id: str,
    app_id: str,
    session_key: str | None,
    event_types: set[str] | None,
    run_id: str | None,
    cursor: int | None,
    since_ts: datetime | None,
    until_ts: datetime | None,
) -> tuple[list[dict[str, Any]], bool, int, dict[str, Any]]:
    if source == "memory":
        if cursor is None:
            window = _event_bus.recent(
                limit=limit + 1,
                tenant_id=tenant_id or None,
                app_id=app_id or None,
                session_key=session_key,
                event_types=event_types,
                run_id=run_id,
                since_ts=since_ts,
                until_ts=until_ts,
            )
            has_more = len(window) > limit
            items = window[-limit:]
        else:
            window = _event_bus.since(
                cursor=cursor,
                tenant_id=tenant_id or None,
                app_id=app_id or None,
                session_key=session_key,
                event_types=event_types,
                run_id=run_id,
                since_ts=since_ts,
                until_ts=until_ts,
            )[: limit + 1]
            has_more = len(window) > limit
            items = window[:limit]
        next_cursor = cursor if cursor is not None else 0
        if items:
            next_cursor = int(items[-1]["bus_seq"])
        normalized_cursor = _normalize_next_cursor(
            requested_cursor=cursor,
            resolved_next_cursor=next_cursor,
        )
        return items, has_more, normalized_cursor, _event_bus.stats()

    durable_page = read_event_page(
        limit=limit,
        tenant_id=tenant_id or None,
        app_id=app_id or None,
        session_key=session_key,
        event_types=event_types,
        run_id=run_id,
        since_ts=since_ts,
        until_ts=until_ts,
        cursor=cursor,
    )
    normalized_cursor = _normalize_next_cursor(
        requested_cursor=cursor,
        resolved_next_cursor=int(durable_page["next_cursor"]),
    )
    return (
        list(durable_page["items"]),
        bool(durable_page["has_more"]),
        normalized_cursor,
        dict(durable_page["stats"]),
    )


def _sse_frame(
    *,
    payload: dict[str, Any],
    event: str,
    cursor: int | None = None,
    retry_ms: int | None = None,
) -> str:
    lines: list[str] = []
    if retry_ms is not None:
        lines.append(f"retry: {int(retry_ms)}")
    lines.append(f"event: {event}")
    if cursor is not None:
        lines.append(f"id: {int(cursor)}")
    body = json.dumps(payload, separators=(",", ":"))
    for chunk in body.splitlines() or [body]:
        lines.append(f"data: {chunk}")
    lines.append("")
    return "\n".join(lines) + "\n"


def _runtime_execution_health_target() -> str:
    candidate = str(getattr(_execution_client, "base_url", "")).strip()
    if not candidate:
        candidate = str(os.environ.get("RUNTIME_EXECUTION_BASE_URL", "http://localhost:8003")).strip()
    if not candidate:
        candidate = "http://localhost:8003"
    return f"{candidate.rstrip('/')}/healthz"


def _decode_json_object(raw: bytes) -> dict[str, Any] | None:
    if not raw:
        return None
    try:
        parsed = json.loads(raw.decode("utf-8", errors="replace"))
    except json.JSONDecodeError:
        return None
    if isinstance(parsed, dict):
        return parsed
    return None


def _env_truthy(name: str, *, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _canonical_probe_app_id(raw_app_id: str) -> str:
    normalized = raw_app_id.strip()
    if not normalized:
        return _PROBE_APP_ID_DEFAULT
    return _PROBE_APP_ID_LEGACY_ALIASES.get(normalized.lower(), normalized)


def _probe_runtime_execution_ready(*, timeout_seconds: float) -> tuple[bool, dict[str, Any]]:
    target = _runtime_execution_health_target()
    dependency: dict[str, Any] = {
        "name": "runtime-execution",
        "target": target,
    }
    request = urllib.request.Request(url=target, method="GET", headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            status = int(response.getcode())
            raw = response.read()
    except urllib.error.HTTPError as exc:
        response_payload = _decode_json_object(exc.read())
        dependency |= {
            "status": "down",
            "reason": "upstream_unhealthy",
            "http_status": int(exc.code),
        }
        if isinstance(response_payload, dict):
            upstream_status = response_payload.get("status")
            if isinstance(upstream_status, str) and upstream_status.strip():
                dependency["upstream_status"] = upstream_status
        return False, dependency
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
        dependency |= {
            "status": "down",
            "reason": "unreachable",
            "error": str(exc),
        }
        return False, dependency

    response_payload = _decode_json_object(raw)
    if status != 200:
        dependency |= {
            "status": "down",
            "reason": "upstream_unhealthy",
            "http_status": status,
        }
        if isinstance(response_payload, dict):
            upstream_status = response_payload.get("status")
            if isinstance(upstream_status, str) and upstream_status.strip():
                dependency["upstream_status"] = upstream_status
        return False, dependency

    dependency |= {
        "status": "ok",
        "http_status": status,
    }
    if isinstance(response_payload, dict):
        upstream_status = response_payload.get("status")
        if isinstance(upstream_status, str) and upstream_status.strip():
            dependency["upstream_status"] = upstream_status
    return True, dependency


def _build_runtime_gateway_readiness_payload(*, timeout_seconds: float) -> tuple[bool, dict[str, Any]]:
    ready, dependency = _probe_runtime_execution_ready(timeout_seconds=timeout_seconds)
    payload = {
        "status": "ok" if ready else "not_ready",
        "service": "runtime-gateway",
        "dependencies": [dependency],
    }
    return ready, payload


def _runtime_execution_probe_claims() -> dict[str, Any]:
    tenant_id = str(os.environ.get("RUNTIME_GATEWAY_PROBE_TENANT_ID", "dev-tenant")).strip() or "dev-tenant"
    raw_app_id = str(os.environ.get("RUNTIME_GATEWAY_PROBE_APP_ID", _PROBE_APP_ID_DEFAULT))
    app_id = _canonical_probe_app_id(raw_app_id)
    return {
        "aud": "runtime-execution",
        "sub": "runtime-gateway:runtime-usable-probe",
        "tenant_id": tenant_id,
        "app_id": app_id,
        "trace_id": f"trace-runtime-usable-{datetime.now(timezone.utc).timestamp()}",
        "scope": ["runs:read"],
        "token_use": "service",
    }


def _runtime_execution_probe_token() -> str:
    ttl_seconds = 30
    raw_ttl = os.environ.get("RUNTIME_GATEWAY_PROBE_TOKEN_TTL_SECONDS")
    if isinstance(raw_ttl, str) and raw_ttl.strip():
        try:
            ttl_seconds = max(5, min(300, int(raw_ttl.strip())))
        except ValueError:
            ttl_seconds = 30
    return issue_token(_runtime_execution_probe_claims(), ttl_seconds=ttl_seconds)


def _validate_worker_pool_contract_projection(payload: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    if not isinstance(payload.get("desired_workers"), int):
        failures.append("desired_workers_not_int")
    if not isinstance(payload.get("active_workers"), int):
        failures.append("active_workers_not_int")
    if not isinstance(payload.get("lifecycle_state"), str):
        failures.append("lifecycle_state_not_str")
    if not isinstance(payload.get("is_running"), bool):
        failures.append("is_running_not_bool")
    if not isinstance(payload.get("pool_health_state"), str):
        failures.append("pool_health_state_not_str")
    if not isinstance(payload.get("recommended_poll_after_ms"), int):
        failures.append("recommended_poll_after_ms_not_int")
    return failures


def _validate_backpressure_contract_projection(payload: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    control_counts = payload.get("last_control_counts")
    control_signal = payload.get("last_control_signal")
    control_outcome = payload.get("last_control_outcome")

    if not isinstance(control_counts, dict):
        failures.append("last_control_counts_not_object")
    else:
        for key in ("queue", "defer", "reject"):
            value = control_counts.get(key)
            if not isinstance(value, int) or value < 0:
                failures.append(f"last_control_counts_{key}_invalid")

    if not isinstance(control_signal, dict):
        failures.append("last_control_signal_not_object")
    else:
        for key in ("queue_pending", "defer_pending", "reject_pending"):
            if not isinstance(control_signal.get(key), bool):
                failures.append(f"last_control_signal_{key}_not_bool")

    if not isinstance(control_outcome, str) or control_outcome not in _VALID_CONTROL_OUTCOMES:
        failures.append("last_control_outcome_invalid")
    if not isinstance(payload.get("last_retry_deferred_total"), int):
        failures.append("last_retry_deferred_total_not_int")
    if not isinstance(payload.get("last_retry_rejected_total"), int):
        failures.append("last_retry_rejected_total_not_int")
    if not isinstance(payload.get("last_reject_pending_signal"), bool):
        failures.append("last_reject_pending_signal_not_bool")
    return failures


def _probe_runtime_execution_contract_signals(*, timeout_seconds: float) -> dict[str, Any]:
    result: dict[str, Any] = {
        "runtime_backpressure_contract_ok": None,
        "runtime_backpressure_contract_failures": None,
        "runtime_worker_pool_contract_ok": None,
        "runtime_worker_pool_contract_failures": None,
        "probe": {
            "source": "runtime-execution.worker-pool-status",
            "enabled": False,
            "status": "disabled",
            "ok": True,
            "error": "",
            "worker_pool_failure_details": [],
            "backpressure_failure_details": [],
        },
    }
    if not _env_truthy("RUNTIME_GATEWAY_RUNTIME_USABLE_ENABLE_CONTRACT_PROBE", default=False):
        return result
    result["probe"]["enabled"] = True
    try:
        auth_token = _runtime_execution_probe_token()
    except Exception as exc:  # pragma: no cover - token env misconfiguration path
        result["probe"]["status"] = "failed"
        result["probe"]["ok"] = False
        result["probe"]["error"] = f"token_issue_failed:{exc}"
        return result

    try:
        probe_client = RuntimeExecutionClient(
            base_url=_execution_client.base_url,
            require_https=_execution_client.require_https,
            timeout_seconds=max(0.2, min(5.0, float(timeout_seconds))),
        )
        worker_pool_payload = probe_client.worker_pool_status(auth_token=auth_token)
    except RuntimeExecutionClientError as exc:
        detail = str(exc)
        if isinstance(exc.status_code, int):
            detail = f"{detail} (status={exc.status_code})"
        result["probe"]["status"] = "failed"
        result["probe"]["ok"] = False
        result["probe"]["error"] = detail
        return result
    except Exception as exc:  # pragma: no cover - unexpected transport failure
        result["probe"]["status"] = "failed"
        result["probe"]["ok"] = False
        result["probe"]["error"] = f"runtime_execution_probe_failed:{exc}"
        return result

    worker_pool_failures = _validate_worker_pool_contract_projection(worker_pool_payload)
    backpressure_failures = _validate_backpressure_contract_projection(worker_pool_payload)
    result["runtime_worker_pool_contract_ok"] = len(worker_pool_failures) == 0
    result["runtime_worker_pool_contract_failures"] = len(worker_pool_failures)
    result["runtime_backpressure_contract_ok"] = len(backpressure_failures) == 0
    result["runtime_backpressure_contract_failures"] = len(backpressure_failures)
    result["probe"] = {
        "source": "runtime-execution.worker-pool-status",
        "enabled": True,
        "status": "passed" if len(worker_pool_failures) == 0 and len(backpressure_failures) == 0 else "failed",
        "ok": len(worker_pool_failures) == 0 and len(backpressure_failures) == 0,
        "error": "",
        "worker_pool_failure_details": worker_pool_failures[:8],
        "backpressure_failure_details": backpressure_failures[:8],
    }
    return result


def _resolve_contract_signal(
    *,
    ready: bool,
    raw_ok: Any,
    raw_failures: Any,
) -> tuple[bool, int]:
    if isinstance(raw_ok, bool) and isinstance(raw_failures, int) and raw_failures >= 0:
        return raw_ok, raw_failures
    return bool(ready), 0 if bool(ready) else 1


def _runtime_usable_contract_surface_signal(
    *,
    runtime_backpressure_contract_ok: bool,
    runtime_backpressure_contract_failures: int,
    runtime_worker_pool_contract_ok: bool,
    runtime_worker_pool_contract_failures: int,
    runtime_worker_pool_status_contract_ok: bool,
    runtime_worker_pool_status_contract_failures: int,
) -> tuple[bool, int]:
    usable_surface_ok = bool(
        runtime_backpressure_contract_ok
        and runtime_backpressure_contract_failures == 0
        and runtime_worker_pool_contract_ok
        and runtime_worker_pool_contract_failures == 0
        and runtime_worker_pool_status_contract_ok == runtime_worker_pool_contract_ok
        and runtime_worker_pool_status_contract_failures == runtime_worker_pool_contract_failures
    )
    return usable_surface_ok, 0 if usable_surface_ok else 1


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok", "service": "runtime-gateway"}


@app.get("/readyz", response_model=None)
def readyz() -> Any:
    timeout_seconds = float(os.environ.get("RUNTIME_GATEWAY_READY_TIMEOUT_SECONDS", "1.5"))
    ready, payload = _build_runtime_gateway_readiness_payload(timeout_seconds=timeout_seconds)
    if not ready:
        return JSONResponse(status_code=503, content=payload)
    return payload


@app.get("/v1/runtime/usable", response_model=None)
def runtime_usable() -> Any:
    timeout_seconds = float(os.environ.get("RUNTIME_GATEWAY_READY_TIMEOUT_SECONDS", "1.5"))
    ready, readiness_payload = _build_runtime_gateway_readiness_payload(timeout_seconds=timeout_seconds)
    contract_probe = _probe_runtime_execution_contract_signals(timeout_seconds=timeout_seconds)
    runtime_worker_pool_contract_ok, runtime_worker_pool_contract_failures = _resolve_contract_signal(
        ready=ready,
        raw_ok=contract_probe.get("runtime_worker_pool_contract_ok"),
        raw_failures=contract_probe.get("runtime_worker_pool_contract_failures"),
    )
    runtime_backpressure_contract_ok, runtime_backpressure_contract_failures = _resolve_contract_signal(
        ready=ready,
        raw_ok=contract_probe.get("runtime_backpressure_contract_ok"),
        raw_failures=contract_probe.get("runtime_backpressure_contract_failures"),
    )
    runtime_usable_contract_surface_ok, runtime_usable_contract_surface_failures = (
        _runtime_usable_contract_surface_signal(
            runtime_backpressure_contract_ok=runtime_backpressure_contract_ok,
            runtime_backpressure_contract_failures=runtime_backpressure_contract_failures,
            runtime_worker_pool_contract_ok=runtime_worker_pool_contract_ok,
            runtime_worker_pool_contract_failures=runtime_worker_pool_contract_failures,
            runtime_worker_pool_status_contract_ok=runtime_worker_pool_contract_ok,
            runtime_worker_pool_status_contract_failures=runtime_worker_pool_contract_failures,
        )
    )
    response_payload = {
        "schema_version": "runtime_gateway_usable.v1",
        "ok": bool(ready),
        "status": str(readiness_payload.get("status", "not_ready")),
        "service": "runtime-gateway",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dependencies": readiness_payload.get("dependencies", []),
        "event_bus": _event_bus.stats(),
        "recommended_poll_after_ms": 10000 if ready else 1000,
        "runtime_backpressure_contract_ok": runtime_backpressure_contract_ok,
        "runtime_backpressure_contract_failures": runtime_backpressure_contract_failures,
        "runtime_worker_pool_contract_ok": runtime_worker_pool_contract_ok,
        "runtime_worker_pool_contract_failures": runtime_worker_pool_contract_failures,
        "runtime_worker_pool_status_contract_ok": runtime_worker_pool_contract_ok,
        "runtime_worker_pool_status_contract_failures": runtime_worker_pool_contract_failures,
        "runtime_usable_contract_surface_ok": runtime_usable_contract_surface_ok,
        "runtime_usable_contract_surface_failures": runtime_usable_contract_surface_failures,
        "compatibility_aliases": {
            "runtime_worker_pool_status_contract": "runtime_worker_pool_contract"
        },
        "runtime_contract_probe": contract_probe.get("probe", {}),
    }
    if not ready:
        return JSONResponse(status_code=503, content=response_payload)
    return response_payload


@app.get("/v1/audit/events")
def list_audit_events(limit: int = 50, source: str = "memory") -> dict:
    if source == "durable":
        return {"items": read_audit_log(limit)}
    return {"items": get_audit_events(limit)}


@app.get("/v1/events/recent")
def list_recent_events(
    limit: int = Query(default=50, ge=1, le=500),
    source: str = Query(default="memory"),
    tenant_id: str | None = None,
    app_id: str | None = None,
    session_key: str | None = None,
    event_types: str | None = None,
    run_id: str | None = None,
    cursor: int | None = Query(default=None, ge=0),
    since_ts: str | None = Query(default=None),
    until_ts: str | None = Query(default=None),
    auth_context: AuthContext = Depends(require_events_read_context),
) -> dict:
    (
        effective_tenant,
        effective_app,
        effective_session_key,
        parsed_types,
        run_filter,
        source_value,
        parsed_since_ts,
        parsed_until_ts,
    ) = _resolve_event_read_filters(
        auth_context=auth_context,
        source=source,
        tenant_id=tenant_id,
        app_id=app_id,
        session_key=session_key,
        event_types=event_types,
        run_id=run_id,
        since_ts=since_ts,
        until_ts=until_ts,
    )
    items, has_more, next_cursor, stats = _read_event_chunk(
        source=source_value,
        limit=limit,
        tenant_id=effective_tenant,
        app_id=effective_app,
        session_key=effective_session_key,
        event_types=parsed_types,
        run_id=run_filter,
        cursor=cursor,
        since_ts=parsed_since_ts,
        until_ts=parsed_until_ts,
    )
    recommended_poll_after_ms = _recommended_poll_after_ms_for_recent_events(
        has_more=has_more,
        item_count=len(items),
    )
    response_payload = {
        "items": items,
        "next_cursor": next_cursor,
        "has_more": has_more,
        "recommended_poll_after_ms": recommended_poll_after_ms,
        "stats": stats,
        "source": source_value,
    }
    try:
        validate_runtime_events_page_contract(response_payload)
    except ContractValidationError as exc:
        raise HTTPException(status_code=500, detail=f"invalid recent events response: {exc}") from exc
    return response_payload


@app.get("/v1/events/sse")
async def stream_events_sse(
    request: Request,
    limit: int = Query(default=200, ge=1, le=500),
    source: str = Query(default="memory"),
    tenant_id: str | None = None,
    app_id: str | None = None,
    session_key: str | None = None,
    event_types: str | None = None,
    run_id: str | None = None,
    cursor: int = Query(default=0, ge=0),
    since_ts: str | None = Query(default=None),
    until_ts: str | None = Query(default=None),
    follow: bool = Query(default=True),
    poll_interval_ms: int = Query(default=1000, ge=200, le=10000),
    access_token: str | None = Query(default=None),
    authorization: str | None = Header(default=None),
) -> StreamingResponse:
    auth_context = _resolve_events_read_auth_context(
        authorization=authorization,
        access_token=access_token,
    )
    (
        effective_tenant,
        effective_app,
        effective_session_key,
        parsed_types,
        run_filter,
        source_value,
        parsed_since_ts,
        parsed_until_ts,
    ) = _resolve_event_read_filters(
        auth_context=auth_context,
        source=source,
        tenant_id=tenant_id,
        app_id=app_id,
        session_key=session_key,
        event_types=event_types,
        run_id=run_id,
        since_ts=since_ts,
        until_ts=until_ts,
    )
    retry_ms = max(500, int(poll_interval_ms))

    async def _stream() -> AsyncIterator[str]:
        stream_source = source_value
        next_cursor = max(0, int(cursor))
        ready_payload: dict[str, Any] = {
            "kind": "sse.ready",
            "cursor": next_cursor,
            "source": stream_source,
            "tenant_id": effective_tenant,
            "app_id": effective_app,
            "follow": bool(follow),
            "stats": _event_bus.stats(),
        }
        if effective_session_key is not None:
            ready_payload["session_key"] = effective_session_key
        if run_filter is not None:
            ready_payload["run_id"] = run_filter
        if parsed_since_ts is not None:
            ready_payload["since_ts"] = parsed_since_ts.isoformat()
        if parsed_until_ts is not None:
            ready_payload["until_ts"] = parsed_until_ts.isoformat()
        if parsed_types is not None:
            ready_payload["event_types"] = sorted(parsed_types)
        yield _sse_frame(
            payload=ready_payload,
            event="ready",
            cursor=next_cursor,
            retry_ms=retry_ms,
        )

        while True:
            if await request.is_disconnected():
                return
            items, has_more, chunk_cursor, stats = _read_event_chunk(
                source=stream_source,
                limit=limit,
                tenant_id=effective_tenant,
                app_id=effective_app,
                session_key=effective_session_key,
                event_types=parsed_types,
                run_id=run_filter,
                cursor=next_cursor,
                since_ts=parsed_since_ts,
                until_ts=parsed_until_ts,
            )
            next_cursor = max(next_cursor, int(chunk_cursor))
            for item in items:
                item_cursor = next_cursor
                bus_seq = item.get("bus_seq")
                if isinstance(bus_seq, int):
                    item_cursor = max(next_cursor, bus_seq)
                    next_cursor = item_cursor
                event_name = str(item.get("event", {}).get("event_type", "")).strip() or "message"
                yield _sse_frame(
                    payload=item,
                    event=event_name,
                    cursor=item_cursor,
                )

            if has_more:
                continue

            if stream_source == "durable":
                live_cursor = max(0, int(_event_bus.stats().get("next_seq", 1)) - 1)
                next_cursor = max(next_cursor, live_cursor)
                stream_source = "memory"
                continue

            if not follow:
                done_payload = {
                    "kind": "sse.complete",
                    "cursor": next_cursor,
                    "source": stream_source,
                    "stats": stats,
                }
                yield _sse_frame(
                    payload=done_payload,
                    event="complete",
                    cursor=next_cursor,
                )
                return

            keepalive_payload = {
                "kind": "sse.keepalive",
                "cursor": next_cursor,
                "source": stream_source,
            }
            yield _sse_frame(
                payload=keepalive_payload,
                event="keepalive",
                cursor=next_cursor,
                retry_ms=retry_ms,
            )
            await asyncio.sleep(retry_ms / 1000)

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/v1/events/publish")
def publish_event(
    envelope: dict[str, Any],
    auth_context: AuthContext = Depends(require_events_write_context),
) -> dict[str, Any]:
    claims = auth_context.claims
    try:
        validate_event_envelope(envelope)
    except ValueError as exc:
        emit_audit_event(
            action="events.publish",
            decision="deny",
            actor_id=str(auth_context.claims.get("sub", "unknown")),
            trace_id=str(auth_context.claims.get("trace_id", "")),
            metadata={"reason": f"invalid event envelope: {exc}"},
        )
        raise HTTPException(status_code=422, detail=f"invalid event envelope: {exc}") from exc

    claim_tenant = _claim_or_forbid(field="tenant_id", claims=claims)
    claim_app = _claim_or_forbid(field="app_id", claims=claims)
    event_tenant = str(envelope.get("tenant_id", "")).strip()
    event_app = str(envelope.get("app_id", "")).strip()
    if event_tenant != claim_tenant:
        emit_audit_event(
            action="events.publish",
            decision="deny",
            actor_id=str(claims.get("sub", "unknown")),
            trace_id=str(claims.get("trace_id", "")),
            metadata={"reason": "tenant_id must match token claim"},
        )
        raise HTTPException(status_code=403, detail="tenant_id must match token claim")
    if event_app != claim_app:
        emit_audit_event(
            action="events.publish",
            decision="deny",
            actor_id=str(claims.get("sub", "unknown")),
            trace_id=str(claims.get("trace_id", "")),
            metadata={"reason": "app_id must match token claim"},
        )
        raise HTTPException(status_code=403, detail="app_id must match token claim")

    normalized_envelope, original_event_type = _normalize_direct_alias_envelope(envelope)

    event_type = str(normalized_envelope.get("event_type", ""))
    if not allowed_event_type(event_type):
        raise HTTPException(status_code=422, detail=f"event type not publishable: {event_type}")

    bus_seq = _publish_gateway_event(normalized_envelope)
    audit_metadata = {
        "event_type": event_type,
        "event_id": normalized_envelope.get("event_id"),
        "bus_seq": bus_seq,
    }
    if original_event_type is not None:
        audit_metadata["source_event_type"] = original_event_type
    emit_audit_event(
        action="events.publish",
        decision="allow",
        actor_id=str(auth_context.claims.get("sub", "unknown")),
        trace_id=str(auth_context.claims.get("trace_id", "")),
        metadata=audit_metadata,
    )
    response_payload = {
        "accepted": True,
        "bus_seq": bus_seq,
        "event_id": normalized_envelope.get("event_id"),
        "event_type": event_type,
    }
    if original_event_type is not None:
        response_payload["original_event_type"] = original_event_type
    return response_payload


@app.websocket("/v1/ws/events")
async def websocket_events(websocket: WebSocket) -> None:
    await handle_websocket_events(websocket, _event_bus)


@app.post("/v1/auth/token/exchange", response_model=TokenExchangeResponse)
def token_exchange(req: TokenExchangeRequest) -> TokenExchangeResponse:
    return token_exchange_response(req)


@app.get("/v1/executors/profiles")
def get_executor_profiles(
    auth_context: AuthContext = Depends(require_runs_read_context),
) -> dict[str, Any]:
    _ = auth_context
    payload = {
        "items": list_executor_profiles(),
    }
    try:
        validate_executor_profile_catalog_contract(payload)
    except ContractValidationError as exc:
        raise HTTPException(status_code=500, detail=f"invalid executor profile catalog: {exc}") from exc
    return payload


register_run_routes(
    app=app,
    get_execution_client=lambda: _execution_client,
    publish_gateway_event=_publish_gateway_event,
)

register_capability_routes(
    app=app,
    get_execution_client=lambda: _execution_client,
    publish_gateway_event=_publish_gateway_event,
)
