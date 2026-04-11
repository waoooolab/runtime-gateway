"""Run and worker route registration for runtime-gateway API."""

from __future__ import annotations

import os
from typing import Any, Callable

from fastapi import Depends, FastAPI, HTTPException, Header

from .api.schemas import CreateRunRequest, CreateRunResponse
from .audit.emitter import emit_audit_event
from .error_budget_policy import error_budget_level_from_saturation, parse_reason_codes_query
from .execution_ingress_contract import (
    RUN_SUBMIT_SURFACE,
    SCHEDULER_CANCEL_SURFACE,
    SCHEDULER_ENQUEUE_SURFACE,
    SCHEDULER_HEALTH_SURFACE,
    SCHEDULER_REGISTRY_SURFACE,
    SCHEDULER_TICK_SURFACE,
)
from .integration import RuntimeExecutionClient
from .run_approval import dispatch_approve_run, dispatch_reject_run
from .run_control import (
    dispatch_cancel_run,
    dispatch_complete_run,
    dispatch_preempt_run,
    dispatch_renew_run_lease,
    dispatch_timeout_run,
)
from .run_dispatch import dispatch_create_run
from .run_lease import dispatch_get_run_lease
from .run_scheduler import (
    dispatch_scheduler_cancel,
    dispatch_scheduler_enqueue,
    dispatch_scheduler_health,
    dispatch_scheduler_registry,
    dispatch_scheduler_tick,
)
from .run_status import dispatch_get_run_status
from .run_worker import (
    dispatch_worker_drain,
    dispatch_worker_health,
    dispatch_worker_loop,
    dispatch_worker_restart,
    dispatch_worker_start,
    dispatch_worker_status,
    dispatch_worker_stop,
    dispatch_worker_tick,
)
from .security import AuthContext, require_runs_read_context, require_runs_write_context

_ERROR_BUDGET_ACTIONS = {
    "green": "allow_new_dispatch",
    "yellow": "defer_new_dispatch",
    "red": "pause_new_dispatch",
}
_ERROR_BUDGET_REASON_CODE_RED = "error_budget_red_dispatch_paused"


def _normalize_error_budget_level(value: str | None) -> str | None:
    token = str(value or "").strip().lower()
    if not token:
        return None
    if token in _ERROR_BUDGET_ACTIONS:
        return token
    return error_budget_level_from_saturation(token)


def _resolve_runtime_dispatch_error_budget_decision(
    *,
    level_header: str | None,
    action_header: str | None,
    reason_codes_header: str | None,
) -> dict[str, Any]:
    level = _normalize_error_budget_level(level_header)
    if level is None:
        level = _normalize_error_budget_level(os.environ.get("RUNTIME_GATEWAY_ERROR_BUDGET_FORCE_LEVEL"))
    if level is None:
        level = "green"

    action = str(action_header or "").strip()
    if action not in set(_ERROR_BUDGET_ACTIONS.values()):
        action = _ERROR_BUDGET_ACTIONS[level]
    return {
        "level": level,
        "action": action,
        "reason_codes": parse_reason_codes_query(reason_codes_header),
    }


def _enforce_runtime_dispatch_error_budget_gate_or_raise(
    *,
    claims: dict[str, Any],
    gate_action: str,
    resource: str,
    level_header: str | None,
    action_header: str | None,
    reason_codes_header: str | None,
) -> dict[str, Any]:
    decision = _resolve_runtime_dispatch_error_budget_decision(
        level_header=level_header,
        action_header=action_header,
        reason_codes_header=reason_codes_header,
    )
    level = str(decision.get("level", "green")).strip().lower()
    metadata = {
        "error_budget_level": level,
        "error_budget_action": str(decision.get("action", _ERROR_BUDGET_ACTIONS["green"])),
        "error_budget_reason_codes": list(decision.get("reason_codes", [])),
    }
    actor_id = str(claims.get("sub", "unknown"))
    trace_id = str(claims.get("trace_id", ""))
    if level == "red":
        metadata["error_budget_reason_code"] = _ERROR_BUDGET_REASON_CODE_RED
        emit_audit_event(
            action=gate_action,
            decision="deny",
            actor_id=actor_id,
            trace_id=trace_id,
            resource=resource,
            metadata=metadata,
        )
        raise HTTPException(
            status_code=503,
            detail={
                "code": _ERROR_BUDGET_REASON_CODE_RED,
                "message": "dispatch paused by error-budget policy",
                "error_budget_level": "red",
                "error_budget_action": metadata["error_budget_action"],
                "error_budget_reason_codes": metadata["error_budget_reason_codes"],
            },
        )
    emit_audit_event(
        action=gate_action,
        decision="allow",
        actor_id=actor_id,
        trace_id=trace_id,
        resource=resource,
        metadata=metadata,
    )
    return decision


def _require_non_empty_string(payload: dict[str, Any], key: str) -> str:
    raw = payload.get(key)
    if not isinstance(raw, str) or not raw.strip():
        raise HTTPException(status_code=422, detail=f"{key} is required")
    return raw.strip()


def _optional_non_empty_string(payload: dict[str, Any], key: str) -> str | None:
    raw = payload.get(key)
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw.strip():
        raise HTTPException(status_code=422, detail=f"{key} must be non-empty string when provided")
    return raw.strip()


def _optional_non_negative_int(payload: dict[str, Any], key: str) -> int | None:
    raw = payload.get(key)
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
        raise HTTPException(status_code=422, detail=f"{key} must be integer >= 0")
    return raw


def _optional_positive_int_at_least(payload: dict[str, Any], key: str, minimum: int) -> int | None:
    value = _optional_non_negative_int(payload, key)
    if value is None:
        return None
    if value < minimum:
        raise HTTPException(status_code=422, detail=f"{key} must be integer >= {minimum}")
    return value


def _register_run_create_route(
    *,
    app: FastAPI,
    get_execution_client: Callable[[], RuntimeExecutionClient],
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
) -> None:
    @app.post(RUN_SUBMIT_SURFACE, response_model=CreateRunResponse)
    def create_run(
        req: CreateRunRequest,
        auth_context: AuthContext = Depends(require_runs_write_context),
        error_budget_level: str | None = Header(default=None, alias="X-OWA-Error-Budget-Level"),
        error_budget_action: str | None = Header(default=None, alias="X-OWA-Error-Budget-Action"),
        error_budget_reason_codes: str | None = Header(default=None, alias="X-OWA-Error-Budget-Reason-Codes"),
    ) -> CreateRunResponse:
        _enforce_runtime_dispatch_error_budget_gate_or_raise(
            claims=auth_context.claims,
            gate_action="runs.create.error_budget_gate",
            resource=RUN_SUBMIT_SURFACE,
            level_header=error_budget_level,
            action_header=error_budget_action,
            reason_codes_header=error_budget_reason_codes,
        )
        return dispatch_create_run(
            req=req,
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
            publish_gateway_event=publish_gateway_event,
        )


def _register_run_approval_routes(
    *,
    app: FastAPI,
    get_execution_client: Callable[[], RuntimeExecutionClient],
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
) -> None:
    @app.post("/v1/runs/{run_id}:approve")
    def approve_run(
        run_id: str,
        auth_context: AuthContext = Depends(require_runs_write_context),
    ) -> dict[str, Any]:
        return dispatch_approve_run(
            run_id=run_id,
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
            publish_gateway_event=publish_gateway_event,
        )

    @app.post("/v1/runs/{run_id}:reject")
    def reject_run(
        run_id: str,
        auth_context: AuthContext = Depends(require_runs_write_context),
    ) -> dict[str, Any]:
        return dispatch_reject_run(
            run_id=run_id,
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
            publish_gateway_event=publish_gateway_event,
        )


def _register_run_control_routes(
    *,
    app: FastAPI,
    get_execution_client: Callable[[], RuntimeExecutionClient],
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
) -> None:
    @app.post("/v1/runs/{run_id}:cancel")
    def cancel_run(
        run_id: str,
        body: dict[str, Any] | None = None,
        auth_context: AuthContext = Depends(require_runs_write_context),
    ) -> dict[str, Any]:
        return dispatch_cancel_run(
            run_id=run_id,
            body=body,
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
            publish_gateway_event=publish_gateway_event,
        )

    @app.post("/v1/runs/{run_id}:timeout")
    def timeout_run(
        run_id: str,
        body: dict[str, Any] | None = None,
        auth_context: AuthContext = Depends(require_runs_write_context),
    ) -> dict[str, Any]:
        return dispatch_timeout_run(
            run_id=run_id,
            body=body,
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
            publish_gateway_event=publish_gateway_event,
        )

    @app.post("/v1/runs/{run_id}:preempt")
    def preempt_run(
        run_id: str,
        body: dict[str, Any] | None = None,
        auth_context: AuthContext = Depends(require_runs_write_context),
    ) -> dict[str, Any]:
        return dispatch_preempt_run(
            run_id=run_id,
            body=body,
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
            publish_gateway_event=publish_gateway_event,
        )

    @app.post("/v1/runs/{run_id}:complete")
    def complete_run(
        run_id: str,
        body: dict[str, Any] | None = None,
        auth_context: AuthContext = Depends(require_runs_write_context),
    ) -> dict[str, Any]:
        return dispatch_complete_run(
            run_id=run_id,
            body=body,
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
            publish_gateway_event=publish_gateway_event,
        )

    @app.post("/v1/runs/{run_id}:lease-renew")
    def renew_run_lease(
        run_id: str,
        body: dict[str, Any] | None = None,
        auth_context: AuthContext = Depends(require_runs_write_context),
    ) -> dict[str, Any]:
        return dispatch_renew_run_lease(
            run_id=run_id,
            body=body,
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
            publish_gateway_event=publish_gateway_event,
        )


def _register_run_mutation_routes(
    *,
    app: FastAPI,
    get_execution_client: Callable[[], RuntimeExecutionClient],
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
) -> None:
    _register_run_create_route(
        app=app,
        get_execution_client=get_execution_client,
        publish_gateway_event=publish_gateway_event,
    )
    _register_run_approval_routes(
        app=app,
        get_execution_client=get_execution_client,
        publish_gateway_event=publish_gateway_event,
    )
    _register_run_control_routes(
        app=app,
        get_execution_client=get_execution_client,
        publish_gateway_event=publish_gateway_event,
    )


def _register_run_query_routes(
    *,
    app: FastAPI,
    get_execution_client: Callable[[], RuntimeExecutionClient],
) -> None:
    @app.get("/v1/runs/{run_id}")
    def get_run_status(
        run_id: str,
        auth_context: AuthContext = Depends(require_runs_read_context),
    ) -> dict[str, Any]:
        return dispatch_get_run_status(
            run_id=run_id,
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
        )

    @app.get("/v1/runs/{run_id}/lease")
    def get_run_lease(
        run_id: str,
        auth_context: AuthContext = Depends(require_runs_read_context),
    ) -> dict[str, Any]:
        return dispatch_get_run_lease(
            run_id=run_id,
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
        )


def _register_contract_retirement_routes(
    *,
    app: FastAPI,
    get_execution_client: Callable[[], RuntimeExecutionClient],
) -> None:
    @app.get("/v1/contracts/retirement:status")
    def contract_retirement_status(
        version_type: str | None = None,
        version: str | None = None,
        auth_context: AuthContext = Depends(require_runs_read_context),
    ) -> dict[str, Any]:
        return get_execution_client().contract_retirement_status(
            auth_token=auth_context.subject_token,
            version_type=version_type,
            version=version,
        )

    @app.post("/v1/contracts/retirement:validate")
    def contract_retirement_validate(
        body: dict[str, Any] | None = None,
        auth_context: AuthContext = Depends(require_runs_read_context),
    ) -> dict[str, Any]:
        return get_execution_client().contract_retirement_validate(
            auth_token=auth_context.subject_token,
            body=body,
        )


def _register_worker_routes(
    *,
    app: FastAPI,
    get_execution_client: Callable[[], RuntimeExecutionClient],
) -> None:
    @app.post("/v1/orchestration/worker:tick")
    def worker_tick(
        fair: bool = True,
        auto_start: bool = True,
        auth_context: AuthContext = Depends(require_runs_write_context),
    ) -> dict[str, Any]:
        return dispatch_worker_tick(
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
            fair=fair,
            auto_start=auto_start,
        )

    @app.post("/v1/orchestration/worker:drain")
    def worker_drain(
        max_items: int = 16,
        fair: bool = True,
        auto_start: bool = True,
        auth_context: AuthContext = Depends(require_runs_write_context),
    ) -> dict[str, Any]:
        return dispatch_worker_drain(
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
            max_items=max_items,
            fair=fair,
            auto_start=auto_start,
        )

    @app.post("/v1/orchestration/worker:loop")
    def worker_loop(
        scheduler_max_items: int = 32,
        scheduler_fair: bool = True,
        worker_max_items: int = 16,
        worker_fair: bool = True,
        auto_start: bool = True,
        auth_context: AuthContext = Depends(require_runs_write_context),
    ) -> dict[str, Any]:
        return dispatch_worker_loop(
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
            scheduler_max_items=scheduler_max_items,
            scheduler_fair=scheduler_fair,
            worker_max_items=worker_max_items,
            worker_fair=worker_fair,
            auto_start=auto_start,
        )

    @app.get("/v1/orchestration/worker:health")
    def worker_health(
        auth_context: AuthContext = Depends(require_runs_read_context),
    ) -> dict[str, Any]:
        return dispatch_worker_health(
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
        )

    @app.post("/v1/orchestration/worker:start")
    def worker_start(
        body: dict[str, Any] | None = None,
        auth_context: AuthContext = Depends(require_runs_write_context),
    ) -> dict[str, Any]:
        reason = str(body.get("reason", "")).strip() if isinstance(body, dict) else ""
        return dispatch_worker_start(
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
            reason=reason or None,
        )

    @app.post("/v1/orchestration/worker:stop")
    def worker_stop(
        body: dict[str, Any] | None = None,
        auth_context: AuthContext = Depends(require_runs_write_context),
    ) -> dict[str, Any]:
        reason = str(body.get("reason", "")).strip() if isinstance(body, dict) else ""
        return dispatch_worker_stop(
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
            reason=reason or None,
        )

    @app.post("/v1/orchestration/worker:restart")
    def worker_restart(
        body: dict[str, Any] | None = None,
        auth_context: AuthContext = Depends(require_runs_write_context),
    ) -> dict[str, Any]:
        reason = str(body.get("reason", "")).strip() if isinstance(body, dict) else ""
        return dispatch_worker_restart(
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
            reason=reason or None,
        )

    @app.get("/v1/orchestration/worker:status")
    def worker_status(
        auth_context: AuthContext = Depends(require_runs_read_context),
    ) -> dict[str, Any]:
        return dispatch_worker_status(
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
        )


def _register_scheduler_routes(
    *,
    app: FastAPI,
    get_execution_client: Callable[[], RuntimeExecutionClient],
) -> None:
    @app.post(SCHEDULER_ENQUEUE_SURFACE)
    def scheduler_enqueue(
        body: dict[str, Any] | None = None,
        auth_context: AuthContext = Depends(require_runs_write_context),
        error_budget_level: str | None = Header(default=None, alias="X-OWA-Error-Budget-Level"),
        error_budget_action: str | None = Header(default=None, alias="X-OWA-Error-Budget-Action"),
        error_budget_reason_codes: str | None = Header(default=None, alias="X-OWA-Error-Budget-Reason-Codes"),
    ) -> dict[str, Any]:
        payload = body or {}
        run_id = _require_non_empty_string(payload, "run_id")
        due_at = _optional_non_empty_string(payload, "due_at")
        delay_ms = _optional_non_negative_int(payload, "delay_ms")
        if due_at is not None and delay_ms is not None:
            raise HTTPException(status_code=422, detail="due_at and delay_ms are mutually exclusive")
        reason = _optional_non_empty_string(payload, "reason")
        misfire_policy = _optional_non_empty_string(payload, "misfire_policy")
        if misfire_policy is not None:
            normalized_policy = misfire_policy.lower()
            if normalized_policy not in {"run", "skip"}:
                raise HTTPException(status_code=422, detail="misfire_policy must be 'run' or 'skip'")
            misfire_policy = normalized_policy
        _enforce_runtime_dispatch_error_budget_gate_or_raise(
            claims=auth_context.claims,
            gate_action="scheduler.enqueue.error_budget_gate",
            resource=SCHEDULER_ENQUEUE_SURFACE,
            level_header=error_budget_level,
            action_header=error_budget_action,
            reason_codes_header=error_budget_reason_codes,
        )
        misfire_grace_ms = _optional_non_negative_int(payload, "misfire_grace_ms")
        cron_interval_ms = _optional_positive_int_at_least(payload, "cron_interval_ms", 100)
        return dispatch_scheduler_enqueue(
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
            run_id=run_id,
            due_at=due_at or None,
            delay_ms=delay_ms,
            reason=reason or None,
            misfire_policy=misfire_policy or None,
            misfire_grace_ms=misfire_grace_ms,
            cron_interval_ms=cron_interval_ms,
        )

    @app.post(SCHEDULER_TICK_SURFACE)
    def scheduler_tick(
        max_items: int = 32,
        fair: bool = True,
        auth_context: AuthContext = Depends(require_runs_write_context),
    ) -> dict[str, Any]:
        return dispatch_scheduler_tick(
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
            max_items=max_items,
            fair=fair,
        )

    @app.get(SCHEDULER_HEALTH_SURFACE)
    def scheduler_health(
        auth_context: AuthContext = Depends(require_runs_read_context),
    ) -> dict[str, Any]:
        return dispatch_scheduler_health(
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
        )

    @app.get(SCHEDULER_REGISTRY_SURFACE)
    def scheduler_registry(
        limit: int = 100,
        cursor: int = 0,
        run_id: str | None = None,
        auth_context: AuthContext = Depends(require_runs_read_context),
    ) -> dict[str, Any]:
        return dispatch_scheduler_registry(
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
            limit=limit,
            cursor=cursor,
            run_id=run_id,
        )

    @app.post(SCHEDULER_CANCEL_SURFACE)
    def scheduler_cancel(
        body: dict[str, Any] | None = None,
        auth_context: AuthContext = Depends(require_runs_write_context),
    ) -> dict[str, Any]:
        payload = body or {}
        run_id = _require_non_empty_string(payload, "run_id")
        reason = _optional_non_empty_string(payload, "reason")
        return dispatch_scheduler_cancel(
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
            run_id=run_id,
            reason=reason or None,
        )


def register_run_routes(
    *,
    app: FastAPI,
    get_execution_client: Callable[[], RuntimeExecutionClient],
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
) -> None:
    _register_run_mutation_routes(
        app=app,
        get_execution_client=get_execution_client,
        publish_gateway_event=publish_gateway_event,
    )
    _register_run_query_routes(
        app=app,
        get_execution_client=get_execution_client,
    )
    _register_contract_retirement_routes(
        app=app,
        get_execution_client=get_execution_client,
    )
    _register_worker_routes(
        app=app,
        get_execution_client=get_execution_client,
    )
    _register_scheduler_routes(
        app=app,
        get_execution_client=get_execution_client,
    )
