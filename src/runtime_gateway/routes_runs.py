"""Run and worker route registration for runtime-gateway API."""

from __future__ import annotations

from typing import Any, Callable

from fastapi import Depends, FastAPI

from .api.schemas import CreateRunRequest, CreateRunResponse
from .integration import RuntimeExecutionClient
from .run_approval import dispatch_approve_run, dispatch_reject_run
from .run_control import dispatch_cancel_run, dispatch_timeout_run
from .run_dispatch import dispatch_create_run
from .run_worker import dispatch_worker_drain, dispatch_worker_tick
from .security import AuthContext, require_runs_write_context


def register_run_routes(
    *,
    app: FastAPI,
    get_execution_client: Callable[[], RuntimeExecutionClient],
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
) -> None:
    @app.post("/v1/runs", response_model=CreateRunResponse)
    def create_run(
        req: CreateRunRequest,
        auth_context: AuthContext = Depends(require_runs_write_context),
    ) -> CreateRunResponse:
        return dispatch_create_run(
            req=req,
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
            publish_gateway_event=publish_gateway_event,
        )

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
