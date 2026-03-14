"""Capability route registration for runtime-gateway API."""

from __future__ import annotations

from typing import Any, Callable

from fastapi import Depends, FastAPI

from .capability_dispatch import (
    dispatch_compile_capability,
    dispatch_get_capability,
    dispatch_invoke_capability,
    dispatch_list_capabilities,
    dispatch_publish_capability,
    dispatch_register_capability,
    dispatch_resolve_capability,
)
from .integration import RuntimeExecutionClient
from .security import (
    AuthContext,
    require_capabilities_invoke_context,
    require_capabilities_read_context,
    require_capabilities_write_context,
)


def register_capability_routes(
    *,
    app: FastAPI,
    get_execution_client: Callable[[], RuntimeExecutionClient],
    publish_gateway_event: Callable[[dict[str, Any]], int | None],
) -> None:
    @app.post("/v1/capabilities/register")
    def register_capability(
        body: dict[str, Any] | None = None,
        auth_context: AuthContext = Depends(require_capabilities_write_context),
    ) -> dict[str, Any]:
        return dispatch_register_capability(
            body=body,
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
            publish_gateway_event=publish_gateway_event,
        )

    @app.get("/v1/capabilities")
    def list_capabilities(
        auth_context: AuthContext = Depends(require_capabilities_read_context),
    ) -> dict[str, Any]:
        return dispatch_list_capabilities(
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
            publish_gateway_event=publish_gateway_event,
        )

    @app.get("/v1/capabilities/{capability_id}")
    def get_capability(
        capability_id: str,
        auth_context: AuthContext = Depends(require_capabilities_read_context),
    ) -> dict[str, Any]:
        return dispatch_get_capability(
            capability_id=capability_id,
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
            publish_gateway_event=publish_gateway_event,
        )

    @app.post("/v1/capabilities/resolve")
    def resolve_capability(
        body: dict[str, Any] | None = None,
        auth_context: AuthContext = Depends(require_capabilities_read_context),
    ) -> dict[str, Any]:
        return dispatch_resolve_capability(
            body=body,
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
            publish_gateway_event=publish_gateway_event,
        )

    @app.post("/v1/capabilities/compile")
    def compile_capability(
        body: dict[str, Any] | None = None,
        auth_context: AuthContext = Depends(require_capabilities_write_context),
    ) -> dict[str, Any]:
        return dispatch_compile_capability(
            body=body,
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
            publish_gateway_event=publish_gateway_event,
        )

    @app.post("/v1/capabilities/publish")
    def publish_capability(
        body: dict[str, Any] | None = None,
        auth_context: AuthContext = Depends(require_capabilities_write_context),
    ) -> dict[str, Any]:
        return dispatch_publish_capability(
            body=body,
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
            publish_gateway_event=publish_gateway_event,
        )

    @app.post("/v1/capabilities/{capability_id}:invoke")
    def invoke_capability(
        capability_id: str,
        body: dict[str, Any] | None = None,
        auth_context: AuthContext = Depends(require_capabilities_invoke_context),
    ) -> dict[str, Any]:
        return dispatch_invoke_capability(
            capability_id=capability_id,
            body=body,
            claims=auth_context.claims,
            subject_token=auth_context.subject_token,
            execution_client=get_execution_client(),
            publish_gateway_event=publish_gateway_event,
        )
