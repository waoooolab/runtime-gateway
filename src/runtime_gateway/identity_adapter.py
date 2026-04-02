"""Identity adapter seam for runtime-gateway."""

from __future__ import annotations

import os
from typing import Any, Protocol

from .auth.exchange import ExchangeError, exchange_subject_token as _exchange_subject_token_local

DEFAULT_IDENTITY_ADAPTER_BACKEND = "local"
KEYCLOAK_IDENTITY_ADAPTER_BACKEND = "keycloak"
SUPPORTED_IDENTITY_ADAPTER_BACKENDS = frozenset(
    {
        DEFAULT_IDENTITY_ADAPTER_BACKEND,
        KEYCLOAK_IDENTITY_ADAPTER_BACKEND,
    }
)

_IDENTITY_ADAPTER_BACKEND_ENV = "RUNTIME_GATEWAY_IDENTITY_ADAPTER_BACKEND"
_IDENTITY_ADAPTER_BACKEND_ENV_LEGACY = "WAOOOOLAB_RUNTIME_GATEWAY_IDENTITY_ADAPTER_BACKEND"


class IdentityAdapterBackend(Protocol):
    name: str

    def exchange_subject_token(
        self,
        *,
        subject_token: str,
        requested_token_use: str,
        audience: str,
        scope: list[str],
        requested_ttl_seconds: int,
        tenant_id: str | None = None,
        app_id: str | None = None,
        session_key: str | None = None,
        run_id: str | None = None,
        task_id: str | None = None,
        trace_id: str | None = None,
    ) -> dict[str, Any]:
        ...


class LocalIdentityAdapterBackend:
    name = DEFAULT_IDENTITY_ADAPTER_BACKEND

    def exchange_subject_token(
        self,
        *,
        subject_token: str,
        requested_token_use: str,
        audience: str,
        scope: list[str],
        requested_ttl_seconds: int,
        tenant_id: str | None = None,
        app_id: str | None = None,
        session_key: str | None = None,
        run_id: str | None = None,
        task_id: str | None = None,
        trace_id: str | None = None,
    ) -> dict[str, Any]:
        return _exchange_subject_token_local(
            subject_token=subject_token,
            requested_token_use=requested_token_use,
            audience=audience,
            scope=scope,
            requested_ttl_seconds=requested_ttl_seconds,
            tenant_id=tenant_id,
            app_id=app_id,
            session_key=session_key,
            run_id=run_id,
            task_id=task_id,
            trace_id=trace_id,
        )


class KeycloakIdentityAdapterBackend:
    name = KEYCLOAK_IDENTITY_ADAPTER_BACKEND

    def exchange_subject_token(
        self,
        *,
        subject_token: str,
        requested_token_use: str,
        audience: str,
        scope: list[str],
        requested_ttl_seconds: int,
        tenant_id: str | None = None,
        app_id: str | None = None,
        session_key: str | None = None,
        run_id: str | None = None,
        task_id: str | None = None,
        trace_id: str | None = None,
    ) -> dict[str, Any]:
        _ = (
            subject_token,
            requested_token_use,
            audience,
            scope,
            requested_ttl_seconds,
            tenant_id,
            app_id,
            session_key,
            run_id,
            task_id,
            trace_id,
        )
        _raise_keycloak_not_ready("exchange_subject_token")


def normalize_identity_adapter_backend_name(raw_name: str | None) -> str:
    if raw_name is None:
        return DEFAULT_IDENTITY_ADAPTER_BACKEND
    normalized = raw_name.strip().lower()
    if not normalized:
        return DEFAULT_IDENTITY_ADAPTER_BACKEND
    if normalized not in SUPPORTED_IDENTITY_ADAPTER_BACKENDS:
        supported = ", ".join(sorted(SUPPORTED_IDENTITY_ADAPTER_BACKENDS))
        raise ValueError(
            f"unsupported identity adapter backend '{raw_name}'; expected one of: {supported}"
        )
    return normalized


def build_identity_adapter_backend(*, name: str) -> IdentityAdapterBackend:
    normalized = normalize_identity_adapter_backend_name(name)
    if normalized == KEYCLOAK_IDENTITY_ADAPTER_BACKEND:
        return KeycloakIdentityAdapterBackend()
    return LocalIdentityAdapterBackend()


def resolve_identity_adapter_backend(
    *,
    backend: IdentityAdapterBackend | None,
    backend_name: str | None,
) -> IdentityAdapterBackend:
    if backend is not None:
        return backend
    resolved_name = normalize_identity_adapter_backend_name(backend_name)
    return build_identity_adapter_backend(name=resolved_name)


def identity_adapter_backend_name_from_env() -> str:
    configured = os.environ.get(_IDENTITY_ADAPTER_BACKEND_ENV)
    if configured is None:
        configured = os.environ.get(_IDENTITY_ADAPTER_BACKEND_ENV_LEGACY)
    return normalize_identity_adapter_backend_name(configured)


_identity_adapter_backend = resolve_identity_adapter_backend(
    backend=None,
    backend_name=identity_adapter_backend_name_from_env(),
)


def exchange_subject_token(
    *,
    subject_token: str,
    requested_token_use: str,
    audience: str,
    scope: list[str],
    requested_ttl_seconds: int,
    tenant_id: str | None = None,
    app_id: str | None = None,
    session_key: str | None = None,
    run_id: str | None = None,
    task_id: str | None = None,
    trace_id: str | None = None,
) -> dict[str, Any]:
    return _identity_adapter_backend.exchange_subject_token(
        subject_token=subject_token,
        requested_token_use=requested_token_use,
        audience=audience,
        scope=scope,
        requested_ttl_seconds=requested_ttl_seconds,
        tenant_id=tenant_id,
        app_id=app_id,
        session_key=session_key,
        run_id=run_id,
        task_id=task_id,
        trace_id=trace_id,
    )


def _raise_keycloak_not_ready(method_name: str) -> None:
    raise NotImplementedError(
        "identity adapter backend 'keycloak' is not implemented yet; "
        f"cannot call {method_name}. Use identity adapter backend 'local' for now."
    )
