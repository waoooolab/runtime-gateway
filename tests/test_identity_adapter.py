from __future__ import annotations

import pytest

from runtime_gateway.identity_adapter import (
    DEFAULT_IDENTITY_ADAPTER_BACKEND,
    KEYCLOAK_IDENTITY_ADAPTER_BACKEND,
    LocalIdentityAdapterBackend,
    build_identity_adapter_backend,
    exchange_subject_token,
    identity_adapter_backend_name_from_env,
    normalize_identity_adapter_backend_name,
    resolve_identity_adapter_backend,
)


def test_normalize_identity_adapter_backend_name_defaults_to_local() -> None:
    assert normalize_identity_adapter_backend_name(None) == DEFAULT_IDENTITY_ADAPTER_BACKEND
    assert normalize_identity_adapter_backend_name(" ") == DEFAULT_IDENTITY_ADAPTER_BACKEND
    assert normalize_identity_adapter_backend_name("LOCAL") == DEFAULT_IDENTITY_ADAPTER_BACKEND
    assert (
        normalize_identity_adapter_backend_name("Keycloak")
        == KEYCLOAK_IDENTITY_ADAPTER_BACKEND
    )


def test_normalize_identity_adapter_backend_name_rejects_unsupported_value() -> None:
    with pytest.raises(ValueError, match="unsupported identity adapter backend"):
        normalize_identity_adapter_backend_name("oauth2-proxy")


def test_identity_adapter_backend_name_from_env_prefers_primary_var(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RUNTIME_GATEWAY_IDENTITY_ADAPTER_BACKEND", "keycloak")
    monkeypatch.setenv("WAOOOOLAB_RUNTIME_GATEWAY_IDENTITY_ADAPTER_BACKEND", "local")
    assert identity_adapter_backend_name_from_env() == KEYCLOAK_IDENTITY_ADAPTER_BACKEND


def test_identity_adapter_backend_name_from_env_falls_back_to_legacy_var(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("RUNTIME_GATEWAY_IDENTITY_ADAPTER_BACKEND", raising=False)
    monkeypatch.setenv("WAOOOOLAB_RUNTIME_GATEWAY_IDENTITY_ADAPTER_BACKEND", "keycloak")
    assert identity_adapter_backend_name_from_env() == KEYCLOAK_IDENTITY_ADAPTER_BACKEND


def test_local_identity_adapter_backend_delegates_to_exchange_impl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def _fake_exchange_subject_token_local(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {"access_token": "delegated-token"}

    monkeypatch.setattr(
        "runtime_gateway.identity_adapter._exchange_subject_token_local",
        _fake_exchange_subject_token_local,
    )
    backend = build_identity_adapter_backend(name="local")
    result = backend.exchange_subject_token(
        subject_token="subject-token",
        requested_token_use="service",
        audience="runtime-execution",
        scope=["runs:write"],
        requested_ttl_seconds=300,
        trace_id="trace-1",
    )
    assert result == {"access_token": "delegated-token"}
    assert captured["audience"] == "runtime-execution"
    assert captured["scope"] == ["runs:write"]
    assert captured["requested_ttl_seconds"] == 300
    assert captured["trace_id"] == "trace-1"


def test_resolve_identity_adapter_backend_prefers_explicit_backend() -> None:
    explicit = LocalIdentityAdapterBackend()
    resolved = resolve_identity_adapter_backend(
        backend=explicit,
        backend_name="keycloak",
    )
    assert resolved is explicit


def test_keycloak_identity_adapter_backend_placeholder_raises_not_implemented() -> None:
    backend = build_identity_adapter_backend(name="keycloak")
    with pytest.raises(NotImplementedError, match="identity adapter backend 'keycloak'"):
        backend.exchange_subject_token(
            subject_token="subject-token",
            requested_token_use="service",
            audience="runtime-execution",
            scope=["runs:write"],
            requested_ttl_seconds=300,
            trace_id="trace-1",
        )


def test_exchange_subject_token_uses_resolved_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _StubBackend:
        name = "stub"

        def exchange_subject_token(self, **kwargs: object) -> dict[str, object]:
            return {
                "token_use": kwargs["requested_token_use"],
                "audience": kwargs["audience"],
                "scope": kwargs["scope"],
            }

    monkeypatch.setattr("runtime_gateway.identity_adapter._identity_adapter_backend", _StubBackend())
    result = exchange_subject_token(
        subject_token="subject-token",
        requested_token_use="service",
        audience="runtime-execution",
        scope=["runs:write"],
        requested_ttl_seconds=300,
        trace_id="trace-1",
    )
    assert result == {
        "token_use": "service",
        "audience": "runtime-execution",
        "scope": ["runs:write"],
    }
