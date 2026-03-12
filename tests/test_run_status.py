"""Tests for run status lookup endpoint."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

from runtime_gateway.app import app
from runtime_gateway.audit.emitter import clear_audit_events, get_audit_events
from runtime_gateway.auth.tokens import issue_token
from runtime_gateway.integration import RuntimeExecutionClient, RuntimeExecutionClientError

os.environ["WAOOOOLAB_PLATFORM_CONTRACTS_DIR"] = str(
    Path(__file__).resolve().parent / "fixtures" / "contracts"
)


@pytest.fixture
def mock_execution_client(monkeypatch: pytest.MonkeyPatch) -> Mock:
    mock_client = Mock(spec=RuntimeExecutionClient)
    monkeypatch.setattr("runtime_gateway.app._execution_client", mock_client)
    return mock_client


@pytest.fixture
def mock_token_exchange(monkeypatch: pytest.MonkeyPatch) -> Mock:
    mock_exchange = Mock(return_value={"access_token": "delegated-token"})
    monkeypatch.setattr("runtime_gateway.run_status.exchange_subject_token", mock_exchange)
    return mock_exchange


@pytest.fixture(autouse=True)
def clear_audit_state() -> None:
    clear_audit_events()


def _make_token(audience: str, scope: list[str]) -> str:
    return issue_token(
        {
            "iss": "runtime-gateway",
            "sub": "user:u1",
            "aud": audience,
            "tenant_id": "t1",
            "app_id": "covernow",
            "scope": scope,
            "token_use": "access",
            "trace_id": "trace-test-status",
        },
        ttl_seconds=300,
    )


def _run_status_event(*, run_id: str, status: str) -> dict[str, object]:
    return {
        "event_id": f"evt-{run_id}",
        "event_type": "runtime.run.status",
        "tenant_id": "t1",
        "app_id": "covernow",
        "session_key": "tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
        "trace_id": "trace-test-status",
        "correlation_id": run_id,
        "ts": "2026-03-12T06:00:00+00:00",
        "payload": {
            "run_id": run_id,
            "status": status,
            "retry_attempts": 0,
        },
    }


@pytest.fixture
def read_auth_headers() -> dict[str, str]:
    token = _make_token(audience="runtime-gateway", scope=["runs:read"])
    return {"Authorization": f"Bearer {token}"}


def test_get_run_status_happy_path(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    read_auth_headers: dict[str, str],
) -> None:
    mock_execution_client.get_run_status.return_value = _run_status_event(
        run_id="run-status-1",
        status="queued",
    )
    client = TestClient(app)
    response = client.get(
        "/v1/runs/run-status-1",
        headers=read_auth_headers,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["event_type"] == "runtime.run.status"
    assert payload["payload"]["run_id"] == "run-status-1"
    assert payload["payload"]["status"] == "queued"
    mock_execution_client.get_run_status.assert_called_once_with(
        run_id="run-status-1",
        auth_token="delegated-token",
    )
    mock_token_exchange.assert_called_once()
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "runs.read"
    assert audit["decision"] == "allow"
    assert audit["metadata"]["run_id"] == "run-status-1"
    assert audit["metadata"]["downstream_event_type"] == "runtime.run.status"
    assert audit["metadata"]["downstream_status"] == "queued"


def test_get_run_status_downstream_error_maps_status(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    read_auth_headers: dict[str, str],
) -> None:
    mock_execution_client.get_run_status.side_effect = RuntimeExecutionClientError(
        "HTTP 404 calling run status endpoint",
        status_code=404,
        response_body={"detail": "run not found"},
    )
    client = TestClient(app)
    response = client.get(
        "/v1/runs/run-missing",
        headers=read_auth_headers,
    )
    assert response.status_code == 404
    assert "404" in response.text
    mock_token_exchange.assert_called_once()
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "runs.read"
    assert audit["decision"] == "deny"
    assert audit["metadata"]["run_id"] == "run-missing"


def test_get_run_status_rejects_invalid_event_envelope(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    read_auth_headers: dict[str, str],
) -> None:
    invalid = _run_status_event(run_id="run-status-invalid", status="queued")
    invalid.pop("event_id")
    mock_execution_client.get_run_status.return_value = invalid
    client = TestClient(app)
    response = client.get(
        "/v1/runs/run-status-invalid",
        headers=read_auth_headers,
    )
    assert response.status_code == 502
    assert "invalid execution event envelope" in response.text
    mock_token_exchange.assert_called_once()
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "runs.read"
    assert audit["decision"] == "deny"
    assert audit["metadata"]["run_id"] == "run-status-invalid"


def test_get_run_status_rejects_mismatched_run_id(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    read_auth_headers: dict[str, str],
) -> None:
    mismatched = _run_status_event(run_id="run-status-other", status="queued")
    mock_execution_client.get_run_status.return_value = mismatched
    client = TestClient(app)
    response = client.get(
        "/v1/runs/run-status-expected",
        headers=read_auth_headers,
    )
    assert response.status_code == 502
    assert "run_id mismatch" in response.text
    mock_token_exchange.assert_called_once()
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "runs.read"
    assert audit["decision"] == "deny"
    assert audit["metadata"]["run_id"] == "run-status-expected"
    assert audit["metadata"]["downstream_run_id"] == "run-status-other"


def test_get_run_status_missing_auth() -> None:
    client = TestClient(app)
    response = client.get("/v1/runs/run-1")
    assert response.status_code == 401
