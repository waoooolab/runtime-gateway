"""Tests for run approval/rejection endpoints."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

from runtime_gateway.app import app
from runtime_gateway.auth.tokens import issue_token
from runtime_gateway.integration import RuntimeExecutionClient, RuntimeExecutionClientError

os.environ["WAOOOOLAB_PLATFORM_CONTRACTS_DIR"] = str(
    Path(__file__).resolve().parent / "fixtures" / "contracts"
)


def _run_status_event(*, run_id: str, status: str) -> dict:
    return {
        "event_id": f"evt-{run_id}",
        "event_type": "runtime.run.status",
        "tenant_id": "t1",
        "app_id": "waoooo",
        "session_key": "tenant:t1:app:waoooo:channel:web:actor:u1:thread:main:agent:pm",
        "trace_id": "trace-test",
        "correlation_id": run_id,
        "ts": datetime.now(timezone.utc).isoformat(),
        "payload": {
            "run_id": run_id,
            "status": status,
        },
    }


@pytest.fixture
def mock_execution_client(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock RuntimeExecutionClient for testing."""
    mock_client = Mock(spec=RuntimeExecutionClient)
    monkeypatch.setattr("runtime_gateway.app._execution_client", mock_client)
    return mock_client


@pytest.fixture
def mock_token_exchange(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock token exchange for testing."""
    mock_exchange = Mock(return_value={"access_token": "delegated-token"})
    monkeypatch.setattr("runtime_gateway.run_approval.exchange_subject_token", mock_exchange)
    return mock_exchange


def _make_token(audience: str, scope: list[str]) -> str:
    """Generate a valid test token."""
    return issue_token(
        {
            "iss": "runtime-gateway",
            "sub": "user:u1",
            "aud": audience,
            "tenant_id": "t1",
            "app_id": "waoooo",
            "scope": scope,
            "token_use": "access",
            "trace_id": "trace-test",
        },
        ttl_seconds=300,
    )


@pytest.fixture
def auth_headers() -> dict[str, str]:
    """Standard auth headers for testing."""
    token = _make_token(audience="runtime-gateway", scope=["runs:write"])
    return {
        "Authorization": f"Bearer {token}",
    }


def test_approve_run_happy_path(
    mock_execution_client: Mock, mock_token_exchange: Mock, auth_headers: dict[str, str]
) -> None:
    """Test successful run approval."""
    mock_execution_client.approve_run.return_value = _run_status_event(
        run_id="run-123",
        status="queued",
    )

    client = TestClient(app)
    response = client.post("/v1/runs/run-123:approve", headers=auth_headers)

    assert response.status_code == 200
    data = response.json()
    assert data["event_type"] == "runtime.run.status"
    assert data["payload"]["run_id"] == "run-123"
    assert data["payload"]["status"] == "queued"
    mock_execution_client.approve_run.assert_called_once()
    mock_token_exchange.assert_called_once()
    assert mock_token_exchange.call_args.kwargs["scope"] == ["runs:write"]

    recent = client.get("/v1/events/recent", headers=auth_headers)
    assert recent.status_code == 200
    items = recent.json()["items"]
    assert len(items) >= 1
    assert items[-1]["event"]["event_type"] == "runtime.run.status"


def test_reject_run_happy_path(
    mock_execution_client: Mock, mock_token_exchange: Mock, auth_headers: dict[str, str]
) -> None:
    """Test successful run rejection."""
    mock_execution_client.reject_run.return_value = _run_status_event(
        run_id="run-456",
        status="canceled",
    )

    client = TestClient(app)
    response = client.post("/v1/runs/run-456:reject", headers=auth_headers)

    assert response.status_code == 200
    data = response.json()
    assert data["event_type"] == "runtime.run.status"
    assert data["payload"]["run_id"] == "run-456"
    assert data["payload"]["status"] == "canceled"
    mock_execution_client.reject_run.assert_called_once()
    mock_token_exchange.assert_called_once()
    assert mock_token_exchange.call_args.kwargs["scope"] == ["runs:write"]

    recent = client.get("/v1/events/recent", headers=auth_headers)
    assert recent.status_code == 200
    items = recent.json()["items"]
    assert len(items) >= 1
    assert items[-1]["event"]["event_type"] == "runtime.run.status"


def test_approve_run_downstream_4xx_error(
    mock_execution_client: Mock, mock_token_exchange: Mock, auth_headers: dict[str, str]
) -> None:
    """Test approve run with downstream 404 error."""
    mock_execution_client.approve_run.side_effect = RuntimeExecutionClientError(
        "HTTP 404 calling approve endpoint",
        status_code=404,
        response_body={"error": "run not found"},
    )

    client = TestClient(app)
    response = client.post("/v1/runs/run-999:approve", headers=auth_headers)

    assert response.status_code == 404
    assert "404" in response.text


def test_reject_run_downstream_5xx_error(
    mock_execution_client: Mock, mock_token_exchange: Mock, auth_headers: dict[str, str]
) -> None:
    """Test reject run with downstream 500 error."""
    mock_execution_client.reject_run.side_effect = RuntimeExecutionClientError(
        "HTTP 500 calling reject endpoint",
        status_code=500,
        response_body={"error": "internal server error"},
    )

    client = TestClient(app)
    response = client.post("/v1/runs/run-888:reject", headers=auth_headers)

    assert response.status_code == 500
    assert "500" in response.text


def test_approve_run_downstream_connection_error(
    mock_execution_client: Mock, mock_token_exchange: Mock, auth_headers: dict[str, str]
) -> None:
    """Test approve run with downstream connection error."""
    mock_execution_client.approve_run.side_effect = RuntimeExecutionClientError(
        "connection error calling approve endpoint",
        status_code=None,
    )

    client = TestClient(app)
    response = client.post("/v1/runs/run-777:approve", headers=auth_headers)

    assert response.status_code == 502
    assert "connection error" in response.text


def test_approve_run_missing_auth() -> None:
    """Test approve run without authorization header."""
    client = TestClient(app)
    response = client.post("/v1/runs/run-123:approve")

    assert response.status_code == 401


def test_reject_run_missing_auth() -> None:
    """Test reject run without authorization header."""
    client = TestClient(app)
    response = client.post("/v1/runs/run-456:reject")

    assert response.status_code == 401
