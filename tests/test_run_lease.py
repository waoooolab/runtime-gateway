"""Tests for run lease lookup endpoint."""

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
    monkeypatch.setattr("runtime_gateway.run_lease.exchange_subject_token", mock_exchange)
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
            "trace_id": "trace-test-lease",
        },
        ttl_seconds=300,
    )


@pytest.fixture
def read_auth_headers() -> dict[str, str]:
    token = _make_token(audience="runtime-gateway", scope=["runs:read"])
    return {"Authorization": f"Bearer {token}"}


def test_get_run_lease_happy_path(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    read_auth_headers: dict[str, str],
) -> None:
    mock_execution_client.get_run_lease.return_value = {
        "run_id": "run-lease-1",
        "lease": {"lease_id": "lease-1", "task_id": "run-lease-1:root", "state": "active"},
        "device_hub": {"status": "ok", "snapshot": {"status": "active"}},
    }
    client = TestClient(app)
    response = client.get(
        "/v1/runs/run-lease-1/lease",
        headers=read_auth_headers,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["run_id"] == "run-lease-1"
    assert payload["lease"]["lease_id"] == "lease-1"
    assert payload["lease"]["state"] == "active"
    assert payload["recommended_poll_after_ms"] == 2000
    mock_execution_client.get_run_lease.assert_called_once_with(
        run_id="run-lease-1",
        auth_token="delegated-token",
    )
    mock_token_exchange.assert_called_once()
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "runs.lease"
    assert audit["decision"] == "allow"
    assert audit["metadata"]["run_id"] == "run-lease-1"
    assert audit["metadata"]["lease_state"] == "active"
    assert audit["metadata"]["device_hub_status"] == "ok"
    assert audit["metadata"]["recommended_poll_after_ms"] == 2000


def test_get_run_lease_terminal_recommends_slow_poll(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    read_auth_headers: dict[str, str],
) -> None:
    mock_execution_client.get_run_lease.return_value = {
        "run_id": "run-lease-terminal",
        "lease": {"lease_id": "lease-terminal", "task_id": "run-lease-terminal:root", "state": "expired"},
        "device_hub": {"status": "ok", "snapshot": {"status": "expired"}},
    }
    client = TestClient(app)
    response = client.get(
        "/v1/runs/run-lease-terminal/lease",
        headers=read_auth_headers,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["recommended_poll_after_ms"] == 10000
    mock_token_exchange.assert_called_once()
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "runs.lease"
    assert audit["decision"] == "allow"
    assert audit["metadata"]["run_id"] == "run-lease-terminal"
    assert audit["metadata"]["lease_state"] == "expired"
    assert audit["metadata"]["recommended_poll_after_ms"] == 10000


def test_get_run_lease_downstream_error_maps_status(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    read_auth_headers: dict[str, str],
) -> None:
    mock_execution_client.get_run_lease.side_effect = RuntimeExecutionClientError(
        "HTTP 404 calling lease endpoint",
        status_code=404,
        response_body={"detail": "run not found"},
    )
    client = TestClient(app)
    response = client.get(
        "/v1/runs/run-missing/lease",
        headers=read_auth_headers,
    )
    assert response.status_code == 404
    detail = response.json().get("detail")
    assert isinstance(detail, dict)
    assert detail["status_code"] == 404
    assert detail["requested_run_id"] == "run-missing"
    assert detail["downstream_response"]["detail"] == "run not found"
    assert "HTTP 404" in str(detail["message"])
    mock_token_exchange.assert_called_once()
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "runs.lease"
    assert audit["decision"] == "deny"
    assert audit["metadata"]["run_id"] == "run-missing"


def test_get_run_lease_rejects_invalid_contract_payload(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    read_auth_headers: dict[str, str],
) -> None:
    mock_execution_client.get_run_lease.return_value = {
        "run_id": "run-lease-invalid",
        "lease": {"lease_id": "lease-1", "state": "active"},
        "device_hub": {"status": "ok"},
    }
    client = TestClient(app)
    response = client.get(
        "/v1/runs/run-lease-invalid/lease",
        headers=read_auth_headers,
    )
    assert response.status_code == 502
    assert "invalid run lease response" in response.text
    mock_token_exchange.assert_called_once()
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "runs.lease"
    assert audit["decision"] == "deny"
    assert audit["metadata"]["run_id"] == "run-lease-invalid"
    assert audit["metadata"]["validation_schema"] == "runtime/runtime-run-lease.v1.json"


def test_get_run_lease_rejects_mismatched_run_id(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    read_auth_headers: dict[str, str],
) -> None:
    mock_execution_client.get_run_lease.return_value = {
        "run_id": "run-lease-other",
        "lease": {"lease_id": "lease-1", "task_id": "run-lease-other:root", "state": "active"},
        "device_hub": {"status": "ok", "snapshot": {"status": "active"}},
    }
    client = TestClient(app)
    response = client.get(
        "/v1/runs/run-lease-expected/lease",
        headers=read_auth_headers,
    )
    assert response.status_code == 502
    assert "run_id mismatch" in response.text
    mock_token_exchange.assert_called_once()
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "runs.lease"
    assert audit["decision"] == "deny"
    assert audit["metadata"]["run_id"] == "run-lease-expected"
    assert audit["metadata"]["downstream_run_id"] == "run-lease-other"


def test_get_run_lease_allows_not_bound_payload(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    read_auth_headers: dict[str, str],
) -> None:
    mock_execution_client.get_run_lease.return_value = {
        "run_id": "run-not-bound",
        "lease": None,
        "device_hub": {"status": "not_bound"},
    }
    client = TestClient(app)
    response = client.get(
        "/v1/runs/run-not-bound/lease",
        headers=read_auth_headers,
    )
    assert response.status_code == 200
    assert response.json()["device_hub"]["status"] == "not_bound"
    assert response.json()["recommended_poll_after_ms"] == 5000
    mock_token_exchange.assert_called_once()


def test_get_run_lease_allows_error_payload_with_error_field(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    read_auth_headers: dict[str, str],
) -> None:
    mock_execution_client.get_run_lease.return_value = {
        "run_id": "run-lease-err",
        "lease": {"lease_id": "lease-err", "task_id": "run-lease-err:root", "state": "active"},
        "device_hub": {"status": "error", "error": "lookup timeout"},
    }
    client = TestClient(app)
    response = client.get(
        "/v1/runs/run-lease-err/lease",
        headers=read_auth_headers,
    )
    assert response.status_code == 200
    assert response.json()["device_hub"]["status"] == "error"
    assert response.json()["device_hub"]["error"] == "lookup timeout"
    mock_token_exchange.assert_called_once()


def test_get_run_lease_rejects_invalid_downstream_recommended_poll_hint(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    read_auth_headers: dict[str, str],
) -> None:
    mock_execution_client.get_run_lease.return_value = {
        "run_id": "run-lease-low-poll",
        "lease": {"lease_id": "lease-low-poll", "task_id": "run-lease-low-poll:root", "state": "active"},
        "device_hub": {"status": "ok", "snapshot": {"status": "active"}},
        "recommended_poll_after_ms": 50,
    }
    client = TestClient(app)
    response = client.get(
        "/v1/runs/run-lease-low-poll/lease",
        headers=read_auth_headers,
    )
    assert response.status_code == 502
    assert "invalid run lease response" in response.text
    mock_token_exchange.assert_called_once()
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "runs.lease"
    assert audit["decision"] == "deny"
    assert audit["metadata"]["run_id"] == "run-lease-low-poll"
    assert audit["metadata"]["validation_schema"] == "runtime/runtime-run-lease.v1.json"


def test_get_run_lease_missing_auth() -> None:
    client = TestClient(app)
    response = client.get("/v1/runs/run-1/lease")
    assert response.status_code == 401
