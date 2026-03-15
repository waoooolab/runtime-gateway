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


def _make_token(
    audience: str,
    scope: list[str],
    *,
    token_use: str | None = "access",
) -> str:
    claims: dict[str, object] = {
        "iss": "runtime-gateway",
        "sub": "user:u1",
        "aud": audience,
        "tenant_id": "t1",
        "app_id": "covernow",
        "scope": scope,
        "trace_id": "trace-test-status",
    }
    if token_use is not None:
        claims["token_use"] = token_use
    return issue_token(claims, ttl_seconds=300)


def _run_status_event(
    *,
    run_id: str,
    status: str,
    route: dict[str, object] | None = None,
    failure_reason_code: str | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "run_id": run_id,
        "status": status,
        "retry_attempts": 0,
    }
    if isinstance(failure_reason_code, str) and failure_reason_code.strip():
        payload["orchestration"] = {"failure_reason_code": failure_reason_code.strip()}
    if isinstance(route, dict) and route:
        payload["route"] = dict(route)
    return {
        "event_id": f"evt-{run_id}",
        "event_type": "runtime.run.status",
        "tenant_id": "t1",
        "app_id": "covernow",
        "session_key": "tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
        "trace_id": "trace-test-status",
        "correlation_id": run_id,
        "ts": "2026-03-12T06:00:00+00:00",
        "payload": payload,
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
    assert payload["recommended_poll_after_ms"] == 1500
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
    assert audit["metadata"]["recommended_poll_after_ms"] == 1500


def test_get_run_status_terminal_recommends_slow_poll(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    read_auth_headers: dict[str, str],
) -> None:
    mock_execution_client.get_run_status.return_value = _run_status_event(
        run_id="run-status-terminal",
        status="succeeded",
    )
    client = TestClient(app)
    response = client.get(
        "/v1/runs/run-status-terminal",
        headers=read_auth_headers,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["recommended_poll_after_ms"] == 10000
    mock_token_exchange.assert_called_once()
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "runs.read"
    assert audit["decision"] == "allow"
    assert audit["metadata"]["run_id"] == "run-status-terminal"
    assert audit["metadata"]["downstream_status"] == "succeeded"
    assert audit["metadata"]["recommended_poll_after_ms"] == 10000


def test_get_run_status_preserves_route_metadata(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    read_auth_headers: dict[str, str],
) -> None:
    mock_execution_client.get_run_status.return_value = _run_status_event(
        run_id="run-status-route",
        status="running",
        route={
            "event_type": "runtime.route.decided",
            "execution_mode": "compute",
            "route_target": "device-hub",
            "placement_event_type": "device.lease.acquired",
            "device_id": "gpu-node-test",
            "lease_id": "lease-test",
        },
    )
    client = TestClient(app)
    response = client.get(
        "/v1/runs/run-status-route",
        headers=read_auth_headers,
    )
    assert response.status_code == 200
    payload = response.json()
    route = payload["payload"]["route"]
    assert route["event_type"] == "runtime.route.decided"
    assert route["execution_mode"] == "compute"
    assert route["route_target"] == "device-hub"
    assert route["placement_event_type"] == "device.lease.acquired"
    assert route["device_id"] == "gpu-node-test"
    assert route["lease_id"] == "lease-test"
    assert payload["recommended_poll_after_ms"] == 1000
    mock_token_exchange.assert_called_once()
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "runs.read"
    assert audit["decision"] == "allow"
    assert audit["metadata"]["run_id"] == "run-status-route"
    assert audit["metadata"]["downstream_route_event_type"] == "runtime.route.decided"
    assert audit["metadata"]["downstream_execution_mode"] == "compute"
    assert audit["metadata"]["downstream_route_target"] == "device-hub"
    assert audit["metadata"]["downstream_failure_reason_code"] is None


def test_get_run_status_terminal_failure_emits_failure_reason_metadata(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    read_auth_headers: dict[str, str],
) -> None:
    mock_execution_client.get_run_status.return_value = _run_status_event(
        run_id="run-status-failed",
        status="failed",
        failure_reason_code="capacity_exhausted",
        route={
            "event_type": "runtime.route.decided",
            "execution_mode": "compute",
            "route_target": "device-hub",
            "placement_reason_code": "capacity_exhausted",
        },
    )
    client = TestClient(app)
    response = client.get(
        "/v1/runs/run-status-failed",
        headers=read_auth_headers,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["payload"]["status"] == "failed"
    assert payload["payload"]["orchestration"]["failure_reason_code"] == "capacity_exhausted"
    assert payload["recommended_poll_after_ms"] == 10000
    mock_token_exchange.assert_called_once()
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "runs.read"
    assert audit["decision"] == "allow"
    assert audit["metadata"]["run_id"] == "run-status-failed"
    assert audit["metadata"]["downstream_failure_reason_code"] == "capacity_exhausted"
    assert audit["metadata"]["downstream_route_event_type"] == "runtime.route.decided"
    assert audit["metadata"]["downstream_execution_mode"] == "compute"
    assert audit["metadata"]["downstream_route_target"] == "device-hub"
    assert audit["metadata"]["downstream_placement_reason_code"] == "capacity_exhausted"


def test_get_run_status_normalizes_failure_reason_and_route_reason_codes(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    read_auth_headers: dict[str, str],
) -> None:
    mock_execution_client.get_run_status.return_value = _run_status_event(
        run_id="run-status-failed-mixed",
        status="failed",
        failure_reason_code="Run-Preempted",
        route={
            "event_type": "runtime.route.decided",
            "execution_mode": "compute",
            "route_target": "device-hub",
            "placement_reason_code": "NoEligibleDevice",
        },
    )
    client = TestClient(app)
    response = client.get(
        "/v1/runs/run-status-failed-mixed",
        headers=read_auth_headers,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["payload"]["orchestration"]["failure_reason_code"] == "Run-Preempted"
    assert payload["payload"]["route"]["placement_reason_code"] == "NoEligibleDevice"
    mock_token_exchange.assert_called_once()
    audit = get_audit_events(limit=1)[0]
    assert audit["metadata"]["downstream_failure_reason_code"] == "run_preempted"
    assert audit["metadata"]["downstream_placement_reason_code"] == "no_eligible_device"


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
    detail = response.json().get("detail")
    assert isinstance(detail, dict)
    assert detail["status_code"] == 404
    assert detail["requested_run_id"] == "run-missing"
    assert detail["downstream_response"]["detail"] == "run not found"
    assert "HTTP 404" in str(detail["message"])
    mock_token_exchange.assert_called_once()
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "runs.read"
    assert audit["decision"] == "deny"
    assert audit["metadata"]["run_id"] == "run-missing"


def test_get_run_status_connection_error_returns_structured_retryable_detail(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    read_auth_headers: dict[str, str],
) -> None:
    mock_execution_client.get_run_status.side_effect = RuntimeExecutionClientError(
        "connection error calling run status endpoint",
        status_code=None,
    )
    client = TestClient(app)
    response = client.get(
        "/v1/runs/run-status-connect",
        headers=read_auth_headers,
    )
    assert response.status_code == 503
    detail = response.json().get("detail")
    assert isinstance(detail, dict)
    assert detail["status_code"] == 503
    assert detail["retryable"] is True
    assert detail["failure_classification"] == "upstream_unavailable"
    assert "connection error" in str(detail["message"])
    mock_token_exchange.assert_called_once()
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "runs.read"
    assert audit["decision"] == "deny"
    assert audit["metadata"]["run_id"] == "run-status-connect"
    assert audit["metadata"]["status_code"] == 503
    assert audit["metadata"]["retryable"] is True
    assert audit["metadata"]["failure_classification"] == "upstream_unavailable"


def test_get_run_status_downstream_error_surfaces_failure_and_route_diagnostics(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    read_auth_headers: dict[str, str],
) -> None:
    mock_execution_client.get_run_status.side_effect = RuntimeExecutionClientError(
        "HTTP 503 calling run status endpoint",
        status_code=503,
        response_body=_run_status_event(
            run_id="run-status-upstream-failed",
            status="failed",
            failure_reason_code="run_preempted",
            route={
                "event_type": "runtime.route.decided",
                "execution_mode": "compute",
                "route_target": "device-hub",
                "placement_reason_code": "capacity_exhausted",
            },
        ),
    )
    client = TestClient(app)
    response = client.get(
        "/v1/runs/run-status-upstream-failed",
        headers=read_auth_headers,
    )
    assert response.status_code == 503
    detail = response.json().get("detail")
    assert isinstance(detail, dict)
    assert detail["status_code"] == 503
    assert detail["requested_run_id"] == "run-status-upstream-failed"
    assert detail["downstream_status"] == "failed"
    assert detail["downstream_failure_reason_code"] == "run_preempted"
    assert detail["downstream_route_event_type"] == "runtime.route.decided"
    assert detail["downstream_execution_mode"] == "compute"
    assert detail["downstream_route_target"] == "device-hub"
    assert detail["downstream_placement_reason_code"] == "capacity_exhausted"
    mock_token_exchange.assert_called_once()
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "runs.read"
    assert audit["decision"] == "deny"
    assert audit["metadata"]["run_id"] == "run-status-upstream-failed"
    assert audit["metadata"]["downstream_status"] == "failed"
    assert audit["metadata"]["downstream_failure_reason_code"] == "run_preempted"
    assert audit["metadata"]["downstream_route_event_type"] == "runtime.route.decided"
    assert audit["metadata"]["downstream_execution_mode"] == "compute"
    assert audit["metadata"]["downstream_route_target"] == "device-hub"
    assert audit["metadata"]["downstream_placement_reason_code"] == "capacity_exhausted"


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


def test_get_run_status_rejects_missing_token_use_claim() -> None:
    client = TestClient(app)
    token = _make_token(
        audience="runtime-gateway",
        scope=["runs:read"],
        token_use=None,
    )
    response = client.get(
        "/v1/runs/run-1",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 401
    assert "missing token_use" in response.json()["detail"]
