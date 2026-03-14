"""Tests for run cancel/timeout/preempt endpoints."""

from __future__ import annotations

import os
from datetime import datetime, timezone
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


def _run_status_event(*, run_id: str, status: str) -> dict:
    return {
        "event_id": f"evt-{run_id}",
        "event_type": "runtime.run.status",
        "tenant_id": "t1",
        "app_id": "covernow",
        "session_key": "tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
        "trace_id": "trace-test-control",
        "correlation_id": run_id,
        "ts": datetime.now(timezone.utc).isoformat(),
        "payload": {
            "run_id": run_id,
            "status": status,
        },
    }


@pytest.fixture
def mock_execution_client(monkeypatch: pytest.MonkeyPatch) -> Mock:
    mock_client = Mock(spec=RuntimeExecutionClient)
    monkeypatch.setattr("runtime_gateway.app._execution_client", mock_client)
    return mock_client


@pytest.fixture
def mock_token_exchange(monkeypatch: pytest.MonkeyPatch) -> Mock:
    mock_exchange = Mock(return_value={"access_token": "delegated-token"})
    monkeypatch.setattr("runtime_gateway.run_control.exchange_subject_token", mock_exchange)
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
            "trace_id": "trace-test-control",
        },
        ttl_seconds=300,
    )


@pytest.fixture
def auth_headers() -> dict[str, str]:
    token = _make_token(audience="runtime-gateway", scope=["runs:write"])
    return {"Authorization": f"Bearer {token}"}


def test_cancel_run_happy_path(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    auth_headers: dict[str, str],
) -> None:
    mock_execution_client.cancel_run.return_value = _run_status_event(
        run_id="run-cancel-1",
        status="canceled",
    )
    client = TestClient(app)
    response = client.post(
        "/v1/runs/run-cancel-1:cancel",
        json={
            "reason": "manual_stop",
            "cascade_children": False,
            "canceled_by_run_id": "run-controller",
        },
        headers=auth_headers,
    )
    assert response.status_code == 200
    assert response.json()["payload"]["status"] == "canceled"
    mock_execution_client.cancel_run.assert_called_once_with(
        run_id="run-cancel-1",
        auth_token="delegated-token",
        reason="manual_stop",
        cascade_children=False,
        canceled_by_run_id="run-controller",
    )
    mock_token_exchange.assert_called_once()


def test_timeout_run_happy_path(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    auth_headers: dict[str, str],
) -> None:
    mock_execution_client.timeout_run.return_value = _run_status_event(
        run_id="run-timeout-1",
        status="timed_out",
    )
    client = TestClient(app)
    response = client.post(
        "/v1/runs/run-timeout-1:timeout",
        json={
            "reason": "deadline_exceeded",
            "cascade_children": True,
            "timed_out_by_run_id": "run-watchdog",
        },
        headers=auth_headers,
    )
    assert response.status_code == 200
    assert response.json()["payload"]["status"] == "timed_out"
    mock_execution_client.timeout_run.assert_called_once_with(
        run_id="run-timeout-1",
        auth_token="delegated-token",
        reason="deadline_exceeded",
        cascade_children=True,
        timed_out_by_run_id="run-watchdog",
    )
    mock_token_exchange.assert_called_once()


def test_preempt_run_happy_path(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    auth_headers: dict[str, str],
) -> None:
    mock_execution_client.preempt_run.return_value = _run_status_event(
        run_id="run-preempt-1",
        status="canceled",
    )
    client = TestClient(app)
    response = client.post(
        "/v1/runs/run-preempt-1:preempt",
        json={
            "reason": "resource_preempted",
            "cascade_children": True,
            "preempted_by_run_id": "run-priority-parent",
        },
        headers=auth_headers,
    )
    assert response.status_code == 200
    assert response.json()["payload"]["status"] == "canceled"
    mock_execution_client.preempt_run.assert_called_once_with(
        run_id="run-preempt-1",
        auth_token="delegated-token",
        reason="resource_preempted",
        cascade_children=True,
        preempted_by_run_id="run-priority-parent",
    )
    mock_token_exchange.assert_called_once()


def test_complete_run_happy_path(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    auth_headers: dict[str, str],
) -> None:
    mock_execution_client.complete_run.return_value = _run_status_event(
        run_id="run-complete-1",
        status="succeeded",
    )
    client = TestClient(app)
    response = client.post(
        "/v1/runs/run-complete-1:complete",
        json={"success": True},
        headers=auth_headers,
    )
    assert response.status_code == 200
    assert response.json()["payload"]["status"] == "succeeded"
    mock_execution_client.complete_run.assert_called_once_with(
        run_id="run-complete-1",
        auth_token="delegated-token",
        success=True,
        failure_reason_code=None,
    )
    mock_token_exchange.assert_called_once()


def test_complete_run_rejects_missing_success_field(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    auth_headers: dict[str, str],
) -> None:
    _ = mock_execution_client, mock_token_exchange
    client = TestClient(app)
    response = client.post(
        "/v1/runs/run-complete-2:complete",
        json={},
        headers=auth_headers,
    )
    assert response.status_code == 422
    assert "success must be boolean" in response.json()["detail"]


def test_complete_run_forwards_custom_failure_reason(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    auth_headers: dict[str, str],
) -> None:
    mock_execution_client.complete_run.return_value = _run_status_event(
        run_id="run-complete-failed-1",
        status="failed",
    )
    client = TestClient(app)
    response = client.post(
        "/v1/runs/run-complete-failed-1:complete",
        json={"success": False, "failure_reason_code": "tool_contract_violation"},
        headers=auth_headers,
    )
    assert response.status_code == 200
    assert response.json()["payload"]["status"] == "failed"
    mock_execution_client.complete_run.assert_called_once_with(
        run_id="run-complete-failed-1",
        auth_token="delegated-token",
        success=False,
        failure_reason_code="tool_contract_violation",
    )
    mock_token_exchange.assert_called_once()
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "runs.complete"
    assert audit["decision"] == "allow"
    assert audit["metadata"]["requested_success"] is False
    assert audit["metadata"]["requested_failure_reason_code"] == "tool_contract_violation"
    assert audit["metadata"]["downstream_status"] == "failed"


def test_complete_run_rejects_failure_reason_when_success_true(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    auth_headers: dict[str, str],
) -> None:
    _ = mock_execution_client, mock_token_exchange
    client = TestClient(app)
    response = client.post(
        "/v1/runs/run-complete-3:complete",
        json={"success": True, "failure_reason_code": "should_not_be_set"},
        headers=auth_headers,
    )
    assert response.status_code == 422
    assert "only allowed when success=false" in response.json()["detail"]


def test_renew_run_lease_happy_path(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    auth_headers: dict[str, str],
) -> None:
    mock_execution_client.renew_run_lease.return_value = _run_status_event(
        run_id="run-lease-renew-1",
        status="queued",
    )
    client = TestClient(app)
    response = client.post(
        "/v1/runs/run-lease-renew-1:lease-renew",
        json={"lease_ttl_seconds": 600},
        headers=auth_headers,
    )
    assert response.status_code == 200
    assert response.json()["payload"]["status"] == "queued"
    mock_execution_client.renew_run_lease.assert_called_once_with(
        run_id="run-lease-renew-1",
        auth_token="delegated-token",
        lease_ttl_seconds=600,
    )
    mock_token_exchange.assert_called_once()
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "runs.lease_renew"
    assert audit["decision"] == "allow"
    assert audit["metadata"]["requested_lease_ttl_seconds"] == 600


def test_renew_run_lease_rejects_invalid_ttl_type(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    auth_headers: dict[str, str],
) -> None:
    _ = mock_execution_client, mock_token_exchange
    client = TestClient(app)
    response = client.post(
        "/v1/runs/run-lease-renew-2:lease-renew",
        json={"lease_ttl_seconds": "600"},
        headers=auth_headers,
    )
    assert response.status_code == 422
    assert "lease_ttl_seconds must be integer" in response.json()["detail"]


def test_complete_run_downstream_error_includes_requested_failure_reason_in_audit(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    auth_headers: dict[str, str],
) -> None:
    mock_execution_client.complete_run.side_effect = RuntimeExecutionClientError(
        "HTTP 409 calling complete endpoint",
        status_code=409,
        response_body={
            "event_id": "evt-run-complete-fail",
            "event_type": "runtime.run.status",
            "tenant_id": "t1",
            "app_id": "covernow",
            "session_key": "tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
            "trace_id": "trace-test-control",
            "correlation_id": "run-complete-fail",
            "ts": datetime.now(timezone.utc).isoformat(),
            "payload": {"run_id": "run-complete-fail", "status": "failed"},
        },
    )
    client = TestClient(app)
    response = client.post(
        "/v1/runs/run-complete-fail:complete",
        json={"success": False, "failure_reason_code": "tool_contract_violation"},
        headers=auth_headers,
    )
    assert response.status_code == 409
    detail = response.json().get("detail")
    assert isinstance(detail, dict)
    assert detail["status_code"] == 409
    assert detail["downstream_event_type"] == "runtime.run.status"
    assert detail["run_id"] == "run-complete-fail"
    assert detail["status"] == "failed"
    assert "HTTP 409" in str(detail["message"])
    mock_token_exchange.assert_called_once()
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "runs.complete"
    assert audit["decision"] == "deny"
    assert audit["metadata"]["requested_success"] is False
    assert audit["metadata"]["requested_failure_reason_code"] == "tool_contract_violation"


def test_cancel_run_accepts_requested_by_run_id_alias(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    auth_headers: dict[str, str],
) -> None:
    mock_execution_client.cancel_run.return_value = _run_status_event(
        run_id="run-cancel-alias",
        status="canceled",
    )
    client = TestClient(app)
    response = client.post(
        "/v1/runs/run-cancel-alias:cancel",
        json={"requested_by_run_id": "run-shared-requester"},
        headers=auth_headers,
    )
    assert response.status_code == 200
    mock_execution_client.cancel_run.assert_called_once_with(
        run_id="run-cancel-alias",
        auth_token="delegated-token",
        reason=None,
        cascade_children=None,
        canceled_by_run_id="run-shared-requester",
    )
    mock_token_exchange.assert_called_once()


def test_cancel_run_downstream_error_is_mapped(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    auth_headers: dict[str, str],
) -> None:
    mock_execution_client.cancel_run.side_effect = RuntimeExecutionClientError(
        "HTTP 404 calling cancel endpoint",
        status_code=404,
        response_body={"error": "run not found"},
    )
    client = TestClient(app)
    response = client.post("/v1/runs/run-missing:cancel", headers=auth_headers)
    assert response.status_code == 404
    assert "404" in response.text
    mock_token_exchange.assert_called_once()


def test_timeout_run_connection_error_maps_to_502(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    auth_headers: dict[str, str],
) -> None:
    mock_execution_client.timeout_run.side_effect = RuntimeExecutionClientError(
        "connection error calling timeout endpoint",
        status_code=None,
    )
    client = TestClient(app)
    response = client.post("/v1/runs/run-timeout-2:timeout", headers=auth_headers)
    assert response.status_code == 502
    assert "connection error" in response.text
    mock_token_exchange.assert_called_once()


def test_preempt_run_accepts_requested_by_run_id_alias(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    auth_headers: dict[str, str],
) -> None:
    mock_execution_client.preempt_run.return_value = _run_status_event(
        run_id="run-preempt-alias",
        status="canceled",
    )
    client = TestClient(app)
    response = client.post(
        "/v1/runs/run-preempt-alias:preempt",
        json={"requested_by_run_id": "run-shared-requester"},
        headers=auth_headers,
    )
    assert response.status_code == 200
    mock_execution_client.preempt_run.assert_called_once_with(
        run_id="run-preempt-alias",
        auth_token="delegated-token",
        reason=None,
        cascade_children=None,
        preempted_by_run_id="run-shared-requester",
    )
    mock_token_exchange.assert_called_once()


def test_preempt_run_rejects_conflicting_requested_by_fields(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    auth_headers: dict[str, str],
) -> None:
    _ = (mock_execution_client, mock_token_exchange)
    client = TestClient(app)
    response = client.post(
        "/v1/runs/run-preempt-conflict:preempt",
        json={
            "preempted_by_run_id": "run-a",
            "requested_by_run_id": "run-b",
        },
        headers=auth_headers,
    )
    assert response.status_code == 422
    assert "must match when both are present" in response.json()["detail"]


def test_preempt_run_downstream_error_is_mapped(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    auth_headers: dict[str, str],
) -> None:
    mock_execution_client.preempt_run.side_effect = RuntimeExecutionClientError(
        "HTTP 409 calling preempt endpoint",
        status_code=409,
        response_body={"error": "preempt conflict"},
    )
    client = TestClient(app)
    response = client.post("/v1/runs/run-preempt-conflict:preempt", headers=auth_headers)
    assert response.status_code == 409
    assert "409" in response.text
    mock_token_exchange.assert_called_once()


def test_run_control_rejects_invalid_payload_type(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    auth_headers: dict[str, str],
) -> None:
    _ = (mock_execution_client, mock_token_exchange)
    client = TestClient(app)
    response = client.post(
        "/v1/runs/run-cancel-2:cancel",
        json={"cascade_children": "false"},
        headers=auth_headers,
    )
    assert response.status_code == 422
    assert "cascade_children must be boolean" in response.json()["detail"]


def test_cancel_run_missing_auth() -> None:
    client = TestClient(app)
    response = client.post("/v1/runs/run-1:cancel")
    assert response.status_code == 401


def test_timeout_run_missing_auth() -> None:
    client = TestClient(app)
    response = client.post("/v1/runs/run-1:timeout")
    assert response.status_code == 401


def test_preempt_run_missing_auth() -> None:
    client = TestClient(app)
    response = client.post("/v1/runs/run-1:preempt")
    assert response.status_code == 401


def test_complete_run_missing_auth() -> None:
    client = TestClient(app)
    response = client.post("/v1/runs/run-1:complete")
    assert response.status_code == 401
