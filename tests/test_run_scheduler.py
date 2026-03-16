"""Tests for scheduler orchestration endpoints."""

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
    monkeypatch.setattr("runtime_gateway.run_scheduler.exchange_subject_token", mock_exchange)
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
            "trace_id": "trace-scheduler",
        },
        ttl_seconds=300,
    )


@pytest.fixture
def auth_headers() -> dict[str, str]:
    token = _make_token(audience="runtime-gateway", scope=["runs:write"])
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def read_auth_headers() -> dict[str, str]:
    token = _make_token(audience="runtime-gateway", scope=["runs:read"])
    return {"Authorization": f"Bearer {token}"}


def test_scheduler_enqueue_tick_health_happy_path(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    auth_headers: dict[str, str],
    read_auth_headers: dict[str, str],
) -> None:
    _ = mock_token_exchange
    mock_execution_client.scheduler_enqueue.return_value = {
        "run_id": "run-1",
        "scheduler_depth": 1,
        "recommended_poll_after_ms": 250,
    }
    mock_execution_client.scheduler_tick.return_value = {
        "processed": 1,
        "promoted": 1,
        "deferred": 0,
        "scheduler_depth_after": 0,
        "orchestration_depth_after": 1,
        "recommended_poll_after_ms": 250,
    }
    mock_execution_client.scheduler_health.return_value = {
        "scheduler_depth": 0,
        "orchestration_depth": 1,
        "ticks_total": 1,
        "enqueue_total": 1,
        "recommended_poll_after_ms": 5000,
    }

    client = TestClient(app)
    enqueue = client.post(
        "/v1/orchestration/scheduler:enqueue",
        json={"run_id": "run-1", "delay_ms": 0, "reason": "manual"},
        headers=auth_headers,
    )
    assert enqueue.status_code == 200
    assert enqueue.json()["run_id"] == "run-1"
    mock_execution_client.scheduler_enqueue.assert_called_once_with(
        auth_token="delegated-token",
        run_id="run-1",
        due_at=None,
        delay_ms=0,
        reason="manual",
        misfire_policy=None,
        misfire_grace_ms=None,
        cron_interval_ms=None,
    )

    tick = client.post(
        "/v1/orchestration/scheduler:tick?max_items=5&fair=false",
        headers=auth_headers,
    )
    assert tick.status_code == 200
    assert tick.json()["promoted"] == 1
    mock_execution_client.scheduler_tick.assert_called_once_with(
        auth_token="delegated-token",
        max_items=5,
        fair=False,
    )

    health = client.get("/v1/orchestration/scheduler:health", headers=read_auth_headers)
    assert health.status_code == 200
    assert health.json()["ticks_total"] == 1
    mock_execution_client.scheduler_health.assert_called_once_with(auth_token="delegated-token")


def test_scheduler_tick_downstream_4xx_error_is_structured(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    auth_headers: dict[str, str],
) -> None:
    _ = mock_token_exchange
    mock_execution_client.scheduler_tick.side_effect = RuntimeExecutionClientError(
        "HTTP 422 calling scheduler tick endpoint",
        status_code=422,
        response_body={
            "detail": "max_items must be integer > 0",
            "max_items": 0,
            "processed": 0,
            "promoted": 0,
            "deferred": 0,
            "scheduler_depth_before": 0,
            "scheduler_depth_after": 0,
            "orchestration_depth_after": 0,
            "recommended_poll_after_ms": 5000,
        },
    )

    client = TestClient(app)
    response = client.post("/v1/orchestration/scheduler:tick?max_items=0", headers=auth_headers)
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert detail["status_code"] == 422
    assert detail["downstream_detail"] == "max_items must be integer > 0"
    assert detail["max_items"] == 0
    assert detail["processed"] == 0
    assert detail["scheduler_depth_before"] == 0
    assert detail["recommended_poll_after_ms"] == 5000
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "orchestration.scheduler_tick"
    assert audit["decision"] == "deny"
    assert audit["metadata"]["status_code"] == 422
    assert audit["metadata"]["downstream_detail"] == "max_items must be integer > 0"
    assert audit["metadata"]["max_items"] == 0


def test_scheduler_enqueue_downstream_connection_error(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    auth_headers: dict[str, str],
) -> None:
    _ = mock_token_exchange
    mock_execution_client.scheduler_enqueue.side_effect = RuntimeExecutionClientError(
        "connection error calling scheduler enqueue endpoint",
        status_code=None,
    )

    client = TestClient(app)
    response = client.post(
        "/v1/orchestration/scheduler:enqueue",
        json={"run_id": "run-connect"},
        headers=auth_headers,
    )
    assert response.status_code == 503
    detail = response.json().get("detail")
    assert isinstance(detail, dict)
    assert detail["status_code"] == 503
    assert detail["retryable"] is True
    assert detail["failure_classification"] == "upstream_unavailable"
    assert detail["upstream_error_class"] == "unavailable"
    assert "connection error" in str(detail["message"])
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "orchestration.scheduler_enqueue"
    assert audit["decision"] == "deny"
    assert audit["metadata"]["status_code"] == 503
    assert audit["metadata"]["retryable"] is True
    assert audit["metadata"]["failure_classification"] == "upstream_unavailable"
    assert audit["metadata"]["upstream_error_class"] == "unavailable"


def test_scheduler_tick_downstream_connection_error(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    auth_headers: dict[str, str],
) -> None:
    _ = mock_token_exchange
    mock_execution_client.scheduler_tick.side_effect = RuntimeExecutionClientError(
        "connection error calling scheduler tick endpoint",
        status_code=None,
    )

    client = TestClient(app)
    response = client.post("/v1/orchestration/scheduler:tick", headers=auth_headers)
    assert response.status_code == 503
    detail = response.json().get("detail")
    assert isinstance(detail, dict)
    assert detail["status_code"] == 503
    assert detail["retryable"] is True
    assert detail["failure_classification"] == "upstream_unavailable"
    assert detail["upstream_error_class"] == "unavailable"
    assert "connection error" in str(detail["message"])
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "orchestration.scheduler_tick"
    assert audit["decision"] == "deny"
    assert audit["metadata"]["status_code"] == 503
    assert audit["metadata"]["retryable"] is True
    assert audit["metadata"]["failure_classification"] == "upstream_unavailable"
    assert audit["metadata"]["upstream_error_class"] == "unavailable"


def test_scheduler_health_downstream_connection_error(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    read_auth_headers: dict[str, str],
) -> None:
    _ = mock_token_exchange
    mock_execution_client.scheduler_health.side_effect = RuntimeExecutionClientError(
        "connection error calling scheduler health endpoint",
        status_code=None,
    )

    client = TestClient(app)
    response = client.get("/v1/orchestration/scheduler:health", headers=read_auth_headers)
    assert response.status_code == 503
    detail = response.json().get("detail")
    assert isinstance(detail, dict)
    assert detail["status_code"] == 503
    assert detail["retryable"] is True
    assert detail["failure_classification"] == "upstream_unavailable"
    assert detail["upstream_error_class"] == "unavailable"
    assert "connection error" in str(detail["message"])
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "orchestration.scheduler_health"
    assert audit["decision"] == "deny"
    assert audit["metadata"]["status_code"] == 503
    assert audit["metadata"]["retryable"] is True
    assert audit["metadata"]["failure_classification"] == "upstream_unavailable"
    assert audit["metadata"]["upstream_error_class"] == "unavailable"


def test_scheduler_endpoints_require_bearer_token() -> None:
    client = TestClient(app)
    assert client.post("/v1/orchestration/scheduler:enqueue", json={"run_id": "run-1"}).status_code == 401
    assert client.post("/v1/orchestration/scheduler:tick").status_code == 401
    assert client.get("/v1/orchestration/scheduler:health").status_code == 401


def test_scheduler_endpoints_scope_requirements() -> None:
    read_token = _make_token(audience="runtime-gateway", scope=["runs:read"])
    write_token = _make_token(audience="runtime-gateway", scope=["runs:write"])
    read_headers = {"Authorization": f"Bearer {read_token}"}
    write_headers = {"Authorization": f"Bearer {write_token}"}
    client = TestClient(app)

    enqueue = client.post(
        "/v1/orchestration/scheduler:enqueue",
        json={"run_id": "run-1"},
        headers=read_headers,
    )
    assert enqueue.status_code == 403
    tick = client.post("/v1/orchestration/scheduler:tick", headers=read_headers)
    assert tick.status_code == 403

    health = client.get("/v1/orchestration/scheduler:health", headers=write_headers)
    assert health.status_code == 403


def test_scheduler_enqueue_forwards_misfire_and_cron_settings(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    auth_headers: dict[str, str],
) -> None:
    _ = mock_token_exchange
    mock_execution_client.scheduler_enqueue.return_value = {
        "run_id": "run-misfire-1",
        "scheduler_depth": 1,
        "misfire_policy": "skip",
        "misfire_grace_ms": 500,
        "cron_interval_ms": 1000,
        "recommended_poll_after_ms": 250,
    }

    client = TestClient(app)
    response = client.post(
        "/v1/orchestration/scheduler:enqueue",
        json={
            "run_id": "run-misfire-1",
            "due_at": "2026-03-13T00:00:00+00:00",
            "misfire_policy": "skip",
            "misfire_grace_ms": 500,
            "cron_interval_ms": 1000,
        },
        headers=auth_headers,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["misfire_policy"] == "skip"
    assert payload["misfire_grace_ms"] == 500
    assert payload["cron_interval_ms"] == 1000
    mock_execution_client.scheduler_enqueue.assert_called_once_with(
        auth_token="delegated-token",
        run_id="run-misfire-1",
        due_at="2026-03-13T00:00:00+00:00",
        delay_ms=None,
        reason=None,
        misfire_policy="skip",
        misfire_grace_ms=500,
        cron_interval_ms=1000,
    )
