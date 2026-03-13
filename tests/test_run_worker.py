"""Tests for worker orchestration endpoints."""

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
    monkeypatch.setattr("runtime_gateway.run_worker.exchange_subject_token", mock_exchange)
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
            "trace_id": "trace-worker",
        },
        ttl_seconds=300,
    )


@pytest.fixture
def auth_headers() -> dict[str, str]:
    token = _make_token(audience="runtime-gateway", scope=["runs:write"])
    return {
        "Authorization": f"Bearer {token}",
    }


@pytest.fixture
def read_auth_headers() -> dict[str, str]:
    token = _make_token(audience="runtime-gateway", scope=["runs:read"])
    return {
        "Authorization": f"Bearer {token}",
    }


def test_worker_tick_happy_path(
    mock_execution_client: Mock, mock_token_exchange: Mock, auth_headers: dict[str, str]
) -> None:
    mock_execution_client.worker_tick.return_value = {
        "outcome": "progressed",
        "leased_run_id": "run-1",
        "recommended_poll_after_ms": 1000,
    }

    client = TestClient(app)
    response = client.post("/v1/orchestration/worker:tick?fair=false&auto_start=false", headers=auth_headers)

    assert response.status_code == 200
    assert response.json()["outcome"] == "progressed"
    assert response.json()["recommended_poll_after_ms"] == 1000
    mock_execution_client.worker_tick.assert_called_once_with(
        auth_token="delegated-token",
        fair=False,
        auto_start=False,
    )
    mock_token_exchange.assert_called_once()
    assert mock_token_exchange.call_args.kwargs["scope"] == ["runs:write"]


def test_worker_health_happy_path(
    mock_execution_client: Mock, mock_token_exchange: Mock, read_auth_headers: dict[str, str]
) -> None:
    mock_execution_client.worker_health.return_value = {
        "queue_depth": 0,
        "ticks_total": 12,
        "idle_ticks_total": 4,
        "progressed_ticks_total": 8,
        "missing_run_ticks_total": 0,
        "skipped_ticks_total": 0,
        "drain_calls_total": 3,
        "drain_processed_total": 17,
        "last_tick_at": "2026-03-12T03:00:00+00:00",
        "last_drain_at": "2026-03-12T03:01:00+00:00",
        "last_tick_outcome": "progressed",
        "last_tick_age_seconds": 2.5,
        "last_heartbeat_at": "2026-03-12T03:01:00+00:00",
        "last_heartbeat_age_seconds": 2.5,
        "is_heartbeat_stale": False,
        "is_tick_stale": False,
        "is_backlogged": False,
        "is_stalled": False,
        "health_state": "healthy",
        "lifecycle_state": "running",
        "is_running": True,
        "last_transition": "start",
        "last_transition_at": "2026-03-12T03:00:00+00:00",
        "start_total": 1,
        "stop_total": 0,
        "restart_total": 0,
        "lease_renew_signal": {
            "attempted": 2,
            "renewed": 1,
            "errors": 1,
            "expired_conflicts": 0,
            "released_conflicts": 0,
            "last_observed_at": "2026-03-12T03:01:02+00:00",
            "total_attempted": 9,
            "total_renewed": 7,
            "total_errors": 2,
            "total_expired_conflicts": 1,
            "total_released_conflicts": 0,
        },
        "recommended_poll_after_ms": 5000,
    }

    client = TestClient(app)
    response = client.get("/v1/orchestration/worker:health", headers=read_auth_headers)

    assert response.status_code == 200
    payload = response.json()
    assert payload["ticks_total"] == 12
    assert payload["last_tick_outcome"] == "progressed"
    assert payload["drain_processed_total"] == 17
    assert payload["is_stalled"] is False
    assert payload["health_state"] == "healthy"
    assert payload["lifecycle_state"] == "running"
    assert payload["lease_renew_signal"]["attempted"] == 2
    assert payload["lease_renew_signal"]["renewed"] == 1
    assert payload["lease_renew_signal"]["errors"] == 1
    assert payload["lease_renew_signal"]["total_attempted"] == 9
    assert payload["lease_renew_signal"]["total_renewed"] == 7
    assert payload["lease_renew_signal"]["total_errors"] == 2
    assert payload["recommended_poll_after_ms"] == 5000
    mock_execution_client.worker_health.assert_called_once_with(
        auth_token="delegated-token",
    )
    mock_token_exchange.assert_called_once()
    assert mock_token_exchange.call_args.kwargs["scope"] == ["runs:read"]


def test_worker_drain_happy_path(
    mock_execution_client: Mock, mock_token_exchange: Mock, auth_headers: dict[str, str]
) -> None:
    mock_execution_client.worker_drain.return_value = {
        "processed": 2,
        "remaining": 1,
        "should_continue": True,
        "recommended_poll_after_ms": 250,
        "outcome_counts": {"progressed": 2, "missing_run": 0, "skipped": 0},
        "anomaly_ratio": 0.0,
        "progressed_ratio": 1.0,
        "stalled_signal": False,
    }

    client = TestClient(app)
    response = client.post("/v1/orchestration/worker:drain?max_items=2", headers=auth_headers)

    assert response.status_code == 200
    payload = response.json()
    assert payload["processed"] == 2
    assert payload["remaining"] == 1
    assert payload["should_continue"] is True
    assert payload["recommended_poll_after_ms"] == 250
    assert payload["outcome_counts"]["progressed"] == 2
    assert payload["anomaly_ratio"] == 0.0
    assert payload["stalled_signal"] is False
    mock_execution_client.worker_drain.assert_called_once_with(
        auth_token="delegated-token",
        max_items=2,
        fair=True,
        auto_start=True,
    )
    mock_token_exchange.assert_called_once()
    assert mock_token_exchange.call_args.kwargs["scope"] == ["runs:write"]


def test_worker_loop_happy_path(
    mock_execution_client: Mock, mock_token_exchange: Mock, auth_headers: dict[str, str]
) -> None:
    mock_execution_client.worker_loop.return_value = {
        "scheduler_processed": 1,
        "scheduler_promoted": 1,
        "processed": 1,
        "remaining": 0,
        "recommended_poll_after_ms": 250,
    }

    client = TestClient(app)
    response = client.post(
        "/v1/orchestration/worker:loop?scheduler_max_items=4&scheduler_fair=false&worker_max_items=2&worker_fair=false&auto_start=false",
        headers=auth_headers,
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["scheduler_processed"] == 1
    assert payload["scheduler_promoted"] == 1
    assert payload["processed"] == 1
    assert payload["recommended_poll_after_ms"] == 250
    mock_execution_client.worker_loop.assert_called_once_with(
        auth_token="delegated-token",
        scheduler_max_items=4,
        scheduler_fair=False,
        worker_max_items=2,
        worker_fair=False,
        auto_start=False,
    )
    mock_token_exchange.assert_called_once()
    assert mock_token_exchange.call_args.kwargs["scope"] == ["runs:write"]


def test_worker_lifecycle_happy_path(
    mock_execution_client: Mock,
    mock_token_exchange: Mock,
    auth_headers: dict[str, str],
    read_auth_headers: dict[str, str],
) -> None:
    mock_execution_client.worker_stop.return_value = {
        "action": "stop",
        "lifecycle_state": "stopped",
        "changed": True,
        "recommended_poll_after_ms": 1500,
    }
    mock_execution_client.worker_start.return_value = {
        "action": "start",
        "lifecycle_state": "running",
        "changed": True,
        "recommended_poll_after_ms": 5000,
    }
    mock_execution_client.worker_restart.return_value = {
        "action": "restart",
        "lifecycle_state": "running",
        "changed": True,
        "restart_total": 1,
        "recommended_poll_after_ms": 5000,
    }
    mock_execution_client.worker_status.return_value = {
        "lifecycle_state": "running",
        "is_running": True,
        "lifecycle_health": "healthy",
        "queue_depth": 0,
        "last_transition": "restart",
        "last_transition_at": "2026-03-12T03:02:00+00:00",
        "last_heartbeat_at": "2026-03-12T03:02:00+00:00",
        "last_heartbeat_age_seconds": 1.0,
        "is_heartbeat_stale": False,
        "ticks_total": 12,
        "drain_calls_total": 3,
        "start_total": 2,
        "stop_total": 1,
        "restart_total": 1,
        "recommended_poll_after_ms": 5000,
    }

    client = TestClient(app)
    stop = client.post("/v1/orchestration/worker:stop", json={"reason": "maintenance"}, headers=auth_headers)
    start = client.post("/v1/orchestration/worker:start", json={"reason": "resume"}, headers=auth_headers)
    restart = client.post("/v1/orchestration/worker:restart", json={"reason": "refresh"}, headers=auth_headers)
    status = client.get("/v1/orchestration/worker:status", headers=read_auth_headers)

    assert stop.status_code == 200
    assert stop.json()["lifecycle_state"] == "stopped"
    assert start.status_code == 200
    assert start.json()["action"] == "start"
    assert restart.status_code == 200
    assert restart.json()["action"] == "restart"
    assert status.status_code == 200
    assert status.json()["is_running"] is True
    assert status.json()["lifecycle_health"] == "healthy"
    assert status.json()["queue_depth"] == 0
    mock_execution_client.worker_stop.assert_called_once_with(
        auth_token="delegated-token",
        reason="maintenance",
    )
    mock_execution_client.worker_start.assert_called_once_with(
        auth_token="delegated-token",
        reason="resume",
    )
    mock_execution_client.worker_restart.assert_called_once_with(
        auth_token="delegated-token",
        reason="refresh",
    )
    mock_execution_client.worker_status.assert_called_once_with(auth_token="delegated-token")
    assert mock_token_exchange.call_count == 4


def test_worker_drain_downstream_4xx_error(
    mock_execution_client: Mock, mock_token_exchange: Mock, auth_headers: dict[str, str]
) -> None:
    _ = mock_token_exchange
    mock_execution_client.worker_drain.side_effect = RuntimeExecutionClientError(
        "HTTP 422 calling worker drain endpoint",
        status_code=422,
        response_body={
            "detail": "max_items must be integer > 0",
            "processed": 0,
            "remaining": 3,
            "should_continue": False,
            "stalled_signal": True,
            "anomaly_ratio": 1.0,
            "progressed_ratio": 0.0,
            "outcome_counts": {"progressed": 0, "missing_run": 2, "skipped": 1},
            "anomaly_counts": {"missing_run": 2, "skipped": 1, "total": 3},
            "lease_renew_signal": {
                "attempted": 1,
                "renewed": 0,
                "errors": 1,
                "expired_conflicts": 0,
                "released_conflicts": 0,
            },
            "scheduling_signal": {
                "queue_depth_before": 3,
                "queue_depth_after": 3,
                "processed": 0,
                "max_items": 1,
                "remaining": 3,
                "should_continue": False,
                "stalled_signal": True,
                "anomaly_ratio": 1.0,
                "recommended_poll_after_ms": 1000,
            },
        },
    )

    client = TestClient(app)
    response = client.post("/v1/orchestration/worker:drain?max_items=0", headers=auth_headers)

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert isinstance(detail, dict)
    assert detail["status_code"] == 422
    assert detail["downstream_detail"] == "max_items must be integer > 0"
    assert detail["processed"] == 0
    assert detail["remaining"] == 3
    assert detail["should_continue"] is False
    assert detail["stalled_signal"] is True
    assert detail["anomaly_ratio"] == 1.0
    assert detail["progressed_ratio"] == 0.0
    assert detail["outcome_counts"] == {"progressed": 0, "missing_run": 2, "skipped": 1}
    assert detail["anomaly_counts"] == {"missing_run": 2, "skipped": 1, "total": 3}
    assert detail["lease_renew_signal"] == {
        "attempted": 1,
        "renewed": 0,
        "errors": 1,
        "expired_conflicts": 0,
        "released_conflicts": 0,
    }
    assert detail["scheduling_signal"]["stalled_signal"] is True
    assert detail["scheduling_signal"]["recommended_poll_after_ms"] == 1000
    assert detail["recommended_poll_after_ms"] == 1000
    assert detail["queue_depth_before"] == 3
    assert detail["queue_depth_after"] == 3
    assert detail["max_items"] == 1
    assert "HTTP 422" in detail["message"]
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "orchestration.worker_drain"
    assert audit["decision"] == "deny"
    assert audit["metadata"]["status_code"] == 422
    assert audit["metadata"]["downstream_detail"] == "max_items must be integer > 0"
    assert audit["metadata"]["processed"] == 0
    assert audit["metadata"]["remaining"] == 3
    assert audit["metadata"]["should_continue"] is False
    assert audit["metadata"]["stalled_signal"] is True
    assert audit["metadata"]["anomaly_ratio"] == 1.0
    assert audit["metadata"]["progressed_ratio"] == 0.0
    assert audit["metadata"]["outcome_counts"] == {"progressed": 0, "missing_run": 2, "skipped": 1}
    assert audit["metadata"]["anomaly_counts"] == {"missing_run": 2, "skipped": 1, "total": 3}
    assert audit["metadata"]["lease_renew_signal"] == {
        "attempted": 1,
        "renewed": 0,
        "errors": 1,
        "expired_conflicts": 0,
        "released_conflicts": 0,
    }
    assert audit["metadata"]["scheduling_signal"]["stalled_signal"] is True
    assert audit["metadata"]["scheduling_signal"]["recommended_poll_after_ms"] == 1000
    assert audit["metadata"]["recommended_poll_after_ms"] == 1000
    assert audit["metadata"]["queue_depth_before"] == 3
    assert audit["metadata"]["queue_depth_after"] == 3
    assert audit["metadata"]["max_items"] == 1


def test_worker_tick_downstream_connection_error(
    mock_execution_client: Mock, mock_token_exchange: Mock, auth_headers: dict[str, str]
) -> None:
    _ = mock_token_exchange
    mock_execution_client.worker_tick.side_effect = RuntimeExecutionClientError(
        "connection error calling worker tick endpoint",
        status_code=None,
    )

    client = TestClient(app)
    response = client.post("/v1/orchestration/worker:tick", headers=auth_headers)

    assert response.status_code == 502
    assert "connection error" in response.text
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "orchestration.worker_tick"
    assert audit["decision"] == "deny"
    assert audit["metadata"]["status_code"] is None
    assert "connection error" in audit["metadata"]["reason"]
    assert "downstream_detail" not in audit["metadata"]


def test_worker_tick_downstream_4xx_error_is_structured(
    mock_execution_client: Mock, mock_token_exchange: Mock, auth_headers: dict[str, str]
) -> None:
    _ = mock_token_exchange
    mock_execution_client.worker_tick.side_effect = RuntimeExecutionClientError(
        "HTTP 409 calling worker tick endpoint",
        status_code=409,
        response_body={
            "detail": "worker is not running",
            "requested_action": "tick",
            "lifecycle_state": "stopped",
            "is_running": False,
            "can_start": True,
            "last_transition": "stop",
            "last_transition_at": "2026-03-13T00:00:00+00:00",
            "queue_depth": 3,
            "stalled_signal": True,
        },
    )

    client = TestClient(app)
    response = client.post("/v1/orchestration/worker:tick", headers=auth_headers)
    assert response.status_code == 409
    detail = response.json()["detail"]
    assert isinstance(detail, dict)
    assert detail["status_code"] == 409
    assert detail["downstream_detail"] == "worker is not running"
    assert detail["lifecycle_state"] == "stopped"
    assert detail["is_running"] is False
    assert detail["can_start"] is True
    assert detail["requested_action"] == "tick"
    assert detail["queue_depth"] == 3
    assert detail["stalled_signal"] is True
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "orchestration.worker_tick"
    assert audit["decision"] == "deny"
    assert audit["metadata"]["status_code"] == 409
    assert audit["metadata"]["downstream_detail"] == "worker is not running"
    assert audit["metadata"]["lifecycle_state"] == "stopped"
    assert audit["metadata"]["is_running"] is False
    assert audit["metadata"]["can_start"] is True
    assert audit["metadata"]["requested_action"] == "tick"
    assert audit["metadata"]["queue_depth"] == 3
    assert audit["metadata"]["stalled_signal"] is True


def test_worker_health_downstream_connection_error(
    mock_execution_client: Mock, mock_token_exchange: Mock, read_auth_headers: dict[str, str]
) -> None:
    _ = mock_token_exchange
    mock_execution_client.worker_health.side_effect = RuntimeExecutionClientError(
        "connection error calling worker health endpoint",
        status_code=None,
    )

    client = TestClient(app)
    response = client.get("/v1/orchestration/worker:health", headers=read_auth_headers)

    assert response.status_code == 502
    assert "connection error" in response.text
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "orchestration.worker_health"
    assert audit["decision"] == "deny"
    assert audit["metadata"]["status_code"] is None
    assert "connection error" in audit["metadata"]["reason"]
    assert "downstream_detail" not in audit["metadata"]


def test_worker_drain_downstream_connection_error(
    mock_execution_client: Mock, mock_token_exchange: Mock, auth_headers: dict[str, str]
) -> None:
    _ = mock_token_exchange
    mock_execution_client.worker_drain.side_effect = RuntimeExecutionClientError(
        "connection error calling worker drain endpoint",
        status_code=None,
    )

    client = TestClient(app)
    response = client.post("/v1/orchestration/worker:drain", headers=auth_headers)

    assert response.status_code == 502
    assert "connection error" in response.text
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "orchestration.worker_drain"
    assert audit["decision"] == "deny"
    assert audit["metadata"]["status_code"] is None
    assert "connection error" in audit["metadata"]["reason"]
    assert "downstream_detail" not in audit["metadata"]


def test_worker_health_downstream_4xx_error_is_structured(
    mock_execution_client: Mock, mock_token_exchange: Mock, read_auth_headers: dict[str, str]
) -> None:
    _ = mock_token_exchange
    mock_execution_client.worker_health.side_effect = RuntimeExecutionClientError(
        "HTTP 409 calling worker health endpoint",
        status_code=409,
        response_body={
            "detail": "worker stalled",
            "health_state": "stalled",
            "is_stalled": True,
            "queue_depth": 5,
        },
    )

    client = TestClient(app)
    response = client.get("/v1/orchestration/worker:health", headers=read_auth_headers)
    assert response.status_code == 409
    detail = response.json()["detail"]
    assert isinstance(detail, dict)
    assert detail["status_code"] == 409
    assert detail["downstream_detail"] == "worker stalled"
    assert detail["health_state"] == "stalled"
    assert detail["is_stalled"] is True
    assert detail["queue_depth"] == 5
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "orchestration.worker_health"
    assert audit["decision"] == "deny"
    assert audit["metadata"]["status_code"] == 409
    assert audit["metadata"]["downstream_detail"] == "worker stalled"
    assert audit["metadata"]["health_state"] == "stalled"
    assert audit["metadata"]["is_stalled"] is True
    assert audit["metadata"]["queue_depth"] == 5


def test_worker_health_rejects_invalid_downstream_contract(
    mock_execution_client: Mock, mock_token_exchange: Mock, read_auth_headers: dict[str, str]
) -> None:
    _ = mock_token_exchange
    mock_execution_client.worker_health.return_value = {
        "queue_depth": 1,
    }

    client = TestClient(app)
    response = client.get("/v1/orchestration/worker:health", headers=read_auth_headers)

    assert response.status_code == 502
    assert "invalid worker health response" in response.text
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "orchestration.worker_health"
    assert audit["decision"] == "deny"
    assert audit["metadata"]["validation_schema"] == "runtime/runtime-worker-health.v1.json"


def test_worker_status_rejects_invalid_downstream_contract(
    mock_execution_client: Mock, mock_token_exchange: Mock, read_auth_headers: dict[str, str]
) -> None:
    _ = mock_token_exchange
    mock_execution_client.worker_status.return_value = {
        "lifecycle_state": "running",
    }

    client = TestClient(app)
    response = client.get("/v1/orchestration/worker:status", headers=read_auth_headers)

    assert response.status_code == 502
    assert "invalid worker status response" in response.text
    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "orchestration.worker_status"
    assert audit["decision"] == "deny"
    assert audit["metadata"]["validation_schema"] == "runtime/runtime-worker-status.v1.json"


def test_worker_endpoints_require_bearer_token() -> None:
    client = TestClient(app)

    tick = client.post("/v1/orchestration/worker:tick")
    assert tick.status_code == 401

    drain = client.post("/v1/orchestration/worker:drain")
    assert drain.status_code == 401

    loop = client.post("/v1/orchestration/worker:loop")
    assert loop.status_code == 401

    health = client.get("/v1/orchestration/worker:health")
    assert health.status_code == 401

    start = client.post("/v1/orchestration/worker:start")
    assert start.status_code == 401

    stop = client.post("/v1/orchestration/worker:stop")
    assert stop.status_code == 401

    restart = client.post("/v1/orchestration/worker:restart")
    assert restart.status_code == 401

    status = client.get("/v1/orchestration/worker:status")
    assert status.status_code == 401


def test_worker_endpoints_require_runs_write_scope() -> None:
    token = _make_token(audience="runtime-gateway", scope=["runs:read"])
    headers = {"Authorization": f"Bearer {token}"}
    client = TestClient(app)

    tick = client.post("/v1/orchestration/worker:tick", headers=headers)
    assert tick.status_code == 403

    drain = client.post("/v1/orchestration/worker:drain", headers=headers)
    assert drain.status_code == 403

    loop = client.post("/v1/orchestration/worker:loop", headers=headers)
    assert loop.status_code == 403

    start = client.post("/v1/orchestration/worker:start", headers=headers)
    assert start.status_code == 403

    stop = client.post("/v1/orchestration/worker:stop", headers=headers)
    assert stop.status_code == 403

    restart = client.post("/v1/orchestration/worker:restart", headers=headers)
    assert restart.status_code == 403


def test_worker_health_requires_runs_read_scope() -> None:
    token = _make_token(audience="runtime-gateway", scope=["runs:write"])
    headers = {"Authorization": f"Bearer {token}"}
    client = TestClient(app)

    response = client.get("/v1/orchestration/worker:health", headers=headers)
    assert response.status_code == 403

    status = client.get("/v1/orchestration/worker:status", headers=headers)
    assert status.status_code == 403
