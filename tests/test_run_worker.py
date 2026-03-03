"""Tests for worker orchestration endpoints."""

from __future__ import annotations

import os
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


def test_worker_tick_happy_path(
    mock_execution_client: Mock, mock_token_exchange: Mock, auth_headers: dict[str, str]
) -> None:
    mock_execution_client.worker_tick.return_value = {
        "outcome": "progressed",
        "leased_run_id": "run-1",
    }

    client = TestClient(app)
    response = client.post("/v1/orchestration/worker:tick?fair=false&auto_start=false", headers=auth_headers)

    assert response.status_code == 200
    assert response.json()["outcome"] == "progressed"
    mock_execution_client.worker_tick.assert_called_once_with(
        auth_token="delegated-token",
        fair=False,
        auto_start=False,
    )
    mock_token_exchange.assert_called_once()
    assert mock_token_exchange.call_args.kwargs["scope"] == ["runs:write"]


def test_worker_drain_happy_path(
    mock_execution_client: Mock, mock_token_exchange: Mock, auth_headers: dict[str, str]
) -> None:
    mock_execution_client.worker_drain.return_value = {
        "processed": 2,
        "remaining": 1,
        "should_continue": True,
    }

    client = TestClient(app)
    response = client.post("/v1/orchestration/worker:drain?max_items=2", headers=auth_headers)

    assert response.status_code == 200
    payload = response.json()
    assert payload["processed"] == 2
    assert payload["remaining"] == 1
    assert payload["should_continue"] is True
    mock_execution_client.worker_drain.assert_called_once_with(
        auth_token="delegated-token",
        max_items=2,
        fair=True,
        auto_start=True,
    )
    mock_token_exchange.assert_called_once()
    assert mock_token_exchange.call_args.kwargs["scope"] == ["runs:write"]


def test_worker_drain_downstream_4xx_error(
    mock_execution_client: Mock, mock_token_exchange: Mock, auth_headers: dict[str, str]
) -> None:
    _ = mock_token_exchange
    mock_execution_client.worker_drain.side_effect = RuntimeExecutionClientError(
        "HTTP 422 calling worker drain endpoint",
        status_code=422,
    )

    client = TestClient(app)
    response = client.post("/v1/orchestration/worker:drain?max_items=0", headers=auth_headers)

    assert response.status_code == 422
    assert "422" in response.text


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


def test_worker_endpoints_require_bearer_token() -> None:
    client = TestClient(app)

    tick = client.post("/v1/orchestration/worker:tick")
    assert tick.status_code == 401

    drain = client.post("/v1/orchestration/worker:drain")
    assert drain.status_code == 401


def test_worker_endpoints_require_runs_write_scope() -> None:
    token = _make_token(audience="runtime-gateway", scope=["runs:read"])
    headers = {"Authorization": f"Bearer {token}"}
    client = TestClient(app)

    tick = client.post("/v1/orchestration/worker:tick", headers=headers)
    assert tick.status_code == 403

    drain = client.post("/v1/orchestration/worker:drain", headers=headers)
    assert drain.status_code == 403
