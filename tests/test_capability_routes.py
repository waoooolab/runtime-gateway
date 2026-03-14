"""Tests for capability forwarding endpoints in runtime-gateway."""

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


def _capability_event(*, event_type: str, capability_id: str, status: str) -> dict:
    return {
        "event_id": f"evt-{capability_id}-{status}",
        "event_type": event_type,
        "tenant_id": "t1",
        "app_id": "covernow",
        "session_key": "tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
        "trace_id": "trace-capability-test",
        "correlation_id": f"{capability_id}:{status}",
        "ts": datetime.now(timezone.utc).isoformat(),
        "payload": {
            "capability_id": capability_id,
            "capability_version": "1.0.0",
            "status": status,
        },
    }


@pytest.fixture(autouse=True)
def clear_gateway_state(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_audit_events()
    from runtime_gateway import app as app_module

    app_module._event_bus.clear()
    mock_client = Mock(spec=RuntimeExecutionClient)
    monkeypatch.setattr("runtime_gateway.app._execution_client", mock_client)
    mock_exchange = Mock(return_value={"access_token": "delegated-capability-token"})
    monkeypatch.setattr("runtime_gateway.capability_dispatch.exchange_subject_token", mock_exchange)


def _token(scope: list[str]) -> str:
    return issue_token(
        {
            "iss": "runtime-gateway",
            "sub": "user:u1",
            "aud": "runtime-gateway",
            "tenant_id": "t1",
            "app_id": "covernow",
            "scope": scope,
            "token_use": "access",
            "trace_id": "trace-capability-test",
        },
        ttl_seconds=300,
    )


def test_register_and_read_capability_forwarding() -> None:
    client = TestClient(app)
    from runtime_gateway import app as app_module

    app_module._execution_client.register_capability.return_value = {
        "status": "registered",
        "capability_id": "cap.demo",
        "version": "1.0.0",
    }
    app_module._execution_client.list_capabilities.return_value = {
        "count": 1,
        "items": [{"capability_id": "cap.demo", "latest_version": "1.0.0"}],
    }
    app_module._execution_client.get_capability.return_value = {
        "capability_id": "cap.demo",
        "latest_version": "1.0.0",
        "capability": {"kind": "app"},
    }

    write_headers = {"Authorization": f"Bearer {_token(['capabilities:write'])}"}
    read_headers = {"Authorization": f"Bearer {_token(['capabilities:read'])}"}

    register = client.post(
        "/v1/capabilities/register",
        json={"capability": {"capability_id": "cap.demo", "version": "1.0.0"}},
        headers=write_headers,
    )
    assert register.status_code == 200
    assert register.json()["status"] == "registered"

    listed = client.get("/v1/capabilities", headers=read_headers)
    assert listed.status_code == 200
    assert listed.json()["count"] == 1

    got = client.get("/v1/capabilities/cap.demo", headers=read_headers)
    assert got.status_code == 200
    assert got.json()["capability_id"] == "cap.demo"

    app_module._execution_client.register_capability.assert_called_once()
    app_module._execution_client.list_capabilities.assert_called_once()
    app_module._execution_client.get_capability.assert_called_once_with(
        capability_id="cap.demo",
        auth_token="delegated-capability-token",
    )


def test_compile_capability_publishes_event_to_gateway_bus() -> None:
    client = TestClient(app)
    from runtime_gateway import app as app_module

    app_module._execution_client.compile_capability.return_value = _capability_event(
        event_type="app.capability.compiled.v1",
        capability_id="cap.compile",
        status="compiled",
    )

    write_headers = {"Authorization": f"Bearer {_token(['capabilities:write'])}"}
    read_headers = {"Authorization": f"Bearer {_token(['capabilities:read'])}"}

    compile_response = client.post(
        "/v1/capabilities/compile",
        json={
            "capability_id": "cap.compile",
            "version": "1.0.0",
            "workflow_source": {
                "workflow_id": "wf-1",
                "workflow_version": "1",
                "graph_hash": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "mode": "node",
            },
        },
        headers=write_headers,
    )
    assert compile_response.status_code == 200
    assert compile_response.json()["event_type"] == "app.capability.compiled.v1"

    recent = client.get(
        "/v1/events/recent?event_types=app.capability.compiled.v1&limit=10",
        headers=read_headers,
    )
    assert recent.status_code == 200
    items = recent.json()["items"]
    assert len(items) >= 1
    assert items[-1]["event"]["event_type"] == "app.capability.compiled.v1"
    assert items[-1]["event"]["payload"]["capability_id"] == "cap.compile"

    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "capabilities.compile"
    assert audit["decision"] == "allow"
    assert audit["metadata"]["downstream_status"] == "compiled"


def test_invoke_capability_downstream_event_error_is_mapped_and_published() -> None:
    client = TestClient(app)
    from runtime_gateway import app as app_module

    app_module._execution_client.invoke_capability.side_effect = RuntimeExecutionClientError(
        "HTTP 404 calling capability invoke endpoint",
        status_code=404,
        response_body={
            **_capability_event(
                event_type="app.capability.invoke_failed.v1",
                capability_id="cap.missing",
                status="failed",
            ),
            "payload": {
                "capability_id": "cap.missing",
                "capability_version": "9.9.9",
                "status": "failed",
                "failure": {"code": "capability_not_found", "classification": "not_found"},
            },
        },
    )

    invoke_headers = {"Authorization": f"Bearer {_token(['capabilities:invoke'])}"}
    read_headers = {"Authorization": f"Bearer {_token(['capabilities:read'])}"}

    invoke = client.post(
        "/v1/capabilities/cap.missing:invoke",
        json={"version": "9.9.9"},
        headers=invoke_headers,
    )
    assert invoke.status_code == 404
    detail = invoke.json()["detail"]
    assert detail["status_code"] == 404
    assert detail["downstream_event_type"] == "app.capability.invoke_failed.v1"
    assert detail["capability_id"] == "cap.missing"

    recent = client.get(
        "/v1/events/recent?event_types=app.capability.invoke_failed.v1&limit=10",
        headers=read_headers,
    )
    assert recent.status_code == 200
    items = recent.json()["items"]
    assert len(items) >= 1
    assert items[-1]["event"]["event_type"] == "app.capability.invoke_failed.v1"

    audit = get_audit_events(limit=1)[0]
    assert audit["action"] == "capabilities.invoke"
    assert audit["decision"] == "deny"
    assert audit["metadata"]["downstream_event_type"] == "app.capability.invoke_failed.v1"


def test_capability_routes_require_matching_scope() -> None:
    client = TestClient(app)
    bad_headers = {"Authorization": f"Bearer {_token(['runs:write'])}"}

    register = client.post(
        "/v1/capabilities/register",
        json={"capability": {"capability_id": "cap.demo", "version": "1.0.0"}},
        headers=bad_headers,
    )
    assert register.status_code == 403
    assert "capabilities:write" in register.json()["detail"]

    listed = client.get("/v1/capabilities", headers=bad_headers)
    assert listed.status_code == 403
    assert "capabilities:read" in listed.json()["detail"]

    invoke = client.post(
        "/v1/capabilities/cap.demo:invoke",
        json={"version": "1.0.0"},
        headers=bad_headers,
    )
    assert invoke.status_code == 403
    assert "capabilities:invoke" in invoke.json()["detail"]
