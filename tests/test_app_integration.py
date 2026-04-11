from __future__ import annotations

import io
import os
import tempfile
import urllib.error
import unittest
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from runtime_gateway.audit.emitter import clear_audit_events, get_audit_events
from runtime_gateway.auth.tokens import issue_token
from runtime_gateway.execution_ingress_contract import (
    RUN_SUBMIT_SURFACE,
    SCHEDULER_CANCEL_SURFACE,
    SCHEDULER_ENQUEUE_SURFACE,
    SCHEDULER_HEALTH_SURFACE,
    SCHEDULER_REGISTRY_SURFACE,
    SCHEDULER_TICK_SURFACE,
)
from runtime_gateway.integration import RuntimeExecutionClientError, RuntimeExecutionClientPool

os.environ["WAOOOOLAB_PLATFORM_CONTRACTS_DIR"] = str(
    Path(__file__).resolve().parent / "fixtures" / "contracts"
)

try:
    from fastapi.testclient import TestClient
    from runtime_gateway import app as gateway_app_module
except ModuleNotFoundError:
    FASTAPI_STACK_AVAILABLE = False
else:
    FASTAPI_STACK_AVAILABLE = True


@dataclass
class _FakeExecutionClient:
    last_submit: dict | None = None
    last_contract_retirement_status: dict | None = None
    last_contract_retirement_validate: dict | None = None

    def submit_command(self, *, envelope: dict, auth_token: str) -> dict:
        self.last_submit = {
            "envelope": envelope,
            "auth_token": auth_token,
        }
        return {
            "event_id": "evt-run-1",
            "event_type": "runtime.run.requested",
            "tenant_id": str(envelope["tenant_id"]),
            "app_id": str(envelope["app_id"]),
            "session_key": str(envelope["session_key"]),
            "trace_id": str(envelope["trace_id"]),
            "correlation_id": str(envelope["command_id"]),
            "ts": "2026-03-01T12:00:00+00:00",
            "payload": {
                "run_id": "run-test-integration",
                "status": "queued",
                "retry_attempts": 0,
            },
        }

    def contract_retirement_status(
        self,
        *,
        auth_token: str,
        version_type: str | None = None,
        version: str | None = None,
    ) -> dict:
        self.last_contract_retirement_status = {
            "auth_token": auth_token,
            "version_type": version_type,
            "version": version,
        }
        return {
            "schema_version": "runtime.contract_retirement_status.v1",
            "observed_at": "2026-03-26T00:00:00+00:00",
            "version_type_filter": version_type,
            "version_filter": version,
            "versions": {
                "task_contract_version": [
                    {
                        "version_type": "task_contract_version",
                        "version": "task-envelope.v1",
                        "total_runs": 2,
                        "active_runs": 1,
                        "terminal_runs": 1,
                        "status_counts": {
                            "requested": 0,
                            "queued": 0,
                            "dispatching": 0,
                            "running": 1,
                            "waiting_approval": 0,
                            "retrying": 0,
                            "succeeded": 1,
                            "failed": 0,
                            "dlq": 0,
                            "canceled": 0,
                            "timed_out": 0,
                        },
                    }
                ]
            },
        }

    def contract_retirement_validate(
        self,
        *,
        auth_token: str,
        body: dict | None = None,
    ) -> dict:
        self.last_contract_retirement_validate = {
            "auth_token": auth_token,
            "body": body if isinstance(body, dict) else {},
        }
        return {
            "schema_version": "runtime.contract_retirement_gate.v1",
            "observed_at": "2026-03-26T00:00:00+00:00",
            "requested": body if isinstance(body, dict) else {},
            "eligible_to_retire": False,
            "eligible": [],
            "blocked": [
                {
                    "version_type": "task_contract_version",
                    "version": "task-envelope.v1",
                    "active_runs": 1,
                    "total_runs": 2,
                    "reason_code": "contract_retirement_blocked_active_bindings",
                }
            ],
        }


@dataclass
class _FakeExecutionClientContractVersionDrift:
    def submit_command(self, *, envelope: dict, auth_token: str) -> dict:
        _ = auth_token
        return {
            "event_id": "evt-run-drift-1",
            "event_type": "runtime.run.requested",
            "tenant_id": str(envelope["tenant_id"]),
            "app_id": str(envelope["app_id"]),
            "session_key": str(envelope["session_key"]),
            "trace_id": str(envelope["trace_id"]),
            "correlation_id": str(envelope["command_id"]),
            "task_contract_version": str(envelope.get("task_contract_version", "task-envelope.v1")),
            "agent_contract_version": str(envelope.get("agent_contract_version", "assistant-decision.v1")),
            "event_schema_version": "event-envelope.v999",
            "ts": "2026-03-01T12:00:00+00:00",
            "payload": {
                "run_id": "run-test-contract-version-drift",
                "status": "queued",
                "retry_attempts": 0,
            },
        }


@dataclass
class _FakeExecutionClientComputeAccepted:
    def submit_command(self, *, envelope: dict, auth_token: str) -> dict:
        _ = auth_token
        return {
            "event_id": "evt-run-compute-1",
            "event_type": "runtime.run.requested",
            "tenant_id": str(envelope["tenant_id"]),
            "app_id": str(envelope["app_id"]),
            "session_key": str(envelope["session_key"]),
            "trace_id": str(envelope["trace_id"]),
            "correlation_id": str(envelope["command_id"]),
            "ts": "2026-03-01T12:00:00+00:00",
            "payload": {
                "run_id": "run-test-compute",
                "status": "queued",
                "retry_attempts": 0,
                "route": {
                    "event_type": "runtime.route.decided",
                    "execution_mode": "compute",
                    "route_target": "device-hub",
                    "placement_event_type": "device.lease.acquired",
                    "placement_reason_code": "local_preference_fallback",
                    "placement_reason": "no local device has free capacity; fallback to non-local device",
                    "placement_score": 0.42,
                    "placement_queue_depth": 3,
                    "placement_resource_snapshot": {"queue_depth": 3},
                },
            },
        }


@dataclass
class _FakeExecutionClientRejected:
    def submit_command(self, *, envelope: dict, auth_token: str) -> dict:
        _ = auth_token
        event = {
            "event_id": "evt-route-failed-1",
            "event_type": "runtime.route.failed",
            "tenant_id": str(envelope["tenant_id"]),
            "app_id": str(envelope["app_id"]),
            "session_key": str(envelope["session_key"]),
            "trace_id": str(envelope["trace_id"]),
            "correlation_id": str(envelope["command_id"]),
            "ts": datetime.now(timezone.utc).isoformat(),
            "payload": {
                "run_id": "run-test-failed",
                "task_id": "run-test-failed:root",
                "status": "queued",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {"tenant_id": "t1"},
                },
                "decision": {
                    "outcome": "rejected",
                    "route_target": "none",
                    "policy_version": "execution-profile.v1",
                    "reason": "no eligible device",
                    "reason_code": "capacity_exhausted",
                    "placement_event_type": "device.route.rejected",
                    "resource_snapshot": {
                        "eligible_devices": 1,
                        "active_leases": 1,
                        "available_slots": 0,
                    },
                },
                "failure": {
                    "code": "no_eligible_device",
                    "message": "no eligible device",
                    "classification": "capacity",
                    "details": ["compute route dispatch failed"],
                },
                "scheduling_signal": {
                    "recommended_poll_after_ms": 1200,
                },
            },
        }
        raise RuntimeExecutionClientError(
            "HTTP 409 calling runtime-execution",
            status_code=409,
            response_body=event,
        )


@dataclass
class _FakeExecutionClientRetryableCapacity:
    def submit_command(self, *, envelope: dict, auth_token: str) -> dict:
        _ = auth_token
        event = {
            "event_id": "evt-route-failed-2",
            "event_type": "runtime.route.failed",
            "tenant_id": str(envelope["tenant_id"]),
            "app_id": str(envelope["app_id"]),
            "session_key": str(envelope["session_key"]),
            "trace_id": str(envelope["trace_id"]),
            "correlation_id": str(envelope["command_id"]),
            "ts": datetime.now(timezone.utc).isoformat(),
            "payload": {
                "run_id": "run-test-retryable",
                "task_id": "run-test-retryable:root",
                "status": "queued",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {"tenant_id": "t1"},
                },
                "decision": {
                    "outcome": "rejected",
                    "route_target": "none",
                    "policy_version": "execution-profile.v1",
                    "reason": "device-hub overloaded",
                    "reason_code": "placement_throttled",
                    "placement_event_type": "device.route.rejected",
                },
                "failure": {
                    "code": "placement_throttled",
                    "message": "device-hub overloaded",
                    "classification": "capacity",
                    "details": ["retry with backoff"],
                },
                "scheduling_signal": {
                    "recommended_poll_after_ms": 1500,
                },
            },
        }
        raise RuntimeExecutionClientError(
            "HTTP 503 calling runtime-execution",
            status_code=503,
            response_body=event,
        )


@dataclass
class _FakeExecutionClientPolicyRejected:
    def submit_command(self, *, envelope: dict, auth_token: str) -> dict:
        _ = auth_token
        event = {
            "event_id": "evt-route-failed-policy-1",
            "event_type": "runtime.route.failed",
            "tenant_id": str(envelope["tenant_id"]),
            "app_id": str(envelope["app_id"]),
            "session_key": str(envelope["session_key"]),
            "trace_id": str(envelope["trace_id"]),
            "correlation_id": str(envelope["command_id"]),
            "ts": datetime.now(timezone.utc).isoformat(),
            "payload": {
                "run_id": "run-test-policy-rejected",
                "task_id": "run-test-policy-rejected:root",
                "status": "failed",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {
                        "tenant_id": "t1",
                        "required_capabilities": ["compute.comfyui.local", "model.flux.1"],
                    },
                },
                "decision": {
                    "outcome": "rejected",
                    "route_target": "none",
                    "policy_version": "execution-profile.v1",
                    "reason": "no eligible device satisfies required capabilities",
                    "reason_code": "required_capabilities_unavailable",
                    "placement_event_type": "device.route.rejected",
                },
                "failure": {
                    "code": "required_capabilities_unavailable",
                    "message": "no eligible device satisfies required capabilities",
                    "classification": "policy",
                    "details": ["compute route dispatch failed"],
                },
            },
        }
        raise RuntimeExecutionClientError(
            "HTTP 409 calling runtime-execution",
            status_code=409,
            response_body=event,
        )


@dataclass
class _FakeExecutionClientCapacityTerminal:
    def submit_command(self, *, envelope: dict, auth_token: str) -> dict:
        _ = auth_token
        event = {
            "event_id": "evt-route-failed-capacity-terminal-1",
            "event_type": "runtime.route.failed",
            "tenant_id": str(envelope["tenant_id"]),
            "app_id": str(envelope["app_id"]),
            "session_key": str(envelope["session_key"]),
            "trace_id": str(envelope["trace_id"]),
            "correlation_id": str(envelope["command_id"]),
            "ts": datetime.now(timezone.utc).isoformat(),
            "payload": {
                "run_id": "run-test-capacity-terminal",
                "task_id": "run-test-capacity-terminal:root",
                "status": "failed",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {"tenant_id": "t1"},
                },
                "decision": {
                    "outcome": "rejected",
                    "route_target": "none",
                    "policy_version": "execution-profile.v1",
                    "reason": "capacity exhausted and retry budget exhausted",
                    "reason_code": "capacity_exhausted",
                    "placement_event_type": "device.route.rejected",
                },
                "failure": {
                    "code": "capacity_exhausted",
                    "message": "capacity exhausted and retry budget exhausted",
                    "classification": "capacity",
                    "details": ["terminal dispatch capacity failure"],
                },
            },
        }
        raise RuntimeExecutionClientError(
            "HTTP 503 calling runtime-execution",
            status_code=503,
            response_body=event,
        )


@dataclass
class _FakeExecutionClientPolicyRejectedTerminalRetryable:
    def submit_command(self, *, envelope: dict, auth_token: str) -> dict:
        _ = auth_token
        event = {
            "event_id": "evt-route-failed-policy-terminal-retryable-1",
            "event_type": "runtime.route.failed",
            "tenant_id": str(envelope["tenant_id"]),
            "app_id": str(envelope["app_id"]),
            "session_key": str(envelope["session_key"]),
            "trace_id": str(envelope["trace_id"]),
            "correlation_id": str(envelope["command_id"]),
            "ts": datetime.now(timezone.utc).isoformat(),
            "payload": {
                "run_id": "run-test-policy-terminal-retryable",
                "task_id": "run-test-policy-terminal-retryable:root",
                "status": "rejected",
                "execution_profile": {
                    "execution_mode": "compute",
                    "inference_target": "none",
                    "resource_class": "gpu",
                    "placement_constraints": {"tenant_id": "t1"},
                },
                "decision": {
                    "outcome": "rejected",
                    "route_target": "none",
                    "policy_version": "execution-profile.v1",
                    "reason": "run already terminally rejected",
                    "reason_code": "policy_terminal_rejected",
                    "placement_event_type": "device.route.rejected",
                },
                "failure": {
                    "code": "policy_terminal_rejected",
                    "message": "run already terminally rejected",
                    "classification": "policy",
                    "details": ["terminal policy rejection"],
                },
            },
        }
        raise RuntimeExecutionClientError(
            "HTTP 503 calling runtime-execution",
            status_code=503,
            response_body=event,
        )


@dataclass
class _FakeExecutionClientTransportUnavailable:
    def submit_command(self, *, envelope: dict, auth_token: str) -> dict:
        _ = envelope, auth_token
        raise RuntimeExecutionClientError(
            "connection error calling runtime-execution: connection refused",
            status_code=None,
            detail={
                "category": "upstream_unavailable",
                "code": "upstream_connection_error",
                "retryable": True,
                "message": "connection refused",
            },
        )


@dataclass
class _FakeExecutionClientHttpErrorDetail:
    def submit_command(self, *, envelope: dict, auth_token: str) -> dict:
        _ = envelope, auth_token
        raise RuntimeExecutionClientError(
            "HTTP 500 calling runtime-execution: invalid route event",
            status_code=500,
            response_body={
                "detail": (
                    "invalid route event: 'agent-orchestrator' is not one of "
                    "['agent-orchestrator', 'device-hub', 'none']"
                )
            },
            detail=(
                "invalid route event: 'agent-orchestrator' is not one of "
                "['agent-orchestrator', 'device-hub', 'none']"
            ),
        )


@dataclass
class _FakeHealthResponse:
    status_code: int
    payload: bytes = b"{}"

    def __enter__(self) -> "_FakeHealthResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        _ = exc_type, exc, tb

    def getcode(self) -> int:
        return self.status_code

    def read(self) -> bytes:
        return self.payload


@unittest.skipUnless(FASTAPI_STACK_AVAILABLE, "fastapi stack not installed")
class AppIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_audit_events()
        gateway_app_module._event_bus.clear()
        gateway_app_module._direct_idempotency_index.clear()
        gateway_app_module._direct_idempotency_order.clear()
        self._original_execution_client = gateway_app_module._execution_client
        self.fake_execution_client = _FakeExecutionClient()
        gateway_app_module._execution_client = self.fake_execution_client
        self.client = TestClient(gateway_app_module.app)
        self.payload = {
            "tenant_id": "t1",
            "app_id": "covernow",
            "session_key": "tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
            "payload": {"goal": "build feature"},
        }

    def tearDown(self) -> None:
        gateway_app_module._execution_client = self._original_execution_client

    def _token(self, audience: str, scope: list[str]) -> str:
        return issue_token(
            {
                "iss": "runtime-gateway",
                "sub": "user:u1",
                "aud": audience,
                "tenant_id": "t1",
                "app_id": "covernow",
                "scope": scope,
                "token_use": "access",
                "trace_id": "trace-1",
            },
            ttl_seconds=300,
        )

    def _event_publish_envelope(
        self,
        *,
        event_id: str,
        event_type: str,
        correlation_id: str,
        payload: dict,
    ) -> dict:
        return {
            "event_id": event_id,
            "event_type": event_type,
            "tenant_id": "t1",
            "app_id": "covernow",
            "session_key": "tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
            "trace_id": "trace-1",
            "correlation_id": correlation_id,
            "ts": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
        }

    def test_runs_requires_bearer_token(self) -> None:
        response = self.client.post("/v1/runs", json=self.payload)
        self.assertEqual(response.status_code, 401)

    def test_runs_rejects_wrong_audience(self) -> None:
        token = self._token(audience="device-hub", scope=["runs:write"])
        response = self.client.post(
            "/v1/runs",
            json=self.payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 401)

    def test_runs_rejects_missing_scope(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:read"])
        response = self.client.post(
            "/v1/runs",
            json=self.payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 403)

    def test_runs_blocks_on_error_budget_red_header_gate(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        response = self.client.post(
            "/v1/runs",
            json=self.payload,
            headers={
                "Authorization": f"Bearer {token}",
                "X-OWA-Error-Budget-Level": "red",
                "X-OWA-Error-Budget-Action": "pause_new_dispatch",
                "X-OWA-Error-Budget-Reason-Codes": "worker_stalled",
            },
        )
        self.assertEqual(response.status_code, 503)
        detail = response.json().get("detail", {})
        self.assertEqual(detail.get("code"), "error_budget_red_dispatch_paused")
        self.assertEqual(detail.get("error_budget_level"), "red")
        self.assertEqual(detail.get("error_budget_action"), "pause_new_dispatch")
        self.assertIn("worker_stalled", detail.get("error_budget_reason_codes", []))
        self.assertIsNone(self.fake_execution_client.last_submit)
        audit_latest = get_audit_events(1)[0]
        self.assertEqual(audit_latest["action"], "runs.create.error_budget_gate")
        self.assertEqual(audit_latest["decision"], "deny")
        self.assertEqual(audit_latest["metadata"]["error_budget_reason_code"], "error_budget_red_dispatch_paused")

    def test_scheduler_enqueue_blocks_on_error_budget_red_header_gate(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        response = self.client.post(
            "/v1/orchestration/scheduler:enqueue",
            json={"run_id": "run-budget-blocked"},
            headers={
                "Authorization": f"Bearer {token}",
                "X-OWA-Error-Budget-Level": "red",
                "X-OWA-Error-Budget-Action": "pause_new_dispatch",
                "X-OWA-Error-Budget-Reason-Codes": "worker_stalled",
            },
        )
        self.assertEqual(response.status_code, 503)
        detail = response.json().get("detail", {})
        self.assertEqual(detail.get("code"), "error_budget_red_dispatch_paused")
        self.assertEqual(detail.get("error_budget_level"), "red")
        audit_latest = get_audit_events(1)[0]
        self.assertEqual(audit_latest["action"], "scheduler.enqueue.error_budget_gate")
        self.assertEqual(audit_latest["decision"], "deny")
        self.assertEqual(audit_latest["metadata"]["error_budget_reason_code"], "error_budget_red_dispatch_paused")

    @patch("runtime_gateway.app.urllib.request.urlopen")
    def test_readyz_reports_runtime_execution_dependency_ok(self, mock_urlopen) -> None:
        with patch.dict(
            os.environ,
            {"RUNTIME_EXECUTION_BASE_URL": "http://runtime-execution.internal:8003"},
            clear=False,
        ):
            mock_urlopen.return_value = _FakeHealthResponse(
                status_code=200,
                payload=b'{"status":"ok","service":"runtime-execution","scheduler_backend":"local","capability_backend":"local","workflow_dispatch_backend":"local"}',
            )
            response = self.client.get("/readyz")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["service"], "runtime-gateway")
        self.assertEqual(len(payload["dependencies"]), 1)
        dependency = payload["dependencies"][0]
        self.assertEqual(dependency["name"], "runtime-execution")
        self.assertEqual(dependency["status"], "ok")
        self.assertEqual(dependency["http_status"], 200)
        self.assertEqual(dependency["upstream_status"], "ok")
        self.assertEqual(
            dependency["upstream_backends"],
            {
                "scheduler": "local",
                "capability": "local",
                "workflow_dispatch": "local",
            },
        )
        self.assertEqual(
            dependency["target"],
            "http://runtime-execution.internal:8003/healthz",
        )

    @patch("runtime_gateway.app.urllib.request.urlopen")
    def test_readyz_returns_503_when_runtime_execution_unreachable(self, mock_urlopen) -> None:
        with patch.dict(
            os.environ,
            {"RUNTIME_EXECUTION_BASE_URL": "http://runtime-execution.internal:8003"},
            clear=False,
        ):
            mock_urlopen.side_effect = urllib.error.URLError("connection refused")
            response = self.client.get("/readyz")
        self.assertEqual(response.status_code, 503)
        payload = response.json()
        self.assertEqual(payload["status"], "not_ready")
        self.assertEqual(payload["service"], "runtime-gateway")
        self.assertEqual(len(payload["dependencies"]), 1)
        dependency = payload["dependencies"][0]
        self.assertEqual(dependency["name"], "runtime-execution")
        self.assertEqual(dependency["status"], "down")
        self.assertEqual(dependency["reason"], "unreachable")

    @patch("runtime_gateway.app.urllib.request.urlopen")
    def test_readyz_returns_503_when_runtime_execution_unhealthy(self, mock_urlopen) -> None:
        with patch.dict(
            os.environ,
            {"RUNTIME_EXECUTION_BASE_URL": "http://runtime-execution.internal:8003"},
            clear=False,
        ):
            mock_urlopen.side_effect = urllib.error.HTTPError(
                url="http://runtime-execution.internal:8003/healthz",
                code=503,
                msg="service unavailable",
                hdrs=None,
                fp=io.BytesIO(b'{"status":"degraded"}'),
            )
            response = self.client.get("/readyz")
        self.assertEqual(response.status_code, 503)
        payload = response.json()
        self.assertEqual(payload["status"], "not_ready")
        self.assertEqual(payload["service"], "runtime-gateway")
        self.assertEqual(len(payload["dependencies"]), 1)
        dependency = payload["dependencies"][0]
        self.assertEqual(dependency["name"], "runtime-execution")
        self.assertEqual(dependency["status"], "down")
        self.assertEqual(dependency["reason"], "upstream_unhealthy")
        self.assertEqual(dependency["http_status"], 503)
        self.assertEqual(dependency["upstream_status"], "degraded")

    @patch("runtime_gateway.app.urllib.request.urlopen")
    def test_runtime_usable_reports_gateway_summary_when_ready(self, mock_urlopen) -> None:
        with patch.dict(
            os.environ,
            {
                "RUNTIME_EXECUTION_BASE_URL": "http://runtime-execution.internal:8003",
                "RUNTIME_GATEWAY_NODE_ID": "gw-local",
                "RUNTIME_GATEWAY_WORKSPACE_ID": "workspace-main",
                "RUNTIME_GATEWAY_STICKY_LINEAGE_ENABLED": "true",
                "RUNTIME_GATEWAY_STICKY_LINEAGE_TTL_SECONDS": "1200",
            },
            clear=False,
        ):
            mock_urlopen.return_value = _FakeHealthResponse(
                status_code=200,
                payload=b'{"status":"ok","service":"runtime-execution","scheduler_backend":"local","capability_backend":"local","workflow_dispatch_backend":"local"}',
            )
            response = self.client.get("/v1/runtime/usable")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["schema_version"], "runtime_gateway_usable.v1")
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["service"], "runtime-gateway")
        self.assertEqual(payload["recommended_poll_after_ms"], 10000)
        self.assertEqual(payload["runtime_backpressure_contract_ok"], True)
        self.assertEqual(payload["runtime_backpressure_contract_failures"], 0)
        self.assertEqual(payload["runtime_worker_pool_contract_ok"], True)
        self.assertEqual(payload["runtime_worker_pool_contract_failures"], 0)
        self.assertEqual(payload["runtime_worker_pool_status_contract_ok"], True)
        self.assertEqual(payload["runtime_worker_pool_status_contract_failures"], 0)
        self.assertEqual(payload["runtime_usable_contract_surface_ok"], True)
        self.assertEqual(payload["runtime_usable_contract_surface_failures"], 0)
        self.assertEqual(
            payload["compatibility_aliases"]["runtime_worker_pool_status_contract"],
            "runtime_worker_pool_contract",
        )
        self.assertIn("generated_at", payload)
        self.assertEqual(len(payload["dependencies"]), 1)
        dependency = payload["dependencies"][0]
        self.assertEqual(dependency["name"], "runtime-execution")
        self.assertEqual(dependency["status"], "ok")
        self.assertEqual(
            dependency["upstream_backends"],
            {
                "scheduler": "local",
                "capability": "local",
                "workflow_dispatch": "local",
            },
        )
        event_bus = payload["event_bus"]
        self.assertIn("connections", event_bus)
        self.assertIn("buffered_events", event_bus)
        self.assertIn("next_seq", event_bus)
        federation_node = payload["federation_node"]
        self.assertEqual(federation_node["schema_version"], "runtime_gateway_federation_node.v1")
        self.assertEqual(federation_node["gateway_id"], "gw-local")
        self.assertEqual(federation_node["workspace_scope"]["workspace_id"], "workspace-main")
        self.assertTrue(federation_node["routing_policy"]["sticky_lineage_enabled"])
        self.assertEqual(federation_node["routing_policy"]["sticky_lineage_ttl_seconds"], 1200)
        self.assertEqual(federation_node["capacity_snapshot"]["runtime_worker_pool_contract_ok"], True)

    @patch("runtime_gateway.app.urllib.request.urlopen")
    def test_runtime_usable_returns_503_when_not_ready(self, mock_urlopen) -> None:
        with patch.dict(
            os.environ,
            {"RUNTIME_EXECUTION_BASE_URL": "http://runtime-execution.internal:8003"},
            clear=False,
        ):
            mock_urlopen.side_effect = urllib.error.URLError("connection refused")
            response = self.client.get("/v1/runtime/usable")
        self.assertEqual(response.status_code, 503)
        payload = response.json()
        self.assertEqual(payload["schema_version"], "runtime_gateway_usable.v1")
        self.assertEqual(payload["ok"], False)
        self.assertEqual(payload["status"], "not_ready")
        self.assertEqual(payload["recommended_poll_after_ms"], 1000)
        self.assertEqual(payload["runtime_backpressure_contract_ok"], False)
        self.assertEqual(payload["runtime_backpressure_contract_failures"], 1)
        self.assertEqual(payload["runtime_worker_pool_contract_ok"], False)
        self.assertEqual(payload["runtime_worker_pool_contract_failures"], 1)
        self.assertEqual(payload["runtime_worker_pool_status_contract_ok"], False)
        self.assertEqual(payload["runtime_worker_pool_status_contract_failures"], 1)
        self.assertEqual(payload["runtime_usable_contract_surface_ok"], False)
        self.assertEqual(payload["runtime_usable_contract_surface_failures"], 1)
        self.assertEqual(
            payload["compatibility_aliases"]["runtime_worker_pool_status_contract"],
            "runtime_worker_pool_contract",
        )
        self.assertEqual(payload["service"], "runtime-gateway")
        self.assertEqual(len(payload["dependencies"]), 1)
        dependency = payload["dependencies"][0]
        self.assertEqual(dependency["name"], "runtime-execution")
        self.assertEqual(dependency["status"], "down")
        self.assertEqual(dependency["reason"], "unreachable")
        federation_node = payload["federation_node"]
        self.assertEqual(federation_node["schema_version"], "runtime_gateway_federation_node.v1")
        self.assertEqual(federation_node["capacity_snapshot"]["runtime_worker_pool_contract_ok"], False)
        self.assertEqual(federation_node["capacity_snapshot"]["runtime_backpressure_contract_ok"], False)

    def test_executor_profiles_requires_runs_read_scope(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        response = self.client.get(
            "/v1/executors/profiles",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 403)

    def test_executor_profiles_returns_profile_catalog(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:read"])
        response = self.client.get(
            "/v1/executors/profiles",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("items", data)
        self.assertTrue(isinstance(data["items"], list))
        families = {item["family"] for item in data["items"]}
        self.assertIn("acp", families)
        self.assertIn("workflow_runtime", families)
        acp = next(item for item in data["items"] if item["family"] == "acp")
        self.assertEqual(acp["adapters"], ["orchestrator", "ccb"])
        self.assertEqual(acp["access_modes"], ["direct", "api"])
        self.assertEqual(acp["window_modes"], ["inline", "terminal_mux"])

    def test_executor_profiles_rejects_invalid_catalog_payload(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:read"])
        invalid_items = [
            {
                "family": "acp",
                "engines": ["claude_code"],
                "adapters": ["invalid_adapter"],
            }
        ]
        with patch.object(gateway_app_module, "list_executor_profiles", return_value=invalid_items):
            response = self.client.get(
                "/v1/executors/profiles",
                headers={"Authorization": f"Bearer {token}"},
            )

        self.assertEqual(response.status_code, 500)
        self.assertIn("invalid executor profile catalog", response.json()["detail"])

    def test_runs_accepts_valid_token(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        response = self.client.post(
            "/v1/runs",
            json=self.payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("run_id", data)
        self.assertEqual(data["status"], "queued")
        execution_ingress = data["execution_ingress"]
        self.assertEqual(execution_ingress["authority"], "runtime-gateway")
        self.assertEqual(execution_ingress["run_submit_surface"], RUN_SUBMIT_SURFACE)
        self.assertEqual(execution_ingress["scheduler"]["enqueue_surface"], SCHEDULER_ENQUEUE_SURFACE)
        self.assertEqual(execution_ingress["scheduler"]["tick_surface"], SCHEDULER_TICK_SURFACE)
        self.assertEqual(execution_ingress["scheduler"]["health_surface"], SCHEDULER_HEALTH_SURFACE)
        self.assertEqual(execution_ingress["scheduler"]["registry_surface"], SCHEDULER_REGISTRY_SURFACE)
        self.assertEqual(execution_ingress["scheduler"]["cancel_surface"], SCHEDULER_CANCEL_SURFACE)

    def test_runs_allow_audit_includes_compute_route_metadata(self) -> None:
        gateway_app_module._execution_client = _FakeExecutionClientComputeAccepted()
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        response = self.client.post(
            "/v1/runs",
            json=self.payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)

        audit_latest = get_audit_events(limit=1)[0]
        self.assertEqual(audit_latest["action"], "runs.dispatch")
        self.assertEqual(audit_latest["decision"], "allow")
        metadata = audit_latest["metadata"]
        self.assertEqual(metadata["execution_mode"], "compute")
        self.assertEqual(metadata["route_target"], "device-hub")
        self.assertEqual(metadata["placement_event_type"], "device.lease.acquired")
        self.assertEqual(metadata["placement_reason_code"], "local_preference_fallback")
        self.assertEqual(metadata["placement_queue_depth"], 3)
        self.assertEqual(metadata["placement_resource_snapshot"], {"queue_depth": 3})

    def test_runs_rejects_invalid_execution_context(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        payload = dict(self.payload)
        payload["payload"] = {
            "goal": "build feature",
            "execution_context": {
                "task_plane": "runtime_workload",
                "executor": {
                    "family": "acp",
                    "engine": "claude_code",
                    "adapter": "ccb",
                },
                "runtime": {
                    "execution_mode": "control",
                },
            },
        }
        response = self.client.post(
            "/v1/runs",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 422)
        self.assertIn("execution-context.v1.json", response.json()["detail"])
        self.assertIsNone(self.fake_execution_client.last_submit)

    def test_runs_rejects_invalid_orchestration_hints(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        payload = dict(self.payload)
        payload["payload"] = {
            "goal": "build feature",
            "orchestration": {
                "parent_task_id": "orphan:root",
            },
        }
        response = self.client.post(
            "/v1/runs",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 422)
        self.assertIn("orchestration-hints.v1.json", response.json()["detail"])
        self.assertIsNone(self.fake_execution_client.last_submit)

    def test_runs_forwards_nested_orchestration_contracts(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        payload = dict(self.payload)
        payload["payload"] = {
            "goal": "build feature",
            "orchestration": {
                "nested_leader_contract": {
                    "delegation_mode": "nested",
                    "lifecycle_ack_mode": "progress_and_completion",
                    "failure_takeover_mode": "outer_failover",
                    "completion_aggregation_mode": "both",
                    "ack_timeout_ms": 3000,
                    "max_failure_takeovers": 2,
                },
                "nested_autonomy_policy": {
                    "autonomy_level": "guided",
                    "suggestion_trigger_mode": "on_blocker",
                    "handoff_mode": "plan_and_constraints",
                    "max_inner_steps": 8,
                    "allow_inner_replan": True,
                    "require_outer_approval": False,
                },
            },
        }
        response = self.client.post(
            "/v1/runs",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)
        assert self.fake_execution_client.last_submit is not None
        envelope = self.fake_execution_client.last_submit["envelope"]
        forwarded_orchestration = envelope["payload"]["orchestration"]
        self.assertEqual(
            forwarded_orchestration["nested_leader_contract"]["delegation_mode"],
            "nested",
        )
        self.assertEqual(
            forwarded_orchestration["nested_autonomy_policy"]["handoff_mode"],
            "plan_and_constraints",
        )

    def test_runs_forwards_template_capability_binding_contract(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        payload = dict(self.payload)
        payload["payload"] = {
            "goal": "build feature",
            "template_capability_binding": {
                "schema_version": "workflow_template_capability_binding_contract.v1",
                "template_package": {
                    "package_id": "pkg.visual-starter",
                    "package_version": "1.0.0",
                    "template_id": "tpl.visual.pipeline",
                    "template_version": "2026.03.27",
                    "lifecycle_stage": "draft",
                },
                "capability_bindings": [
                    {
                        "binding_id": "bind-1",
                        "step_id": "phase-1:step-1",
                        "capability_id": "cap.image.upscale",
                        "capability_version": "1.2.0",
                        "binding_mode": "required",
                        "executor_profile": {
                            "family": "python",
                            "engine": "default",
                            "adapter": "runtime_api",
                        },
                    }
                ],
                "resolution_policy": {
                    "on_missing_capability": "deny",
                    "on_version_mismatch": "deny",
                    "on_compile_failure": "halt",
                },
                "acceptance_criteria": [
                    "binding_references_resolvable",
                    "package_lifecycle_declared",
                    "version_pinning_enforced",
                ],
            },
        }
        response = self.client.post(
            "/v1/runs",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)
        assert self.fake_execution_client.last_submit is not None
        envelope = self.fake_execution_client.last_submit["envelope"]
        binding = envelope["payload"]["template_capability_binding"]
        self.assertEqual(binding["template_package"]["package_id"], "pkg.visual-starter")
        self.assertEqual(binding["capability_bindings"][0]["capability_id"], "cap.image.upscale")

    def test_runs_rejects_invalid_template_capability_binding_contract(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        payload = dict(self.payload)
        payload["payload"] = {
            "goal": "build feature",
            "template_capability_binding": {
                "schema_version": "workflow_template_capability_binding_contract.v1",
                "template_package": {
                    "package_id": "pkg.visual-starter",
                    "package_version": "1.0.0",
                    "template_id": "tpl.visual.pipeline",
                    "template_version": "2026.03.27",
                    "lifecycle_stage": "draft",
                },
                "capability_bindings": [
                    {
                        "binding_id": "bind-1",
                        "step_id": "phase-1:step-1",
                        "capability_id": "cap.image.upscale",
                        "binding_mode": "required",
                        "executor_profile": {
                            "family": "python",
                            "engine": "default",
                            "adapter": "runtime_api",
                        },
                    }
                ],
                "resolution_policy": {
                    "on_missing_capability": "deny",
                    "on_version_mismatch": "deny",
                    "on_compile_failure": "halt",
                },
                "acceptance_criteria": [
                    "binding_references_resolvable",
                    "package_lifecycle_declared",
                    "version_pinning_enforced",
                ],
            },
        }
        response = self.client.post(
            "/v1/runs",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 422)
        self.assertIn(
            "workflow-template-capability-binding-contract.v1.json",
            str(response.json()["detail"]),
        )
        self.assertIsNone(self.fake_execution_client.last_submit)

    def test_runs_rejects_execution_context_mode_mismatch(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        payload = dict(self.payload)
        payload["payload"] = {
            "goal": "build feature",
            "execution_profile": {
                "execution_mode": "compute",
                "inference_target": "none",
                "resource_class": "gpu",
                "placement_constraints": {
                    "tenant_id": "t1",
                    "region": "us-west",
                    "cost_tier": "balanced",
                },
            },
            "execution_context": {
                "task_plane": "runtime_workload",
                "runtime": {
                    "execution_mode": "control",
                },
            },
        }
        response = self.client.post(
            "/v1/runs",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 422)
        self.assertIn("must match execution_profile.execution_mode", response.json()["detail"])
        self.assertIsNone(self.fake_execution_client.last_submit)

    def test_runs_accepts_runtime_id_hint_and_routes_pool_submit(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        calls: list[tuple[str, dict]] = []

        class _PoolFakeClient:
            def __init__(self, runtime_id: str, base_url: str):
                self.runtime_id = runtime_id
                self.base_url = base_url

            def submit_command(self, *, envelope: dict, auth_token: str) -> dict:
                _ = auth_token
                calls.append((self.runtime_id, envelope))
                return {
                    "event_id": "evt-run-runtime-hint-1",
                    "event_type": "runtime.run.requested",
                    "tenant_id": str(envelope["tenant_id"]),
                    "app_id": str(envelope["app_id"]),
                    "session_key": str(envelope["session_key"]),
                    "trace_id": str(envelope["trace_id"]),
                    "correlation_id": str(envelope["command_id"]),
                    "ts": "2026-04-01T00:00:00+00:00",
                    "payload": {
                        "run_id": f"run-{self.runtime_id}",
                        "status": "queued",
                        "retry_attempts": 0,
                    },
                }

        gateway_app_module._execution_client = RuntimeExecutionClientPool(
            route_table={
                "default_runtime_id": "rt-a",
                "runtimes": [
                    {"runtime_id": "rt-a", "base_url": "http://rt-a.local"},
                    {"runtime_id": "rt-b", "base_url": "http://rt-b.local"},
                ],
            },
            client_factory=lambda runtime_id, base_url: _PoolFakeClient(runtime_id, base_url),
        )
        payload = dict(self.payload)
        payload["payload"] = {
            "goal": "build feature",
            "execution_context": {
                "task_plane": "runtime_workload",
                "runtime": {
                    "execution_mode": "control",
                    "runtime_id": "rt-b",
                },
            },
        }
        response = self.client.post(
            "/v1/runs",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["run_id"], "run-rt-b")
        self.assertEqual(response.json()["status"], "queued")

        self.assertEqual(len(calls), 1)
        selected_runtime_id, forwarded_envelope = calls[0]
        self.assertEqual(selected_runtime_id, "rt-b")
        self.assertEqual(
            forwarded_envelope["payload"]["execution_context"]["runtime"]["runtime_id"],
            "rt-b",
        )

        audit_latest = get_audit_events(limit=1)[0]
        self.assertEqual(audit_latest["action"], "runs.dispatch")
        self.assertEqual(audit_latest["decision"], "allow")
        self.assertEqual(audit_latest["metadata"]["route_target"], "rt-b")

    def test_runs_rejects_unsupported_executor_engine_profile(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        payload = dict(self.payload)
        payload["payload"] = {
            "goal": "build feature",
            "execution_context": {
                "task_plane": "agent_work",
                "executor": {
                    "family": "acp",
                    "engine": "my_custom_cli",
                    "adapter": "ccb",
                },
            },
        }
        response = self.client.post(
            "/v1/runs",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 422)
        self.assertIn("unsupported for family 'acp'", response.json()["detail"])
        self.assertIsNone(self.fake_execution_client.last_submit)

    def test_runs_rejects_unsupported_executor_adapter_profile(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        payload = dict(self.payload)
        payload["payload"] = {
            "goal": "build feature",
            "execution_context": {
                "task_plane": "agent_work",
                "executor": {
                    "family": "acp",
                    "engine": "claude_code",
                    "adapter": "runtime_api",
                },
            },
        }
        response = self.client.post(
            "/v1/runs",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 422)
        self.assertIn("unsupported for family 'acp'", response.json()["detail"])
        self.assertIsNone(self.fake_execution_client.last_submit)

    def test_runs_propagates_trace_id_to_runtime_execution(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        response = self.client.post(
            "/v1/runs",
            json=self.payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)
        assert self.fake_execution_client.last_submit is not None
        self.assertEqual(self.fake_execution_client.last_submit["envelope"]["trace_id"], "trace-1")

    def test_runs_uses_default_retry_policy_when_not_provided(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        response = self.client.post(
            "/v1/runs",
            json=self.payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)
        assert self.fake_execution_client.last_submit is not None
        self.assertEqual(
            self.fake_execution_client.last_submit["envelope"]["retry_policy"],
            {
                "max_attempts": 3,
                "backoff_ms": 250,
                "strategy": "fixed",
            },
        )

    def test_runs_propagates_custom_retry_policy_to_runtime_execution(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        payload = dict(self.payload)
        payload["retry_policy"] = {
            "max_attempts": 7,
            "backoff_ms": 900,
            "strategy": "exponential",
        }
        response = self.client.post(
            "/v1/runs",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)
        assert self.fake_execution_client.last_submit is not None
        self.assertEqual(
            self.fake_execution_client.last_submit["envelope"]["retry_policy"],
            payload["retry_policy"],
        )

    def test_runs_propagates_contract_versions_to_runtime_execution(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        payload = dict(self.payload)
        payload["contract_versions"] = {
            "task_contract_version": "task-envelope.v1",
            "agent_contract_version": "assistant-decision.v1",
            "event_schema_version": "event-envelope.v1",
        }
        response = self.client.post(
            "/v1/runs",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)
        assert self.fake_execution_client.last_submit is not None
        envelope = self.fake_execution_client.last_submit["envelope"]
        self.assertEqual(envelope["task_contract_version"], "task-envelope.v1")
        self.assertEqual(envelope["agent_contract_version"], "assistant-decision.v1")
        self.assertEqual(envelope["event_schema_version"], "event-envelope.v1")

    def test_runs_binds_default_contract_versions_when_not_provided(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        response = self.client.post(
            "/v1/runs",
            json=self.payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)
        assert self.fake_execution_client.last_submit is not None
        envelope = self.fake_execution_client.last_submit["envelope"]
        self.assertEqual(envelope["task_contract_version"], "task-envelope.v1")
        self.assertEqual(envelope["agent_contract_version"], "assistant-decision.v1")
        self.assertEqual(envelope["event_schema_version"], "event-envelope.v1")

    def test_runs_derives_scope_axis_from_session_key_by_default(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        response = self.client.post(
            "/v1/runs",
            json=self.payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)
        assert self.fake_execution_client.last_submit is not None
        envelope = self.fake_execution_client.last_submit["envelope"]
        self.assertEqual(
            envelope["scope_id"],
            "tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
        )
        self.assertEqual(envelope["scope_type"], "session")

    def test_runs_accepts_explicit_scope_axis(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        payload = dict(self.payload)
        payload["scope_id"] = "scope:tenant:t1:workspace:creative-lab"
        payload["scope_type"] = "workspace"
        response = self.client.post(
            "/v1/runs",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)
        assert self.fake_execution_client.last_submit is not None
        envelope = self.fake_execution_client.last_submit["envelope"]
        self.assertEqual(envelope["scope_id"], "scope:tenant:t1:workspace:creative-lab")
        self.assertEqual(envelope["scope_type"], "workspace")

    def test_runs_freezes_control_ingress_contract_for_entry_modes(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        for mode in ("assistant", "workflow", "tools", "mixed"):
            payload = dict(self.payload)
            payload["ingress_mode"] = mode
            payload["ingress_lifecycle_id"] = f"lifecycle-{mode}"
            response = self.client.post(
                "/v1/runs",
                json=payload,
                headers={"Authorization": f"Bearer {token}"},
            )
            self.assertEqual(response.status_code, 200)
            assert self.fake_execution_client.last_submit is not None
            envelope = self.fake_execution_client.last_submit["envelope"]
            control_ingress = envelope["payload"].get("control_ingress")
            self.assertIsInstance(control_ingress, dict)
            assert isinstance(control_ingress, dict)
            self.assertEqual(control_ingress.get("entry_mode"), mode)
            self.assertEqual(control_ingress.get("trace_id"), "trace-1")
            self.assertEqual(control_ingress.get("tenant_id"), "t1")
            self.assertEqual(control_ingress.get("app_id"), "covernow")
            self.assertEqual(
                control_ingress.get("session_key"),
                "tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
            )
            self.assertEqual(
                control_ingress.get("scope_id"),
                "tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
            )
            self.assertEqual(control_ingress.get("scope_type"), "session")
            self.assertEqual(control_ingress.get("lifecycle_id"), f"lifecycle-{mode}")

    def test_runs_projects_explicit_scope_axis_to_control_ingress_contract(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        payload = dict(self.payload)
        payload["scope_id"] = "scope:tenant:t1:workspace:creative-lab"
        payload["scope_type"] = "workspace"
        payload["ingress_mode"] = "workflow"
        response = self.client.post(
            "/v1/runs",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)
        assert self.fake_execution_client.last_submit is not None
        envelope = self.fake_execution_client.last_submit["envelope"]
        control_ingress = envelope["payload"].get("control_ingress")
        self.assertIsInstance(control_ingress, dict)
        assert isinstance(control_ingress, dict)
        self.assertEqual(control_ingress.get("entry_mode"), "workflow")
        self.assertEqual(control_ingress.get("scope_id"), "scope:tenant:t1:workspace:creative-lab")
        self.assertEqual(control_ingress.get("scope_type"), "workspace")

    def test_runs_rejects_mismatched_ingress_trace_id(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        payload = dict(self.payload)
        payload["ingress_mode"] = "assistant"
        payload["ingress_trace_id"] = "trace-2"
        response = self.client.post(
            "/v1/runs",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 422)
        self.assertIn("ingress_trace_id", str(response.json().get("detail")))
        self.assertIsNone(self.fake_execution_client.last_submit)

    def test_runs_projects_scope_axis_on_downstream_event_when_missing(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write", "runs:read"])
        response = self.client.post(
            "/v1/runs",
            json=self.payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)

        recent = self.client.get(
            "/v1/events/recent?event_types=runtime.run.requested",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(recent.status_code, 200)
        items = recent.json()["items"]
        self.assertGreaterEqual(len(items), 1)
        event = items[-1]["event"]
        self.assertEqual(
            event["scope_id"],
            "tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
        )
        self.assertEqual(event["scope_type"], "session")

    def test_contract_retirement_status_forwards_scope_and_filters(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:read"])
        response = self.client.get(
            "/v1/contracts/retirement:status?version_type=task_contract_version&version=task-envelope.v1",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["schema_version"], "runtime.contract_retirement_status.v1")
        self.assertEqual(body["version_type_filter"], "task_contract_version")
        self.assertEqual(body["version_filter"], "task-envelope.v1")
        assert self.fake_execution_client.last_contract_retirement_status is not None
        self.assertEqual(
            self.fake_execution_client.last_contract_retirement_status["version_type"],
            "task_contract_version",
        )
        self.assertEqual(
            self.fake_execution_client.last_contract_retirement_status["version"],
            "task-envelope.v1",
        )

    def test_contract_retirement_validate_forwards_body(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:read"])
        response = self.client.post(
            "/v1/contracts/retirement:validate",
            json={"task_contract_versions": ["task-envelope.v1"]},
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["schema_version"], "runtime.contract_retirement_gate.v1")
        self.assertFalse(body["eligible_to_retire"])
        self.assertEqual(
            body["blocked"][0]["reason_code"],
            "contract_retirement_blocked_active_bindings",
        )
        assert self.fake_execution_client.last_contract_retirement_validate is not None
        self.assertEqual(
            self.fake_execution_client.last_contract_retirement_validate["body"],
            {"task_contract_versions": ["task-envelope.v1"]},
        )

    def test_runs_rejects_downstream_contract_version_drift(self) -> None:
        gateway_app_module._execution_client = _FakeExecutionClientContractVersionDrift()
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        response = self.client.post(
            "/v1/runs",
            json=self.payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 502)
        detail = response.json().get("detail")
        self.assertIsInstance(detail, str)
        assert isinstance(detail, str)
        self.assertIn("contract version drift detected", detail)
        audit_latest = get_audit_events(limit=1)[0]
        self.assertEqual(audit_latest["decision"], "deny")
        self.assertIn("contract version drift detected", str(audit_latest["metadata"].get("reason", "")))

    def test_runs_accepts_v2_versions_within_active_pool(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        payload = dict(self.payload)
        payload["contract_versions"] = {
            "task_contract_version": "task-envelope.v2",
            "agent_contract_version": "assistant-decision.v2",
            "event_schema_version": "event-envelope.v2",
        }
        with patch.dict(
            os.environ,
            {
                "OWA_ACTIVE_TASK_CONTRACT_VERSIONS": "task-envelope.v1,task-envelope.v2",
                "OWA_ACTIVE_AGENT_CONTRACT_VERSIONS": "assistant-decision.v1,assistant-decision.v2",
                "OWA_ACTIVE_EVENT_SCHEMA_VERSIONS": "event-envelope.v1,event-envelope.v2",
            },
            clear=False,
        ):
            response = self.client.post(
                "/v1/runs",
                json=payload,
                headers={"Authorization": f"Bearer {token}"},
            )
        self.assertEqual(response.status_code, 200)
        assert self.fake_execution_client.last_submit is not None
        envelope = self.fake_execution_client.last_submit["envelope"]
        self.assertEqual(envelope["task_contract_version"], "task-envelope.v2")
        self.assertEqual(envelope["agent_contract_version"], "assistant-decision.v2")
        self.assertEqual(envelope["event_schema_version"], "event-envelope.v2")

    def test_runs_rejects_versions_outside_active_pool(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        payload = dict(self.payload)
        payload["contract_versions"] = {
            "task_contract_version": "task-envelope.v9",
            "agent_contract_version": "assistant-decision.v1",
            "event_schema_version": "event-envelope.v1",
        }
        with patch.dict(
            os.environ,
            {
                "OWA_ACTIVE_TASK_CONTRACT_VERSIONS": "task-envelope.v1,task-envelope.v2",
                "OWA_ACTIVE_AGENT_CONTRACT_VERSIONS": "assistant-decision.v1,assistant-decision.v2",
                "OWA_ACTIVE_EVENT_SCHEMA_VERSIONS": "event-envelope.v1,event-envelope.v2",
            },
            clear=False,
        ):
            response = self.client.post(
                "/v1/runs",
                json=payload,
                headers={"Authorization": f"Bearer {token}"},
            )
        self.assertEqual(response.status_code, 422)
        detail = str(response.json().get("detail", ""))
        self.assertIn("task_contract_version=task-envelope.v9 is not in active version pool", detail)
        self.assertIsNone(self.fake_execution_client.last_submit)

    def test_runs_rejects_contract_versions_unknown_fields(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        payload = dict(self.payload)
        payload["contract_versions"] = {
            "task_contract_version": "task-envelope.v1",
            "unexpected": "not-allowed",
        }
        response = self.client.post(
            "/v1/runs",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 422)
        detail = response.json().get("detail")
        self.assertIsInstance(detail, list)
        assert isinstance(detail, list)
        self.assertTrue(
            any(
                isinstance(item, dict)
                and list(item.get("loc", []))[-1:] == ["unexpected"]
                and item.get("type") == "extra_forbidden"
                for item in detail
            )
        )
        self.assertIsNone(self.fake_execution_client.last_submit)

    def test_runs_rejects_invalid_retry_policy_strategy(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        payload = dict(self.payload)
        payload["retry_policy"] = {
            "max_attempts": 3,
            "backoff_ms": 250,
            "strategy": "linear",
        }
        response = self.client.post(
            "/v1/runs",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 422)
        detail = response.json().get("detail")
        self.assertIsInstance(detail, list)
        assert isinstance(detail, list)
        self.assertTrue(
            any(
                isinstance(item, dict)
                and list(item.get("loc", []))[-1:] == ["strategy"]
                and item.get("type") == "string_pattern_mismatch"
                for item in detail
            )
        )
        self.assertIsNone(self.fake_execution_client.last_submit)

    def test_runs_rejects_retry_policy_unknown_fields(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        payload = dict(self.payload)
        payload["retry_policy"] = {
            "max_attempts": 3,
            "backoff_ms": 250,
            "strategy": "fixed",
            "jitter_ms": 50,
        }
        response = self.client.post(
            "/v1/runs",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 422)
        detail = response.json().get("detail")
        self.assertIsInstance(detail, list)
        assert isinstance(detail, list)
        self.assertTrue(
            any(
                isinstance(item, dict)
                and list(item.get("loc", []))[-1:] == ["jitter_ms"]
                and item.get("type") == "extra_forbidden"
                for item in detail
            )
        )
        self.assertIsNone(self.fake_execution_client.last_submit)

    def test_runs_rejects_unknown_top_level_fields(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        payload = dict(self.payload)
        payload["dispatch_hint"] = "prefer-fast"
        response = self.client.post(
            "/v1/runs",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 422)
        detail = response.json().get("detail")
        self.assertIsInstance(detail, list)
        assert isinstance(detail, list)
        self.assertTrue(
            any(
                isinstance(item, dict)
                and list(item.get("loc", []))[-1:] == ["dispatch_hint"]
                and item.get("type") == "extra_forbidden"
                for item in detail
            )
        )
        self.assertIsNone(self.fake_execution_client.last_submit)

    def test_runs_allow_audit_includes_effective_retry_policy(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        response = self.client.post(
            "/v1/runs",
            json=self.payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)
        audit_latest = get_audit_events(limit=1)[0]
        self.assertEqual(
            audit_latest["metadata"].get("retry_policy"),
            {
                "max_attempts": 3,
                "backoff_ms": 250,
                "strategy": "fixed",
            },
        )

    def test_runs_deny_audit_includes_effective_retry_policy(self) -> None:
        gateway_app_module._execution_client = _FakeExecutionClientRetryableCapacity()
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        payload = dict(self.payload)
        payload["retry_policy"] = {
            "max_attempts": 5,
            "backoff_ms": 700,
            "strategy": "exponential",
        }
        response = self.client.post(
            "/v1/runs",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 503)
        detail = response.json().get("detail")
        self.assertIsInstance(detail, dict)
        assert isinstance(detail, dict)
        self.assertEqual(detail.get("retry_policy"), payload["retry_policy"])
        audit_latest = get_audit_events(limit=1)[0]
        self.assertEqual(audit_latest["decision"], "deny")
        self.assertEqual(
            audit_latest["metadata"].get("retry_policy"),
            payload["retry_policy"],
        )

    def test_runs_publish_downstream_event_to_gateway_event_bus(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        response = self.client.post(
            "/v1/runs",
            json=self.payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)

        recent = self.client.get(
            "/v1/events/recent",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(recent.status_code, 200)
        items = recent.json()["items"]
        self.assertGreaterEqual(len(items), 1)
        self.assertEqual(items[-1]["event"]["event_type"], "runtime.run.requested")

    def test_events_publish_accepts_optional_contract_version_fields(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["events:write", "events:read"])
        envelope = self._event_publish_envelope(
            event_id="evt-contract-version-1",
            event_type="runtime.run.status",
            correlation_id="corr-contract-version-1",
            payload={"run_id": "run-contract-version-1", "status": "queued"},
        )
        envelope["task_contract_version"] = "task-envelope.v1"
        envelope["agent_contract_version"] = "assistant-decision.v1"
        envelope["event_schema_version"] = "event-envelope.v1"

        publish = self.client.post(
            "/v1/events/publish",
            json=envelope,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(publish.status_code, 200)
        publish_payload = publish.json()
        self.assertEqual(publish_payload["accepted"], True)
        self.assertEqual(publish_payload["event_type"], "runtime.run.status")

        recent = self.client.get(
            "/v1/events/recent?event_types=runtime.run.status",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(recent.status_code, 200)
        items = recent.json()["items"]
        self.assertGreaterEqual(len(items), 1)
        event = items[-1]["event"]
        self.assertEqual(event["task_contract_version"], "task-envelope.v1")
        self.assertEqual(event["agent_contract_version"], "assistant-decision.v1")
        self.assertEqual(event["event_schema_version"], "event-envelope.v1")

    def test_run_lifecycle_replay_filters_single_run(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["events:write", "events:read"])
        publish_payloads = [
            self._event_publish_envelope(
                event_id="evt-run-a-queued",
                event_type="runtime.run.status",
                correlation_id="corr-run-a",
                payload={"run_id": "run-a", "status": "queued"},
            ),
            self._event_publish_envelope(
                event_id="evt-run-b-queued",
                event_type="runtime.run.status",
                correlation_id="corr-run-b",
                payload={"run_id": "run-b", "status": "queued"},
            ),
            self._event_publish_envelope(
                event_id="evt-run-a-running",
                event_type="runtime.run.status",
                correlation_id="corr-run-a",
                payload={"run_id": "run-a", "status": "running"},
            ),
        ]
        for envelope in publish_payloads:
            publish = self.client.post(
                "/v1/events/publish",
                json=envelope,
                headers={"Authorization": f"Bearer {token}"},
            )
            self.assertEqual(publish.status_code, 200)

        replay = self.client.get(
            "/v1/runs/run-a/lifecycle/replay",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(replay.status_code, 200)
        body = replay.json()
        self.assertEqual(body["schema_version"], "runtime.run.lifecycle_replay.v1")
        self.assertEqual(body["run_id"], "run-a")
        items = body["items"]
        self.assertEqual(len(items), 2)
        self.assertTrue(all(item["event"]["payload"]["run_id"] == "run-a" for item in items))
        projection = body["lifecycle_projection"]
        self.assertEqual(projection["latest_status"], "running")
        self.assertEqual(projection["is_terminal"], False)
        self.assertEqual(projection["run_status_counts"]["queued"], 1)
        self.assertEqual(projection["run_status_counts"]["running"], 1)

    def test_run_lifecycle_replay_supports_cursor_and_terminal_projection(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["events:write", "events:read"])
        publish_payloads = [
            self._event_publish_envelope(
                event_id="evt-run-c-queued",
                event_type="runtime.run.status",
                correlation_id="corr-run-c",
                payload={"run_id": "run-c", "status": "queued"},
            ),
            self._event_publish_envelope(
                event_id="evt-run-c-running",
                event_type="runtime.run.status",
                correlation_id="corr-run-c",
                payload={"run_id": "run-c", "status": "running"},
            ),
            self._event_publish_envelope(
                event_id="evt-run-c-succeeded",
                event_type="runtime.run.status",
                correlation_id="corr-run-c",
                payload={"run_id": "run-c", "status": "succeeded"},
            ),
        ]
        for envelope in publish_payloads:
            publish = self.client.post(
                "/v1/events/publish",
                json=envelope,
                headers={"Authorization": f"Bearer {token}"},
            )
            self.assertEqual(publish.status_code, 200)

        initial = self.client.get(
            "/v1/runs/run-c/lifecycle/replay?limit=10",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(initial.status_code, 200)
        initial_body = initial.json()
        self.assertEqual(len(initial_body["items"]), 3)
        resume_cursor = int(initial_body["items"][0]["bus_seq"])

        replay = self.client.get(
            f"/v1/runs/run-c/lifecycle/replay?cursor={resume_cursor}",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(replay.status_code, 200)
        body = replay.json()
        items = body["items"]
        self.assertEqual(len(items), 2)
        self.assertEqual([item["event"]["payload"]["status"] for item in items], ["running", "succeeded"])
        projection = body["lifecycle_projection"]
        self.assertEqual(projection["latest_status"], "succeeded")
        self.assertEqual(projection["is_terminal"], True)
        self.assertEqual(projection["event_count"], 2)

    def test_run_lifecycle_replay_durable_consumer_resume_cursor(self) -> None:
        read_token = self._token(audience="runtime-gateway", scope=["events:read"])
        write_token = self._token(audience="runtime-gateway", scope=["events:write", "events:read"])
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "runtime-events.sqlite")
            with patch.dict(os.environ, {"RUNTIME_GATEWAY_EVENT_DB_PATH": db_path}, clear=False):
                for idx, status in enumerate(["queued", "running", "succeeded"], start=1):
                    envelope = self._event_publish_envelope(
                        event_id=f"evt-lifecycle-ack-{idx}",
                        event_type="runtime.run.status",
                        correlation_id=f"corr-lifecycle-ack-{idx}",
                        payload={"run_id": "run-lifecycle-ack", "status": status},
                    )
                    publish = self.client.post(
                        "/v1/events/publish",
                        json=envelope,
                        headers={"Authorization": f"Bearer {write_token}"},
                    )
                    self.assertEqual(publish.status_code, 200)

                ack = self.client.post(
                    "/v1/events/ack",
                    json={
                        "source": "durable",
                        "consumer_id": "consumer-lifecycle-r1",
                        "cursor": 1,
                        "run_id": "run-lifecycle-ack",
                    },
                    headers={"Authorization": f"Bearer {read_token}"},
                )
                self.assertEqual(ack.status_code, 200)
                self.assertEqual(ack.json()["ack_cursor"], 1)

                replay = self.client.get(
                    "/v1/runs/run-lifecycle-ack/lifecycle/replay?source=durable&consumer_id=consumer-lifecycle-r1&limit=10",
                    headers={"Authorization": f"Bearer {read_token}"},
                )
                self.assertEqual(replay.status_code, 200)
                payload = replay.json()
                items = payload["items"]
                self.assertEqual(len(items), 2)
                self.assertEqual([item["event"]["payload"]["status"] for item in items], ["running", "succeeded"])
                consumer_cursor = payload.get("consumer_cursor") or {}
                self.assertEqual(consumer_cursor.get("resume_strategy"), "consumer_ack_cursor")
                self.assertEqual(consumer_cursor.get("ack_cursor"), 1)
                self.assertEqual(consumer_cursor.get("resume_cursor"), 1)
                projection = payload["lifecycle_projection"]
                self.assertEqual(projection["latest_status"], "succeeded")
                self.assertEqual(projection["is_terminal"], True)

    def test_run_lifecycle_replay_rejects_consumer_id_on_memory_source(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["events:read"])
        response = self.client.get(
            "/v1/runs/run-consumer-mem/lifecycle/replay?consumer_id=consumer-r1&source=memory",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 422)
        self.assertIn("consumer_id requires source=durable", str(response.json().get("detail", "")))

    def test_events_publish_accepts_direct_completion_alias_and_normalizes(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["events:write", "events:read"])
        envelope = self._event_publish_envelope(
            event_id="evt-direct-completion-1",
            event_type="direct_completion.v1",
            correlation_id="corr-direct-completion-1",
            payload={
                "schema_version": "direct_completion.v1",
                "task_id": "TASK-ACK-003",
                "provider": "claude",
                "request_id": "owa:req:ack-003",
                "request_state": "completed",
                "ack_stage": "completed",
                "idempotency_key": "TASK-ACK-003:r0:a1",
                "completed_at": "2026-03-23T12:00:00+00:00",
                "callback_at": "2026-03-23T12:00:01+00:00",
            },
        )

        publish = self.client.post(
            "/v1/events/publish",
            json=envelope,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(publish.status_code, 200)
        publish_payload = publish.json()
        self.assertEqual(publish_payload["accepted"], True)
        self.assertEqual(publish_payload["event_type"], "runtime.run.direct.completion")
        self.assertEqual(publish_payload["original_event_type"], "direct_completion.v1")

        recent = self.client.get(
            "/v1/events/recent",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(recent.status_code, 200)
        items = recent.json()["items"]
        self.assertEqual(len(items), 1)
        event = items[-1]["event"]
        self.assertEqual(event["event_type"], "runtime.run.direct.completion")
        self.assertEqual(event["payload"]["source_event_type"], "direct_completion.v1")
        self.assertEqual(event["payload"]["task_id"], "TASK-ACK-003")

    def test_events_publish_direct_completion_alias_requires_idempotency_key(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["events:write", "events:read"])
        envelope = self._event_publish_envelope(
            event_id="evt-direct-completion-missing-idempotency",
            event_type="direct_completion.v1",
            correlation_id="corr-direct-completion-missing-idempotency",
            payload={
                "schema_version": "direct_completion.v1",
                "task_id": "TASK-ACK-004",
                "provider": "claude",
                "request_id": "owa:req:ack-004",
                "request_state": "completed",
                "ack_stage": "completed",
                "completed_at": "2026-03-23T12:00:00+00:00",
                "callback_at": "2026-03-23T12:00:01+00:00",
            },
        )
        publish = self.client.post(
            "/v1/events/publish",
            json=envelope,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(publish.status_code, 422)
        self.assertIn("payload.idempotency_key is required", str(publish.json().get("detail", "")))

    def test_events_publish_direct_completion_alias_requires_completed_ack_stage(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["events:write", "events:read"])
        envelope = self._event_publish_envelope(
            event_id="evt-direct-completion-invalid-ack-stage",
            event_type="direct_completion.v1",
            correlation_id="corr-direct-completion-invalid-ack-stage",
            payload={
                "schema_version": "direct_completion.v1",
                "task_id": "TASK-ACK-005",
                "provider": "claude",
                "request_id": "owa:req:ack-005",
                "request_state": "completed",
                "ack_stage": "provider_seen",
                "idempotency_key": "TASK-ACK-005:r0:a1",
                "completed_at": "2026-03-23T12:00:00+00:00",
                "callback_at": "2026-03-23T12:00:01+00:00",
            },
        )
        publish = self.client.post(
            "/v1/events/publish",
            json=envelope,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(publish.status_code, 422)
        self.assertIn("payload.ack_stage must be completed", str(publish.json().get("detail", "")))

    def test_events_publish_direct_completion_alias_replay_is_idempotent(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["events:write", "events:read"])
        first_envelope = self._event_publish_envelope(
            event_id="evt-direct-completion-replay-1",
            event_type="direct_completion.v1",
            correlation_id="corr-direct-completion-replay-1",
            payload={
                "schema_version": "direct_completion.v1",
                "task_id": "TASK-ACK-006",
                "provider": "claude",
                "request_id": "owa:req:ack-006",
                "request_state": "completed",
                "ack_stage": "completed",
                "idempotency_key": "TASK-ACK-006:r0:a1",
                "completed_at": "2026-03-23T12:00:00+00:00",
                "callback_at": "2026-03-23T12:00:01+00:00",
            },
        )
        second_envelope = self._event_publish_envelope(
            event_id="evt-direct-completion-replay-2",
            event_type="direct_completion.v1",
            correlation_id="corr-direct-completion-replay-2",
            payload={
                "schema_version": "direct_completion.v1",
                "task_id": "TASK-ACK-006",
                "provider": "claude",
                "request_id": "owa:req:ack-006",
                "request_state": "completed",
                "ack_stage": "completed",
                "idempotency_key": "TASK-ACK-006:r0:a1",
                "completed_at": "2026-03-23T12:00:00+00:00",
                "callback_at": "2026-03-23T12:00:01+00:00",
            },
        )

        first_publish = self.client.post(
            "/v1/events/publish",
            json=first_envelope,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(first_publish.status_code, 200)
        first_payload = first_publish.json()
        self.assertEqual(first_payload["idempotent_replay"], False)

        second_publish = self.client.post(
            "/v1/events/publish",
            json=second_envelope,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(second_publish.status_code, 200)
        second_payload = second_publish.json()
        self.assertEqual(second_payload["idempotent_replay"], True)
        self.assertEqual(second_payload["bus_seq"], first_payload["bus_seq"])
        self.assertEqual(second_payload["event_id"], first_payload["event_id"])
        self.assertEqual(second_payload["correlation_id"], "owa:req:ack-006")

        recent = self.client.get(
            "/v1/events/recent?event_types=runtime.run.direct.completion",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(recent.status_code, 200)
        items = recent.json()["items"]
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["event"]["payload"]["idempotency_key"], "TASK-ACK-006:r0:a1")

    def test_events_publish_direct_alias_projects_correlation_id_from_request_id(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["events:write", "events:read"])
        envelope = self._event_publish_envelope(
            event_id="evt-direct-correlation-1",
            event_type="direct_audit.v1",
            correlation_id="corr-direct-correlation-legacy",
            payload={
                "schema_version": "direct_audit.v1",
                "event_type": "dispatch_sent",
                "task_id": "TASK-AUDIT-003",
                "provider": "codex",
                "request_id": "owa:req:audit-003",
                "ack_stage": "sent",
                "idempotency_key": "TASK-AUDIT-003:r0:a1",
                "reason_code": "dispatch_sent",
            },
        )
        publish = self.client.post(
            "/v1/events/publish",
            json=envelope,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(publish.status_code, 200)
        publish_payload = publish.json()
        self.assertEqual(publish_payload["correlation_id"], "owa:req:audit-003")

        recent = self.client.get(
            "/v1/events/recent?event_types=runtime.run.direct.audit.dispatch_sent",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(recent.status_code, 200)
        items = recent.json()["items"]
        self.assertEqual(len(items), 1)
        event = items[0]["event"]
        self.assertEqual(event["correlation_id"], "owa:req:audit-003")
        self.assertEqual(event["payload"]["ack_stage"], "sent")
        self.assertEqual(event["payload"]["idempotency_key"], "TASK-AUDIT-003:r0:a1")

    def test_events_publish_accepts_direct_audit_alias_with_runtime_event_hint(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["events:write", "events:read"])
        envelope = self._event_publish_envelope(
            event_id="evt-direct-audit-1",
            event_type="direct_audit.v1",
            correlation_id="corr-direct-audit-1",
            payload={
                "schema_version": "direct_audit.v1",
                "event_type": "dispatch_sent",
                "runtime_event_type": "runtime.run.direct.audit.dispatch_sent",
                "task_id": "TASK-AUDIT-001",
                "provider": "codex",
                "request_id": "owa:req:audit-001",
                "ack_stage": "sent",
                "idempotency_key": "TASK-AUDIT-001:r0:a1",
                "reason_code": "dispatch_sent",
            },
        )

        publish = self.client.post(
            "/v1/events/publish",
            json=envelope,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(publish.status_code, 200)
        publish_payload = publish.json()
        self.assertEqual(publish_payload["event_type"], "runtime.run.direct.audit.dispatch_sent")
        self.assertEqual(publish_payload["original_event_type"], "direct_audit.v1")

        recent = self.client.get(
            "/v1/events/recent",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(recent.status_code, 200)
        items = recent.json()["items"]
        self.assertEqual(len(items), 1)
        event = items[-1]["event"]
        self.assertEqual(event["event_type"], "runtime.run.direct.audit.dispatch_sent")
        self.assertEqual(event["payload"]["source_event_type"], "direct_audit.v1")
        self.assertEqual(
            event["payload"]["runtime_event_type"],
            "runtime.run.direct.audit.dispatch_sent",
        )

    def test_events_publish_rejects_unknown_direct_alias_event_type(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["events:write", "events:read"])
        envelope = self._event_publish_envelope(
            event_id="evt-direct-unknown-1",
            event_type="direct_unknown.v1",
            correlation_id="corr-direct-unknown-1",
            payload={"schema_version": "direct_unknown.v1"},
        )

        publish = self.client.post(
            "/v1/events/publish",
            json=envelope,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(publish.status_code, 422)
        self.assertIn("event type not publishable", str(publish.json().get("detail", "")))

        recent = self.client.get(
            "/v1/events/recent",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(recent.status_code, 200)
        self.assertEqual(recent.json()["items"], [])

    @patch("runtime_gateway.app.read_event_page")
    def test_recent_events_durable_keeps_next_cursor_monotonic(self, mock_read_event_page) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:read"])
        mock_read_event_page.return_value = {
            "items": [],
            "next_cursor": 3,
            "has_more": False,
            "stats": {
                "connections": 0,
                "buffered_events": 0,
                "next_seq": 1,
            },
        }

        response = self.client.get(
            "/v1/events/recent?source=durable&cursor=10&limit=5",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["source"], "durable")
        self.assertEqual(payload["next_cursor"], 10)
        self.assertEqual(payload["items"], [])
        self.assertEqual(payload["has_more"], False)
        self.assertEqual(payload["recommended_poll_after_ms"], 5000)
        mock_read_event_page.assert_called_once()

    @patch("runtime_gateway.app.read_event_page")
    def test_recent_events_durable_keeps_next_cursor_monotonic_with_items(self, mock_read_event_page) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:read"])
        mock_read_event_page.return_value = {
            "items": [
                {
                    "bus_seq": 8,
                    "event": {
                        "event_id": "evt-durable-regress-1",
                        "event_type": "runtime.run.status",
                        "tenant_id": "t1",
                        "app_id": "covernow",
                        "session_key": "tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
                        "trace_id": "trace-durable-regress-1",
                        "correlation_id": "corr-durable-regress-1",
                        "ts": datetime.now(timezone.utc).isoformat(),
                        "payload": {"run_id": "run-durable-regress", "status": "queued"},
                    },
                }
            ],
            "next_cursor": 8,
            "has_more": False,
            "stats": {
                "connections": 0,
                "buffered_events": 1,
                "next_seq": 9,
            },
        }

        response = self.client.get(
            "/v1/events/recent?source=durable&cursor=12&limit=5",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["source"], "durable")
        self.assertEqual(payload["next_cursor"], 12)
        self.assertEqual(len(payload["items"]), 1)
        self.assertEqual(payload["items"][0]["bus_seq"], 8)
        self.assertEqual(payload["items"][0]["event"]["event_type"], "runtime.run.status")
        self.assertEqual(payload["recommended_poll_after_ms"], 1500)
        mock_read_event_page.assert_called_once()

    def test_event_consumer_ack_roundtrip_and_recent_resume_cursor(self) -> None:
        read_token = self._token(audience="runtime-gateway", scope=["events:read"])
        write_token = self._token(audience="runtime-gateway", scope=["events:write", "events:read"])
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "runtime-events.sqlite")
            with patch.dict(os.environ, {"RUNTIME_GATEWAY_EVENT_DB_PATH": db_path}, clear=False):
                for idx, event_type in enumerate(
                    ["runtime.run.started", "runtime.run.status", "runtime.run.completed"],
                    start=1,
                ):
                    envelope = self._event_publish_envelope(
                        event_id=f"evt-ack-resume-{idx}",
                        event_type=event_type,
                        correlation_id=f"corr-ack-resume-{idx}",
                        payload={"run_id": "run-ack-resume", "status": "queued"},
                    )
                    publish = self.client.post(
                        "/v1/events/publish",
                        json=envelope,
                        headers={"Authorization": f"Bearer {write_token}"},
                    )
                    self.assertEqual(publish.status_code, 200)

                pre_ack = self.client.get(
                    "/v1/events/ack?source=durable&consumer_id=consumer-r1&run_id=run-ack-resume",
                    headers={"Authorization": f"Bearer {read_token}"},
                )
                self.assertEqual(pre_ack.status_code, 200)
                self.assertEqual(pre_ack.json()["ack_cursor"], 0)
                self.assertEqual(pre_ack.json()["has_ack"], False)

                ack = self.client.post(
                    "/v1/events/ack",
                    json={
                        "source": "durable",
                        "consumer_id": "consumer-r1",
                        "cursor": 1,
                        "run_id": "run-ack-resume",
                    },
                    headers={"Authorization": f"Bearer {read_token}"},
                )
                self.assertEqual(ack.status_code, 200)
                ack_payload = ack.json()
                self.assertEqual(ack_payload["ack_cursor"], 1)
                self.assertTrue(ack_payload["applied"])

                post_ack = self.client.get(
                    "/v1/events/ack?source=durable&consumer_id=consumer-r1&run_id=run-ack-resume",
                    headers={"Authorization": f"Bearer {read_token}"},
                )
                self.assertEqual(post_ack.status_code, 200)
                self.assertEqual(post_ack.json()["ack_cursor"], 1)
                self.assertEqual(post_ack.json()["has_ack"], True)

                resumed = self.client.get(
                    "/v1/events/recent?source=durable&consumer_id=consumer-r1&run_id=run-ack-resume&limit=10",
                    headers={"Authorization": f"Bearer {read_token}"},
                )
                self.assertEqual(resumed.status_code, 200)
                resumed_payload = resumed.json()
                items = resumed_payload["items"]
                self.assertEqual(len(items), 2)
                self.assertEqual(items[0]["event"]["event_type"], "runtime.run.status")
                self.assertEqual(items[1]["event"]["event_type"], "runtime.run.completed")
                self.assertEqual(resumed_payload["consumer_cursor"]["resume_strategy"], "consumer_ack_cursor")
                self.assertEqual(resumed_payload["consumer_cursor"]["resume_cursor"], 1)

    def test_recent_events_rejects_consumer_id_on_memory_source(self) -> None:
        read_token = self._token(audience="runtime-gateway", scope=["events:read"])
        response = self.client.get(
            "/v1/events/recent?consumer_id=consumer-r1&source=memory",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(response.status_code, 422)
        self.assertIn("consumer_id requires source=durable", str(response.json().get("detail", "")))

    def test_runs_connection_error_returns_structured_retryable_detail(self) -> None:
        gateway_app_module._execution_client = _FakeExecutionClientTransportUnavailable()
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        response = self.client.post(
            "/v1/runs",
            json=self.payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 503)
        detail = response.json().get("detail")
        self.assertIsInstance(detail, dict)
        assert isinstance(detail, dict)
        self.assertEqual(detail.get("status_code"), 503)
        self.assertEqual(detail.get("retryable"), True)
        self.assertEqual(detail.get("failure_classification"), "upstream_unavailable")
        self.assertEqual(detail.get("upstream_error_class"), "unavailable")
        self.assertEqual(
            detail.get("retry_policy"),
            {"max_attempts": 3, "backoff_ms": 250, "strategy": "fixed"},
        )
        self.assertIn("connection error", str(detail.get("message", "")))

        audit_latest = get_audit_events(limit=1)[0]
        self.assertEqual(audit_latest["decision"], "deny")
        self.assertEqual(audit_latest["metadata"].get("status_code"), 503)
        self.assertEqual(audit_latest["metadata"].get("retryable"), True)
        self.assertEqual(
            audit_latest["metadata"].get("failure_classification"),
            "upstream_unavailable",
        )
        self.assertEqual(audit_latest["metadata"].get("upstream_error_class"), "unavailable")
        self.assertEqual(audit_latest["metadata"].get("downstream_event_type"), None)

    def test_runs_keeps_upstream_http_detail_when_error_body_is_not_event_envelope(self) -> None:
        gateway_app_module._execution_client = _FakeExecutionClientHttpErrorDetail()
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        response = self.client.post(
            "/v1/runs",
            json=self.payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 500)
        detail = response.json().get("detail")
        self.assertIsInstance(detail, dict)
        assert isinstance(detail, dict)
        self.assertEqual(detail.get("status_code"), 500)
        self.assertEqual(detail.get("retryable"), True)
        self.assertEqual(detail.get("failure_classification"), "upstream_error")
        self.assertEqual(detail.get("upstream_error_class"), "retryable")
        self.assertIn("invalid route event", str(detail.get("message", "")))
        self.assertNotEqual(detail.get("upstream_error_class"), "contract_drift")

        audit_latest = get_audit_events(limit=1)[0]
        self.assertEqual(audit_latest["decision"], "deny")
        self.assertEqual(audit_latest["metadata"].get("status_code"), 500)
        self.assertEqual(audit_latest["metadata"].get("downstream_event_type"), None)
        self.assertEqual(audit_latest["metadata"].get("upstream_error_class"), "retryable")

    def test_runs_propagates_downstream_route_failed_status_and_publishes_event(self) -> None:
        gateway_app_module._execution_client = _FakeExecutionClientRejected()
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        response = self.client.post(
            "/v1/runs",
            json=self.payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 409)
        detail = response.json().get("detail")
        self.assertIsInstance(detail, dict)
        assert isinstance(detail, dict)
        self.assertEqual(detail.get("status_code"), 409)
        self.assertEqual(detail.get("downstream_event_type"), "runtime.route.failed")
        self.assertEqual(detail.get("run_id"), "run-test-failed")
        self.assertEqual(detail.get("task_id"), "run-test-failed:root")
        self.assertEqual(detail.get("run_status"), "queued")
        failure = detail.get("failure")
        self.assertIsInstance(failure, dict)
        assert isinstance(failure, dict)
        self.assertEqual(failure.get("code"), "no_eligible_device")
        self.assertEqual(failure.get("classification"), "capacity")
        self.assertEqual(detail.get("failure_code"), "no_eligible_device")
        self.assertEqual(detail.get("failure_classification"), "capacity")
        self.assertEqual(detail.get("failure_message"), "no eligible device")
        self.assertEqual(detail.get("recommended_poll_after_ms"), 1200)
        self.assertEqual(detail.get("placement_reason_code"), "capacity_exhausted")
        self.assertEqual(detail.get("placement_event_type"), "device.route.rejected")
        detail_snapshot = detail.get("placement_resource_snapshot")
        self.assertIsInstance(detail_snapshot, dict)
        assert isinstance(detail_snapshot, dict)
        self.assertEqual(detail_snapshot.get("eligible_devices"), 1)
        self.assertEqual(detail_snapshot.get("active_leases"), 1)
        self.assertEqual(detail_snapshot.get("available_slots"), 0)
        self.assertEqual(detail.get("retryable"), True)
        self.assertEqual(
            detail.get("retry_policy"),
            {"max_attempts": 3, "backoff_ms": 250, "strategy": "fixed"},
        )
        self.assertIn("HTTP 409", str(detail.get("message", "")))

        recent = self.client.get(
            "/v1/events/recent",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(recent.status_code, 200)
        items = recent.json()["items"]
        self.assertGreaterEqual(len(items), 1)
        self.assertEqual(items[-1]["event"]["event_type"], "runtime.route.failed")

        audit_latest = get_audit_events(limit=1)[0]
        self.assertEqual(audit_latest["action"], "runs.dispatch")
        self.assertEqual(audit_latest["decision"], "deny")
        self.assertEqual(audit_latest["metadata"]["failure_code"], "no_eligible_device")
        self.assertEqual(audit_latest["metadata"]["failure_classification"], "capacity")
        self.assertEqual(audit_latest["metadata"]["run_status"], "queued")
        self.assertEqual(audit_latest["metadata"]["recommended_poll_after_ms"], 1200)
        self.assertEqual(audit_latest["metadata"]["placement_reason_code"], "capacity_exhausted")
        self.assertEqual(audit_latest["metadata"]["placement_event_type"], "device.route.rejected")
        audit_snapshot = audit_latest["metadata"]["placement_resource_snapshot"]
        self.assertIsInstance(audit_snapshot, dict)
        assert isinstance(audit_snapshot, dict)
        self.assertEqual(audit_snapshot.get("eligible_devices"), 1)
        self.assertEqual(audit_snapshot.get("active_leases"), 1)
        self.assertEqual(audit_snapshot.get("available_slots"), 0)
        self.assertEqual(audit_latest["metadata"]["retryable"], True)

    def test_runs_propagates_retryable_capacity_failure_status_and_audit_metadata(self) -> None:
        gateway_app_module._execution_client = _FakeExecutionClientRetryableCapacity()
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        response = self.client.post(
            "/v1/runs",
            json=self.payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 503)
        detail = response.json().get("detail")
        self.assertIsInstance(detail, dict)
        assert isinstance(detail, dict)
        self.assertEqual(detail.get("status_code"), 503)
        self.assertEqual(detail.get("downstream_event_type"), "runtime.route.failed")
        self.assertEqual(detail.get("failure_code"), "placement_throttled")
        self.assertEqual(detail.get("failure_classification"), "capacity")
        self.assertEqual(detail.get("run_status"), "queued")
        self.assertEqual(detail.get("recommended_poll_after_ms"), 1500)
        self.assertEqual(detail.get("placement_reason_code"), "placement_throttled")
        self.assertEqual(detail.get("placement_event_type"), "device.route.rejected")
        self.assertEqual(detail.get("retryable"), True)
        self.assertEqual(
            detail.get("retry_policy"),
            {"max_attempts": 3, "backoff_ms": 250, "strategy": "fixed"},
        )

        recent = self.client.get(
            "/v1/events/recent",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(recent.status_code, 200)
        items = recent.json()["items"]
        self.assertGreaterEqual(len(items), 1)
        self.assertEqual(items[-1]["event"]["event_type"], "runtime.route.failed")

        audit_latest = get_audit_events(limit=1)[0]
        self.assertEqual(audit_latest["action"], "runs.dispatch")
        self.assertEqual(audit_latest["decision"], "deny")
        self.assertEqual(audit_latest["metadata"]["status_code"], 503)
        self.assertEqual(audit_latest["metadata"]["failure_code"], "placement_throttled")
        self.assertEqual(audit_latest["metadata"]["failure_classification"], "capacity")
        self.assertEqual(audit_latest["metadata"]["run_status"], "queued")
        self.assertEqual(audit_latest["metadata"]["recommended_poll_after_ms"], 1500)
        self.assertEqual(audit_latest["metadata"]["placement_reason_code"], "placement_throttled")
        self.assertEqual(audit_latest["metadata"]["placement_event_type"], "device.route.rejected")
        self.assertEqual(audit_latest["metadata"]["retryable"], True)

    def test_runs_propagates_policy_failure_status_and_run_status_metadata(self) -> None:
        gateway_app_module._execution_client = _FakeExecutionClientPolicyRejected()
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        response = self.client.post(
            "/v1/runs",
            json=self.payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 409)
        detail = response.json().get("detail")
        self.assertIsInstance(detail, dict)
        assert isinstance(detail, dict)
        self.assertEqual(detail.get("run_id"), "run-test-policy-rejected")
        self.assertEqual(detail.get("task_id"), "run-test-policy-rejected:root")
        self.assertEqual(detail.get("run_status"), "failed")
        self.assertEqual(detail.get("failure_code"), "required_capabilities_unavailable")
        self.assertEqual(detail.get("failure_classification"), "policy")
        self.assertEqual(detail.get("retryable"), False)
        self.assertEqual(
            detail.get("retry_policy"),
            {"max_attempts": 3, "backoff_ms": 250, "strategy": "fixed"},
        )
        self.assertNotIn("recommended_poll_after_ms", detail)

        audit_latest = get_audit_events(limit=1)[0]
        self.assertEqual(audit_latest["action"], "runs.dispatch")
        self.assertEqual(audit_latest["decision"], "deny")
        self.assertEqual(audit_latest["metadata"]["status_code"], 409)
        self.assertEqual(audit_latest["metadata"]["run_status"], "failed")
        self.assertEqual(
            audit_latest["metadata"]["failure_code"],
            "required_capabilities_unavailable",
        )
        self.assertEqual(audit_latest["metadata"]["failure_classification"], "policy")
        self.assertEqual(audit_latest["metadata"]["retryable"], False)

    def test_runs_marks_retryable_false_for_terminal_capacity_failure(self) -> None:
        gateway_app_module._execution_client = _FakeExecutionClientCapacityTerminal()
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        response = self.client.post(
            "/v1/runs",
            json=self.payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 503)
        detail = response.json().get("detail")
        self.assertIsInstance(detail, dict)
        assert isinstance(detail, dict)
        self.assertEqual(detail.get("run_status"), "failed")
        self.assertEqual(detail.get("failure_code"), "capacity_exhausted")
        self.assertEqual(detail.get("failure_classification"), "capacity")
        self.assertEqual(detail.get("retryable"), False)
        self.assertNotIn("recommended_poll_after_ms", detail)
        self.assertEqual(
            detail.get("retry_policy"),
            {"max_attempts": 3, "backoff_ms": 250, "strategy": "fixed"},
        )

        audit_latest = get_audit_events(limit=1)[0]
        self.assertEqual(audit_latest["metadata"]["run_status"], "failed")
        self.assertEqual(audit_latest["metadata"]["retryable"], False)

    def test_runs_marks_retryable_false_for_terminal_rejected_status(self) -> None:
        gateway_app_module._execution_client = _FakeExecutionClientPolicyRejectedTerminalRetryable()
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        response = self.client.post(
            "/v1/runs",
            json=self.payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 503)
        detail = response.json().get("detail")
        self.assertIsInstance(detail, dict)
        assert isinstance(detail, dict)
        self.assertEqual(detail.get("run_status"), "rejected")
        self.assertEqual(detail.get("failure_code"), "policy_terminal_rejected")
        self.assertEqual(detail.get("failure_classification"), "policy")
        self.assertEqual(detail.get("retryable"), False)
        self.assertNotIn("recommended_poll_after_ms", detail)

        audit_latest = get_audit_events(limit=1)[0]
        self.assertEqual(audit_latest["metadata"]["run_status"], "rejected")
        self.assertEqual(audit_latest["metadata"]["failure_code"], "policy_terminal_rejected")
        self.assertEqual(audit_latest["metadata"]["failure_classification"], "policy")
        self.assertEqual(audit_latest["metadata"]["retryable"], False)


if __name__ == "__main__":
    unittest.main()
