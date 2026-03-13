from __future__ import annotations

import os
import unittest
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from runtime_gateway.audit.emitter import clear_audit_events, get_audit_events
from runtime_gateway.auth.tokens import issue_token
from runtime_gateway.integration import RuntimeExecutionClientError

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


@unittest.skipUnless(FASTAPI_STACK_AVAILABLE, "fastapi stack not installed")
class AppIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_audit_events()
        gateway_app_module._event_bus.clear()
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
        self.assertEqual(
            detail.get("placement_resource_snapshot"),
            {"eligible_devices": 1, "active_leases": 1, "available_slots": 0},
        )
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
        self.assertEqual(
            audit_latest["metadata"]["placement_resource_snapshot"],
            {"eligible_devices": 1, "active_leases": 1, "available_slots": 0},
        )
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
