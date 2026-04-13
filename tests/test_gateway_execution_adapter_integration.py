from __future__ import annotations

import io
import importlib
import os
import sys
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlsplit

from runtime_gateway.auth.tokens import issue_token
from runtime_gateway.integration.runtime_execution import RuntimeExecutionClient

try:
    from fastapi.testclient import TestClient
    from runtime_gateway import app as gateway_app_module
except ModuleNotFoundError:
    FASTAPI_STACK_AVAILABLE = False
else:
    FASTAPI_STACK_AVAILABLE = True

_RUNTIME_EXECUTION_SRC = os.environ.get("WAOOOOLAB_RUNTIME_EXECUTION_SRC_DIR")
if _RUNTIME_EXECUTION_SRC:
    candidate_src = Path(_RUNTIME_EXECUTION_SRC)
else:
    candidate_src = Path(__file__).resolve().parents[2] / "runtime-execution" / "src"

if candidate_src.exists():
    sys.path.insert(0, str(candidate_src))

try:
    from runtime_execution.service import RuntimeExecutionService
except Exception:
    RUNTIME_EXECUTION_AVAILABLE = False
else:
    execution_app_module = importlib.import_module("runtime_execution.service_api.app")
    RUNTIME_EXECUTION_AVAILABLE = True


class _ProxyResponse:
    def __init__(self, status: int, body: bytes):
        self._status = status
        self._body = body

    def __enter__(self) -> "_ProxyResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        _ = exc_type, exc, tb

    def getcode(self) -> int:
        return self._status

    def read(self) -> bytes:
        return self._body


class _FakeOrchestratorDecisionClient:
    def __init__(self) -> None:
        self.called = False
        self.last_payload: dict | None = None

    def create_decision(
        self,
        *,
        run_id: str,
        session_key: str,
        payload: dict,
        trace_id: str,
    ) -> dict:
        _ = session_key
        self.called = True
        self.last_payload = dict(payload)
        return {
            "decision_version": "assistant-boundary.v1",
            "execution_path": "runtime_governed",
            "decision_reason_code": "dispatch_policy_present",
            "entry_mode": "workflow",
            "task_plane": "workflow_work",
            "governance_signals": {
                "has_dispatch_policy": True,
                "requires_durable_state": True,
                "retry_attempts": 3,
            },
            "scenario_profile": {
                "scenario_id": "operations",
                "decision_basis": "user_entry",
            },
            "chain_profile": {
                "chain_id": "workflow_pipeline",
                "planning_mode": "compiled",
            },
            "agent_dispatch_policy": {
                "dispatch_mode": "delegate_team",
                "target_executor": "mixed",
                "preferred_providers": ["claude"],
            },
            "task_governance_policy": {
                "runtime_required": True,
                "hitl_required": False,
                "approval_mode": "none",
            },
            "layered_authority_contract": {
                "schema_version": "orchestration_layered_authority.v1",
                "upstream_decision_plane": "agent_orchestrator",
                "upstream_decision_role": "decision_planning",
                "downstream_execution_governance_owner": "runtime_execution",
                "runtime_lifecycle_truth_owner": True,
                "peer_parallel_scheduler_allowed": False,
                "runtime_may_request_step_planning": True,
            },
            "path_promotion_decision_matrix": {
                "schema_version": "assistant_path_promotion_matrix.v1",
                "promotion_direction": "assistant_direct_to_runtime_governed",
                "promotion_target_path": "runtime_governed",
                "deterministic_rule_order": [
                    "hitl_gate",
                    "reliability_guard",
                    "policy_gate",
                    "audit_gate",
                ],
                "trigger_snapshot": {
                    "hitl_required": False,
                    "reliability_risk_level": "high",
                    "policy_gate_required": True,
                    "audit_trail_required": True,
                },
                "promotion_required": True,
                "promotion_reason_codes": ["dispatch_policy_present"],
            },
            "run_id": run_id,
            "trace_id": trace_id,
        }


@unittest.skipUnless(
    FASTAPI_STACK_AVAILABLE and RUNTIME_EXECUTION_AVAILABLE,
    "fastapi/runtime-execution stack not available",
)
class GatewayExecutionAdapterIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        contracts_dir = Path(__file__).resolve().parents[2] / "runtime-execution" / "tests" / "fixtures" / "contracts"
        self._contracts_patch = patch.dict(
            os.environ,
            {"WAOOOOLAB_PLATFORM_CONTRACTS_DIR": str(contracts_dir)},
            clear=False,
        )
        self._contracts_patch.start()

        self._gateway_original_client = gateway_app_module._execution_client
        self._execution_original_runtime = execution_app_module._runtime
        self.orchestrator = _FakeOrchestratorDecisionClient()
        execution_app_module.configure_runtime_api(
            runtime=RuntimeExecutionService(orchestrator_client=self.orchestrator)
        )
        self.execution_client = TestClient(execution_app_module.app)

        def transport(request, timeout=10.0):
            _ = timeout
            parsed = urlsplit(request.full_url)
            path = parsed.path
            if parsed.query:
                path = f"{path}?{parsed.query}"
            body = request.data if request.data is not None else b""
            headers = dict(request.header_items())
            response = self.execution_client.request(
                method=request.get_method(),
                url=path,
                content=body,
                headers=headers,
            )
            if response.status_code >= 400:
                raise urllib.error.HTTPError(
                    request.full_url,
                    response.status_code,
                    "upstream error",
                    hdrs=None,
                    fp=io.BytesIO(response.content),
                )
            return _ProxyResponse(response.status_code, response.content)

        gateway_app_module._execution_client = RuntimeExecutionClient(
            base_url="http://runtime-execution.local",
            _transport=transport,
        )
        self.gateway_client = TestClient(gateway_app_module.app)

    def tearDown(self) -> None:
        gateway_app_module._execution_client = self._gateway_original_client
        execution_app_module._runtime = self._execution_original_runtime
        self._contracts_patch.stop()

    def _gateway_token(self, scopes: list[str]) -> str:
        return issue_token(
            {
                "iss": "runtime-gateway",
                "sub": "user:u-bridge",
                "aud": "runtime-gateway",
                "tenant_id": "t1",
                "app_id": "covernow",
                "scope": scopes,
                "token_use": "access",
                "trace_id": "trace-bridge-1",
                "session_key": "tenant:t1:app:covernow:channel:web:actor:u-bridge:thread:main:agent:pm",
            },
            ttl_seconds=300,
        )

    def test_gateway_submit_exercises_runtime_control_adapter_with_orchestrator_decision(self) -> None:
        response = self.gateway_client.post(
            "/v1/runs",
            json={
                "tenant_id": "t1",
                "app_id": "covernow",
                "session_key": "tenant:t1:app:covernow:channel:web:actor:u-bridge:thread:main:agent:pm",
                "ingress_mode": "workflow",
                "payload": {
                    "goal": "verify control adapter bridge",
                    "entry_mode": "workflow",
                    "dispatch_policy": {"queue": {"dispatch_min_score": 0}},
                },
            },
            headers={"Authorization": f"Bearer {self._gateway_token(['runs:write'])}"},
        )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        run_id = body["run_id"]
        self.assertEqual(body["status"], "queued")

        run = execution_app_module._runtime.runs[run_id]
        route_marker = run.payload.get("_runtime_route")
        self.assertIsInstance(route_marker, dict)
        assert isinstance(route_marker, dict)
        self.assertEqual(route_marker.get("event_type"), "runtime.route.decided")
        self.assertEqual(route_marker.get("execution_mode"), "control")
        self.assertEqual(route_marker.get("route_target"), "agent-orchestrator")

        self.assertTrue(self.orchestrator.called)
        assert self.orchestrator.last_payload is not None
        self.assertEqual(self.orchestrator.last_payload["entry_mode"], "workflow")
        self.assertEqual(
            self.orchestrator.last_payload["execution_context"]["task_plane"],
            "workflow_work",
        )
        self.assertEqual(
            self.orchestrator.last_payload["assistant_contract"]["version"],
            "assistant-boundary.v1",
        )
