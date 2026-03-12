from __future__ import annotations

import io
import importlib
import os
import sys
import unittest
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

os.environ["WAOOOOLAB_PLATFORM_CONTRACTS_DIR"] = str(
    Path(__file__).resolve().parent / "fixtures" / "contracts"
)

try:
    from fastapi.testclient import TestClient
    from runtime_gateway import app as gateway_app_module
    from runtime_gateway.auth.tokens import issue_token
    from runtime_gateway.integration import RuntimeExecutionClient
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
    from runtime_execution.modules.integration.envelope import build_command_envelope
except Exception:
    RUNTIME_EXECUTION_AVAILABLE = False
else:
    execution_app_module = importlib.import_module("runtime_execution.service_api.app")
    RUNTIME_EXECUTION_AVAILABLE = True

_DEVICE_HUB_SRC = os.environ.get("WAOOOOLAB_DEVICE_HUB_SRC_DIR")
if _DEVICE_HUB_SRC:
    candidate_device_src = Path(_DEVICE_HUB_SRC)
else:
    candidate_device_src = Path(__file__).resolve().parents[2] / "device-hub" / "src"

if candidate_device_src.exists():
    sys.path.insert(0, str(candidate_device_src))

try:
    from device_hub.service import DeviceHubService
except Exception:
    DEVICE_HUB_AVAILABLE = False
else:
    device_hub_app_module = importlib.import_module("device_hub.service_api.app")
    DEVICE_HUB_AVAILABLE = True


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


class _DeviceHubBoundaryClient:
    def __init__(self, *, client: TestClient, token_factory):
        self._client = client
        self._token_factory = token_factory

    def _headers(self, scopes: list[str]) -> dict[str, str]:
        token = self._token_factory(scopes)
        return {"Authorization": f"Bearer {token}"}

    def allocate_placement(
        self,
        *,
        run_id: str,
        task_id: str,
        session_key: str,
        trace_id: str,
        execution_profile: dict,
    ) -> dict:
        envelope = build_command_envelope(
            command_type="device.placement.allocate",
            payload={
                "run_id": run_id,
                "task_id": task_id,
                "execution_profile": execution_profile,
            },
            session_key=session_key,
            trace_id=trace_id,
            run_id=run_id,
            task_id=task_id,
        )
        response = self._client.post(
            "/v1/placements/allocate",
            json=envelope,
            headers=self._headers(["devices:write", "devices:read"]),
        )
        if response.status_code >= 400:
            raise RuntimeError(f"device-hub allocate failed: {response.status_code} {response.text}")
        return response.json()

    def release_placement(
        self,
        *,
        lease_id: str,
        session_key: str,
        trace_id: str,
        run_id: str | None = None,
        task_id: str | None = None,
    ) -> dict:
        payload = {"lease_id": lease_id}
        if run_id:
            payload["run_id"] = run_id
        if task_id:
            payload["task_id"] = task_id
        envelope = build_command_envelope(
            command_type="device.placement.release",
            payload=payload,
            session_key=session_key,
            trace_id=trace_id,
            run_id=run_id,
            task_id=task_id,
        )
        response = self._client.post(
            "/v1/placements/release",
            json=envelope,
            headers=self._headers(["devices:write", "devices:read"]),
        )
        if response.status_code >= 400:
            raise RuntimeError(f"device-hub release failed: {response.status_code} {response.text}")
        return response.json()

    def expire_placement(
        self,
        *,
        lease_id: str,
        reason_code: str,
        session_key: str,
        trace_id: str,
        run_id: str | None = None,
        task_id: str | None = None,
    ) -> dict:
        payload = {
            "lease_id": lease_id,
            "reason_code": reason_code,
        }
        if run_id:
            payload["run_id"] = run_id
        if task_id:
            payload["task_id"] = task_id
        envelope = build_command_envelope(
            command_type="device.placement.expire",
            payload=payload,
            session_key=session_key,
            trace_id=trace_id,
            run_id=run_id,
            task_id=task_id,
        )
        response = self._client.post(
            "/v1/placements/expire",
            json=envelope,
            headers=self._headers(["devices:write", "devices:read"]),
        )
        if response.status_code >= 400:
            raise RuntimeError(f"device-hub expire failed: {response.status_code} {response.text}")
        return response.json()

    def fetch_placement_capacity(
        self,
        *,
        session_key: str,
        trace_id: str,
    ) -> dict:
        _ = session_key, trace_id
        response = self._client.get(
            "/v1/placements/capacity",
            headers=self._headers(["devices:read"]),
        )
        if response.status_code >= 400:
            raise RuntimeError(f"device-hub capacity failed: {response.status_code} {response.text}")
        return response.json()


@unittest.skipUnless(
    FASTAPI_STACK_AVAILABLE and RUNTIME_EXECUTION_AVAILABLE,
    "fastapi/runtime-execution stack not available",
)
class EndToEndRunFlowTests(unittest.TestCase):
    def setUp(self) -> None:
        self._gateway_original_client = gateway_app_module._execution_client
        self._execution_original_runtime = execution_app_module._runtime

        execution_app_module._runtime = RuntimeExecutionService()
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

    def _gateway_token(self, scope: list[str]) -> str:
        return issue_token(
            {
                "iss": "runtime-gateway",
                "sub": "user:u-e2e",
                "aud": "runtime-gateway",
                "tenant_id": "t1",
                "app_id": "covernow",
                "scope": scope,
                "token_use": "access",
                "trace_id": "trace-e2e-1",
                "session_key": "tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
            },
            ttl_seconds=300,
        )

    def _execution_token(self, scope: list[str]) -> str:
        return issue_token(
            {
                "iss": "runtime-gateway",
                "sub": "svc:runtime-gateway",
                "aud": "runtime-execution",
                "tenant_id": "t1",
                "app_id": "covernow",
                "scope": scope,
                "token_use": "service",
                "trace_id": "trace-e2e-1",
                "session_key": "tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
            },
            ttl_seconds=300,
        )

    def _device_hub_token(self, scope: list[str]) -> str:
        return issue_token(
            {
                "iss": "runtime-gateway",
                "sub": "svc:runtime-gateway",
                "aud": "device-hub",
                "tenant_id": "t1",
                "app_id": "covernow",
                "scope": scope,
                "token_use": "service",
                "trace_id": "trace-e2e-device-hub",
                "session_key": "tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
            },
            ttl_seconds=300,
        )

    def _submit_run(self, token: str, goal: str, execution_profile: dict | None = None) -> str:
        payload = {
            "tenant_id": "t1",
            "app_id": "covernow",
            "session_key": "tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
            "payload": {"goal": goal},
        }
        if execution_profile is not None:
            payload["payload"]["execution_profile"] = execution_profile
        response = self.gateway_client.post(
            "/v1/runs",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "queued")
        return str(data["run_id"])

    def test_gateway_to_execution_e2e_run_flow(self) -> None:
        token = self._gateway_token(["runs:write"])
        run_id = self._submit_run(token, "verify e2e run flow")

        self.assertIn(run_id, execution_app_module._runtime.runs)
        run = execution_app_module._runtime.runs[run_id]
        self.assertEqual(run.trace_id, "trace-e2e-1")
        self.assertEqual(run.payload, {"goal": "verify e2e run flow"})

        read_response = self.execution_client.get(
            f"/v1/runs/{run_id}",
            headers={"Authorization": f"Bearer {self._execution_token(['runs:read'])}"},
        )
        self.assertEqual(read_response.status_code, 200)
        event = read_response.json()
        self.assertEqual(event["event_type"], "runtime.run.status")
        self.assertEqual(event["payload"]["run_id"], run_id)
        self.assertEqual(event["payload"]["status"], "queued")

        recent_response = self.gateway_client.get(
            "/v1/events/recent?limit=10",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(recent_response.status_code, 200)
        recent_items = recent_response.json()["items"]
        matching = [
            item["event"]
            for item in recent_items
            if item.get("event", {}).get("event_type") == "runtime.run.requested"
            and item.get("event", {}).get("payload", {}).get("run_id") == run_id
        ]
        self.assertEqual(len(matching), 1)
        signal = matching[0]["payload"]["scheduling_signal"]
        self.assertIsInstance(signal["queue_score"], float)
        self.assertIsInstance(signal["dispatch_min_score"], float)
        self.assertEqual(matching[0]["payload"]["route"]["scheduling_signal"], signal)

    def test_gateway_to_execution_cancel_flow(self) -> None:
        token = self._gateway_token(["runs:write"])
        run_id = self._submit_run(token, "verify cancel flow")

        cancel_response = self.gateway_client.post(
            f"/v1/runs/{run_id}:cancel",
            json={"reason": "operator_cancel_test", "cascade_children": True},
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(cancel_response.status_code, 200)
        canceled_event = cancel_response.json()
        self.assertEqual(canceled_event["event_type"], "runtime.run.status")
        self.assertEqual(canceled_event["payload"]["run_id"], run_id)
        self.assertEqual(canceled_event["payload"]["status"], "canceled")

    def test_gateway_to_execution_timeout_flow(self) -> None:
        token = self._gateway_token(["runs:write"])
        run_id = self._submit_run(token, "verify timeout flow")

        timeout_response = self.gateway_client.post(
            f"/v1/runs/{run_id}:timeout",
            json={"reason": "deadline_exceeded_test", "cascade_children": True},
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(timeout_response.status_code, 200)
        timed_out_event = timeout_response.json()
        self.assertEqual(timed_out_event["event_type"], "runtime.run.status")
        self.assertEqual(timed_out_event["payload"]["run_id"], run_id)
        self.assertEqual(timed_out_event["payload"]["status"], "timed_out")

    def test_gateway_to_execution_worker_health_flow(self) -> None:
        write_token = self._gateway_token(["runs:write"])
        read_token = self._gateway_token(["runs:read"])
        _ = self._submit_run(write_token, "verify worker health e2e flow")

        tick = self.gateway_client.post(
            "/v1/orchestration/worker:tick?fair=true&auto_start=true",
            headers={"Authorization": f"Bearer {write_token}"},
        )
        self.assertEqual(tick.status_code, 200)
        tick_payload = tick.json()
        self.assertIn("outcome", tick_payload)

        health = self.gateway_client.get(
            "/v1/orchestration/worker:health",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(health.status_code, 200)
        health_payload = health.json()
        self.assertGreaterEqual(int(health_payload["ticks_total"]), 1)
        self.assertIn("last_tick_outcome", health_payload)

    def test_gateway_and_execution_profile_catalog_are_aligned(self) -> None:
        gateway_response = self.gateway_client.get(
            "/v1/executors/profiles",
            headers={"Authorization": f"Bearer {self._gateway_token(['runs:read'])}"},
        )
        self.assertEqual(gateway_response.status_code, 200)

        execution_response = self.execution_client.get(
            "/v1/executors/profiles",
            headers={"Authorization": f"Bearer {self._execution_token(['runs:read'])}"},
        )
        self.assertEqual(execution_response.status_code, 200)

        gateway_items = gateway_response.json()["items"]
        execution_items = execution_response.json()["items"]

        normalize = lambda items: sorted(  # noqa: E731
            (
                item["family"],
                tuple(item["engines"]),
                tuple(item["adapters"]),
                tuple(item["access_modes"]),
                tuple(item["window_modes"]),
            )
            for item in items
        )
        self.assertEqual(normalize(gateway_items), normalize(execution_items))

    def test_gateway_cancel_expires_device_hub_lease_e2e(self) -> None:
        if not DEVICE_HUB_AVAILABLE:
            self.skipTest("device-hub stack not available")

        device_hub_app_module._hub = DeviceHubService()
        device_hub_client = TestClient(device_hub_app_module.app)
        execution_app_module._runtime = RuntimeExecutionService(
            device_hub_client=_DeviceHubBoundaryClient(
                client=device_hub_client,
                token_factory=self._device_hub_token,
            )
        )
        self.execution_client = TestClient(execution_app_module.app)

        register = device_hub_client.post(
            "/v1/devices/register",
            json=build_command_envelope(
                command_type="device.register",
                payload={"device_id": "gpu-node-gateway-e2e", "capabilities": ["compute.comfyui.local"]},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-device-register",
                run_id="run-gateway-device-bootstrap",
                task_id="task-gateway-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(register.status_code, 200)
        pair_request = device_hub_client.post(
            "/v1/devices/pairing/request",
            json=build_command_envelope(
                command_type="device.pairing.request",
                payload={"device_id": "gpu-node-gateway-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-device-pair",
                run_id="run-gateway-device-bootstrap",
                task_id="task-gateway-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(pair_request.status_code, 200)
        pair_code = pair_request.json()["payload"]["code"]
        approve = device_hub_client.post(
            "/v1/devices/pairing/approve",
            json=build_command_envelope(
                command_type="device.pairing.approve",
                payload={"code": pair_code},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-device-approve",
                run_id="run-gateway-device-bootstrap",
                task_id="task-gateway-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(approve.status_code, 200)
        heartbeat = device_hub_client.post(
            "/v1/devices/heartbeat",
            json=build_command_envelope(
                command_type="device.heartbeat",
                payload={"device_id": "gpu-node-gateway-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-device-heartbeat",
                run_id="run-gateway-device-bootstrap",
                task_id="task-gateway-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(heartbeat.status_code, 200)

        run_id = self._submit_run(
            self._gateway_token(["runs:write"]),
            "gateway compute cancel flow",
            execution_profile={
                "execution_mode": "compute",
                "inference_target": "none",
                "resource_class": "gpu",
                "placement_constraints": {
                    "tenant_id": "t1",
                    "required_capabilities": ["compute.comfyui.local"],
                },
            },
        )
        leased_run = execution_app_module._runtime.runs[run_id]
        self.assertEqual(leased_run.device_lease_state, "active")
        lease_id = leased_run.device_lease_id
        self.assertIsInstance(lease_id, str)

        cancel_response = self.gateway_client.post(
            f"/v1/runs/{run_id}:cancel",
            json={"reason": "operator_cancel_gateway_e2e", "cascade_children": True},
            headers={"Authorization": f"Bearer {self._gateway_token(['runs:write'])}"},
        )
        self.assertEqual(cancel_response.status_code, 200)
        self.assertEqual(cancel_response.json()["payload"]["status"], "canceled")

        self.assertEqual(execution_app_module._runtime.runs[run_id].device_lease_state, "expired")
        assert lease_id is not None
        lease = device_hub_app_module._hub.leases[lease_id]
        self.assertEqual(lease.status, "expired")
        self.assertEqual(lease.expire_reason_code, "run_canceled")

        capacity_response = device_hub_client.get(
            "/v1/placements/capacity",
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:read'])}"},
        )
        self.assertEqual(capacity_response.status_code, 200)
        self.assertEqual(capacity_response.json()["active_leases"], 0)


if __name__ == "__main__":
    unittest.main()
