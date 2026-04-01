from __future__ import annotations

import io
import importlib
import os
import sys
import time
import unittest
import urllib.error
from datetime import datetime, timedelta, timezone
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
    from runtime_execution.core.queue.queue_fabric import QueueType
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

    def preempt_placement(
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
            command_type="device.placement.preempt",
            payload=payload,
            session_key=session_key,
            trace_id=trace_id,
            run_id=run_id,
            task_id=task_id,
        )
        response = self._client.post(
            "/v1/placements/preempt",
            json=envelope,
            headers=self._headers(["devices:write", "devices:read"]),
        )
        if response.status_code >= 400:
            raise RuntimeError(f"device-hub preempt failed: {response.status_code} {response.text}")
        return response.json()

    def renew_placement(
        self,
        *,
        lease_id: str,
        lease_ttl_seconds: int,
        session_key: str,
        trace_id: str,
        run_id: str | None = None,
        task_id: str | None = None,
    ) -> dict:
        payload = {"lease_id": lease_id, "lease_ttl_seconds": lease_ttl_seconds}
        if run_id:
            payload["run_id"] = run_id
        if task_id:
            payload["task_id"] = task_id
        envelope = build_command_envelope(
            command_type="device.placement.renew",
            payload=payload,
            session_key=session_key,
            trace_id=trace_id,
            run_id=run_id,
            task_id=task_id,
        )
        response = self._client.post(
            "/v1/placements/renew",
            json=envelope,
            headers=self._headers(["devices:write", "devices:read"]),
        )
        if response.status_code >= 400:
            raise RuntimeError(f"device-hub renew failed: {response.status_code} {response.text}")
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

    def get_placement_lease(
        self,
        *,
        lease_id: str,
        session_key: str,
        trace_id: str,
        run_id: str | None = None,
        task_id: str | None = None,
    ) -> dict:
        _ = session_key, trace_id, run_id, task_id
        response = self._client.get(
            f"/v1/placements/leases/{lease_id}",
            headers=self._headers(["devices:read"]),
        )
        if response.status_code >= 400:
            raise RuntimeError(f"device-hub lease failed: {response.status_code} {response.text}")
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

    def _submit_run(
        self,
        token: str,
        goal: str,
        execution_profile: dict | None = None,
        retry_policy: dict | None = None,
    ) -> str:
        payload = {
            "tenant_id": "t1",
            "app_id": "covernow",
            "session_key": "tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
            "payload": {"goal": goal},
        }
        if execution_profile is not None:
            payload["payload"]["execution_profile"] = execution_profile
        if retry_policy is not None:
            payload["retry_policy"] = retry_policy
        response = self.gateway_client.post(
            "/v1/runs",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "queued")
        return str(data["run_id"])

    @staticmethod
    def _app_capability(
        *,
        capability_id: str,
        version: str = "1.0.0",
    ) -> dict:
        return {
            "schema_version": "app-capability.v1",
            "kind": "app",
            "capability_id": capability_id,
            "version": version,
            "display_name": f"{capability_id} name",
            "description": "gateway e2e app capability",
            "visibility": "private",
            "source": {
                "workflow_id": "wf-gateway-e2e",
                "workflow_version": "1",
                "graph_hash": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
                "mode": "node",
                "entry_nodes": ["n-start"],
                "exit_nodes": ["n-end"],
            },
            "interface": {
                "input_schema": {"type": "object"},
                "output_schema": {"type": "object"},
                "ui": {"default_mode": "tools", "switchable_modes": ["tools", "canvas"]},
            },
            "execution": {
                "requested_mode": "control",
                "route_policy": "default",
                "timeout_ms": 120000,
            },
            "governance": {
                "owner": "team-platform",
                "tags": ["gateway-e2e"],
                "review_status": "approved",
                "trust_level": "internal",
                "policy_profile": "internal_default",
            },
        }

    def test_gateway_capability_flow_end_to_end(self) -> None:
        write_token = self._gateway_token(["capabilities:write"])
        read_token = self._gateway_token(["capabilities:read"])
        invoke_token = self._gateway_token(["capabilities:invoke"])
        capability_id = "cap_gateway_e2e"

        register = self.gateway_client.post(
            "/v1/capabilities/register",
            json={"capability": self._app_capability(capability_id=capability_id)},
            headers={"Authorization": f"Bearer {write_token}"},
        )
        self.assertEqual(register.status_code, 200)
        self.assertEqual(register.json()["status"], "registered")

        listed = self.gateway_client.get(
            "/v1/capabilities",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(listed.status_code, 200)
        listed_items = listed.json()["items"]
        self.assertTrue(any(item.get("capability_id") == capability_id for item in listed_items))

        resolved = self.gateway_client.post(
            "/v1/capabilities/resolve",
            json={"capability_id": capability_id},
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(resolved.status_code, 200)
        self.assertEqual(resolved.json()["resolved_version"], "1.0.0")

        published = self.gateway_client.post(
            "/v1/capabilities/publish",
            json={"capability": self._app_capability(capability_id=capability_id, version="1.0.1")},
            headers={"Authorization": f"Bearer {write_token}"},
        )
        self.assertEqual(published.status_code, 200)
        self.assertEqual(published.json()["event_type"], "app.capability.published.v1")
        self.assertEqual(published.json()["payload"]["status"], "published")

        invoked = self.gateway_client.post(
            f"/v1/capabilities/{capability_id}:invoke",
            json={"version": "1.0.1", "input": {"prompt": "hello capability"}},
            headers={"Authorization": f"Bearer {invoke_token}"},
        )
        self.assertEqual(invoked.status_code, 200)
        self.assertEqual(invoked.json()["event_type"], "app.capability.invoked.v1")
        self.assertEqual(invoked.json()["payload"]["status"], "invoked")
        self.assertIsInstance(invoked.json()["payload"]["run_id"], str)

        recent = self.gateway_client.get(
            "/v1/events/recent?event_types=app.capability.invoked.v1&limit=10",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(recent.status_code, 200)
        recent_items = recent.json()["items"]
        self.assertGreaterEqual(len(recent_items), 1)
        self.assertEqual(recent_items[-1]["event"]["event_type"], "app.capability.invoked.v1")
        self.assertEqual(recent_items[-1]["event"]["payload"]["capability_id"], capability_id)

    def test_gateway_capability_compile_publish_envelope_flow_end_to_end(self) -> None:
        write_token = self._gateway_token(["capabilities:write"])
        invoke_token = self._gateway_token(["capabilities:invoke"])
        capability_id = "cap_gateway_compile_publish_e2e"

        compiled = self.gateway_client.post(
            "/v1/capabilities/compile",
            json={
                "capability_id": capability_id,
                "version": "0.2.0",
                "workflow_source": {
                    "workflow_id": "wf-gateway-cap-compile-publish",
                    "workflow_version": "1",
                    "graph_hash": "sha256:4444444444444444444444444444444444444444444444444444444444444444",
                    "mode": "node",
                },
                "trace_id": "trace-gateway-cap-compile-publish",
                "session_key": "tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
            },
            headers={"Authorization": f"Bearer {write_token}"},
        )
        self.assertEqual(compiled.status_code, 200)
        compiled_event = compiled.json()
        self.assertEqual(compiled_event["event_type"], "app.capability.compiled.v1")
        self.assertEqual(compiled_event["payload"]["capability_id"], capability_id)

        published = self.gateway_client.post(
            "/v1/capabilities/publish",
            json=compiled_event,
            headers={"Authorization": f"Bearer {write_token}"},
        )
        self.assertEqual(published.status_code, 200)
        published_event = published.json()
        self.assertEqual(published_event["event_type"], "app.capability.published.v1")
        self.assertEqual(published_event["payload"]["capability_id"], capability_id)
        self.assertEqual(published_event["payload"]["capability_version"], "0.2.0")
        self.assertEqual(published_event["trace_id"], "trace-gateway-cap-compile-publish")

        invoked = self.gateway_client.post(
            f"/v1/capabilities/{capability_id}:invoke",
            json={"version": "0.2.0", "input": {"prompt": "hello compile publish"}},
            headers={"Authorization": f"Bearer {invoke_token}"},
        )
        self.assertEqual(invoked.status_code, 200)
        invoked_event = invoked.json()
        self.assertEqual(invoked_event["event_type"], "app.capability.invoked.v1")
        self.assertEqual(invoked_event["payload"]["capability_id"], capability_id)
        self.assertEqual(invoked_event["payload"]["status"], "invoked")

    def test_gateway_to_execution_e2e_run_flow(self) -> None:
        token = self._gateway_token(["runs:write"])
        run_id = self._submit_run(token, "verify e2e run flow")

        self.assertIn(run_id, execution_app_module._runtime.runs)
        run = execution_app_module._runtime.runs[run_id]
        self.assertEqual(run.trace_id, "trace-e2e-1")
        self.assertEqual(run.payload.get("goal"), "verify e2e run flow")
        route_marker = run.payload.get("_runtime_route")
        self.assertIsInstance(route_marker, dict)
        assert isinstance(route_marker, dict)
        self.assertEqual(route_marker.get("event_type"), "runtime.route.decided")
        self.assertEqual(route_marker.get("execution_mode"), "control")
        self.assertEqual(route_marker.get("route_target"), "langgraph-core")

        read_response = self.execution_client.get(
            f"/v1/runs/{run_id}",
            headers={"Authorization": f"Bearer {self._execution_token(['runs:read'])}"},
        )
        self.assertEqual(read_response.status_code, 200)
        event = read_response.json()
        self.assertEqual(event["event_type"], "runtime.run.status")
        self.assertEqual(event["payload"]["run_id"], run_id)
        self.assertEqual(event["payload"]["status"], "queued")

        read_token = self._gateway_token(["runs:read"])
        gateway_status_response = self.gateway_client.get(
            f"/v1/runs/{run_id}",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(gateway_status_response.status_code, 200)
        gateway_event = gateway_status_response.json()
        self.assertEqual(gateway_event["event_type"], "runtime.run.status")
        self.assertEqual(gateway_event["payload"]["run_id"], run_id)
        self.assertEqual(gateway_event["payload"]["status"], "queued")
        self.assertEqual(gateway_event["recommended_poll_after_ms"], 1500)

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
        self.assertEqual(canceled_event["payload"]["orchestration"]["failure_reason_code"], "run_canceled")

    def test_gateway_websocket_run_filter_only_receives_target_run_status(self) -> None:
        write_token = self._gateway_token(["runs:write"])
        read_token = self._gateway_token(["runs:read"])
        target_run_id = self._submit_run(write_token, "verify ws run filter target")
        other_run_id = self._submit_run(write_token, "verify ws run filter other")

        with self.gateway_client.websocket_connect(
            f"/v1/ws/events?access_token={read_token}&tenant_id=t1&app_id=covernow"
            f"&run_id={target_run_id}&event_types=runtime.run.status"
        ) as ws:
            ready = ws.receive_json()
            self.assertEqual(ready["kind"], "ws.ready")
            self.assertEqual(ready["run_id"], target_run_id)

            other_cancel = self.gateway_client.post(
                f"/v1/runs/{other_run_id}:cancel",
                json={"reason": "ws-filter-ignore", "cascade_children": True},
                headers={"Authorization": f"Bearer {write_token}"},
            )
            self.assertEqual(other_cancel.status_code, 200)
            self.assertEqual(other_cancel.json()["payload"]["status"], "canceled")

            target_cancel = self.gateway_client.post(
                f"/v1/runs/{target_run_id}:cancel",
                json={"reason": "ws-filter-target", "cascade_children": True},
                headers={"Authorization": f"Bearer {write_token}"},
            )
            self.assertEqual(target_cancel.status_code, 200)
            self.assertEqual(target_cancel.json()["payload"]["status"], "canceled")

            event = ws.receive_json()
            self.assertEqual(event["event"]["event_type"], "runtime.run.status")
            self.assertEqual(event["event"]["payload"]["run_id"], target_run_id)
            self.assertEqual(event["event"]["payload"]["status"], "canceled")
            self.assertEqual(
                event["event"]["payload"]["orchestration"]["failure_reason_code"],
                "run_canceled",
            )
            route = event["event"]["payload"].get("route")
            self.assertIsInstance(route, dict)
            assert isinstance(route, dict)
            self.assertEqual(route.get("execution_mode"), "control")
            self.assertEqual(route.get("route_target"), "langgraph-core")

        target_recent = self.gateway_client.get(
            f"/v1/events/recent?run_id={target_run_id}&event_types=runtime.run.status&limit=10",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(target_recent.status_code, 200)
        target_items = target_recent.json()["items"]
        target_status_events = [
            item["event"]
            for item in target_items
            if item.get("event", {}).get("payload", {}).get("run_id") == target_run_id
        ]
        self.assertGreaterEqual(len(target_status_events), 1)
        latest_target_event = target_status_events[-1]
        self.assertEqual(latest_target_event["event_type"], "runtime.run.status")
        self.assertEqual(latest_target_event["payload"]["status"], "canceled")
        self.assertEqual(
            latest_target_event["payload"]["orchestration"]["failure_reason_code"],
            "run_canceled",
        )
        latest_route = latest_target_event["payload"].get("route")
        self.assertIsInstance(latest_route, dict)
        assert isinstance(latest_route, dict)
        self.assertEqual(latest_route.get("execution_mode"), "control")
        self.assertEqual(latest_route.get("route_target"), "langgraph-core")

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
        self.assertEqual(timed_out_event["payload"]["orchestration"]["failure_reason_code"], "run_timed_out")

    def test_gateway_to_execution_complete_success_flow(self) -> None:
        token = self._gateway_token(["runs:write"])
        run_id = self._submit_run(token, "verify complete success flow")

        tick = self.gateway_client.post(
            "/v1/orchestration/worker:tick?fair=true&auto_start=true",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(tick.status_code, 200)
        self.assertEqual(tick.json()["outcome"], "progressed")

        complete_response = self.gateway_client.post(
            f"/v1/runs/{run_id}:complete",
            json={"success": True},
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(complete_response.status_code, 200)
        completed_event = complete_response.json()
        self.assertEqual(completed_event["event_type"], "runtime.run.status")
        self.assertEqual(completed_event["payload"]["run_id"], run_id)
        self.assertEqual(completed_event["payload"]["status"], "succeeded")
        self.assertEqual(execution_app_module._runtime.runs[run_id].status.value, "succeeded")

    def test_gateway_to_execution_complete_failure_requeues_flow(self) -> None:
        token = self._gateway_token(["runs:write"])
        run_id = self._submit_run(token, "verify complete failure flow")

        tick = self.gateway_client.post(
            "/v1/orchestration/worker:tick?fair=true&auto_start=true",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(tick.status_code, 200)
        self.assertEqual(tick.json()["outcome"], "progressed")

        complete_response = self.gateway_client.post(
            f"/v1/runs/{run_id}:complete",
            json={"success": False},
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(complete_response.status_code, 200)
        completed_event = complete_response.json()
        self.assertEqual(completed_event["event_type"], "runtime.run.status")
        self.assertEqual(completed_event["payload"]["run_id"], run_id)
        self.assertEqual(completed_event["payload"]["status"], "queued")
        self.assertEqual(completed_event["payload"]["retry_attempts"], 1)
        self.assertEqual(execution_app_module._runtime.runs[run_id].status.value, "queued")

    def test_gateway_to_execution_complete_conflict_flow(self) -> None:
        token = self._gateway_token(["runs:write"])
        run_id = self._submit_run(token, "verify complete conflict flow")

        complete_response = self.gateway_client.post(
            f"/v1/runs/{run_id}:complete",
            json={"success": True},
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(complete_response.status_code, 409)
        detail = complete_response.json().get("detail")
        self.assertIsNotNone(detail)
        self.assertIn("invalid run transition", str(detail))
        self.assertEqual(execution_app_module._runtime.runs[run_id].status.value, "queued")

    def test_gateway_to_execution_approve_waiting_run_flow(self) -> None:
        token = self._gateway_token(["runs:write"])
        response = self.gateway_client.post(
            "/v1/runs",
            json={
                "tenant_id": "t1",
                "app_id": "covernow",
                "session_key": "tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                "payload": {
                    "goal": "verify approve waiting flow",
                    "dispatch_policy": {
                        "autonomy": {
                            "uncertainty": "high",
                        }
                    },
                },
            },
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 409)
        detail = response.json().get("detail")
        self.assertIsInstance(detail, dict)
        assert isinstance(detail, dict)
        self.assertEqual(detail.get("downstream_event_type"), "runtime.route.failed")
        self.assertEqual(detail.get("failure_classification"), "policy")
        self.assertEqual(detail.get("run_status"), "waiting_approval")
        self.assertEqual(detail.get("retryable"), False)

        self.assertEqual(len(execution_app_module._runtime.runs), 1)
        run_id = next(iter(execution_app_module._runtime.runs.keys()))
        self.assertEqual(execution_app_module._runtime.runs[run_id].status.value, "waiting_approval")

        approve = self.gateway_client.post(
            f"/v1/runs/{run_id}:approve",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(approve.status_code, 200)
        approved_event = approve.json()
        self.assertEqual(approved_event["event_type"], "runtime.run.status")
        self.assertEqual(approved_event["payload"]["run_id"], run_id)
        self.assertEqual(approved_event["payload"]["status"], "queued")
        self.assertEqual(execution_app_module._runtime.runs[run_id].status.value, "queued")

    def test_gateway_to_execution_approve_conflict_flow(self) -> None:
        token = self._gateway_token(["runs:write"])
        run_id = self._submit_run(token, "verify approve conflict flow")

        approve = self.gateway_client.post(
            f"/v1/runs/{run_id}:approve",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(approve.status_code, 409)
        detail = approve.json().get("detail")
        self.assertIsNotNone(detail)
        self.assertIn("invalid run transition", str(detail))
        self.assertEqual(execution_app_module._runtime.runs[run_id].status.value, "queued")

    def test_gateway_to_execution_approve_then_worker_tick_progresses_run_flow(self) -> None:
        token = self._gateway_token(["runs:write"])
        response = self.gateway_client.post(
            "/v1/runs",
            json={
                "tenant_id": "t1",
                "app_id": "covernow",
                "session_key": "tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                "payload": {
                    "goal": "verify approve then worker tick flow",
                    "dispatch_policy": {
                        "autonomy": {
                            "uncertainty": "high",
                        }
                    },
                },
            },
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 409)
        run_id = next(iter(execution_app_module._runtime.runs.keys()))

        approve = self.gateway_client.post(
            f"/v1/runs/{run_id}:approve",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(approve.status_code, 200)
        self.assertEqual(approve.json()["payload"]["status"], "queued")

        tick = self.gateway_client.post(
            "/v1/orchestration/worker:tick?fair=true&auto_start=true",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(tick.status_code, 200)
        tick_payload = tick.json()
        self.assertEqual(tick_payload["outcome"], "progressed")
        self.assertEqual(tick_payload["leased_run_id"], run_id)
        self.assertEqual(tick_payload["before_status"], "queued")
        self.assertEqual(tick_payload["after_status"], "running")
        self.assertEqual(execution_app_module._runtime.runs[run_id].status.value, "running")

    def test_gateway_to_execution_worker_health_flow(self) -> None:
        write_token = self._gateway_token(["runs:write"])
        read_token = self._gateway_token(["runs:read"])
        _ = self._submit_run(write_token, "verify worker health e2e flow")

        health_before = self.gateway_client.get(
            "/v1/orchestration/worker:health",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(health_before.status_code, 200)
        health_before_payload = health_before.json()
        self.assertEqual(int(health_before_payload["queue_depth"]), 1)
        self.assertEqual(int(health_before_payload["ticks_total"]), 0)
        self.assertIsNone(health_before_payload["last_tick_outcome"])
        self.assertEqual(int(health_before_payload["missing_run_ticks_total"]), 0)
        self.assertEqual(int(health_before_payload["skipped_ticks_total"]), 0)
        self.assertEqual(int(health_before_payload["drain_processed_total"]), 0)
        self.assertIs(health_before_payload["is_backlogged"], True)
        self.assertIs(health_before_payload["is_stalled"], True)
        self.assertEqual(health_before_payload["health_state"], "stalled")

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
        self.assertEqual(int(health_payload["queue_depth"]), 0)
        self.assertGreaterEqual(int(health_payload["progressed_ticks_total"]), 1)
        self.assertEqual(health_payload["last_tick_outcome"], "progressed")
        self.assertEqual(int(health_payload["missing_run_ticks_total"]), 0)
        self.assertEqual(int(health_payload["skipped_ticks_total"]), 0)
        self.assertGreaterEqual(int(health_payload["drain_processed_total"]), 0)
        self.assertIs(health_payload["is_backlogged"], False)
        self.assertIs(health_payload["is_stalled"], False)
        self.assertIs(health_payload["is_tick_stale"], False)
        self.assertEqual(health_payload["health_state"], "healthy")

    def test_gateway_to_execution_worker_lifecycle_flow(self) -> None:
        write_token = self._gateway_token(["runs:write"])
        read_token = self._gateway_token(["runs:read"])

        status_before = self.gateway_client.get(
            "/v1/orchestration/worker:status",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(status_before.status_code, 200)
        self.assertEqual(status_before.json()["lifecycle_state"], "running")
        self.assertIs(status_before.json()["is_running"], True)

        stop = self.gateway_client.post(
            "/v1/orchestration/worker:stop",
            json={"reason": "maintenance"},
            headers={"Authorization": f"Bearer {write_token}"},
        )
        self.assertEqual(stop.status_code, 200)
        self.assertEqual(stop.json()["action"], "stop")
        self.assertEqual(stop.json()["lifecycle_state"], "stopped")
        self.assertIs(stop.json()["is_running"], False)

        tick_while_stopped = self.gateway_client.post(
            "/v1/orchestration/worker:tick?fair=true&auto_start=true",
            headers={"Authorization": f"Bearer {write_token}"},
        )
        self.assertEqual(tick_while_stopped.status_code, 409)
        detail = tick_while_stopped.json()["detail"]
        self.assertEqual(detail["lifecycle_state"], "stopped")
        self.assertIs(detail["is_running"], False)
        self.assertEqual(detail["requested_action"], "tick")
        self.assertIn("recommended_poll_after_ms", detail)

        start = self.gateway_client.post(
            "/v1/orchestration/worker:start",
            json={"reason": "resume"},
            headers={"Authorization": f"Bearer {write_token}"},
        )
        self.assertEqual(start.status_code, 200)
        self.assertEqual(start.json()["action"], "start")
        self.assertEqual(start.json()["lifecycle_state"], "running")
        self.assertIs(start.json()["is_running"], True)

        run_id = self._submit_run(write_token, "verify worker lifecycle e2e flow")
        tick_after_start = self.gateway_client.post(
            "/v1/orchestration/worker:tick?fair=true&auto_start=true",
            headers={"Authorization": f"Bearer {write_token}"},
        )
        self.assertEqual(tick_after_start.status_code, 200)
        self.assertEqual(tick_after_start.json()["outcome"], "progressed")
        self.assertEqual(tick_after_start.json()["leased_run_id"], run_id)

        restart = self.gateway_client.post(
            "/v1/orchestration/worker:restart",
            json={"reason": "refresh"},
            headers={"Authorization": f"Bearer {write_token}"},
        )
        self.assertEqual(restart.status_code, 200)
        self.assertEqual(restart.json()["action"], "restart")
        self.assertEqual(restart.json()["lifecycle_state"], "running")
        self.assertGreaterEqual(int(restart.json()["restart_total"]), 1)

        status_after = self.gateway_client.get(
            "/v1/orchestration/worker:status",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(status_after.status_code, 200)
        self.assertEqual(status_after.json()["lifecycle_state"], "running")
        self.assertIs(status_after.json()["is_running"], True)
        self.assertGreaterEqual(int(status_after.json()["restart_total"]), 1)

    def test_gateway_to_execution_worker_drain_signal_flow(self) -> None:
        write_token = self._gateway_token(["runs:write"])
        _ = self._submit_run(write_token, "verify worker drain signal e2e flow #1")
        _ = self._submit_run(write_token, "verify worker drain signal e2e flow #2")

        drain = self.gateway_client.post(
            "/v1/orchestration/worker:drain?max_items=1&fair=true&auto_start=true",
            headers={"Authorization": f"Bearer {write_token}"},
        )
        self.assertEqual(drain.status_code, 200)
        payload = drain.json()
        self.assertEqual(int(payload["max_items"]), 1)
        self.assertEqual(int(payload["processed"]), 1)
        self.assertGreaterEqual(int(payload["remaining"]), 1)
        self.assertIs(payload["should_continue"], True)
        self.assertIn("outcome_counts", payload)
        self.assertEqual(int(payload["outcome_counts"]["progressed"]), 1)
        self.assertEqual(int(payload["outcome_counts"]["missing_run"]), 0)
        self.assertEqual(int(payload["outcome_counts"]["skipped"]), 0)
        self.assertAlmostEqual(float(payload["anomaly_ratio"]), 0.0, places=6)
        self.assertAlmostEqual(float(payload["progressed_ratio"]), 1.0, places=6)
        self.assertIs(payload["stalled_signal"], False)
        self.assertIn("scheduling_signal", payload)
        self.assertIs(payload["scheduling_signal"]["stalled_signal"], False)
        self.assertAlmostEqual(float(payload["scheduling_signal"]["anomaly_ratio"]), 0.0, places=6)

    def test_gateway_to_execution_worker_drain_invalid_max_items_returns_structured_error(self) -> None:
        write_token = self._gateway_token(["runs:write"])

        drain = self.gateway_client.post(
            "/v1/orchestration/worker:drain?max_items=600&fair=true&auto_start=true",
            headers={"Authorization": f"Bearer {write_token}"},
        )
        self.assertEqual(drain.status_code, 422)
        detail = drain.json()["detail"]
        self.assertEqual(detail["status_code"], 422)
        self.assertIn("max_items must be <= 512", detail["downstream_detail"])
        self.assertEqual(int(detail["max_items"]), 600)
        self.assertEqual(int(detail["processed"]), 0)
        self.assertEqual(int(detail["queue_depth_before"]), 0)
        self.assertEqual(int(detail["queue_depth_after"]), 0)
        self.assertEqual(int(detail["remaining"]), 0)
        self.assertIs(detail["should_continue"], False)
        self.assertEqual(detail["outcome_counts"], {"progressed": 0, "missing_run": 0, "skipped": 0})
        self.assertEqual(
            detail["anomaly_counts"],
            {
                "missing_run": 0,
                "skipped": 0,
                "dispatch_retry_deferred": 0,
                "dispatch_retry_failed": 0,
                "total": 0,
            },
        )
        self.assertAlmostEqual(float(detail["anomaly_ratio"]), 0.0, places=6)
        self.assertAlmostEqual(float(detail["progressed_ratio"]), 0.0, places=6)
        self.assertIs(detail["stalled_signal"], False)
        self.assertEqual(int(detail["recommended_poll_after_ms"]), 5000)
        self.assertIn("scheduling_signal", detail)
        self.assertEqual(int(detail["scheduling_signal"]["recommended_poll_after_ms"]), 5000)
        self.assertIs(detail["scheduling_signal"]["stalled_signal"], False)

    def test_gateway_to_execution_scheduler_flow(self) -> None:
        write_token = self._gateway_token(["runs:write"])
        read_token = self._gateway_token(["runs:read"])
        run_id = self._submit_run(write_token, "verify scheduler e2e flow")
        _ = execution_app_module._runtime.queue_fabric.lease_one(
            QueueType.ORCHESTRATION
        )

        enqueue = self.gateway_client.post(
            "/v1/orchestration/scheduler:enqueue",
            headers={"Authorization": f"Bearer {write_token}"},
            json={"run_id": run_id, "delay_ms": 0, "reason": "manual-schedule"},
        )
        self.assertEqual(enqueue.status_code, 200)
        enqueue_payload = enqueue.json()
        self.assertEqual(enqueue_payload["run_id"], run_id)
        self.assertEqual(int(enqueue_payload["scheduler_depth"]), 1)
        self.assertIn("recommended_poll_after_ms", enqueue_payload)

        tick = self.gateway_client.post(
            "/v1/orchestration/scheduler:tick?max_items=2&fair=true",
            headers={"Authorization": f"Bearer {write_token}"},
        )
        self.assertEqual(tick.status_code, 200)
        tick_payload = tick.json()
        self.assertEqual(int(tick_payload["processed"]), 1)
        self.assertEqual(int(tick_payload["promoted"]), 1)
        self.assertEqual(int(tick_payload["deferred"]), 0)
        self.assertEqual(int(tick_payload["scheduler_depth_after"]), 0)
        self.assertEqual(int(tick_payload["orchestration_depth_after"]), 1)
        self.assertEqual(tick_payload["outcomes"][0]["run_id"], run_id)

        health = self.gateway_client.get(
            "/v1/orchestration/scheduler:health",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(health.status_code, 200)
        health_payload = health.json()
        self.assertEqual(int(health_payload["scheduler_depth"]), 0)
        self.assertGreaterEqual(int(health_payload["ticks_total"]), 1)
        self.assertGreaterEqual(int(health_payload["enqueue_total"]), 1)

    def test_gateway_scheduler_to_worker_loop_chain_e2e(self) -> None:
        write_token = self._gateway_token(["runs:write"])
        read_token = self._gateway_token(["runs:read"])
        run_id = self._submit_run(write_token, "verify scheduler worker-loop chain e2e flow")
        _ = execution_app_module._runtime.queue_fabric.lease_one(
            QueueType.ORCHESTRATION
        )

        enqueue = self.gateway_client.post(
            "/v1/orchestration/scheduler:enqueue",
            headers={"Authorization": f"Bearer {write_token}"},
            json={"run_id": run_id, "delay_ms": 0, "reason": "scheduler-worker-loop-chain"},
        )
        self.assertEqual(enqueue.status_code, 200)
        self.assertEqual(int(enqueue.json()["scheduler_depth"]), 1)

        loop = self.gateway_client.post(
            "/v1/orchestration/worker:loop"
            "?scheduler_max_items=1&scheduler_fair=true"
            "&worker_max_items=1&worker_fair=true&auto_start=true",
            headers={"Authorization": f"Bearer {write_token}"},
        )
        self.assertEqual(loop.status_code, 200)
        payload = loop.json()
        self.assertEqual(int(payload["scheduler_processed"]), 1)
        self.assertEqual(int(payload["scheduler_promoted"]), 1)
        self.assertEqual(int(payload["processed"]), 1)
        self.assertEqual(int(payload["remaining"]), 0)
        self.assertEqual(int(payload["queue_depth_after"]), 0)
        self.assertEqual(int(payload["scheduler_depth_after"]), 0)
        self.assertEqual(
            payload["outcome_counts"],
            {"progressed": 1, "missing_run": 0, "skipped": 0},
        )
        self.assertEqual(payload["lifecycle_state"], "running")
        self.assertIs(payload["is_running"], True)

        status = self.gateway_client.get(
            f"/v1/runs/{run_id}",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(status.status_code, 200)
        status_payload = status.json()["payload"]
        self.assertEqual(status_payload["run_id"], run_id)
        self.assertEqual(status_payload["status"], "running")
        self.assertEqual(execution_app_module._runtime.runs[run_id].status.value, "running")

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

    def test_gateway_run_lease_lookup_reconciles_ttl_expired_state_e2e(self) -> None:
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
                payload={
                    "device_id": "gpu-node-gateway-lease-sync-e2e",
                    "capabilities": ["compute.comfyui.local"],
                },
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-lease-sync-device-register",
                run_id="run-gateway-lease-sync-device-bootstrap",
                task_id="task-gateway-lease-sync-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(register.status_code, 200)
        pair_request = device_hub_client.post(
            "/v1/devices/pairing/request",
            json=build_command_envelope(
                command_type="device.pairing.request",
                payload={"device_id": "gpu-node-gateway-lease-sync-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-lease-sync-device-pair",
                run_id="run-gateway-lease-sync-device-bootstrap",
                task_id="task-gateway-lease-sync-device-bootstrap",
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
                trace_id="trace-gateway-lease-sync-device-approve",
                run_id="run-gateway-lease-sync-device-bootstrap",
                task_id="task-gateway-lease-sync-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(approve.status_code, 200)
        heartbeat = device_hub_client.post(
            "/v1/devices/heartbeat",
            json=build_command_envelope(
                command_type="device.heartbeat",
                payload={"device_id": "gpu-node-gateway-lease-sync-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-lease-sync-device-heartbeat",
                run_id="run-gateway-lease-sync-device-bootstrap",
                task_id="task-gateway-lease-sync-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(heartbeat.status_code, 200)

        run_id = self._submit_run(
            self._gateway_token(["runs:write"]),
            "gateway lease ttl sync flow",
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
        assert lease_id is not None

        device_hub_app_module._hub.leases[lease_id].lease_expires_at = (
            datetime.now(timezone.utc) - timedelta(seconds=5)
        ).isoformat()

        lease_expired = self.gateway_client.get(
            f"/v1/runs/{run_id}/lease",
            headers={"Authorization": f"Bearer {self._gateway_token(['runs:read'])}"},
        )
        self.assertEqual(lease_expired.status_code, 200)
        payload = lease_expired.json()
        self.assertEqual(payload["run_id"], run_id)
        self.assertEqual(payload["lease"]["state"], "expired")
        self.assertEqual(payload["device_hub"]["status"], "ok")
        self.assertEqual(payload["device_hub"]["snapshot"]["status"], "expired")
        self.assertEqual(payload["device_hub"]["snapshot"]["expire_reason_code"], "ttl_expired")
        self.assertEqual(payload["recommended_poll_after_ms"], 10000)

        self.assertEqual(execution_app_module._runtime.runs[run_id].device_lease_state, "expired")
        resumed = execution_app_module._runtime.resume_from_checkpoint(run_id)
        self.assertEqual(resumed.device_lease_state, "expired")
        self.assertEqual(device_hub_app_module._hub.leases[lease_id].status, "expired")
        self.assertEqual(device_hub_app_module._hub.leases[lease_id].expire_reason_code, "ttl_expired")

    def test_gateway_run_lease_lookup_reconciles_invalid_expiry_state_e2e(self) -> None:
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
                payload={
                    "device_id": "gpu-node-gateway-invalid-expiry-sync-e2e",
                    "capabilities": ["compute.comfyui.local"],
                },
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-invalid-expiry-sync-device-register",
                run_id="run-gateway-invalid-expiry-sync-device-bootstrap",
                task_id="task-gateway-invalid-expiry-sync-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(register.status_code, 200)
        pair_request = device_hub_client.post(
            "/v1/devices/pairing/request",
            json=build_command_envelope(
                command_type="device.pairing.request",
                payload={"device_id": "gpu-node-gateway-invalid-expiry-sync-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-invalid-expiry-sync-device-pair",
                run_id="run-gateway-invalid-expiry-sync-device-bootstrap",
                task_id="task-gateway-invalid-expiry-sync-device-bootstrap",
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
                trace_id="trace-gateway-invalid-expiry-sync-device-approve",
                run_id="run-gateway-invalid-expiry-sync-device-bootstrap",
                task_id="task-gateway-invalid-expiry-sync-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(approve.status_code, 200)
        heartbeat = device_hub_client.post(
            "/v1/devices/heartbeat",
            json=build_command_envelope(
                command_type="device.heartbeat",
                payload={"device_id": "gpu-node-gateway-invalid-expiry-sync-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-invalid-expiry-sync-device-heartbeat",
                run_id="run-gateway-invalid-expiry-sync-device-bootstrap",
                task_id="task-gateway-invalid-expiry-sync-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(heartbeat.status_code, 200)

        run_id = self._submit_run(
            self._gateway_token(["runs:write"]),
            "gateway lease invalid expiry sync flow",
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
        assert lease_id is not None

        device_hub_app_module._hub.leases[lease_id].lease_expires_at = "invalid-datetime"

        lease_expired = self.gateway_client.get(
            f"/v1/runs/{run_id}/lease",
            headers={"Authorization": f"Bearer {self._gateway_token(['runs:read'])}"},
        )
        self.assertEqual(lease_expired.status_code, 200)
        payload = lease_expired.json()
        self.assertEqual(payload["run_id"], run_id)
        self.assertEqual(payload["lease"]["state"], "expired")
        self.assertEqual(payload["device_hub"]["status"], "ok")
        self.assertEqual(payload["device_hub"]["snapshot"]["status"], "expired")
        self.assertEqual(payload["device_hub"]["snapshot"]["expire_reason_code"], "ttl_expired")
        self.assertEqual(payload["recommended_poll_after_ms"], 10000)

        self.assertEqual(execution_app_module._runtime.runs[run_id].device_lease_state, "expired")
        resumed = execution_app_module._runtime.resume_from_checkpoint(run_id)
        self.assertEqual(resumed.device_lease_state, "expired")
        self.assertEqual(device_hub_app_module._hub.leases[lease_id].status, "expired")
        self.assertEqual(device_hub_app_module._hub.leases[lease_id].expire_reason_code, "ttl_expired")

    def test_gateway_worker_tick_auto_renews_due_active_lease_e2e(self) -> None:
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
                payload={
                    "device_id": "gpu-node-gateway-auto-renew-e2e",
                    "capabilities": ["compute.comfyui.local"],
                },
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-auto-renew-device-register",
                run_id="run-gateway-auto-renew-device-bootstrap",
                task_id="task-gateway-auto-renew-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(register.status_code, 200)
        pair_request = device_hub_client.post(
            "/v1/devices/pairing/request",
            json=build_command_envelope(
                command_type="device.pairing.request",
                payload={"device_id": "gpu-node-gateway-auto-renew-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-auto-renew-device-pair",
                run_id="run-gateway-auto-renew-device-bootstrap",
                task_id="task-gateway-auto-renew-device-bootstrap",
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
                trace_id="trace-gateway-auto-renew-device-approve",
                run_id="run-gateway-auto-renew-device-bootstrap",
                task_id="task-gateway-auto-renew-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(approve.status_code, 200)
        heartbeat = device_hub_client.post(
            "/v1/devices/heartbeat",
            json=build_command_envelope(
                command_type="device.heartbeat",
                payload={"device_id": "gpu-node-gateway-auto-renew-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-auto-renew-device-heartbeat",
                run_id="run-gateway-auto-renew-device-bootstrap",
                task_id="task-gateway-auto-renew-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(heartbeat.status_code, 200)

        write_token = self._gateway_token(["runs:write"])
        run_id = self._submit_run(
            write_token,
            "gateway worker tick auto renew flow",
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

        start = self.gateway_client.post(
            "/v1/orchestration/worker:tick?fair=true&auto_start=true",
            headers={"Authorization": f"Bearer {write_token}"},
        )
        self.assertEqual(start.status_code, 200)
        self.assertEqual(start.json()["outcome"], "progressed")
        self.assertEqual(start.json()["after_status"], "running")

        leased_run = execution_app_module._runtime.runs[run_id]
        self.assertEqual(leased_run.device_lease_state, "active")
        lease_id = leased_run.device_lease_id
        self.assertIsInstance(lease_id, str)
        assert lease_id is not None
        lease_before = device_hub_app_module._hub.leases[lease_id].lease_expires_at
        leased_run.device_lease_next_renew_due_at = (
            datetime.now(timezone.utc) - timedelta(seconds=5)
        ).isoformat()

        renew_tick = self.gateway_client.post(
            "/v1/orchestration/worker:tick?fair=true&auto_start=true",
            headers={"Authorization": f"Bearer {write_token}"},
        )
        self.assertEqual(renew_tick.status_code, 200)
        renew_payload = renew_tick.json()
        self.assertEqual(renew_payload["outcome"], "idle")
        self.assertEqual(renew_payload["lease_renew_signal"]["attempted"], 1)
        self.assertEqual(renew_payload["lease_renew_signal"]["renewed"], 1)

        health = self.gateway_client.get(
            "/v1/orchestration/worker:health",
            headers={"Authorization": f"Bearer {self._gateway_token(['runs:read'])}"},
        )
        self.assertEqual(health.status_code, 200)
        health_payload = health.json()
        self.assertIn("lease_renew_signal", health_payload)
        renew_health = health_payload["lease_renew_signal"]
        self.assertEqual(int(renew_health["attempted"]), 1)
        self.assertEqual(int(renew_health["renewed"]), 1)
        self.assertEqual(int(renew_health["errors"]), 0)
        self.assertGreaterEqual(int(renew_health["total_attempted"]), 1)
        self.assertGreaterEqual(int(renew_health["total_renewed"]), 1)
        self.assertEqual(int(renew_health["total_errors"]), 0)
        self.assertIsInstance(renew_health["last_observed_at"], str)

        lease_after = device_hub_app_module._hub.leases[lease_id].lease_expires_at
        before_dt = datetime.fromisoformat(lease_before)
        after_dt = datetime.fromisoformat(lease_after)
        self.assertGreater(after_dt, before_dt)
        self.assertIsNotNone(execution_app_module._runtime.runs[run_id].device_lease_last_renew_at)

    def test_gateway_submit_compute_rejected_when_tenant_quota_exhausted_e2e(self) -> None:
        if not DEVICE_HUB_AVAILABLE:
            self.skipTest("device-hub stack not available")

        device_hub_app_module._hub = DeviceHubService(max_active_leases_per_tenant=1)
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
                payload={"device_id": "gpu-node-gateway-tenant-quota-e2e", "capabilities": ["compute.comfyui.local"]},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-tenant-quota-device-register",
                run_id="run-gateway-tenant-quota-device-bootstrap",
                task_id="task-gateway-tenant-quota-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(register.status_code, 200)
        pair_request = device_hub_client.post(
            "/v1/devices/pairing/request",
            json=build_command_envelope(
                command_type="device.pairing.request",
                payload={"device_id": "gpu-node-gateway-tenant-quota-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-tenant-quota-device-pair",
                run_id="run-gateway-tenant-quota-device-bootstrap",
                task_id="task-gateway-tenant-quota-device-bootstrap",
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
                trace_id="trace-gateway-tenant-quota-device-approve",
                run_id="run-gateway-tenant-quota-device-bootstrap",
                task_id="task-gateway-tenant-quota-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(approve.status_code, 200)
        heartbeat = device_hub_client.post(
            "/v1/devices/heartbeat",
            json=build_command_envelope(
                command_type="device.heartbeat",
                payload={"device_id": "gpu-node-gateway-tenant-quota-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-tenant-quota-device-heartbeat",
                run_id="run-gateway-tenant-quota-device-bootstrap",
                task_id="task-gateway-tenant-quota-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(heartbeat.status_code, 200)

        write_token = self._gateway_token(["runs:write"])
        first_run_id = self._submit_run(
            write_token,
            "gateway compute tenant quota first run",
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
        first_run = execution_app_module._runtime.runs[first_run_id]
        self.assertEqual(first_run.device_lease_state, "active")
        first_lease_id = first_run.device_lease_id
        self.assertIsInstance(first_lease_id, str)
        assert first_lease_id is not None
        self.assertEqual(device_hub_app_module._hub.leases[first_lease_id].tenant_id, "t1")
        run_ids_before_second = set(execution_app_module._runtime.runs.keys())

        second = self.gateway_client.post(
            "/v1/runs",
            json={
                "tenant_id": "t1",
                "app_id": "covernow",
                "session_key": "tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                "payload": {
                    "goal": "gateway compute tenant quota second run",
                    "dispatch_policy": {
                        "queue": {
                            "dispatch_min_score": -999,
                            "max_queue_depth_for_dispatch": 0,
                        }
                    },
                    "execution_profile": {
                        "execution_mode": "compute",
                        "inference_target": "none",
                        "resource_class": "gpu",
                        "placement_constraints": {
                            "tenant_id": "t1",
                            "required_capabilities": ["compute.comfyui.local"],
                        },
                    },
                },
            },
            headers={"Authorization": f"Bearer {write_token}"},
        )
        self.assertEqual(second.status_code, 503)
        detail = second.json().get("detail")
        self.assertIsInstance(detail, dict)
        assert isinstance(detail, dict)
        self.assertEqual(detail.get("status_code"), 503)
        self.assertEqual(detail.get("downstream_event_type"), "runtime.route.failed")
        failure = detail.get("failure")
        self.assertIsInstance(failure, dict)
        assert isinstance(failure, dict)
        self.assertEqual(failure.get("code"), "tenant_quota_exhausted")
        self.assertEqual(failure.get("classification"), "capacity")
        self.assertEqual(detail.get("placement_reason_code"), "tenant_quota_exhausted")
        self.assertEqual(detail.get("placement_event_type"), "device.route.rejected")
        detail_snapshot = detail.get("placement_resource_snapshot")
        self.assertIsInstance(detail_snapshot, dict)
        assert isinstance(detail_snapshot, dict)
        self.assertEqual(detail_snapshot.get("tenant_id"), "t1")
        self.assertEqual(detail_snapshot.get("tenant_active_leases"), 1)
        self.assertEqual(detail_snapshot.get("tenant_limit"), 1)
        self.assertEqual(detail_snapshot.get("eligible_devices"), 1)
        self.assertEqual(detail_snapshot.get("active_leases"), 1)
        self.assertEqual(detail_snapshot.get("available_slots"), 0)
        self.assertIn("HTTP 503", str(detail.get("message", "")))

        run_ids_after_second = set(execution_app_module._runtime.runs.keys())
        rejected_run_ids = run_ids_after_second - run_ids_before_second
        self.assertEqual(len(rejected_run_ids), 1)
        rejected_run_id = next(iter(rejected_run_ids))
        self.assertEqual(detail.get("run_id"), rejected_run_id)

        recent = self.gateway_client.get(
            f"/v1/events/recent?run_id={rejected_run_id}&limit=10",
            headers={"Authorization": f"Bearer {self._gateway_token(['runs:read'])}"},
        )
        self.assertEqual(recent.status_code, 200)
        recent_items = recent.json()["items"]
        matching = [
            item["event"]
            for item in recent_items
            if item.get("event", {}).get("event_type") == "runtime.route.failed"
            and item.get("event", {}).get("payload", {}).get("run_id") == rejected_run_id
            and item.get("event", {}).get("payload", {}).get("failure", {}).get("code")
            == "tenant_quota_exhausted"
        ]
        self.assertGreaterEqual(len(matching), 1)
        self.assertEqual(matching[0]["payload"]["failure"]["classification"], "capacity")
        route_snapshot = matching[0]["payload"]["decision"]["resource_snapshot"]
        self.assertIsInstance(route_snapshot, dict)
        assert isinstance(route_snapshot, dict)
        self.assertEqual(route_snapshot.get("tenant_id"), "t1")
        self.assertEqual(route_snapshot.get("tenant_active_leases"), 1)
        self.assertEqual(route_snapshot.get("tenant_limit"), 1)
        self.assertEqual(route_snapshot.get("eligible_devices"), 1)
        self.assertEqual(route_snapshot.get("active_leases"), 1)
        self.assertEqual(route_snapshot.get("available_slots"), 0)

    def test_gateway_submit_compute_recovers_tenant_quota_when_active_lease_expiry_invalid_e2e(self) -> None:
        if not DEVICE_HUB_AVAILABLE:
            self.skipTest("device-hub stack not available")

        device_hub_app_module._hub = DeviceHubService(max_active_leases_per_tenant=1)
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
                payload={
                    "device_id": "gpu-node-gateway-tenant-quota-invalid-expiry-recover-e2e",
                    "capabilities": ["compute.comfyui.local"],
                },
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-tenant-quota-invalid-expiry-recover-device-register",
                run_id="run-gateway-tenant-quota-invalid-expiry-recover-device-bootstrap",
                task_id="task-gateway-tenant-quota-invalid-expiry-recover-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(register.status_code, 200)
        pair_request = device_hub_client.post(
            "/v1/devices/pairing/request",
            json=build_command_envelope(
                command_type="device.pairing.request",
                payload={"device_id": "gpu-node-gateway-tenant-quota-invalid-expiry-recover-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-tenant-quota-invalid-expiry-recover-device-pair",
                run_id="run-gateway-tenant-quota-invalid-expiry-recover-device-bootstrap",
                task_id="task-gateway-tenant-quota-invalid-expiry-recover-device-bootstrap",
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
                trace_id="trace-gateway-tenant-quota-invalid-expiry-recover-device-approve",
                run_id="run-gateway-tenant-quota-invalid-expiry-recover-device-bootstrap",
                task_id="task-gateway-tenant-quota-invalid-expiry-recover-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(approve.status_code, 200)
        heartbeat = device_hub_client.post(
            "/v1/devices/heartbeat",
            json=build_command_envelope(
                command_type="device.heartbeat",
                payload={"device_id": "gpu-node-gateway-tenant-quota-invalid-expiry-recover-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-tenant-quota-invalid-expiry-recover-device-heartbeat",
                run_id="run-gateway-tenant-quota-invalid-expiry-recover-device-bootstrap",
                task_id="task-gateway-tenant-quota-invalid-expiry-recover-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(heartbeat.status_code, 200)

        write_token = self._gateway_token(["runs:write"])
        first_run_id = self._submit_run(
            write_token,
            "gateway compute tenant quota invalid-expiry recover first run",
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
        first_run = execution_app_module._runtime.runs[first_run_id]
        self.assertEqual(first_run.device_lease_state, "active")
        first_lease_id = first_run.device_lease_id
        self.assertIsInstance(first_lease_id, str)
        assert first_lease_id is not None

        second = self.gateway_client.post(
            "/v1/runs",
            json={
                "tenant_id": "t1",
                "app_id": "covernow",
                "session_key": "tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                "payload": {
                    "goal": "gateway compute tenant quota invalid-expiry recover second run",
                    "dispatch_policy": {
                        "queue": {
                            "dispatch_min_score": -999,
                            "max_queue_depth_for_dispatch": 0,
                        }
                    },
                    "execution_profile": {
                        "execution_mode": "compute",
                        "inference_target": "none",
                        "resource_class": "gpu",
                        "placement_constraints": {
                            "tenant_id": "t1",
                            "required_capabilities": ["compute.comfyui.local"],
                        },
                    },
                },
            },
            headers={"Authorization": f"Bearer {write_token}"},
        )
        self.assertEqual(second.status_code, 503)
        detail = second.json().get("detail")
        self.assertIsInstance(detail, dict)
        assert isinstance(detail, dict)
        self.assertEqual(detail.get("status_code"), 503)
        self.assertEqual(detail.get("downstream_event_type"), "runtime.route.failed")
        failure = detail.get("failure")
        self.assertIsInstance(failure, dict)
        assert isinstance(failure, dict)
        self.assertEqual(failure.get("code"), "tenant_quota_exhausted")
        self.assertEqual(failure.get("classification"), "capacity")

        device_hub_app_module._hub.leases[first_lease_id].lease_expires_at = "invalid-datetime"

        third = self.gateway_client.post(
            "/v1/runs",
            json={
                "tenant_id": "t1",
                "app_id": "covernow",
                "session_key": "tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                "payload": {
                    "goal": "gateway compute tenant quota invalid-expiry recover third run",
                    "dispatch_policy": {
                        "queue": {
                            "dispatch_min_score": -999,
                            "max_queue_depth_for_dispatch": 0,
                        }
                    },
                    "execution_profile": {
                        "execution_mode": "compute",
                        "inference_target": "none",
                        "resource_class": "gpu",
                        "placement_constraints": {
                            "tenant_id": "t1",
                            "required_capabilities": ["compute.comfyui.local"],
                        },
                    },
                },
            },
            headers={"Authorization": f"Bearer {write_token}"},
        )
        self.assertEqual(third.status_code, 200)
        third_run_id = str(third.json()["run_id"])
        self.assertNotEqual(third_run_id, first_run_id)

        third_run = execution_app_module._runtime.runs[third_run_id]
        self.assertEqual(third_run.device_lease_state, "active")
        self.assertIsInstance(third_run.device_lease_id, str)
        assert third_run.device_lease_id is not None
        self.assertNotEqual(third_run.device_lease_id, first_lease_id)
        self.assertEqual(device_hub_app_module._hub.leases[first_lease_id].status, "expired")
        self.assertEqual(device_hub_app_module._hub.leases[first_lease_id].expire_reason_code, "ttl_expired")

        first_lease = self.gateway_client.get(
            f"/v1/runs/{first_run_id}/lease",
            headers={"Authorization": f"Bearer {self._gateway_token(['runs:read'])}"},
        )
        self.assertEqual(first_lease.status_code, 200)
        first_lease_payload = first_lease.json()
        self.assertEqual(first_lease_payload["lease"]["lease_id"], first_lease_id)
        self.assertEqual(first_lease_payload["lease"]["state"], "expired")
        self.assertEqual(first_lease_payload["device_hub"]["snapshot"]["status"], "expired")
        self.assertEqual(
            first_lease_payload["device_hub"]["snapshot"]["expire_reason_code"],
            "ttl_expired",
        )

    def test_gateway_scheduler_retry_recovers_after_invalid_expiry_on_capacity_rejection_e2e(self) -> None:
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
                payload={
                    "device_id": "gpu-node-gateway-invalid-expiry-retry-recover-e2e",
                    "capabilities": ["compute.comfyui.local"],
                },
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-invalid-expiry-retry-recover-device-register",
                run_id="run-gateway-invalid-expiry-retry-recover-device-bootstrap",
                task_id="task-gateway-invalid-expiry-retry-recover-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(register.status_code, 200)
        pair_request = device_hub_client.post(
            "/v1/devices/pairing/request",
            json=build_command_envelope(
                command_type="device.pairing.request",
                payload={"device_id": "gpu-node-gateway-invalid-expiry-retry-recover-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-invalid-expiry-retry-recover-device-pair",
                run_id="run-gateway-invalid-expiry-retry-recover-device-bootstrap",
                task_id="task-gateway-invalid-expiry-retry-recover-device-bootstrap",
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
                trace_id="trace-gateway-invalid-expiry-retry-recover-device-approve",
                run_id="run-gateway-invalid-expiry-retry-recover-device-bootstrap",
                task_id="task-gateway-invalid-expiry-retry-recover-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(approve.status_code, 200)
        heartbeat = device_hub_client.post(
            "/v1/devices/heartbeat",
            json=build_command_envelope(
                command_type="device.heartbeat",
                payload={"device_id": "gpu-node-gateway-invalid-expiry-retry-recover-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-invalid-expiry-retry-recover-device-heartbeat",
                run_id="run-gateway-invalid-expiry-retry-recover-device-bootstrap",
                task_id="task-gateway-invalid-expiry-retry-recover-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(heartbeat.status_code, 200)

        write_token = self._gateway_token(["runs:write"])
        first_run_id = self._submit_run(
            write_token,
            "gateway compute invalid-expiry retry recover first run",
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
        first_run = execution_app_module._runtime.runs[first_run_id]
        self.assertEqual(first_run.device_lease_state, "active")
        first_lease_id = first_run.device_lease_id
        self.assertIsInstance(first_lease_id, str)
        assert first_lease_id is not None
        _ = execution_app_module._runtime.queue_fabric.lease_one(QueueType.ORCHESTRATION)

        run_ids_before_second = set(execution_app_module._runtime.runs.keys())
        second = self.gateway_client.post(
            "/v1/runs",
            json={
                "tenant_id": "t1",
                "app_id": "covernow",
                "session_key": "tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                "retry_policy": {
                    "max_attempts": 3,
                    "backoff_ms": 0,
                    "strategy": "fixed",
                },
                "payload": {
                    "goal": "gateway compute invalid-expiry retry recover second run",
                    "dispatch_policy": {
                        "queue": {
                            "dispatch_min_score": -999.0,
                            "max_queue_depth_for_dispatch": 0,
                        }
                    },
                    "execution_profile": {
                        "execution_mode": "compute",
                        "inference_target": "none",
                        "resource_class": "gpu",
                        "placement_constraints": {
                            "tenant_id": "t1",
                            "required_capabilities": ["compute.comfyui.local"],
                        },
                    },
                },
            },
            headers={"Authorization": f"Bearer {write_token}"},
        )
        self.assertEqual(second.status_code, 503)
        detail = second.json().get("detail")
        self.assertIsInstance(detail, dict)
        assert isinstance(detail, dict)
        self.assertEqual(detail.get("status_code"), 503)
        self.assertEqual(detail.get("downstream_event_type"), "runtime.route.failed")
        failure = detail.get("failure")
        self.assertIsInstance(failure, dict)
        assert isinstance(failure, dict)
        self.assertEqual(failure.get("code"), "capacity_exhausted")
        self.assertEqual(failure.get("classification"), "capacity")

        run_ids_after_second = set(execution_app_module._runtime.runs.keys())
        rejected_run_ids = run_ids_after_second - run_ids_before_second
        self.assertEqual(len(rejected_run_ids), 1)
        second_run_id = next(iter(rejected_run_ids))

        device_hub_app_module._hub.leases[first_lease_id].lease_expires_at = "invalid-datetime"

        scheduler_tick = self.gateway_client.post(
            "/v1/orchestration/scheduler:tick?max_items=1&fair=true",
            headers={"Authorization": f"Bearer {write_token}"},
        )
        self.assertEqual(scheduler_tick.status_code, 200)
        self.assertGreaterEqual(int(scheduler_tick.json()["processed"]), 1)

        recovered = False
        for _ in range(5):
            worker_tick = self.gateway_client.post(
                "/v1/orchestration/worker:tick?fair=true&auto_start=true",
                headers={"Authorization": f"Bearer {write_token}"},
            )
            self.assertEqual(worker_tick.status_code, 200)
            refreshed = execution_app_module._runtime.runs[second_run_id]
            if refreshed.device_lease_id:
                recovered = True
                break
        self.assertTrue(recovered)

        self.assertEqual(device_hub_app_module._hub.leases[first_lease_id].status, "expired")
        self.assertEqual(
            device_hub_app_module._hub.leases[first_lease_id].expire_reason_code,
            "ttl_expired",
        )
        second_run = execution_app_module._runtime.runs[second_run_id]
        self.assertEqual(second_run.device_lease_state, "active")
        self.assertIsInstance(second_run.device_lease_id, str)

    def test_gateway_mixed_quota_and_capacity_rejections_then_retry_recovery_e2e(self) -> None:
        if not DEVICE_HUB_AVAILABLE:
            self.skipTest("device-hub stack not available")

        device_hub_app_module._hub = DeviceHubService(max_active_leases_per_tenant=1)
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
                payload={
                    "device_id": "gpu-node-gateway-mixed-pressure-recovery-e2e",
                    "capabilities": ["compute.comfyui.local"],
                },
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-mixed-pressure-recovery-device-register",
                run_id="run-gateway-mixed-pressure-recovery-device-bootstrap",
                task_id="task-gateway-mixed-pressure-recovery-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(register.status_code, 200)
        pair_request = device_hub_client.post(
            "/v1/devices/pairing/request",
            json=build_command_envelope(
                command_type="device.pairing.request",
                payload={"device_id": "gpu-node-gateway-mixed-pressure-recovery-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-mixed-pressure-recovery-device-pair",
                run_id="run-gateway-mixed-pressure-recovery-device-bootstrap",
                task_id="task-gateway-mixed-pressure-recovery-device-bootstrap",
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
                trace_id="trace-gateway-mixed-pressure-recovery-device-approve",
                run_id="run-gateway-mixed-pressure-recovery-device-bootstrap",
                task_id="task-gateway-mixed-pressure-recovery-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(approve.status_code, 200)
        heartbeat = device_hub_client.post(
            "/v1/devices/heartbeat",
            json=build_command_envelope(
                command_type="device.heartbeat",
                payload={"device_id": "gpu-node-gateway-mixed-pressure-recovery-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-mixed-pressure-recovery-device-heartbeat",
                run_id="run-gateway-mixed-pressure-recovery-device-bootstrap",
                task_id="task-gateway-mixed-pressure-recovery-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(heartbeat.status_code, 200)

        write_token = self._gateway_token(["runs:write"])
        first_run_id = self._submit_run(
            write_token,
            "gateway mixed pressure recovery first run",
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
        first_run = execution_app_module._runtime.runs[first_run_id]
        self.assertEqual(first_run.device_lease_state, "active")
        first_lease_id = first_run.device_lease_id
        self.assertIsInstance(first_lease_id, str)
        assert first_lease_id is not None
        _ = execution_app_module._runtime.queue_fabric.lease_one(QueueType.ORCHESTRATION)

        quota_reject = self.gateway_client.post(
            "/v1/runs",
            json={
                "tenant_id": "t1",
                "app_id": "covernow",
                "session_key": "tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                "retry_policy": {
                    "max_attempts": 1,
                    "backoff_ms": 0,
                    "strategy": "fixed",
                },
                "payload": {
                    "goal": "gateway mixed pressure recovery quota reject",
                    "dispatch_policy": {
                        "queue": {
                            "dispatch_min_score": -999.0,
                            "max_queue_depth_for_dispatch": 0,
                        }
                    },
                    "execution_profile": {
                        "execution_mode": "compute",
                        "inference_target": "none",
                        "resource_class": "gpu",
                        "placement_constraints": {
                            "tenant_id": "t1",
                            "required_capabilities": ["compute.comfyui.local"],
                        },
                    },
                },
            },
            headers={"Authorization": f"Bearer {write_token}"},
        )
        self.assertEqual(quota_reject.status_code, 503)
        quota_detail = quota_reject.json().get("detail")
        self.assertIsInstance(quota_detail, dict)
        assert isinstance(quota_detail, dict)
        quota_failure = quota_detail.get("failure")
        self.assertIsInstance(quota_failure, dict)
        assert isinstance(quota_failure, dict)
        self.assertEqual(quota_failure.get("code"), "tenant_quota_exhausted")
        self.assertEqual(quota_failure.get("classification"), "capacity")

        device_hub_app_module._hub.max_active_leases_per_tenant = 2

        run_ids_before_capacity = set(execution_app_module._runtime.runs.keys())
        capacity_reject = self.gateway_client.post(
            "/v1/runs",
            json={
                "tenant_id": "t1",
                "app_id": "covernow",
                "session_key": "tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                "retry_policy": {
                    "max_attempts": 3,
                    "backoff_ms": 0,
                    "strategy": "fixed",
                },
                "payload": {
                    "goal": "gateway mixed pressure recovery capacity reject",
                    "dispatch_policy": {
                        "queue": {
                            "dispatch_min_score": -999.0,
                            "max_queue_depth_for_dispatch": 0,
                        }
                    },
                    "execution_profile": {
                        "execution_mode": "compute",
                        "inference_target": "none",
                        "resource_class": "gpu",
                        "placement_constraints": {
                            "tenant_id": "t1",
                            "required_capabilities": ["compute.comfyui.local"],
                        },
                    },
                },
            },
            headers={"Authorization": f"Bearer {write_token}"},
        )
        self.assertEqual(capacity_reject.status_code, 503)
        capacity_detail = capacity_reject.json().get("detail")
        self.assertIsInstance(capacity_detail, dict)
        assert isinstance(capacity_detail, dict)
        capacity_failure = capacity_detail.get("failure")
        self.assertIsInstance(capacity_failure, dict)
        assert isinstance(capacity_failure, dict)
        self.assertEqual(capacity_failure.get("code"), "capacity_exhausted")
        self.assertEqual(capacity_failure.get("classification"), "capacity")

        run_ids_after_capacity = set(execution_app_module._runtime.runs.keys())
        retry_candidates = run_ids_after_capacity - run_ids_before_capacity
        self.assertEqual(len(retry_candidates), 1)
        retry_run_id = next(iter(retry_candidates))

        device_hub_app_module._hub.leases[first_lease_id].lease_expires_at = "invalid-datetime"

        scheduler_tick = self.gateway_client.post(
            "/v1/orchestration/scheduler:tick?max_items=2&fair=true",
            headers={"Authorization": f"Bearer {write_token}"},
        )
        self.assertEqual(scheduler_tick.status_code, 200)
        self.assertGreaterEqual(int(scheduler_tick.json()["processed"]), 1)

        recovered = False
        for _ in range(6):
            worker_tick = self.gateway_client.post(
                "/v1/orchestration/worker:tick?fair=true&auto_start=true",
                headers={"Authorization": f"Bearer {write_token}"},
            )
            self.assertEqual(worker_tick.status_code, 200)
            refreshed = execution_app_module._runtime.runs[retry_run_id]
            if refreshed.device_lease_id:
                recovered = True
                break
        self.assertTrue(recovered)

        self.assertEqual(device_hub_app_module._hub.leases[first_lease_id].status, "expired")
        self.assertEqual(device_hub_app_module._hub.leases[first_lease_id].expire_reason_code, "ttl_expired")
        retry_run = execution_app_module._runtime.runs[retry_run_id]
        self.assertEqual(retry_run.device_lease_state, "active")
        self.assertIsInstance(retry_run.device_lease_id, str)

    def test_gateway_concurrent_capacity_retries_recover_after_invalid_expiry_reconciliation_e2e(self) -> None:
        if not DEVICE_HUB_AVAILABLE:
            self.skipTest("device-hub stack not available")

        device_hub_app_module._hub = DeviceHubService(max_active_leases_per_tenant=4)
        device_hub_client = TestClient(device_hub_app_module.app)
        execution_app_module._runtime = RuntimeExecutionService(
            device_hub_client=_DeviceHubBoundaryClient(
                client=device_hub_client,
                token_factory=self._device_hub_token,
            )
        )
        self.execution_client = TestClient(execution_app_module.app)

        def _register_online_compute_device(*, device_id: str, trace_prefix: str) -> None:
            register = device_hub_client.post(
                "/v1/devices/register",
                json=build_command_envelope(
                    command_type="device.register",
                    payload={"device_id": device_id, "capabilities": ["compute.comfyui.local"]},
                    session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                    trace_id=f"{trace_prefix}-register",
                    run_id=f"run-{trace_prefix}-bootstrap",
                    task_id=f"task-{trace_prefix}-bootstrap",
                ),
                headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
            )
            self.assertEqual(register.status_code, 200)
            pair_request = device_hub_client.post(
                "/v1/devices/pairing/request",
                json=build_command_envelope(
                    command_type="device.pairing.request",
                    payload={"device_id": device_id},
                    session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                    trace_id=f"{trace_prefix}-pair",
                    run_id=f"run-{trace_prefix}-bootstrap",
                    task_id=f"task-{trace_prefix}-bootstrap",
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
                    trace_id=f"{trace_prefix}-approve",
                    run_id=f"run-{trace_prefix}-bootstrap",
                    task_id=f"task-{trace_prefix}-bootstrap",
                ),
                headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
            )
            self.assertEqual(approve.status_code, 200)
            heartbeat = device_hub_client.post(
                "/v1/devices/heartbeat",
                json=build_command_envelope(
                    command_type="device.heartbeat",
                    payload={"device_id": device_id},
                    session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                    trace_id=f"{trace_prefix}-heartbeat",
                    run_id=f"run-{trace_prefix}-bootstrap",
                    task_id=f"task-{trace_prefix}-bootstrap",
                ),
                headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
            )
            self.assertEqual(heartbeat.status_code, 200)

        _register_online_compute_device(
            device_id="gpu-node-gateway-concurrent-retry-recovery-a-e2e",
            trace_prefix="trace-gateway-concurrent-retry-recovery-a-device",
        )
        _register_online_compute_device(
            device_id="gpu-node-gateway-concurrent-retry-recovery-b-e2e",
            trace_prefix="trace-gateway-concurrent-retry-recovery-b-device",
        )

        write_token = self._gateway_token(["runs:write"])
        execution_profile = {
            "execution_mode": "compute",
            "inference_target": "none",
            "resource_class": "gpu",
            "placement_constraints": {
                "tenant_id": "t1",
                "required_capabilities": ["compute.comfyui.local"],
            },
        }

        def _submit_seed_run(*, goal: str) -> str:
            seed = self.gateway_client.post(
                "/v1/runs",
                json={
                    "tenant_id": "t1",
                    "app_id": "covernow",
                    "session_key": "tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                    "payload": {
                        "goal": goal,
                        "dispatch_policy": {
                            "queue": {
                                "dispatch_min_score": -999.0,
                                "max_queue_depth_for_dispatch": 0,
                            }
                        },
                        "execution_profile": execution_profile,
                    },
                },
                headers={"Authorization": f"Bearer {write_token}"},
            )
            self.assertEqual(seed.status_code, 200)
            return str(seed.json()["run_id"])

        first_run_id = _submit_seed_run(goal="gateway concurrent retry recovery first run")
        second_run_id = _submit_seed_run(goal="gateway concurrent retry recovery second run")
        self.assertNotEqual(first_run_id, second_run_id)
        for _ in range(8):
            scheduler_tick = self.gateway_client.post(
                "/v1/orchestration/scheduler:tick?max_items=2&fair=true",
                headers={"Authorization": f"Bearer {write_token}"},
            )
            self.assertEqual(scheduler_tick.status_code, 200)
            worker_tick = self.gateway_client.post(
                "/v1/orchestration/worker:tick?fair=true&auto_start=true",
                headers={"Authorization": f"Bearer {write_token}"},
            )
            self.assertEqual(worker_tick.status_code, 200)
            first_run = execution_app_module._runtime.runs[first_run_id]
            second_run = execution_app_module._runtime.runs[second_run_id]
            if first_run.device_lease_id and second_run.device_lease_id:
                break

        first_run = execution_app_module._runtime.runs[first_run_id]
        second_run = execution_app_module._runtime.runs[second_run_id]
        self.assertEqual(first_run.device_lease_state, "active")
        self.assertEqual(second_run.device_lease_state, "active")
        first_lease_id = first_run.device_lease_id
        second_lease_id = second_run.device_lease_id
        self.assertIsInstance(first_lease_id, str)
        self.assertIsInstance(second_lease_id, str)
        assert first_lease_id is not None
        assert second_lease_id is not None
        _ = execution_app_module._runtime.queue_fabric.lease_one(QueueType.ORCHESTRATION)
        _ = execution_app_module._runtime.queue_fabric.lease_one(QueueType.ORCHESTRATION)

        def _submit_capacity_retry_rejection(*, goal: str) -> str:
            run_ids_before = set(execution_app_module._runtime.runs.keys())
            rejected = self.gateway_client.post(
                "/v1/runs",
                json={
                    "tenant_id": "t1",
                    "app_id": "covernow",
                    "session_key": "tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                    "retry_policy": {
                        "max_attempts": 4,
                        "backoff_ms": 0,
                        "strategy": "fixed",
                    },
                    "payload": {
                        "goal": goal,
                        "dispatch_policy": {
                            "queue": {
                                "dispatch_min_score": -999.0,
                                "max_queue_depth_for_dispatch": 0,
                            }
                        },
                        "execution_profile": execution_profile,
                    },
                },
                headers={"Authorization": f"Bearer {write_token}"},
            )
            self.assertEqual(rejected.status_code, 503)
            detail = rejected.json().get("detail")
            self.assertIsInstance(detail, dict)
            assert isinstance(detail, dict)
            failure = detail.get("failure")
            self.assertIsInstance(failure, dict)
            assert isinstance(failure, dict)
            self.assertEqual(failure.get("code"), "capacity_exhausted")
            self.assertEqual(failure.get("classification"), "capacity")

            run_ids_after = set(execution_app_module._runtime.runs.keys())
            retry_candidates = run_ids_after - run_ids_before
            self.assertEqual(len(retry_candidates), 1)
            return next(iter(retry_candidates))

        retry_run_ids = {
            _submit_capacity_retry_rejection(goal="gateway concurrent retry recovery third run"),
            _submit_capacity_retry_rejection(goal="gateway concurrent retry recovery fourth run"),
        }
        self.assertEqual(len(retry_run_ids), 2)
        for run_id in retry_run_ids:
            retry_run = execution_app_module._runtime.runs[run_id]
            self.assertEqual(retry_run.status.value, "queued")
            self.assertIsNone(retry_run.device_lease_id)

        device_hub_app_module._hub.leases[first_lease_id].lease_expires_at = "invalid-datetime"
        device_hub_app_module._hub.leases[second_lease_id].lease_expires_at = "invalid-datetime"

        recovered_run_ids: set[str] = set()
        for _ in range(12):
            scheduler_tick = self.gateway_client.post(
                "/v1/orchestration/scheduler:tick?max_items=4&fair=true",
                headers={"Authorization": f"Bearer {write_token}"},
            )
            self.assertEqual(scheduler_tick.status_code, 200)

            worker_tick = self.gateway_client.post(
                "/v1/orchestration/worker:tick?fair=true&auto_start=true",
                headers={"Authorization": f"Bearer {write_token}"},
            )
            self.assertEqual(worker_tick.status_code, 200)
            for run_id in retry_run_ids:
                refreshed = execution_app_module._runtime.runs[run_id]
                if refreshed.device_lease_id:
                    recovered_run_ids.add(run_id)
            if recovered_run_ids == retry_run_ids:
                break
        self.assertEqual(recovered_run_ids, retry_run_ids)

        for lease_id in (first_lease_id, second_lease_id):
            lease = device_hub_app_module._hub.leases[lease_id]
            self.assertEqual(lease.status, "expired")
            self.assertEqual(lease.expire_reason_code, "ttl_expired")

        for run_id in retry_run_ids:
            retry_run = execution_app_module._runtime.runs[run_id]
            self.assertEqual(retry_run.device_lease_state, "active")
            self.assertIsInstance(retry_run.device_lease_id, str)

    def test_gateway_submit_compute_rejected_when_capacity_exhausted_e2e(self) -> None:
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
                payload={"device_id": "gpu-node-gateway-capacity-e2e", "capabilities": ["compute.comfyui.local"]},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-capacity-device-register",
                run_id="run-gateway-capacity-device-bootstrap",
                task_id="task-gateway-capacity-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(register.status_code, 200)
        pair_request = device_hub_client.post(
            "/v1/devices/pairing/request",
            json=build_command_envelope(
                command_type="device.pairing.request",
                payload={"device_id": "gpu-node-gateway-capacity-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-capacity-device-pair",
                run_id="run-gateway-capacity-device-bootstrap",
                task_id="task-gateway-capacity-device-bootstrap",
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
                trace_id="trace-gateway-capacity-device-approve",
                run_id="run-gateway-capacity-device-bootstrap",
                task_id="task-gateway-capacity-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(approve.status_code, 200)
        heartbeat = device_hub_client.post(
            "/v1/devices/heartbeat",
            json=build_command_envelope(
                command_type="device.heartbeat",
                payload={"device_id": "gpu-node-gateway-capacity-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-capacity-device-heartbeat",
                run_id="run-gateway-capacity-device-bootstrap",
                task_id="task-gateway-capacity-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(heartbeat.status_code, 200)

        write_token = self._gateway_token(["runs:write"])
        first_run_id = self._submit_run(
            write_token,
            "gateway compute capacity first run",
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
        first_run = execution_app_module._runtime.runs[first_run_id]
        self.assertEqual(first_run.device_lease_state, "active")
        first_lease_id = first_run.device_lease_id
        self.assertIsInstance(first_lease_id, str)

        run_ids_before_second = set(execution_app_module._runtime.runs.keys())
        second = self.gateway_client.post(
            "/v1/runs",
            json={
                "tenant_id": "t1",
                "app_id": "covernow",
                "session_key": "tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                "payload": {
                    "goal": "gateway compute capacity second run",
                    "dispatch_policy": {
                        "queue": {
                            "dispatch_min_score": -999,
                            "max_queue_depth_for_dispatch": 0,
                        }
                    },
                    "execution_profile": {
                        "execution_mode": "compute",
                        "inference_target": "none",
                        "resource_class": "gpu",
                        "placement_constraints": {
                            "tenant_id": "t1",
                            "required_capabilities": ["compute.comfyui.local"],
                        },
                    },
                },
            },
            headers={"Authorization": f"Bearer {write_token}"},
        )
        self.assertEqual(second.status_code, 503)
        detail = second.json().get("detail")
        self.assertIsInstance(detail, dict)
        assert isinstance(detail, dict)
        self.assertEqual(detail.get("status_code"), 503)
        self.assertEqual(detail.get("downstream_event_type"), "runtime.route.failed")
        failure = detail.get("failure")
        self.assertIsInstance(failure, dict)
        assert isinstance(failure, dict)
        self.assertEqual(failure.get("code"), "capacity_exhausted")
        self.assertEqual(failure.get("classification"), "capacity")
        self.assertEqual(detail.get("placement_reason_code"), "capacity_exhausted")
        self.assertEqual(detail.get("placement_event_type"), "device.route.rejected")
        detail_snapshot = detail.get("placement_resource_snapshot")
        self.assertIsInstance(detail_snapshot, dict)
        assert isinstance(detail_snapshot, dict)
        self.assertEqual(detail_snapshot.get("eligible_devices"), 1)
        self.assertEqual(detail_snapshot.get("active_leases"), 1)
        self.assertEqual(detail_snapshot.get("available_slots"), 0)
        self.assertEqual(detail_snapshot.get("tenant_id"), "t1")
        self.assertEqual(detail_snapshot.get("tenant_active_leases"), 1)

        run_ids_after_second = set(execution_app_module._runtime.runs.keys())
        rejected_run_ids = run_ids_after_second - run_ids_before_second
        self.assertEqual(len(rejected_run_ids), 1)
        rejected_run_id = next(iter(rejected_run_ids))
        self.assertEqual(detail.get("run_id"), rejected_run_id)

        recent = self.gateway_client.get(
            f"/v1/events/recent?run_id={rejected_run_id}&limit=10",
            headers={"Authorization": f"Bearer {self._gateway_token(['runs:read'])}"},
        )
        self.assertEqual(recent.status_code, 200)
        recent_items = recent.json()["items"]
        matching = [
            item["event"]
            for item in recent_items
            if item.get("event", {}).get("event_type") == "runtime.route.failed"
            and item.get("event", {}).get("payload", {}).get("run_id") == rejected_run_id
            and item.get("event", {}).get("payload", {}).get("failure", {}).get("code")
            == "capacity_exhausted"
        ]
        self.assertGreaterEqual(len(matching), 1)
        self.assertEqual(matching[0]["payload"]["failure"]["classification"], "capacity")
        route_snapshot = matching[0]["payload"]["decision"]["resource_snapshot"]
        self.assertIsInstance(route_snapshot, dict)
        assert isinstance(route_snapshot, dict)
        self.assertEqual(route_snapshot.get("eligible_devices"), 1)
        self.assertEqual(route_snapshot.get("active_leases"), 1)
        self.assertEqual(route_snapshot.get("available_slots"), 0)
        self.assertEqual(route_snapshot.get("tenant_id"), "t1")
        self.assertEqual(route_snapshot.get("tenant_active_leases"), 1)

    def test_gateway_compute_capacity_retry_waits_for_free_slot_then_recovers_e2e(self) -> None:
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
                payload={"device_id": "gpu-node-gateway-capacity-retry-e2e", "capabilities": ["compute.comfyui.local"]},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-capacity-retry-device-register",
                run_id="run-gateway-capacity-retry-device-bootstrap",
                task_id="task-gateway-capacity-retry-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(register.status_code, 200)
        pair_request = device_hub_client.post(
            "/v1/devices/pairing/request",
            json=build_command_envelope(
                command_type="device.pairing.request",
                payload={"device_id": "gpu-node-gateway-capacity-retry-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-capacity-retry-device-pair",
                run_id="run-gateway-capacity-retry-device-bootstrap",
                task_id="task-gateway-capacity-retry-device-bootstrap",
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
                trace_id="trace-gateway-capacity-retry-device-approve",
                run_id="run-gateway-capacity-retry-device-bootstrap",
                task_id="task-gateway-capacity-retry-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(approve.status_code, 200)
        heartbeat = device_hub_client.post(
            "/v1/devices/heartbeat",
            json=build_command_envelope(
                command_type="device.heartbeat",
                payload={"device_id": "gpu-node-gateway-capacity-retry-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-capacity-retry-device-heartbeat",
                run_id="run-gateway-capacity-retry-device-bootstrap",
                task_id="task-gateway-capacity-retry-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(heartbeat.status_code, 200)

        token = self._gateway_token(["runs:write"])
        read_token = self._gateway_token(["runs:read"])
        first_run_id = self._submit_run(
            token,
            "gateway compute capacity retry first run",
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
        self.assertEqual(execution_app_module._runtime.runs[first_run_id].device_lease_state, "active")
        first_tick = self.gateway_client.post(
            "/v1/orchestration/worker:tick?fair=true&auto_start=true",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(first_tick.status_code, 200)
        self.assertEqual(first_tick.json()["outcome"], "progressed")
        self.assertEqual(first_tick.json()["leased_run_id"], first_run_id)

        second = self.gateway_client.post(
            "/v1/runs",
            json={
                "tenant_id": "t1",
                "app_id": "covernow",
                "session_key": "tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                "payload": {
                    "goal": "gateway compute capacity retry second run",
                    "dispatch_policy": {
                        "queue": {
                            "dispatch_min_score": -999,
                            "max_queue_depth_for_dispatch": 0,
                        }
                    },
                    "execution_profile": {
                        "execution_mode": "compute",
                        "inference_target": "none",
                        "resource_class": "gpu",
                        "placement_constraints": {
                            "tenant_id": "t1",
                            "required_capabilities": ["compute.comfyui.local"],
                        },
                    },
                },
            },
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(second.status_code, 503)
        detail = second.json()["detail"]
        second_run_id = str(detail["run_id"])
        self.assertEqual(detail["placement_reason_code"], "capacity_exhausted")
        self.assertEqual(detail["run_status"], "queued")
        self.assertEqual(detail["retryable"], True)
        self.assertEqual(detail["recommended_poll_after_ms"], 250)

        def _promote_retry_once() -> None:
            for _ in range(6):
                scheduler_tick = self.gateway_client.post(
                    "/v1/orchestration/scheduler:tick?max_items=1&fair=true",
                    headers={"Authorization": f"Bearer {token}"},
                )
                self.assertEqual(scheduler_tick.status_code, 200)
                payload = scheduler_tick.json()
                if int(payload.get("promoted", 0)) >= 1:
                    return
                next_due_in_ms = int(payload.get("next_due_in_ms") or 0)
                if next_due_in_ms > 0:
                    time.sleep(min(0.2, (next_due_in_ms + 20) / 1000.0))
            self.fail("scheduler did not promote retry message in expected retries")

        _promote_retry_once()

        blocked_tick = self.gateway_client.post(
            "/v1/orchestration/worker:tick?fair=true&auto_start=true",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(blocked_tick.status_code, 200)
        blocked_payload = blocked_tick.json()
        self.assertEqual(blocked_payload["outcome"], "skipped")
        self.assertEqual(blocked_payload["reason"], "dispatch_retry_deferred")
        self.assertEqual(blocked_payload["before_status"], "queued")
        self.assertEqual(blocked_payload["after_status"], "queued")

        second_status = self.gateway_client.get(
            f"/v1/runs/{second_run_id}",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(second_status.status_code, 200)
        self.assertEqual(second_status.json()["payload"]["status"], "queued")
        self.assertEqual(
            second_status.json()["payload"]["orchestration"]["failure_reason_code"],
            "capacity_exhausted",
        )

        cancel_first = self.gateway_client.post(
            f"/v1/runs/{first_run_id}:cancel",
            json={"reason": "free_slot_for_capacity_retry", "cascade_children": True},
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(cancel_first.status_code, 200)
        self.assertEqual(cancel_first.json()["payload"]["status"], "canceled")

        capacity_after_cancel = device_hub_client.get(
            "/v1/placements/capacity",
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:read'])}"},
        )
        self.assertEqual(capacity_after_cancel.status_code, 200)
        self.assertEqual(int(capacity_after_cancel.json()["active_leases"]), 0)

        _promote_retry_once()

        recovered_tick = self.gateway_client.post(
            "/v1/orchestration/worker:tick?fair=true&auto_start=true",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(recovered_tick.status_code, 200)
        recovered_payload = recovered_tick.json()
        self.assertEqual(recovered_payload["outcome"], "progressed")
        self.assertEqual(recovered_payload["before_status"], "queued")
        self.assertEqual(recovered_payload["after_status"], "running")
        self.assertEqual(recovered_payload["leased_run_id"], second_run_id)

        second_run = execution_app_module._runtime.runs[second_run_id]
        self.assertEqual(second_run.status.value, "running")
        self.assertEqual(second_run.device_lease_state, "active")
        self.assertIsInstance(second_run.device_lease_id, str)
        recovered_status = self.gateway_client.get(
            f"/v1/runs/{second_run_id}",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(recovered_status.status_code, 200)
        self.assertEqual(recovered_status.json()["payload"]["status"], "running")
        self.assertNotIn(
            "failure_reason_code",
            recovered_status.json()["payload"]["orchestration"],
        )

    def test_gateway_compute_capacity_rejection_without_retry_policy_fails_terminally_e2e(self) -> None:
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
                payload={"device_id": "gpu-node-gateway-no-retry-e2e", "capabilities": ["compute.comfyui.local"]},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-no-retry-device-register",
                run_id="run-gateway-no-retry-device-bootstrap",
                task_id="task-gateway-no-retry-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(register.status_code, 200)
        pair_request = device_hub_client.post(
            "/v1/devices/pairing/request",
            json=build_command_envelope(
                command_type="device.pairing.request",
                payload={"device_id": "gpu-node-gateway-no-retry-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-no-retry-device-pair",
                run_id="run-gateway-no-retry-device-bootstrap",
                task_id="task-gateway-no-retry-device-bootstrap",
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
                trace_id="trace-gateway-no-retry-device-approve",
                run_id="run-gateway-no-retry-device-bootstrap",
                task_id="task-gateway-no-retry-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(approve.status_code, 200)
        heartbeat = device_hub_client.post(
            "/v1/devices/heartbeat",
            json=build_command_envelope(
                command_type="device.heartbeat",
                payload={"device_id": "gpu-node-gateway-no-retry-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-no-retry-device-heartbeat",
                run_id="run-gateway-no-retry-device-bootstrap",
                task_id="task-gateway-no-retry-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(heartbeat.status_code, 200)

        token = self._gateway_token(["runs:write"])
        read_token = self._gateway_token(["runs:read"])
        first_run_id = self._submit_run(
            token,
            "gateway compute no retry first run",
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
        first_tick = self.gateway_client.post(
            "/v1/orchestration/worker:tick?fair=true&auto_start=true",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(first_tick.status_code, 200)
        self.assertEqual(first_tick.json()["outcome"], "progressed")
        self.assertEqual(first_tick.json()["leased_run_id"], first_run_id)

        second = self.gateway_client.post(
            "/v1/runs",
            json={
                "tenant_id": "t1",
                "app_id": "covernow",
                "session_key": "tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                "retry_policy": {
                    "max_attempts": 1,
                    "backoff_ms": 25,
                    "strategy": "fixed",
                },
                "payload": {
                    "goal": "gateway compute no retry second run",
                    "dispatch_policy": {
                        "queue": {
                            "dispatch_min_score": -999,
                            "max_queue_depth_for_dispatch": 0,
                        }
                    },
                    "execution_profile": {
                        "execution_mode": "compute",
                        "inference_target": "none",
                        "resource_class": "gpu",
                        "placement_constraints": {
                            "tenant_id": "t1",
                            "required_capabilities": ["compute.comfyui.local"],
                        },
                    },
                },
            },
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(second.status_code, 503)
        detail = second.json().get("detail")
        self.assertIsInstance(detail, dict)
        assert isinstance(detail, dict)
        second_run_id = str(detail.get("run_id"))
        self.assertEqual(detail.get("placement_reason_code"), "capacity_exhausted")
        self.assertEqual(detail.get("run_status"), "dlq")
        self.assertEqual(detail.get("retryable"), False)
        self.assertEqual(
            detail.get("retry_policy"),
            {"max_attempts": 1, "backoff_ms": 25, "strategy": "fixed"},
        )
        self.assertNotIn("recommended_poll_after_ms", detail)

        second_status = self.gateway_client.get(
            f"/v1/runs/{second_run_id}",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(second_status.status_code, 200)
        status_payload = second_status.json()["payload"]
        self.assertEqual(status_payload["status"], "dlq")
        self.assertEqual(
            status_payload["orchestration"]["failure_reason_code"],
            "capacity_exhausted",
        )

        scheduler_health = self.gateway_client.get(
            "/v1/orchestration/scheduler:health",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(scheduler_health.status_code, 200)
        self.assertEqual(int(scheduler_health.json()["scheduler_depth"]), 0)

    def test_gateway_compute_capacity_retry_exhaustion_fails_terminally_e2e(self) -> None:
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
                payload={"device_id": "gpu-node-gateway-retry-exhaust-e2e", "capabilities": ["compute.comfyui.local"]},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-retry-exhaust-device-register",
                run_id="run-gateway-retry-exhaust-device-bootstrap",
                task_id="task-gateway-retry-exhaust-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(register.status_code, 200)
        pair_request = device_hub_client.post(
            "/v1/devices/pairing/request",
            json=build_command_envelope(
                command_type="device.pairing.request",
                payload={"device_id": "gpu-node-gateway-retry-exhaust-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-retry-exhaust-device-pair",
                run_id="run-gateway-retry-exhaust-device-bootstrap",
                task_id="task-gateway-retry-exhaust-device-bootstrap",
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
                trace_id="trace-gateway-retry-exhaust-device-approve",
                run_id="run-gateway-retry-exhaust-device-bootstrap",
                task_id="task-gateway-retry-exhaust-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(approve.status_code, 200)
        heartbeat = device_hub_client.post(
            "/v1/devices/heartbeat",
            json=build_command_envelope(
                command_type="device.heartbeat",
                payload={"device_id": "gpu-node-gateway-retry-exhaust-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-retry-exhaust-device-heartbeat",
                run_id="run-gateway-retry-exhaust-device-bootstrap",
                task_id="task-gateway-retry-exhaust-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(heartbeat.status_code, 200)

        token = self._gateway_token(["runs:write"])
        read_token = self._gateway_token(["runs:read"])
        first_run_id = self._submit_run(
            token,
            "gateway compute retry exhaust first run",
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
        first_tick = self.gateway_client.post(
            "/v1/orchestration/worker:tick?fair=true&auto_start=true",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(first_tick.status_code, 200)
        self.assertEqual(first_tick.json()["outcome"], "progressed")
        self.assertEqual(first_tick.json()["leased_run_id"], first_run_id)

        second = self.gateway_client.post(
            "/v1/runs",
            json={
                "tenant_id": "t1",
                "app_id": "covernow",
                "session_key": "tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                "retry_policy": {
                    "max_attempts": 2,
                    "backoff_ms": 25,
                    "strategy": "fixed",
                },
                "payload": {
                    "goal": "gateway compute retry exhaust second run",
                    "dispatch_policy": {
                        "queue": {
                            "dispatch_min_score": -999,
                            "max_queue_depth_for_dispatch": 0,
                        }
                    },
                    "execution_profile": {
                        "execution_mode": "compute",
                        "inference_target": "none",
                        "resource_class": "gpu",
                        "placement_constraints": {
                            "tenant_id": "t1",
                            "required_capabilities": ["compute.comfyui.local"],
                        },
                    },
                },
            },
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(second.status_code, 503)
        detail = second.json().get("detail")
        self.assertIsInstance(detail, dict)
        assert isinstance(detail, dict)
        second_run_id = str(detail.get("run_id"))
        self.assertEqual(detail.get("placement_reason_code"), "capacity_exhausted")
        self.assertEqual(detail.get("run_status"), "queued")
        self.assertEqual(detail.get("retryable"), True)
        self.assertEqual(
            detail.get("retry_policy"),
            {"max_attempts": 2, "backoff_ms": 25, "strategy": "fixed"},
        )
        self.assertEqual(detail.get("recommended_poll_after_ms"), 25)

        for _ in range(6):
            scheduler_tick = self.gateway_client.post(
                "/v1/orchestration/scheduler:tick?max_items=1&fair=true",
                headers={"Authorization": f"Bearer {token}"},
            )
            self.assertEqual(scheduler_tick.status_code, 200)
            tick_payload = scheduler_tick.json()
            if int(tick_payload.get("promoted", 0)) >= 1:
                break
            next_due_in_ms = int(tick_payload.get("next_due_in_ms") or 0)
            if next_due_in_ms > 0:
                time.sleep(min(0.2, (next_due_in_ms + 20) / 1000.0))
        else:
            self.fail("scheduler did not promote retry message in expected retries")

        retry_tick = self.gateway_client.post(
            "/v1/orchestration/worker:tick?fair=true&auto_start=true",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(retry_tick.status_code, 200)
        retry_tick_payload = retry_tick.json()
        self.assertEqual(retry_tick_payload["outcome"], "skipped")
        self.assertEqual(retry_tick_payload["reason"], "dispatch_retry_failed")
        self.assertEqual(retry_tick_payload["before_status"], "queued")
        self.assertEqual(retry_tick_payload["after_status"], "dlq")

        second_status = self.gateway_client.get(
            f"/v1/runs/{second_run_id}",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(second_status.status_code, 200)
        status_payload = second_status.json()["payload"]
        self.assertEqual(status_payload["status"], "dlq")
        self.assertEqual(status_payload["retry_attempts"], 1)
        self.assertEqual(
            status_payload["orchestration"]["failure_reason_code"],
            "capacity_exhausted",
        )

        scheduler_health = self.gateway_client.get(
            "/v1/orchestration/scheduler:health",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(scheduler_health.status_code, 200)
        self.assertEqual(int(scheduler_health.json()["scheduler_depth"]), 0)

    def test_gateway_compute_capacity_retry_exponential_backoff_sequence_e2e(self) -> None:
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
                payload={
                    "device_id": "gpu-node-gateway-retry-exp-backoff-e2e",
                    "capabilities": ["compute.comfyui.local"],
                },
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-retry-exp-backoff-device-register",
                run_id="run-gateway-retry-exp-backoff-device-bootstrap",
                task_id="task-gateway-retry-exp-backoff-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(register.status_code, 200)
        pair_request = device_hub_client.post(
            "/v1/devices/pairing/request",
            json=build_command_envelope(
                command_type="device.pairing.request",
                payload={"device_id": "gpu-node-gateway-retry-exp-backoff-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-retry-exp-backoff-device-pair",
                run_id="run-gateway-retry-exp-backoff-device-bootstrap",
                task_id="task-gateway-retry-exp-backoff-device-bootstrap",
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
                trace_id="trace-gateway-retry-exp-backoff-device-approve",
                run_id="run-gateway-retry-exp-backoff-device-bootstrap",
                task_id="task-gateway-retry-exp-backoff-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(approve.status_code, 200)
        heartbeat = device_hub_client.post(
            "/v1/devices/heartbeat",
            json=build_command_envelope(
                command_type="device.heartbeat",
                payload={"device_id": "gpu-node-gateway-retry-exp-backoff-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-retry-exp-backoff-device-heartbeat",
                run_id="run-gateway-retry-exp-backoff-device-bootstrap",
                task_id="task-gateway-retry-exp-backoff-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(heartbeat.status_code, 200)

        token = self._gateway_token(["runs:write"])
        read_token = self._gateway_token(["runs:read"])
        first_run_id = self._submit_run(
            token,
            "gateway compute retry exponential first run",
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
        first_tick = self.gateway_client.post(
            "/v1/orchestration/worker:tick?fair=true&auto_start=true",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(first_tick.status_code, 200)
        self.assertEqual(first_tick.json()["outcome"], "progressed")
        self.assertEqual(first_tick.json()["leased_run_id"], first_run_id)

        second = self.gateway_client.post(
            "/v1/runs",
            json={
                "tenant_id": "t1",
                "app_id": "covernow",
                "session_key": "tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                "retry_policy": {
                    "max_attempts": 3,
                    "backoff_ms": 25,
                    "strategy": "exponential",
                },
                "payload": {
                    "goal": "gateway compute retry exponential second run",
                    "dispatch_policy": {
                        "queue": {
                            "dispatch_min_score": -999,
                            "max_queue_depth_for_dispatch": 0,
                        }
                    },
                    "execution_profile": {
                        "execution_mode": "compute",
                        "inference_target": "none",
                        "resource_class": "gpu",
                        "placement_constraints": {
                            "tenant_id": "t1",
                            "required_capabilities": ["compute.comfyui.local"],
                        },
                    },
                },
            },
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(second.status_code, 503)
        detail = second.json().get("detail")
        self.assertIsInstance(detail, dict)
        assert isinstance(detail, dict)
        second_run_id = str(detail.get("run_id"))
        self.assertEqual(detail.get("placement_reason_code"), "capacity_exhausted")
        self.assertEqual(detail.get("run_status"), "queued")
        self.assertEqual(detail.get("retryable"), True)
        self.assertEqual(
            detail.get("retry_policy"),
            {"max_attempts": 3, "backoff_ms": 25, "strategy": "exponential"},
        )
        self.assertEqual(detail.get("recommended_poll_after_ms"), 25)

        scheduled_first_retry = execution_app_module._runtime.queue_fabric.lease_one(QueueType.SCHEDULER)
        self.assertIsNotNone(scheduled_first_retry)
        assert scheduled_first_retry is not None
        self.assertEqual(scheduled_first_retry.run_id, second_run_id)
        self.assertEqual(scheduled_first_retry.retry_count, 1)
        self.assertEqual(scheduled_first_retry.backoff_ms, 25)
        first_marker = scheduled_first_retry.payload["orchestration_payload"]["_runtime_retry"]
        self.assertEqual(first_marker["kind"], "dispatch_retry")
        self.assertEqual(first_marker["attempt"], 1)
        execution_app_module._runtime.queue_fabric.publish(QueueType.SCHEDULER, scheduled_first_retry)

        def _promote_retry_once() -> None:
            for _ in range(8):
                scheduler_tick = self.gateway_client.post(
                    "/v1/orchestration/scheduler:tick?max_items=1&fair=true",
                    headers={"Authorization": f"Bearer {token}"},
                )
                self.assertEqual(scheduler_tick.status_code, 200)
                tick_payload = scheduler_tick.json()
                if int(tick_payload.get("promoted", 0)) >= 1:
                    return
                next_due_in_ms = int(tick_payload.get("next_due_in_ms") or 0)
                if next_due_in_ms > 0:
                    time.sleep(min(0.2, (next_due_in_ms + 20) / 1000.0))
            self.fail("scheduler did not promote retry message in expected retries")

        _promote_retry_once()

        retry_tick_one = self.gateway_client.post(
            "/v1/orchestration/worker:tick?fair=true&auto_start=true",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(retry_tick_one.status_code, 200)
        retry_tick_one_payload = retry_tick_one.json()
        self.assertEqual(retry_tick_one_payload["outcome"], "skipped")
        self.assertEqual(retry_tick_one_payload["reason"], "dispatch_retry_deferred")
        self.assertEqual(retry_tick_one_payload["before_status"], "queued")
        self.assertEqual(retry_tick_one_payload["after_status"], "queued")

        scheduled_second_retry = execution_app_module._runtime.queue_fabric.lease_one(QueueType.SCHEDULER)
        self.assertIsNotNone(scheduled_second_retry)
        assert scheduled_second_retry is not None
        self.assertEqual(scheduled_second_retry.run_id, second_run_id)
        self.assertEqual(scheduled_second_retry.retry_count, 2)
        self.assertEqual(scheduled_second_retry.backoff_ms, 50)
        second_marker = scheduled_second_retry.payload["orchestration_payload"]["_runtime_retry"]
        self.assertEqual(second_marker["kind"], "dispatch_retry")
        self.assertEqual(second_marker["attempt"], 2)
        execution_app_module._runtime.queue_fabric.publish(QueueType.SCHEDULER, scheduled_second_retry)

        _promote_retry_once()

        retry_tick_two = self.gateway_client.post(
            "/v1/orchestration/worker:tick?fair=true&auto_start=true",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(retry_tick_two.status_code, 200)
        retry_tick_two_payload = retry_tick_two.json()
        self.assertEqual(retry_tick_two_payload["outcome"], "skipped")
        self.assertEqual(retry_tick_two_payload["reason"], "dispatch_retry_failed")
        self.assertEqual(retry_tick_two_payload["before_status"], "queued")
        self.assertEqual(retry_tick_two_payload["after_status"], "dlq")

        second_status = self.gateway_client.get(
            f"/v1/runs/{second_run_id}",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(second_status.status_code, 200)
        status_payload = second_status.json()["payload"]
        self.assertEqual(status_payload["status"], "dlq")
        self.assertEqual(status_payload["retry_attempts"], 2)
        self.assertEqual(
            status_payload["orchestration"]["failure_reason_code"],
            "capacity_exhausted",
        )

        scheduler_health = self.gateway_client.get(
            "/v1/orchestration/scheduler:health",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(scheduler_health.status_code, 200)
        self.assertEqual(int(scheduler_health.json()["scheduler_depth"]), 0)

    def test_gateway_compute_policy_rejection_returns_failed_status_e2e(self) -> None:
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
                payload={
                    "device_id": "gpu-node-gateway-policy-reject-e2e",
                    "capabilities": ["compute.comfyui.local"],
                },
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-policy-reject-device-register",
                run_id="run-gateway-policy-reject-device-bootstrap",
                task_id="task-gateway-policy-reject-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(register.status_code, 200)
        pair_request = device_hub_client.post(
            "/v1/devices/pairing/request",
            json=build_command_envelope(
                command_type="device.pairing.request",
                payload={"device_id": "gpu-node-gateway-policy-reject-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-policy-reject-device-pair",
                run_id="run-gateway-policy-reject-device-bootstrap",
                task_id="task-gateway-policy-reject-device-bootstrap",
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
                trace_id="trace-gateway-policy-reject-device-approve",
                run_id="run-gateway-policy-reject-device-bootstrap",
                task_id="task-gateway-policy-reject-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(approve.status_code, 200)
        heartbeat = device_hub_client.post(
            "/v1/devices/heartbeat",
            json=build_command_envelope(
                command_type="device.heartbeat",
                payload={"device_id": "gpu-node-gateway-policy-reject-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-policy-reject-device-heartbeat",
                run_id="run-gateway-policy-reject-device-bootstrap",
                task_id="task-gateway-policy-reject-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(heartbeat.status_code, 200)

        token = self._gateway_token(["runs:write"])
        read_token = self._gateway_token(["runs:read"])
        response = self.gateway_client.post(
            "/v1/runs",
            json={
                "tenant_id": "t1",
                "app_id": "covernow",
                "session_key": "tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                "payload": {
                    "goal": "gateway compute policy reject run",
                    "dispatch_policy": {
                        "queue": {
                            "dispatch_min_score": -999,
                            "max_queue_depth_for_dispatch": 0,
                        }
                    },
                    "execution_profile": {
                        "execution_mode": "compute",
                        "inference_target": "none",
                        "resource_class": "gpu",
                        "placement_constraints": {
                            "tenant_id": "t1",
                            "required_capabilities": [
                                "compute.comfyui.local",
                                "model.flux.1",
                            ],
                        },
                    },
                },
            },
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 409)
        detail = response.json().get("detail")
        self.assertIsInstance(detail, dict)
        assert isinstance(detail, dict)
        run_id = str(detail.get("run_id"))
        self.assertEqual(detail.get("downstream_event_type"), "runtime.route.failed")
        self.assertEqual(detail.get("failure_code"), "required_capabilities_unavailable")
        self.assertEqual(detail.get("failure_classification"), "policy")
        self.assertEqual(detail.get("placement_reason_code"), "required_capabilities_unavailable")
        self.assertEqual(detail.get("run_status"), "failed")
        self.assertEqual(detail.get("retryable"), False)
        self.assertNotIn("recommended_poll_after_ms", detail)
        self.assertEqual(
            detail.get("retry_policy"),
            {"max_attempts": 3, "backoff_ms": 250, "strategy": "fixed"},
        )

        status = self.gateway_client.get(
            f"/v1/runs/{run_id}",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(status.status_code, 200)
        status_payload = status.json()["payload"]
        self.assertEqual(status_payload["status"], "failed")
        self.assertEqual(status_payload["retry_attempts"], 0)
        self.assertEqual(
            status_payload["orchestration"]["failure_reason_code"],
            "required_capabilities_unavailable",
        )

        scheduler_health = self.gateway_client.get(
            "/v1/orchestration/scheduler:health",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(scheduler_health.status_code, 200)
        self.assertEqual(int(scheduler_health.json()["scheduler_depth"]), 0)

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
        read_token = self._gateway_token(["runs:read"])

        cancel_response = self.gateway_client.post(
            f"/v1/runs/{run_id}:cancel",
            json={"reason": "operator_cancel_gateway_e2e", "cascade_children": True},
            headers={"Authorization": f"Bearer {self._gateway_token(['runs:write'])}"},
        )
        self.assertEqual(cancel_response.status_code, 200)
        self.assertEqual(cancel_response.json()["payload"]["status"], "canceled")

        status_canceled = self.gateway_client.get(
            f"/v1/runs/{run_id}",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(status_canceled.status_code, 200)
        self.assertEqual(status_canceled.json()["payload"]["run_id"], run_id)
        self.assertEqual(status_canceled.json()["payload"]["status"], "canceled")
        self.assertEqual(status_canceled.json()["recommended_poll_after_ms"], 10000)

        lease_expired = self.gateway_client.get(
            f"/v1/runs/{run_id}/lease",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(lease_expired.status_code, 200)
        self.assertEqual(lease_expired.json()["run_id"], run_id)
        self.assertEqual(lease_expired.json()["lease"]["state"], "expired")
        self.assertEqual(lease_expired.json()["device_hub"]["status"], "ok")
        self.assertEqual(
            lease_expired.json()["device_hub"]["snapshot"]["expire_reason_code"],
            "run_canceled",
        )
        self.assertEqual(lease_expired.json()["recommended_poll_after_ms"], 10000)

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

    def test_gateway_reject_expires_device_hub_lease_e2e(self) -> None:
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
                payload={"device_id": "gpu-node-gateway-reject-e2e", "capabilities": ["compute.comfyui.local"]},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-reject-device-register",
                run_id="run-gateway-reject-device-bootstrap",
                task_id="task-gateway-reject-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(register.status_code, 200)
        pair_request = device_hub_client.post(
            "/v1/devices/pairing/request",
            json=build_command_envelope(
                command_type="device.pairing.request",
                payload={"device_id": "gpu-node-gateway-reject-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-reject-device-pair",
                run_id="run-gateway-reject-device-bootstrap",
                task_id="task-gateway-reject-device-bootstrap",
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
                trace_id="trace-gateway-reject-device-approve",
                run_id="run-gateway-reject-device-bootstrap",
                task_id="task-gateway-reject-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(approve.status_code, 200)
        heartbeat = device_hub_client.post(
            "/v1/devices/heartbeat",
            json=build_command_envelope(
                command_type="device.heartbeat",
                payload={"device_id": "gpu-node-gateway-reject-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-reject-device-heartbeat",
                run_id="run-gateway-reject-device-bootstrap",
                task_id="task-gateway-reject-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(heartbeat.status_code, 200)

        run_id = self._submit_run(
            self._gateway_token(["runs:write"]),
            "gateway compute reject flow",
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
        read_token = self._gateway_token(["runs:read"])

        reject_response = self.gateway_client.post(
            f"/v1/runs/{run_id}:reject",
            headers={"Authorization": f"Bearer {self._gateway_token(['runs:write'])}"},
        )
        self.assertEqual(reject_response.status_code, 200)
        self.assertEqual(reject_response.json()["payload"]["status"], "canceled")

        status_rejected = self.gateway_client.get(
            f"/v1/runs/{run_id}",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(status_rejected.status_code, 200)
        self.assertEqual(status_rejected.json()["payload"]["run_id"], run_id)
        self.assertEqual(status_rejected.json()["payload"]["status"], "canceled")
        self.assertEqual(status_rejected.json()["recommended_poll_after_ms"], 10000)

        lease_expired = self.gateway_client.get(
            f"/v1/runs/{run_id}/lease",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(lease_expired.status_code, 200)
        self.assertEqual(lease_expired.json()["run_id"], run_id)
        self.assertEqual(lease_expired.json()["lease"]["state"], "expired")
        self.assertEqual(lease_expired.json()["device_hub"]["status"], "ok")
        self.assertEqual(
            lease_expired.json()["device_hub"]["snapshot"]["expire_reason_code"],
            "approval_rejected",
        )
        self.assertEqual(lease_expired.json()["recommended_poll_after_ms"], 10000)

        self.assertEqual(execution_app_module._runtime.runs[run_id].device_lease_state, "expired")
        assert lease_id is not None
        lease = device_hub_app_module._hub.leases[lease_id]
        self.assertEqual(lease.status, "expired")
        self.assertEqual(lease.expire_reason_code, "approval_rejected")

        capacity_response = device_hub_client.get(
            "/v1/placements/capacity",
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:read'])}"},
        )
        self.assertEqual(capacity_response.status_code, 200)
        self.assertEqual(capacity_response.json()["active_leases"], 0)

    def test_gateway_timeout_expires_device_hub_lease_e2e(self) -> None:
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
                payload={"device_id": "gpu-node-gateway-timeout-e2e", "capabilities": ["compute.comfyui.local"]},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-timeout-device-register",
                run_id="run-gateway-timeout-device-bootstrap",
                task_id="task-gateway-timeout-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(register.status_code, 200)
        pair_request = device_hub_client.post(
            "/v1/devices/pairing/request",
            json=build_command_envelope(
                command_type="device.pairing.request",
                payload={"device_id": "gpu-node-gateway-timeout-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-timeout-device-pair",
                run_id="run-gateway-timeout-device-bootstrap",
                task_id="task-gateway-timeout-device-bootstrap",
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
                trace_id="trace-gateway-timeout-device-approve",
                run_id="run-gateway-timeout-device-bootstrap",
                task_id="task-gateway-timeout-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(approve.status_code, 200)
        heartbeat = device_hub_client.post(
            "/v1/devices/heartbeat",
            json=build_command_envelope(
                command_type="device.heartbeat",
                payload={"device_id": "gpu-node-gateway-timeout-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-timeout-device-heartbeat",
                run_id="run-gateway-timeout-device-bootstrap",
                task_id="task-gateway-timeout-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(heartbeat.status_code, 200)

        run_id = self._submit_run(
            self._gateway_token(["runs:write"]),
            "gateway compute timeout flow",
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
        read_token = self._gateway_token(["runs:read"])

        timeout_response = self.gateway_client.post(
            f"/v1/runs/{run_id}:timeout",
            json={"reason": "deadline_exceeded_gateway_e2e", "cascade_children": True},
            headers={"Authorization": f"Bearer {self._gateway_token(['runs:write'])}"},
        )
        self.assertEqual(timeout_response.status_code, 200)
        self.assertEqual(timeout_response.json()["payload"]["status"], "timed_out")

        status_timed_out = self.gateway_client.get(
            f"/v1/runs/{run_id}",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(status_timed_out.status_code, 200)
        self.assertEqual(status_timed_out.json()["payload"]["run_id"], run_id)
        self.assertEqual(status_timed_out.json()["payload"]["status"], "timed_out")
        self.assertEqual(status_timed_out.json()["recommended_poll_after_ms"], 10000)

        lease_expired = self.gateway_client.get(
            f"/v1/runs/{run_id}/lease",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(lease_expired.status_code, 200)
        self.assertEqual(lease_expired.json()["run_id"], run_id)
        self.assertEqual(lease_expired.json()["lease"]["state"], "expired")
        self.assertEqual(lease_expired.json()["device_hub"]["status"], "ok")
        self.assertEqual(
            lease_expired.json()["device_hub"]["snapshot"]["expire_reason_code"],
            "run_timed_out",
        )
        self.assertEqual(lease_expired.json()["recommended_poll_after_ms"], 10000)

        self.assertEqual(execution_app_module._runtime.runs[run_id].device_lease_state, "expired")
        assert lease_id is not None
        lease = device_hub_app_module._hub.leases[lease_id]
        self.assertEqual(lease.status, "expired")
        self.assertEqual(lease.expire_reason_code, "run_timed_out")

        capacity_response = device_hub_client.get(
            "/v1/placements/capacity",
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:read'])}"},
        )
        self.assertEqual(capacity_response.status_code, 200)
        self.assertEqual(capacity_response.json()["active_leases"], 0)

    def test_gateway_preempt_preempts_device_hub_lease_e2e(self) -> None:
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
                payload={"device_id": "gpu-node-gateway-preempt-e2e", "capabilities": ["compute.comfyui.local"]},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-preempt-device-register",
                run_id="run-gateway-preempt-device-bootstrap",
                task_id="task-gateway-preempt-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(register.status_code, 200)
        pair_request = device_hub_client.post(
            "/v1/devices/pairing/request",
            json=build_command_envelope(
                command_type="device.pairing.request",
                payload={"device_id": "gpu-node-gateway-preempt-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-preempt-device-pair",
                run_id="run-gateway-preempt-device-bootstrap",
                task_id="task-gateway-preempt-device-bootstrap",
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
                trace_id="trace-gateway-preempt-device-approve",
                run_id="run-gateway-preempt-device-bootstrap",
                task_id="task-gateway-preempt-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(approve.status_code, 200)
        heartbeat = device_hub_client.post(
            "/v1/devices/heartbeat",
            json=build_command_envelope(
                command_type="device.heartbeat",
                payload={"device_id": "gpu-node-gateway-preempt-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-preempt-device-heartbeat",
                run_id="run-gateway-preempt-device-bootstrap",
                task_id="task-gateway-preempt-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(heartbeat.status_code, 200)

        run_id = self._submit_run(
            self._gateway_token(["runs:write"]),
            "gateway compute preempt flow",
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
        read_token = self._gateway_token(["runs:read"])

        preempt_response = self.gateway_client.post(
            f"/v1/runs/{run_id}:preempt",
            json={"reason": "resource_preempted_gateway_e2e", "cascade_children": True},
            headers={"Authorization": f"Bearer {self._gateway_token(['runs:write'])}"},
        )
        self.assertEqual(preempt_response.status_code, 200)
        self.assertEqual(preempt_response.json()["payload"]["status"], "canceled")
        self.assertEqual(
            preempt_response.json()["payload"]["orchestration"]["failure_reason_code"],
            "run_preempted",
        )

        status_preempted = self.gateway_client.get(
            f"/v1/runs/{run_id}",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(status_preempted.status_code, 200)
        self.assertEqual(status_preempted.json()["payload"]["run_id"], run_id)
        self.assertEqual(status_preempted.json()["payload"]["status"], "canceled")
        self.assertEqual(status_preempted.json()["recommended_poll_after_ms"], 10000)

        lease_expired = self.gateway_client.get(
            f"/v1/runs/{run_id}/lease",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(lease_expired.status_code, 200)
        self.assertEqual(lease_expired.json()["run_id"], run_id)
        self.assertEqual(lease_expired.json()["lease"]["state"], "expired")
        self.assertEqual(lease_expired.json()["device_hub"]["status"], "ok")
        self.assertEqual(
            lease_expired.json()["device_hub"]["snapshot"]["expire_reason_code"],
            "run_preempted",
        )
        self.assertEqual(lease_expired.json()["recommended_poll_after_ms"], 10000)

        self.assertEqual(execution_app_module._runtime.runs[run_id].device_lease_state, "expired")
        assert lease_id is not None
        lease = device_hub_app_module._hub.leases[lease_id]
        self.assertEqual(lease.status, "expired")
        self.assertEqual(lease.expire_reason_code, "run_preempted")

        capacity_response = device_hub_client.get(
            "/v1/placements/capacity",
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:read'])}"},
        )
        self.assertEqual(capacity_response.status_code, 200)
        self.assertEqual(capacity_response.json()["active_leases"], 0)

    def test_gateway_complete_releases_device_hub_lease_e2e(self) -> None:
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
                payload={"device_id": "gpu-node-gateway-complete-e2e", "capabilities": ["compute.comfyui.local"]},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-complete-device-register",
                run_id="run-gateway-complete-device-bootstrap",
                task_id="task-gateway-complete-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(register.status_code, 200)
        pair_request = device_hub_client.post(
            "/v1/devices/pairing/request",
            json=build_command_envelope(
                command_type="device.pairing.request",
                payload={"device_id": "gpu-node-gateway-complete-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-complete-device-pair",
                run_id="run-gateway-complete-device-bootstrap",
                task_id="task-gateway-complete-device-bootstrap",
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
                trace_id="trace-gateway-complete-device-approve",
                run_id="run-gateway-complete-device-bootstrap",
                task_id="task-gateway-complete-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(approve.status_code, 200)
        heartbeat = device_hub_client.post(
            "/v1/devices/heartbeat",
            json=build_command_envelope(
                command_type="device.heartbeat",
                payload={"device_id": "gpu-node-gateway-complete-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-complete-device-heartbeat",
                run_id="run-gateway-complete-device-bootstrap",
                task_id="task-gateway-complete-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(heartbeat.status_code, 200)

        token = self._gateway_token(["runs:write"])
        run_id = self._submit_run(
            token,
            "gateway compute complete flow",
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
        read_token = self._gateway_token(["runs:read"])

        lease_active = self.gateway_client.get(
            f"/v1/runs/{run_id}/lease",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(lease_active.status_code, 200)
        self.assertEqual(lease_active.json()["run_id"], run_id)
        self.assertEqual(lease_active.json()["lease"]["state"], "active")
        self.assertEqual(lease_active.json()["device_hub"]["status"], "ok")
        self.assertEqual(lease_active.json()["recommended_poll_after_ms"], 2000)

        tick = self.gateway_client.post(
            "/v1/orchestration/worker:tick?fair=true&auto_start=true",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(tick.status_code, 200)
        self.assertEqual(tick.json()["outcome"], "progressed")
        self.assertEqual(tick.json()["after_status"], "running")

        status_running = self.gateway_client.get(
            f"/v1/runs/{run_id}",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(status_running.status_code, 200)
        self.assertEqual(status_running.json()["payload"]["run_id"], run_id)
        self.assertEqual(status_running.json()["payload"]["status"], "running")
        self.assertEqual(status_running.json()["recommended_poll_after_ms"], 1000)

        complete_response = self.gateway_client.post(
            f"/v1/runs/{run_id}:complete",
            json={"success": True},
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(complete_response.status_code, 200)
        self.assertEqual(complete_response.json()["payload"]["status"], "succeeded")

        status_succeeded = self.gateway_client.get(
            f"/v1/runs/{run_id}",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(status_succeeded.status_code, 200)
        self.assertEqual(status_succeeded.json()["payload"]["run_id"], run_id)
        self.assertEqual(status_succeeded.json()["payload"]["status"], "succeeded")
        self.assertEqual(status_succeeded.json()["recommended_poll_after_ms"], 10000)

        lease_released = self.gateway_client.get(
            f"/v1/runs/{run_id}/lease",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(lease_released.status_code, 200)
        self.assertEqual(lease_released.json()["run_id"], run_id)
        self.assertEqual(lease_released.json()["lease"]["state"], "released")
        self.assertEqual(lease_released.json()["device_hub"]["status"], "ok")
        self.assertEqual(lease_released.json()["recommended_poll_after_ms"], 10000)

        self.assertEqual(execution_app_module._runtime.runs[run_id].device_lease_state, "released")
        assert lease_id is not None
        lease = device_hub_app_module._hub.leases[lease_id]
        self.assertEqual(lease.status, "released")

        capacity_response = device_hub_client.get(
            "/v1/placements/capacity",
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:read'])}"},
        )
        self.assertEqual(capacity_response.status_code, 200)
        self.assertEqual(capacity_response.json()["active_leases"], 0)

    def test_gateway_complete_maps_ttl_expired_release_race_to_expired_e2e(self) -> None:
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
                payload={"device_id": "gpu-node-gateway-race-e2e", "capabilities": ["compute.comfyui.local"]},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-race-device-register",
                run_id="run-gateway-race-device-bootstrap",
                task_id="task-gateway-race-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(register.status_code, 200)
        pair_request = device_hub_client.post(
            "/v1/devices/pairing/request",
            json=build_command_envelope(
                command_type="device.pairing.request",
                payload={"device_id": "gpu-node-gateway-race-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-race-device-pair",
                run_id="run-gateway-race-device-bootstrap",
                task_id="task-gateway-race-device-bootstrap",
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
                trace_id="trace-gateway-race-device-approve",
                run_id="run-gateway-race-device-bootstrap",
                task_id="task-gateway-race-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(approve.status_code, 200)
        heartbeat = device_hub_client.post(
            "/v1/devices/heartbeat",
            json=build_command_envelope(
                command_type="device.heartbeat",
                payload={"device_id": "gpu-node-gateway-race-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-race-device-heartbeat",
                run_id="run-gateway-race-device-bootstrap",
                task_id="task-gateway-race-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(heartbeat.status_code, 200)

        token = self._gateway_token(["runs:write"])
        run_id = self._submit_run(
            token,
            "gateway lease release race flow",
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
        read_token = self._gateway_token(["runs:read"])

        tick = self.gateway_client.post(
            "/v1/orchestration/worker:tick?fair=true&auto_start=true",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(tick.status_code, 200)
        self.assertEqual(tick.json()["outcome"], "progressed")
        self.assertEqual(tick.json()["after_status"], "running")

        assert lease_id is not None
        device_hub_app_module._hub.leases[lease_id].lease_expires_at = (
            datetime.now(timezone.utc) - timedelta(seconds=5)
        ).isoformat()

        complete_response = self.gateway_client.post(
            f"/v1/runs/{run_id}:complete",
            json={"success": True},
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(complete_response.status_code, 200)
        self.assertEqual(complete_response.json()["payload"]["status"], "succeeded")

        status_succeeded = self.gateway_client.get(
            f"/v1/runs/{run_id}",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(status_succeeded.status_code, 200)
        self.assertEqual(status_succeeded.json()["payload"]["run_id"], run_id)
        self.assertEqual(status_succeeded.json()["payload"]["status"], "succeeded")
        self.assertEqual(status_succeeded.json()["recommended_poll_after_ms"], 10000)

        lease_expired = self.gateway_client.get(
            f"/v1/runs/{run_id}/lease",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(lease_expired.status_code, 200)
        self.assertEqual(lease_expired.json()["run_id"], run_id)
        self.assertEqual(lease_expired.json()["lease"]["state"], "expired")
        self.assertEqual(lease_expired.json()["device_hub"]["status"], "ok")
        self.assertEqual(lease_expired.json()["device_hub"]["snapshot"]["status"], "expired")
        self.assertEqual(lease_expired.json()["device_hub"]["snapshot"]["expire_reason_code"], "ttl_expired")
        self.assertEqual(lease_expired.json()["recommended_poll_after_ms"], 10000)

        run = execution_app_module._runtime.runs[run_id]
        self.assertEqual(run.device_lease_state, "expired")
        self.assertIsNone(run.lease_transition_error)
        resumed = execution_app_module._runtime.resume_from_checkpoint(run_id)
        self.assertEqual(resumed.device_lease_state, "expired")
        self.assertIsNone(resumed.lease_transition_error)
        lease = device_hub_app_module._hub.leases[lease_id]
        self.assertEqual(lease.status, "expired")
        self.assertEqual(lease.expire_reason_code, "ttl_expired")

    def test_gateway_complete_failure_exhausted_retries_expires_device_hub_lease_e2e(self) -> None:
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
                payload={
                    "device_id": "gpu-node-gateway-complete-fail-e2e",
                    "capabilities": ["compute.comfyui.local"],
                },
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-complete-fail-device-register",
                run_id="run-gateway-complete-fail-device-bootstrap",
                task_id="task-gateway-complete-fail-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(register.status_code, 200)
        pair_request = device_hub_client.post(
            "/v1/devices/pairing/request",
            json=build_command_envelope(
                command_type="device.pairing.request",
                payload={"device_id": "gpu-node-gateway-complete-fail-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-complete-fail-device-pair",
                run_id="run-gateway-complete-fail-device-bootstrap",
                task_id="task-gateway-complete-fail-device-bootstrap",
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
                trace_id="trace-gateway-complete-fail-device-approve",
                run_id="run-gateway-complete-fail-device-bootstrap",
                task_id="task-gateway-complete-fail-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(approve.status_code, 200)
        heartbeat = device_hub_client.post(
            "/v1/devices/heartbeat",
            json=build_command_envelope(
                command_type="device.heartbeat",
                payload={"device_id": "gpu-node-gateway-complete-fail-e2e"},
                session_key="tenant:t1:app:covernow:channel:web:actor:u-e2e:thread:main:agent:pm",
                trace_id="trace-gateway-complete-fail-device-heartbeat",
                run_id="run-gateway-complete-fail-device-bootstrap",
                task_id="task-gateway-complete-fail-device-bootstrap",
            ),
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:write'])}"},
        )
        self.assertEqual(heartbeat.status_code, 200)

        token = self._gateway_token(["runs:write"])
        run_id = self._submit_run(
            token,
            "gateway compute complete failure exhausted retries flow",
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
        read_token = self._gateway_token(["runs:read"])

        def _promote_retry_once() -> None:
            for _ in range(6):
                scheduler_tick = self.gateway_client.post(
                    "/v1/orchestration/scheduler:tick?max_items=1&fair=true",
                    headers={"Authorization": f"Bearer {token}"},
                )
                self.assertEqual(scheduler_tick.status_code, 200)
                payload = scheduler_tick.json()
                if int(payload.get("promoted", 0)) >= 1:
                    return
                next_due_in_ms = int(payload.get("next_due_in_ms") or 0)
                if next_due_in_ms > 0:
                    time.sleep(min(0.2, (next_due_in_ms + 20) / 1000.0))
            self.fail("scheduler did not promote retry message in expected retries")

        for attempt in range(2):
            if attempt > 0:
                _promote_retry_once()

            tick = self.gateway_client.post(
                "/v1/orchestration/worker:tick?fair=true&auto_start=true",
                headers={"Authorization": f"Bearer {token}"},
            )
            self.assertEqual(tick.status_code, 200)
            self.assertEqual(tick.json()["outcome"], "progressed")
            self.assertEqual(tick.json()["after_status"], "running")

            complete_response = self.gateway_client.post(
                f"/v1/runs/{run_id}:complete",
                json={"success": False},
                headers={"Authorization": f"Bearer {token}"},
            )
            self.assertEqual(complete_response.status_code, 200)
            self.assertEqual(complete_response.json()["payload"]["status"], "queued")
            self.assertEqual(complete_response.json()["payload"]["retry_attempts"], attempt + 1)
            self.assertEqual(execution_app_module._runtime.runs[run_id].device_lease_state, "active")

        _promote_retry_once()

        final_tick = self.gateway_client.post(
            "/v1/orchestration/worker:tick?fair=true&auto_start=true",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(final_tick.status_code, 200)
        self.assertEqual(final_tick.json()["outcome"], "progressed")
        self.assertEqual(final_tick.json()["after_status"], "running")

        final_complete = self.gateway_client.post(
            f"/v1/runs/{run_id}:complete",
            json={"success": False, "failure_reason_code": "tool_contract_violation"},
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(final_complete.status_code, 200)
        self.assertEqual(final_complete.json()["payload"]["status"], "dlq")
        self.assertEqual(
            final_complete.json()["payload"]["orchestration"]["failure_reason_code"],
            "tool_contract_violation",
        )

        status_failed = self.gateway_client.get(
            f"/v1/runs/{run_id}",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(status_failed.status_code, 200)
        self.assertEqual(status_failed.json()["payload"]["run_id"], run_id)
        self.assertEqual(status_failed.json()["payload"]["status"], "dlq")
        self.assertEqual(status_failed.json()["recommended_poll_after_ms"], 10000)
        self.assertEqual(
            status_failed.json()["payload"]["orchestration"]["failure_reason_code"],
            "tool_contract_violation",
        )

        lease_expired = self.gateway_client.get(
            f"/v1/runs/{run_id}/lease",
            headers={"Authorization": f"Bearer {read_token}"},
        )
        self.assertEqual(lease_expired.status_code, 200)
        self.assertEqual(lease_expired.json()["run_id"], run_id)
        self.assertEqual(lease_expired.json()["lease"]["state"], "expired")
        self.assertEqual(lease_expired.json()["device_hub"]["status"], "ok")
        self.assertEqual(lease_expired.json()["recommended_poll_after_ms"], 10000)

        self.assertEqual(execution_app_module._runtime.runs[run_id].device_lease_state, "expired")
        assert lease_id is not None
        lease = device_hub_app_module._hub.leases[lease_id]
        self.assertEqual(lease.status, "expired")
        self.assertEqual(lease.expire_reason_code, "tool_contract_violation")

        capacity_response = device_hub_client.get(
            "/v1/placements/capacity",
            headers={"Authorization": f"Bearer {self._device_hub_token(['devices:read'])}"},
        )
        self.assertEqual(capacity_response.status_code, 200)
        self.assertEqual(capacity_response.json()["active_leases"], 0)


if __name__ == "__main__":
    unittest.main()
