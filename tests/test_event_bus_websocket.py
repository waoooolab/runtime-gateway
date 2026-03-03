from __future__ import annotations

import os
import unittest
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from runtime_gateway.audit.emitter import clear_audit_events
from runtime_gateway.auth.tokens import issue_token

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


def _event_envelope(event_type: str = "runtime.task.updated", run_id: str | None = None) -> dict:
    payload: dict = {
        "task_id": "task-1",
        "status": "leased",
    }
    if run_id is not None:
        payload["run_id"] = run_id
    return {
        "event_id": str(uuid4()),
        "event_type": event_type,
        "tenant_id": "t1",
        "app_id": "waoooo",
        "session_key": "tenant:t1:app:waoooo:channel:web:actor:u1:thread:main:agent:pm",
        "trace_id": "trace-events-1",
        "correlation_id": "corr-events-1",
        "ts": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }


@unittest.skipUnless(FASTAPI_STACK_AVAILABLE, "fastapi stack not installed")
class EventBusWebsocketTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_audit_events()
        gateway_app_module._event_bus.clear()
        self.client = TestClient(gateway_app_module.app)

    def _token(self, *, audience: str = "runtime-gateway", scope: list[str]) -> str:
        return issue_token(
            {
                "iss": "runtime-gateway",
                "sub": "svc:test",
                "aud": audience,
                "tenant_id": "t1",
                "app_id": "waoooo",
                "scope": scope,
                "token_use": "service",
                "trace_id": "trace-events-1",
            },
            ttl_seconds=300,
        )

    def test_publish_event_requires_bearer_token(self) -> None:
        response = self.client.post("/v1/events/publish", json=_event_envelope())
        self.assertEqual(response.status_code, 401)

    def test_publish_event_and_list_recent(self) -> None:
        token = self._token(scope=["runs:write"])
        publish = self.client.post(
            "/v1/events/publish",
            json=_event_envelope("runtime.route.decided"),
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(publish.status_code, 200)
        body = publish.json()
        self.assertTrue(body["accepted"])
        self.assertGreaterEqual(int(body["bus_seq"]), 1)

        recent = self.client.get("/v1/events/recent", headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(recent.status_code, 200)
        items = recent.json()["items"]
        self.assertGreaterEqual(len(items), 1)
        self.assertEqual(items[-1]["event"]["event_type"], "runtime.route.decided")

    def test_recent_events_requires_bearer_token(self) -> None:
        response = self.client.get("/v1/events/recent")
        self.assertEqual(response.status_code, 401)

    def test_recent_events_can_filter_by_event_type(self) -> None:
        token = self._token(scope=["runs:write"])
        self.client.post(
            "/v1/events/publish",
            json=_event_envelope("runtime.route.decided"),
            headers={"Authorization": f"Bearer {token}"},
        )
        self.client.post(
            "/v1/events/publish",
            json=_event_envelope("device.lease.acquired"),
            headers={"Authorization": f"Bearer {token}"},
        )

        filtered = self.client.get(
            "/v1/events/recent?event_types=device.lease.acquired",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(filtered.status_code, 200)
        items = filtered.json()["items"]
        self.assertGreaterEqual(len(items), 1)
        self.assertTrue(all(i["event"]["event_type"] == "device.lease.acquired" for i in items))

    def test_recent_events_can_filter_by_run_id(self) -> None:
        token = self._token(scope=["runs:write"])
        self.client.post(
            "/v1/events/publish",
            json=_event_envelope("runtime.run.started", run_id="run-123"),
            headers={"Authorization": f"Bearer {token}"},
        )
        self.client.post(
            "/v1/events/publish",
            json=_event_envelope("runtime.run.started", run_id="run-456"),
            headers={"Authorization": f"Bearer {token}"},
        )
        self.client.post(
            "/v1/events/publish",
            json=_event_envelope("runtime.run.completed", run_id="run-123"),
            headers={"Authorization": f"Bearer {token}"},
        )

        filtered = self.client.get(
            "/v1/events/recent?run_id=run-123",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(filtered.status_code, 200)
        items = filtered.json()["items"]
        self.assertEqual(len(items), 2)
        self.assertTrue(all(i["event"]["payload"]["run_id"] == "run-123" for i in items))

    def test_recent_events_can_filter_by_run_id_and_event_type(self) -> None:
        token = self._token(scope=["runs:write"])
        self.client.post(
            "/v1/events/publish",
            json=_event_envelope("runtime.run.started", run_id="run-789"),
            headers={"Authorization": f"Bearer {token}"},
        )
        self.client.post(
            "/v1/events/publish",
            json=_event_envelope("runtime.run.completed", run_id="run-789"),
            headers={"Authorization": f"Bearer {token}"},
        )
        self.client.post(
            "/v1/events/publish",
            json=_event_envelope("runtime.run.completed", run_id="run-999"),
            headers={"Authorization": f"Bearer {token}"},
        )

        filtered = self.client.get(
            "/v1/events/recent?run_id=run-789&event_types=runtime.run.completed",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(filtered.status_code, 200)
        items = filtered.json()["items"]
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["event"]["payload"]["run_id"], "run-789")
        self.assertEqual(items[0]["event"]["event_type"], "runtime.run.completed")

    def test_websocket_receives_published_event(self) -> None:
        ws_token = self._token(scope=["runs:read"])
        publish_token = self._token(scope=["runs:write"])

        with self.client.websocket_connect(
            f"/v1/ws/events?access_token={ws_token}&tenant_id=t1&app_id=waoooo"
        ) as ws:
            ready = ws.receive_json()
            self.assertEqual(ready["kind"], "ws.ready")

            publish = self.client.post(
                "/v1/events/publish",
                json=_event_envelope("device.lease.acquired"),
                headers={"Authorization": f"Bearer {publish_token}"},
            )
            self.assertEqual(publish.status_code, 200)

            event = None
            for _ in range(5):
                msg = ws.receive_json()
                if msg.get("event", {}).get("event_type") == "device.lease.acquired":
                    event = msg
                    break
                if msg.get("kind") == "ws.pong":
                    continue
                ws.send_text("ping")

            self.assertIsNotNone(event)
            assert event is not None
            self.assertIn("bus_seq", event)
            self.assertEqual(event["event"]["tenant_id"], "t1")


if __name__ == "__main__":
    unittest.main()
