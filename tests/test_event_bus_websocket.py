from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from starlette.websockets import WebSocketDisconnect

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
        "app_id": "covernow",
        "session_key": "tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
        "trace_id": "trace-events-1",
        "correlation_id": "corr-events-1",
        "ts": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }


@unittest.skipUnless(FASTAPI_STACK_AVAILABLE, "fastapi stack not installed")
class EventBusWebsocketTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_audit_events()
        os.environ.pop("RUNTIME_GATEWAY_EVENT_LOG_PATH", None)
        gateway_app_module._event_bus.clear()
        self.client = TestClient(gateway_app_module.app)

    def _token(
        self,
        *,
        audience: str = "runtime-gateway",
        scope: list[str],
        tenant_id: str | None = "t1",
        app_id: str | None = "covernow",
        token_use: str | None = "service",
    ) -> str:
        payload = {
            "iss": "runtime-gateway",
            "sub": "svc:test",
            "aud": audience,
            "scope": scope,
            "trace_id": "trace-events-1",
        }
        if token_use is not None:
            payload["token_use"] = token_use
        if tenant_id is not None:
            payload["tenant_id"] = tenant_id
        if app_id is not None:
            payload["app_id"] = app_id
        return issue_token(payload, ttl_seconds=300)

    def test_publish_event_requires_bearer_token(self) -> None:
        response = self.client.post("/v1/events/publish", json=_event_envelope())
        self.assertEqual(response.status_code, 401)

    def test_publish_event_rejects_cross_tenant_payload(self) -> None:
        token = self._token(scope=["runs:write"])
        envelope = _event_envelope("runtime.route.decided")
        envelope["tenant_id"] = "t2"
        response = self.client.post(
            "/v1/events/publish",
            json=envelope,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 403)
        self.assertIn("tenant_id must match token claim", response.json()["detail"])

    def test_publish_event_rejects_missing_scope_claims(self) -> None:
        token = self._token(scope=["runs:write"], app_id=None)
        response = self.client.post(
            "/v1/events/publish",
            json=_event_envelope("runtime.route.decided"),
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 401)
        self.assertIn("missing app_id", response.json()["detail"])

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
        payload = recent.json()
        items = payload["items"]
        self.assertGreaterEqual(len(items), 1)
        self.assertEqual(items[-1]["event"]["event_type"], "runtime.route.decided")
        self.assertFalse(payload["has_more"])
        self.assertEqual(payload["recommended_poll_after_ms"], 1500)

    def test_recent_events_requires_bearer_token(self) -> None:
        response = self.client.get("/v1/events/recent")
        self.assertEqual(response.status_code, 401)

    def test_recent_events_reject_cross_tenant_override(self) -> None:
        token = self._token(scope=["runs:read"])
        response = self.client.get(
            "/v1/events/recent?tenant_id=t2",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 403)
        self.assertIn("tenant_id query must match token claim", response.json()["detail"])

    def test_recent_events_reject_missing_scope_claims(self) -> None:
        token = self._token(scope=["runs:read"], tenant_id=None)
        response = self.client.get(
            "/v1/events/recent",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 401)
        self.assertIn("missing tenant_id", response.json()["detail"])

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

    def test_recent_events_can_filter_by_since_ts(self) -> None:
        token = self._token(scope=["runs:write"])
        old_event = _event_envelope("runtime.run.started", run_id="run-since-1")
        old_event["ts"] = "2026-03-12T00:00:00Z"
        new_event = _event_envelope("runtime.run.completed", run_id="run-since-1")
        new_event["ts"] = "2026-03-12T00:10:00Z"
        self.client.post(
            "/v1/events/publish",
            json=old_event,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.client.post(
            "/v1/events/publish",
            json=new_event,
            headers={"Authorization": f"Bearer {token}"},
        )

        filtered = self.client.get(
            "/v1/events/recent?run_id=run-since-1&since_ts=2026-03-12T00:05:00Z",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(filtered.status_code, 200)
        items = filtered.json()["items"]
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["event"]["event_type"], "runtime.run.completed")
        self.assertEqual(items[0]["event"]["payload"]["run_id"], "run-since-1")

    def test_recent_events_reject_invalid_since_ts(self) -> None:
        token = self._token(scope=["runs:read"])
        response = self.client.get(
            "/v1/events/recent?since_ts=not-a-time",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 422)
        self.assertIn("since_ts must be valid ISO-8601 date-time", response.json()["detail"])

    def test_recent_events_accepts_naive_since_ts_as_utc(self) -> None:
        token = self._token(scope=["runs:write"])
        first_event = _event_envelope("runtime.run.status", run_id="run-since-naive")
        first_event["ts"] = "2026-03-12T00:00:00Z"
        second_event = _event_envelope("runtime.run.completed", run_id="run-since-naive")
        second_event["ts"] = "2026-03-12T00:08:00Z"
        self.client.post(
            "/v1/events/publish",
            json=first_event,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.client.post(
            "/v1/events/publish",
            json=second_event,
            headers={"Authorization": f"Bearer {token}"},
        )

        filtered = self.client.get(
            "/v1/events/recent?run_id=run-since-naive&since_ts=2026-03-12T00:05:00",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(filtered.status_code, 200)
        items = filtered.json()["items"]
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["event"]["event_type"], "runtime.run.completed")

    def test_recent_events_sets_has_more_for_recent_window(self) -> None:
        token = self._token(scope=["runs:write"])
        self.client.post(
            "/v1/events/publish",
            json=_event_envelope("runtime.run.started", run_id="run-limit-1"),
            headers={"Authorization": f"Bearer {token}"},
        )
        self.client.post(
            "/v1/events/publish",
            json=_event_envelope("runtime.run.status", run_id="run-limit-1"),
            headers={"Authorization": f"Bearer {token}"},
        )
        self.client.post(
            "/v1/events/publish",
            json=_event_envelope("runtime.run.completed", run_id="run-limit-1"),
            headers={"Authorization": f"Bearer {token}"},
        )

        response = self.client.get(
            "/v1/events/recent?limit=2&run_id=run-limit-1",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload["items"]), 2)
        self.assertTrue(payload["has_more"])
        self.assertEqual(payload["recommended_poll_after_ms"], 250)
        self.assertEqual(payload["items"][0]["event"]["event_type"], "runtime.run.status")
        self.assertEqual(payload["items"][1]["event"]["event_type"], "runtime.run.completed")

    def test_recent_events_support_cursor_incremental_pull(self) -> None:
        token = self._token(scope=["runs:write"])
        self.client.post(
            "/v1/events/publish",
            json=_event_envelope("runtime.run.started", run_id="run-cursor-1"),
            headers={"Authorization": f"Bearer {token}"},
        )
        self.client.post(
            "/v1/events/publish",
            json=_event_envelope("runtime.run.status", run_id="run-cursor-1"),
            headers={"Authorization": f"Bearer {token}"},
        )
        self.client.post(
            "/v1/events/publish",
            json=_event_envelope("runtime.run.completed", run_id="run-cursor-1"),
            headers={"Authorization": f"Bearer {token}"},
        )

        first = self.client.get(
            "/v1/events/recent?cursor=0&limit=2&run_id=run-cursor-1",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(first.status_code, 200)
        first_payload = first.json()
        self.assertEqual(len(first_payload["items"]), 2)
        self.assertEqual(first_payload["items"][0]["event"]["event_type"], "runtime.run.started")
        self.assertEqual(first_payload["items"][1]["event"]["event_type"], "runtime.run.status")
        self.assertTrue(first_payload["has_more"])
        self.assertEqual(first_payload["recommended_poll_after_ms"], 250)
        cursor = first_payload["next_cursor"]
        self.assertGreaterEqual(int(cursor), 1)

        second = self.client.get(
            f"/v1/events/recent?cursor={cursor}&limit=2&run_id=run-cursor-1",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(second.status_code, 200)
        second_payload = second.json()
        self.assertEqual(len(second_payload["items"]), 1)
        self.assertEqual(second_payload["items"][0]["event"]["event_type"], "runtime.run.completed")
        self.assertGreaterEqual(int(second_payload["next_cursor"]), int(cursor))
        self.assertFalse(second_payload["has_more"])
        self.assertEqual(second_payload["recommended_poll_after_ms"], 1500)

    def test_recent_events_recommend_slow_poll_when_window_empty(self) -> None:
        token = self._token(scope=["runs:read"])
        response = self.client.get(
            "/v1/events/recent?run_id=run-empty",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["items"], [])
        self.assertFalse(payload["has_more"])
        self.assertEqual(payload["recommended_poll_after_ms"], 5000)

    def test_recent_events_can_read_durable_source(self) -> None:
        token = self._token(scope=["runs:write"])
        with tempfile.TemporaryDirectory() as tmp:
            os.environ["RUNTIME_GATEWAY_EVENT_LOG_PATH"] = os.path.join(
                tmp, "events", "runtime-events.ndjson"
            )
            self.client.post(
                "/v1/events/publish",
                json=_event_envelope("runtime.run.started", run_id="run-durable-1"),
                headers={"Authorization": f"Bearer {token}"},
            )
            self.client.post(
                "/v1/events/publish",
                json=_event_envelope("runtime.run.completed", run_id="run-durable-1"),
                headers={"Authorization": f"Bearer {token}"},
            )

            durable = self.client.get(
                "/v1/events/recent?source=durable&run_id=run-durable-1",
                headers={"Authorization": f"Bearer {token}"},
            )
            self.assertEqual(durable.status_code, 200)
            payload = durable.json()
            self.assertEqual(payload["source"], "durable")
            self.assertEqual(len(payload["items"]), 2)
            self.assertEqual(payload["items"][0]["event"]["event_type"], "runtime.run.started")
            self.assertEqual(payload["items"][1]["event"]["event_type"], "runtime.run.completed")
            self.assertEqual(payload["recommended_poll_after_ms"], 1500)
            self.assertGreaterEqual(int(payload["stats"]["buffered_events"]), 2)
            self.assertGreaterEqual(int(payload["stats"]["next_seq"]), 3)

    def test_recent_events_durable_source_supports_cursor(self) -> None:
        token = self._token(scope=["runs:write"])
        with tempfile.TemporaryDirectory() as tmp:
            os.environ["RUNTIME_GATEWAY_EVENT_LOG_PATH"] = os.path.join(
                tmp, "events", "runtime-events.ndjson"
            )
            self.client.post(
                "/v1/events/publish",
                json=_event_envelope("runtime.run.started", run_id="run-durable-cursor"),
                headers={"Authorization": f"Bearer {token}"},
            )
            self.client.post(
                "/v1/events/publish",
                json=_event_envelope("runtime.run.status", run_id="run-durable-cursor"),
                headers={"Authorization": f"Bearer {token}"},
            )
            self.client.post(
                "/v1/events/publish",
                json=_event_envelope("runtime.run.completed", run_id="run-durable-cursor"),
                headers={"Authorization": f"Bearer {token}"},
            )

            first = self.client.get(
                "/v1/events/recent?source=durable&cursor=0&limit=2&run_id=run-durable-cursor",
                headers={"Authorization": f"Bearer {token}"},
            )
            self.assertEqual(first.status_code, 200)
            first_payload = first.json()
            self.assertEqual(len(first_payload["items"]), 2)
            self.assertTrue(first_payload["has_more"])
            self.assertEqual(first_payload["recommended_poll_after_ms"], 250)
            cursor = int(first_payload["next_cursor"])
            self.assertGreaterEqual(cursor, 1)

            second = self.client.get(
                f"/v1/events/recent?source=durable&cursor={cursor}&limit=2&run_id=run-durable-cursor",
                headers={"Authorization": f"Bearer {token}"},
            )
            self.assertEqual(second.status_code, 200)
            second_payload = second.json()
            self.assertEqual(len(second_payload["items"]), 1)
            self.assertEqual(
                second_payload["items"][0]["event"]["event_type"], "runtime.run.completed"
            )
            self.assertFalse(second_payload["has_more"])
            self.assertEqual(second_payload["recommended_poll_after_ms"], 1500)

    def test_recent_events_durable_cursor_survives_in_memory_bus_reset(self) -> None:
        token = self._token(scope=["runs:write"])
        with tempfile.TemporaryDirectory() as tmp:
            os.environ["RUNTIME_GATEWAY_EVENT_LOG_PATH"] = os.path.join(
                tmp, "events", "runtime-events.ndjson"
            )
            self.client.post(
                "/v1/events/publish",
                json=_event_envelope("runtime.run.started", run_id="run-durable-reset"),
                headers={"Authorization": f"Bearer {token}"},
            )
            self.client.post(
                "/v1/events/publish",
                json=_event_envelope("runtime.run.status", run_id="run-durable-reset"),
                headers={"Authorization": f"Bearer {token}"},
            )

            first = self.client.get(
                "/v1/events/recent?source=durable&cursor=0&limit=2&run_id=run-durable-reset",
                headers={"Authorization": f"Bearer {token}"},
            )
            self.assertEqual(first.status_code, 200)
            first_payload = first.json()
            self.assertEqual(len(first_payload["items"]), 2)
            cursor = int(first_payload["next_cursor"])
            self.assertGreaterEqual(cursor, 2)

            gateway_app_module._event_bus.clear()
            self.client.post(
                "/v1/events/publish",
                json=_event_envelope("runtime.run.completed", run_id="run-durable-reset"),
                headers={"Authorization": f"Bearer {token}"},
            )

            second = self.client.get(
                f"/v1/events/recent?source=durable&cursor={cursor}&limit=2&run_id=run-durable-reset",
                headers={"Authorization": f"Bearer {token}"},
            )
            self.assertEqual(second.status_code, 200)
            second_payload = second.json()
            self.assertEqual(len(second_payload["items"]), 1)
            self.assertEqual(
                second_payload["items"][0]["event"]["event_type"], "runtime.run.completed"
            )

    def test_recent_events_rejects_invalid_source(self) -> None:
        token = self._token(scope=["runs:read"])
        response = self.client.get(
            "/v1/events/recent?source=unknown",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 422)
        self.assertIn("source must be one of: memory, durable", response.json()["detail"])

    def test_websocket_receives_published_event(self) -> None:
        ws_token = self._token(scope=["runs:read"])
        publish_token = self._token(scope=["runs:write"])

        with self.client.websocket_connect(
            f"/v1/ws/events?access_token={ws_token}&tenant_id=t1&app_id=covernow"
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

    def test_websocket_rejects_cross_tenant_override(self) -> None:
        ws_token = self._token(scope=["runs:read"])
        with self.assertRaises(WebSocketDisconnect):
            with self.client.websocket_connect(
                f"/v1/ws/events?access_token={ws_token}&tenant_id=t2&app_id=covernow"
            ):
                pass

    def test_websocket_rejects_missing_scope_claims(self) -> None:
        ws_token = self._token(scope=["runs:read"], tenant_id=None)
        with self.assertRaises(WebSocketDisconnect):
            with self.client.websocket_connect(
                f"/v1/ws/events?access_token={ws_token}&app_id=covernow"
            ):
                pass

    def test_websocket_rejects_missing_token_use_claim(self) -> None:
        ws_token = self._token(scope=["runs:read"], token_use=None)
        with self.assertRaises(WebSocketDisconnect):
            with self.client.websocket_connect(
                f"/v1/ws/events?access_token={ws_token}&tenant_id=t1&app_id=covernow"
            ):
                pass

    def test_websocket_rejects_invalid_source(self) -> None:
        ws_token = self._token(scope=["runs:read"])
        with self.assertRaises(WebSocketDisconnect):
            with self.client.websocket_connect(
                f"/v1/ws/events?access_token={ws_token}&tenant_id=t1&app_id=covernow&source=unknown"
            ):
                pass

    def test_websocket_rejects_invalid_since_ts(self) -> None:
        ws_token = self._token(scope=["runs:read"])
        with self.assertRaises(WebSocketDisconnect):
            with self.client.websocket_connect(
                f"/v1/ws/events?access_token={ws_token}&tenant_id=t1&app_id=covernow&since_ts=not-a-time"
            ):
                pass

    def test_websocket_can_filter_by_run_id(self) -> None:
        ws_token = self._token(scope=["runs:read"])
        publish_token = self._token(scope=["runs:write"])

        with self.client.websocket_connect(
            f"/v1/ws/events?access_token={ws_token}&tenant_id=t1&app_id=covernow&run_id=run-123"
        ) as ws:
            ready = ws.receive_json()
            self.assertEqual(ready["kind"], "ws.ready")
            self.assertEqual(ready["run_id"], "run-123")

            first = self.client.post(
                "/v1/events/publish",
                json=_event_envelope("runtime.run.status", run_id="run-999"),
                headers={"Authorization": f"Bearer {publish_token}"},
            )
            self.assertEqual(first.status_code, 200)

            second = self.client.post(
                "/v1/events/publish",
                json=_event_envelope("runtime.run.status", run_id="run-123"),
                headers={"Authorization": f"Bearer {publish_token}"},
            )
            self.assertEqual(second.status_code, 200)

            event = ws.receive_json()
            self.assertEqual(event["event"]["event_type"], "runtime.run.status")
            self.assertEqual(event["event"]["payload"]["run_id"], "run-123")

    def test_websocket_can_replay_durable_source_after_memory_reset(self) -> None:
        ws_token = self._token(scope=["runs:read"])
        publish_token = self._token(scope=["runs:write"])
        with tempfile.TemporaryDirectory() as tmp:
            os.environ["RUNTIME_GATEWAY_EVENT_LOG_PATH"] = os.path.join(
                tmp, "events", "runtime-events.ndjson"
            )
            self.client.post(
                "/v1/events/publish",
                json=_event_envelope("runtime.run.started", run_id="run-ws-durable"),
                headers={"Authorization": f"Bearer {publish_token}"},
            )
            self.client.post(
                "/v1/events/publish",
                json=_event_envelope("runtime.run.completed", run_id="run-ws-durable"),
                headers={"Authorization": f"Bearer {publish_token}"},
            )
            gateway_app_module._event_bus.clear()

            with self.client.websocket_connect(
                f"/v1/ws/events?access_token={ws_token}&tenant_id=t1&app_id=covernow&source=durable&run_id=run-ws-durable&cursor=0"
            ) as ws:
                ready = ws.receive_json()
                self.assertEqual(ready["kind"], "ws.ready")
                self.assertEqual(ready["source"], "durable")

                first = ws.receive_json()
                second = ws.receive_json()
                self.assertEqual(first["event"]["event_type"], "runtime.run.started")
                self.assertEqual(second["event"]["event_type"], "runtime.run.completed")

    def test_websocket_memory_source_applies_since_ts_filter(self) -> None:
        ws_token = self._token(scope=["runs:read"])
        publish_token = self._token(scope=["runs:write"])
        old_event = _event_envelope("runtime.run.started", run_id="run-ws-since-memory")
        old_event["ts"] = "2026-03-12T00:00:00Z"
        new_event = _event_envelope("runtime.run.completed", run_id="run-ws-since-memory")
        new_event["ts"] = "2026-03-12T00:10:00Z"
        self.client.post(
            "/v1/events/publish",
            json=old_event,
            headers={"Authorization": f"Bearer {publish_token}"},
        )
        self.client.post(
            "/v1/events/publish",
            json=new_event,
            headers={"Authorization": f"Bearer {publish_token}"},
        )

        with self.client.websocket_connect(
            f"/v1/ws/events?access_token={ws_token}&tenant_id=t1&app_id=covernow&run_id=run-ws-since-memory&since_ts=2026-03-12T00:05:00Z"
        ) as ws:
            ready = ws.receive_json()
            self.assertEqual(ready["kind"], "ws.ready")
            self.assertIn("since_ts", ready)
            event = ws.receive_json()
            self.assertEqual(event["event"]["event_type"], "runtime.run.completed")
            self.assertEqual(event["event"]["payload"]["run_id"], "run-ws-since-memory")

    def test_websocket_durable_source_applies_since_ts_filter(self) -> None:
        ws_token = self._token(scope=["runs:read"])
        publish_token = self._token(scope=["runs:write"])
        with tempfile.TemporaryDirectory() as tmp:
            os.environ["RUNTIME_GATEWAY_EVENT_LOG_PATH"] = os.path.join(
                tmp, "events", "runtime-events.ndjson"
            )
            old_event = _event_envelope("runtime.run.started", run_id="run-ws-since-durable")
            old_event["ts"] = "2026-03-12T00:00:00Z"
            new_event = _event_envelope("runtime.run.completed", run_id="run-ws-since-durable")
            new_event["ts"] = "2026-03-12T00:10:00Z"
            self.client.post(
                "/v1/events/publish",
                json=old_event,
                headers={"Authorization": f"Bearer {publish_token}"},
            )
            self.client.post(
                "/v1/events/publish",
                json=new_event,
                headers={"Authorization": f"Bearer {publish_token}"},
            )
            gateway_app_module._event_bus.clear()

            with self.client.websocket_connect(
                f"/v1/ws/events?access_token={ws_token}&tenant_id=t1&app_id=covernow&source=durable&run_id=run-ws-since-durable&since_ts=2026-03-12T00:05:00Z&cursor=0"
            ) as ws:
                ready = ws.receive_json()
                self.assertEqual(ready["kind"], "ws.ready")
                self.assertEqual(ready["source"], "durable")
                self.assertIn("since_ts", ready)
                event = ws.receive_json()
                self.assertEqual(event["event"]["event_type"], "runtime.run.completed")
                self.assertEqual(event["event"]["payload"]["run_id"], "run-ws-since-durable")


if __name__ == "__main__":
    unittest.main()
