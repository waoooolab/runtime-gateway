from __future__ import annotations

import io
import json
import unittest
import urllib.error

from runtime_gateway.integration.runtime_execution import (
    RuntimeExecutionClient,
    RuntimeExecutionClientError,
)

class RuntimeExecutionClientTests(unittest.TestCase):
    def test_submit_command_http_error_preserves_status_and_response_body(self) -> None:
        def transport(request, timeout=10.0):
            _ = timeout
            payload = {
                "event_id": "evt-1",
                "event_type": "runtime.route.failed",
                "tenant_id": "t1",
                "app_id": "covernow",
                "session_key": "tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
                "trace_id": "trace-1",
                "correlation_id": "corr-1",
                "ts": "2026-03-02T10:00:00+00:00",
                "payload": {"run_id": "run-1"},
            }
            raise urllib.error.HTTPError(
                request.full_url,
                409,
                "conflict",
                hdrs=None,
                fp=io.BytesIO(json.dumps(payload).encode("utf-8")),
            )

        client = RuntimeExecutionClient(
            base_url="http://runtime-execution.test",
            _transport=transport,
        )
        with self.assertRaises(RuntimeExecutionClientError) as ctx:
            client.submit_command(envelope={"x": 1}, auth_token="token-1")

        exc = ctx.exception
        self.assertEqual(exc.status_code, 409)
        self.assertIsInstance(exc.response_body, dict)
        assert exc.response_body is not None
        self.assertEqual(exc.response_body.get("event_type"), "runtime.route.failed")

    def test_submit_command_http_error_non_json_body_sets_empty_response_body(self) -> None:
        def transport(request, timeout=10.0):
            _ = timeout
            raise urllib.error.HTTPError(
                request.full_url,
                502,
                "bad gateway",
                hdrs=None,
                fp=io.BytesIO(b"upstream unavailable"),
            )

        client = RuntimeExecutionClient(
            base_url="http://runtime-execution.test",
            _transport=transport,
        )
        with self.assertRaises(RuntimeExecutionClientError) as ctx:
            client.submit_command(envelope={"x": 1}, auth_token="token-1")

        exc = ctx.exception
        self.assertEqual(exc.status_code, 502)
        self.assertIsNone(exc.response_body)

    def test_worker_tick_sends_query_parameters(self) -> None:
        captured: dict[str, str] = {}

        class _Response:
            def getcode(self) -> int:
                return 200

            def read(self) -> bytes:
                return b'{"outcome":"idle","leased_run_id":null}'

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                _ = (exc_type, exc, tb)
                return None

        def transport(request, timeout=10.0):
            _ = timeout
            captured["url"] = request.full_url
            return _Response()

        client = RuntimeExecutionClient(
            base_url="http://runtime-execution.test",
            _transport=transport,
        )
        payload = client.worker_tick(
            auth_token="token-1",
            fair=False,
            auto_start=False,
        )

        self.assertEqual(payload["outcome"], "idle")
        self.assertIn("/v1/orchestration/worker:tick?", captured["url"])
        self.assertIn("fair=false", captured["url"])
        self.assertIn("auto_start=false", captured["url"])

    def test_worker_drain_http_error_preserves_status_and_response_body(self) -> None:
        def transport(request, timeout=10.0):
            _ = timeout
            payload = {
                "detail": "max_items must be integer > 0",
            }
            raise urllib.error.HTTPError(
                request.full_url,
                422,
                "unprocessable",
                hdrs=None,
                fp=io.BytesIO(json.dumps(payload).encode("utf-8")),
            )

        client = RuntimeExecutionClient(
            base_url="http://runtime-execution.test",
            _transport=transport,
        )
        with self.assertRaises(RuntimeExecutionClientError) as ctx:
            client.worker_drain(
                auth_token="token-1",
                max_items=0,
            )

        exc = ctx.exception
        self.assertEqual(exc.status_code, 422)
        self.assertIsInstance(exc.response_body, dict)
        assert exc.response_body is not None
        self.assertIn("max_items", str(exc.response_body.get("detail", "")))

    def test_worker_health_uses_get_method(self) -> None:
        captured: dict[str, str] = {}

        class _Response:
            def getcode(self) -> int:
                return 200

            def read(self) -> bytes:
                return b'{"queue_depth":0,"ticks_total":1,"idle_ticks_total":0,"progressed_ticks_total":1,"drain_calls_total":0,"last_tick_at":"2026-03-12T03:00:00+00:00","last_drain_at":null,"last_tick_outcome":"progressed"}'

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                _ = (exc_type, exc, tb)
                return None

        def transport(request, timeout=10.0):
            _ = timeout
            captured["url"] = request.full_url
            captured["method"] = request.get_method()
            return _Response()

        client = RuntimeExecutionClient(
            base_url="http://runtime-execution.test",
            _transport=transport,
        )
        payload = client.worker_health(auth_token="token-1")

        self.assertEqual(payload["ticks_total"], 1)
        self.assertIn("/v1/orchestration/worker:health", captured["url"])
        self.assertEqual(captured["method"], "GET")

    def test_worker_start_posts_reason_payload(self) -> None:
        captured: dict[str, str] = {}

        class _Response:
            def getcode(self) -> int:
                return 200

            def read(self) -> bytes:
                return b'{"action":"start","lifecycle_state":"running","changed":true}'

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                _ = (exc_type, exc, tb)
                return None

        def transport(request, timeout=10.0):
            _ = timeout
            captured["url"] = request.full_url
            captured["method"] = request.get_method()
            captured["body"] = request.data.decode("utf-8")
            return _Response()

        client = RuntimeExecutionClient(
            base_url="http://runtime-execution.test",
            _transport=transport,
        )
        payload = client.worker_start(auth_token="token-1", reason="resume")

        self.assertEqual(payload["action"], "start")
        self.assertIn("/v1/orchestration/worker:start", captured["url"])
        self.assertEqual(captured["method"], "POST")
        self.assertIn('"reason":"resume"', captured["body"])

    def test_worker_stop_and_restart_post_reason_payload(self) -> None:
        captured: dict[str, str] = {}

        class _Response:
            def __init__(self, body: bytes):
                self._body = body

            def getcode(self) -> int:
                return 200

            def read(self) -> bytes:
                return self._body

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                _ = (exc_type, exc, tb)
                return None

        call_count = {"n": 0}

        def transport(request, timeout=10.0):
            _ = timeout
            call_count["n"] += 1
            if call_count["n"] == 1:
                captured["stop_url"] = request.full_url
                captured["stop_body"] = request.data.decode("utf-8")
                return _Response(b'{"action":"stop","lifecycle_state":"stopped","changed":true}')
            captured["restart_url"] = request.full_url
            captured["restart_body"] = request.data.decode("utf-8")
            return _Response(b'{"action":"restart","lifecycle_state":"running","changed":true}')

        client = RuntimeExecutionClient(
            base_url="http://runtime-execution.test",
            _transport=transport,
        )
        stop_payload = client.worker_stop(auth_token="token-1", reason="maintenance")
        restart_payload = client.worker_restart(auth_token="token-1", reason="refresh")

        self.assertEqual(stop_payload["action"], "stop")
        self.assertEqual(restart_payload["action"], "restart")
        self.assertIn("/v1/orchestration/worker:stop", captured["stop_url"])
        self.assertIn("/v1/orchestration/worker:restart", captured["restart_url"])
        self.assertIn('"reason":"maintenance"', captured["stop_body"])
        self.assertIn('"reason":"refresh"', captured["restart_body"])

    def test_worker_status_uses_get_method(self) -> None:
        captured: dict[str, str] = {}

        class _Response:
            def getcode(self) -> int:
                return 200

            def read(self) -> bytes:
                return b'{"lifecycle_state":"running","is_running":true,"restart_total":0}'

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                _ = (exc_type, exc, tb)
                return None

        def transport(request, timeout=10.0):
            _ = timeout
            captured["url"] = request.full_url
            captured["method"] = request.get_method()
            return _Response()

        client = RuntimeExecutionClient(
            base_url="http://runtime-execution.test",
            _transport=transport,
        )
        payload = client.worker_status(auth_token="token-1")

        self.assertEqual(payload["lifecycle_state"], "running")
        self.assertIn("/v1/orchestration/worker:status", captured["url"])
        self.assertEqual(captured["method"], "GET")

    def test_scheduler_enqueue_posts_payload(self) -> None:
        captured: dict[str, str] = {}

        class _Response:
            def getcode(self) -> int:
                return 200

            def read(self) -> bytes:
                return b'{"run_id":"run-1","scheduler_depth":1}'

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                _ = (exc_type, exc, tb)
                return None

        def transport(request, timeout=10.0):
            _ = timeout
            captured["url"] = request.full_url
            captured["method"] = request.get_method()
            captured["body"] = request.data.decode("utf-8")
            return _Response()

        client = RuntimeExecutionClient(base_url="http://runtime-execution.test", _transport=transport)
        payload = client.scheduler_enqueue(
            auth_token="token-1",
            run_id="run-1",
            delay_ms=100,
            reason="manual",
        )
        self.assertEqual(payload["run_id"], "run-1")
        self.assertIn("/v1/orchestration/scheduler:enqueue", captured["url"])
        self.assertEqual(captured["method"], "POST")
        self.assertIn('"run_id":"run-1"', captured["body"])
        self.assertIn('"delay_ms":100', captured["body"])
        self.assertIn('"reason":"manual"', captured["body"])

    def test_scheduler_enqueue_posts_misfire_and_cron_settings_when_provided(self) -> None:
        captured: dict[str, str] = {}

        class _Response:
            def getcode(self) -> int:
                return 200

            def read(self) -> bytes:
                return b'{"run_id":"run-misfire-1","scheduler_depth":1}'

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                _ = (exc_type, exc, tb)
                return None

        def transport(request, timeout=10.0):
            _ = timeout
            captured["url"] = request.full_url
            captured["method"] = request.get_method()
            captured["body"] = request.data.decode("utf-8")
            return _Response()

        client = RuntimeExecutionClient(base_url="http://runtime-execution.test", _transport=transport)
        payload = client.scheduler_enqueue(
            auth_token="token-1",
            run_id="run-misfire-1",
            due_at="2026-03-13T00:00:00+00:00",
            misfire_policy="skip",
            misfire_grace_ms=250,
            cron_interval_ms=1000,
        )
        self.assertEqual(payload["run_id"], "run-misfire-1")
        self.assertIn("/v1/orchestration/scheduler:enqueue", captured["url"])
        self.assertEqual(captured["method"], "POST")
        self.assertIn('"run_id":"run-misfire-1"', captured["body"])
        self.assertIn('"due_at":"2026-03-13T00:00:00+00:00"', captured["body"])
        self.assertIn('"misfire_policy":"skip"', captured["body"])
        self.assertIn('"misfire_grace_ms":250', captured["body"])
        self.assertIn('"cron_interval_ms":1000', captured["body"])

    def test_scheduler_tick_sends_query_parameters(self) -> None:
        captured: dict[str, str] = {}

        class _Response:
            def getcode(self) -> int:
                return 200

            def read(self) -> bytes:
                return b'{"processed":1,"promoted":1,"deferred":0}'

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                _ = (exc_type, exc, tb)
                return None

        def transport(request, timeout=10.0):
            _ = timeout
            captured["url"] = request.full_url
            return _Response()

        client = RuntimeExecutionClient(base_url="http://runtime-execution.test", _transport=transport)
        payload = client.scheduler_tick(auth_token="token-1", max_items=7, fair=False)
        self.assertEqual(payload["promoted"], 1)
        self.assertIn("/v1/orchestration/scheduler:tick?", captured["url"])
        self.assertIn("max_items=7", captured["url"])
        self.assertIn("fair=false", captured["url"])

    def test_scheduler_health_uses_get_method(self) -> None:
        captured: dict[str, str] = {}

        class _Response:
            def getcode(self) -> int:
                return 200

            def read(self) -> bytes:
                return b'{"scheduler_depth":0,"orchestration_depth":2,"ticks_total":3}'

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                _ = (exc_type, exc, tb)
                return None

        def transport(request, timeout=10.0):
            _ = timeout
            captured["url"] = request.full_url
            captured["method"] = request.get_method()
            return _Response()

        client = RuntimeExecutionClient(base_url="http://runtime-execution.test", _transport=transport)
        payload = client.scheduler_health(auth_token="token-1")
        self.assertEqual(payload["ticks_total"], 3)
        self.assertIn("/v1/orchestration/scheduler:health", captured["url"])
        self.assertEqual(captured["method"], "GET")

    def test_get_run_status_uses_get_method(self) -> None:
        captured: dict[str, str] = {}

        class _Response:
            def getcode(self) -> int:
                return 200

            def read(self) -> bytes:
                return b'{"event_id":"evt-run-status-1","event_type":"runtime.run.status","tenant_id":"t1","app_id":"covernow","session_key":"tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm","trace_id":"trace-run-status-1","correlation_id":"run-status-1","ts":"2026-03-12T06:00:00+00:00","payload":{"run_id":"run-status-1","status":"queued","retry_attempts":0}}'

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                _ = (exc_type, exc, tb)
                return None

        def transport(request, timeout=10.0):
            _ = timeout
            captured["url"] = request.full_url
            captured["method"] = request.get_method()
            return _Response()

        client = RuntimeExecutionClient(
            base_url="http://runtime-execution.test",
            _transport=transport,
        )
        payload = client.get_run_status(
            run_id="run-status-1",
            auth_token="token-1",
        )

        self.assertEqual(payload["event_type"], "runtime.run.status")
        self.assertIn("/v1/runs/run-status-1", captured["url"])
        self.assertEqual(captured["method"], "GET")

    def test_get_run_lease_uses_get_method(self) -> None:
        captured: dict[str, str] = {}

        class _Response:
            def getcode(self) -> int:
                return 200

            def read(self) -> bytes:
                return b'{"run_id":"run-lease-1","lease":{"lease_id":"lease-1","state":"active"},"device_hub":{"status":"ok"}}'

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                _ = (exc_type, exc, tb)
                return None

        def transport(request, timeout=10.0):
            _ = timeout
            captured["url"] = request.full_url
            captured["method"] = request.get_method()
            return _Response()

        client = RuntimeExecutionClient(
            base_url="http://runtime-execution.test",
            _transport=transport,
        )
        payload = client.get_run_lease(
            run_id="run-lease-1",
            auth_token="token-1",
        )

        self.assertEqual(payload["run_id"], "run-lease-1")
        self.assertIn("/v1/runs/run-lease-1/lease", captured["url"])
        self.assertEqual(captured["method"], "GET")

    def test_cancel_run_sends_payload_fields(self) -> None:
        captured: dict[str, str] = {}

        class _Response:
            def getcode(self) -> int:
                return 200

            def read(self) -> bytes:
                return b'{"event_type":"runtime.run.status","payload":{"status":"canceled"}}'

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                _ = (exc_type, exc, tb)
                return None

        def transport(request, timeout=10.0):
            _ = timeout
            captured["url"] = request.full_url
            captured["body"] = request.data.decode("utf-8")
            return _Response()

        client = RuntimeExecutionClient(
            base_url="http://runtime-execution.test",
            _transport=transport,
        )
        payload = client.cancel_run(
            run_id="run-cancel-3",
            auth_token="token-1",
            reason="manual_stop",
            cascade_children=False,
            canceled_by_run_id="run-controller",
        )
        self.assertEqual(payload["event_type"], "runtime.run.status")
        self.assertIn("/v1/runs/run-cancel-3:cancel", captured["url"])
        self.assertIn('"reason":"manual_stop"', captured["body"])
        self.assertIn('"cascade_children":false', captured["body"])
        self.assertIn('"canceled_by_run_id":"run-controller"', captured["body"])

    def test_timeout_run_sends_payload_fields(self) -> None:
        captured: dict[str, str] = {}

        class _Response:
            def getcode(self) -> int:
                return 200

            def read(self) -> bytes:
                return b'{"event_type":"runtime.run.status","payload":{"status":"timed_out"}}'

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                _ = (exc_type, exc, tb)
                return None

        def transport(request, timeout=10.0):
            _ = timeout
            captured["url"] = request.full_url
            captured["body"] = request.data.decode("utf-8")
            return _Response()

        client = RuntimeExecutionClient(
            base_url="http://runtime-execution.test",
            _transport=transport,
        )
        payload = client.timeout_run(
            run_id="run-timeout-3",
            auth_token="token-1",
            reason="deadline_exceeded",
            cascade_children=True,
            timed_out_by_run_id="run-watchdog",
        )
        self.assertEqual(payload["event_type"], "runtime.run.status")
        self.assertIn("/v1/runs/run-timeout-3:timeout", captured["url"])
        self.assertIn('"reason":"deadline_exceeded"', captured["body"])
        self.assertIn('"cascade_children":true', captured["body"])
        self.assertIn('"timed_out_by_run_id":"run-watchdog"', captured["body"])

    def test_complete_run_sends_failure_reason_when_provided(self) -> None:
        captured: dict[str, str] = {}

        class _Response:
            def getcode(self) -> int:
                return 200

            def read(self) -> bytes:
                return b'{"event_type":"runtime.run.status","payload":{"status":"failed"}}'

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                _ = (exc_type, exc, tb)
                return None

        def transport(request, timeout=10.0):
            _ = timeout
            captured["url"] = request.full_url
            captured["body"] = request.data.decode("utf-8")
            return _Response()

        client = RuntimeExecutionClient(
            base_url="http://runtime-execution.test",
            _transport=transport,
        )
        payload = client.complete_run(
            run_id="run-complete-9",
            auth_token="token-1",
            success=False,
            failure_reason_code="tool_contract_violation",
        )
        self.assertEqual(payload["event_type"], "runtime.run.status")
        self.assertIn("/v1/runs/run-complete-9:complete", captured["url"])
        self.assertIn('"success":false', captured["body"])
        self.assertIn('"failure_reason_code":"tool_contract_violation"', captured["body"])


if __name__ == "__main__":
    unittest.main()
