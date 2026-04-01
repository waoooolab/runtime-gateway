from __future__ import annotations

import io
import json
import os
import unittest
import urllib.error
from urllib.error import URLError
from unittest.mock import patch

from runtime_gateway.integration.runtime_execution import (
    RuntimeExecutionClient,
    RuntimeExecutionClientError,
    RuntimeExecutionClientPool,
)

class RuntimeExecutionClientTests(unittest.TestCase):
    def test_rejects_empty_base_url(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            RuntimeExecutionClient(base_url=" ")
        self.assertIn("must not be empty", str(ctx.exception))

    def test_rejects_base_url_without_host(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            RuntimeExecutionClient(base_url="http:///")
        self.assertIn("must include host", str(ctx.exception))

    def test_rejects_base_url_with_path(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            RuntimeExecutionClient(base_url="http://runtime-execution.test/v1")
        self.assertIn("must not include path", str(ctx.exception))

    def test_rejects_base_url_with_query(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            RuntimeExecutionClient(base_url="http://runtime-execution.test?env=dev")
        self.assertIn("must not include query or fragment", str(ctx.exception))

    def test_rejects_non_https_base_url_when_tls_required(self) -> None:
        with patch.dict(os.environ, {"WAOOOOLAB_REQUIRE_INTERNAL_TLS": "true"}, clear=False):
            with self.assertRaises(ValueError) as ctx:
                RuntimeExecutionClient(base_url="http://runtime-execution.test")
        self.assertIn("must use https", str(ctx.exception))

    def test_allows_https_base_url_when_tls_required(self) -> None:
        with patch.dict(os.environ, {"WAOOOOLAB_REQUIRE_INTERNAL_TLS": "true"}, clear=False):
            client = RuntimeExecutionClient(base_url="https://runtime-execution.test")
        self.assertEqual(client.base_url, "https://runtime-execution.test")

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
                "payload": {
                    "run_id": "run-1",
                    "failure": {
                        "code": "no_eligible_device",
                        "classification": "capacity",
                        "message": "no eligible device",
                    },
                },
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
        self.assertIsNone(exc.detail)
        self.assertIsNotNone(exc.downstream_failure)
        assert exc.downstream_failure is not None
        self.assertEqual(exc.downstream_failure.get("code"), "no_eligible_device")
        self.assertTrue(exc.retryable)

    def test_retryable_normalizes_capacity_classification_code_term(self) -> None:
        exc = RuntimeExecutionClientError(
            "downstream route failed",
            status_code=409,
            response_body={
                "payload": {
                    "failure": {
                        "classification": "Capacity-Exhausted",
                    }
                }
            },
        )
        self.assertTrue(exc.retryable)

    def test_retryable_normalizes_policy_classification_code_term(self) -> None:
        exc = RuntimeExecutionClientError(
            "downstream route failed",
            status_code=503,
            response_body={
                "payload": {
                    "failure": {
                        "classification": "Policy.Rejected",
                    }
                }
            },
        )
        self.assertFalse(exc.retryable)

    def test_retryable_uses_flat_failure_classification_from_detail(self) -> None:
        exc = RuntimeExecutionClientError(
            "downstream route failed",
            status_code=409,
            detail={"failure_classification": "Capacity-Exhausted"},
        )
        self.assertTrue(exc.retryable)
        self.assertEqual(exc.failure_classification, "capacity_exhausted")

    def test_retryable_uses_flat_failure_classification_from_payload(self) -> None:
        exc = RuntimeExecutionClientError(
            "downstream route failed",
            status_code=409,
            response_body={"payload": {"failure_classification": "Policy.Rejected"}},
        )
        self.assertFalse(exc.retryable)
        self.assertEqual(exc.failure_classification, "policy_rejected")

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
        self.assertIsNone(exc.detail)
        self.assertTrue(exc.retryable)

    def test_submit_command_connection_error_is_retryable_with_structured_detail(self) -> None:
        def transport(request, timeout=10.0):
            _ = timeout
            raise URLError("connection refused")

        client = RuntimeExecutionClient(
            base_url="http://runtime-execution.test",
            _transport=transport,
        )
        with self.assertRaises(RuntimeExecutionClientError) as ctx:
            client.submit_command(envelope={"x": 1}, auth_token="token-1")

        exc = ctx.exception
        self.assertEqual(exc.status_code, 503)
        self.assertTrue(exc.retryable)
        self.assertIsNone(exc.response_body)
        self.assertIsInstance(exc.detail, dict)
        assert isinstance(exc.detail, dict)
        self.assertEqual(exc.detail.get("category"), "upstream_unavailable")
        self.assertEqual(exc.detail.get("code"), "upstream_connection_error")

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
        self.assertEqual(exc.detail, "max_items must be integer > 0")
        self.assertFalse(exc.retryable)

    def test_worker_tick_http_error_with_retryable_detail_sets_retryable_flag(self) -> None:
        def transport(request, timeout=10.0):
            _ = timeout
            payload = {
                "detail": {
                    "error": "provider_error",
                    "category": "retryable",
                    "code": "provider_unavailable",
                    "retryable": True,
                    "message": "provider timeout",
                }
            }
            raise urllib.error.HTTPError(
                request.full_url,
                503,
                "service unavailable",
                hdrs=None,
                fp=io.BytesIO(json.dumps(payload).encode("utf-8")),
            )

        client = RuntimeExecutionClient(
            base_url="http://runtime-execution.test",
            _transport=transport,
        )
        with self.assertRaises(RuntimeExecutionClientError) as ctx:
            client.worker_tick(auth_token="token-1")

        exc = ctx.exception
        self.assertEqual(exc.status_code, 503)
        self.assertIsInstance(exc.detail, dict)
        assert isinstance(exc.detail, dict)
        self.assertEqual(exc.detail.get("code"), "provider_unavailable")
        self.assertTrue(exc.retryable)

    def test_worker_loop_sends_query_parameters(self) -> None:
        captured: dict[str, str] = {}

        class _Response:
            def getcode(self) -> int:
                return 200

            def read(self) -> bytes:
                return b'{"scheduler_processed":1,"processed":1}'

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
        payload = client.worker_loop(
            auth_token="token-1",
            scheduler_max_items=4,
            scheduler_fair=False,
            worker_max_items=2,
            worker_fair=False,
            auto_start=False,
        )

        self.assertEqual(payload["scheduler_processed"], 1)
        self.assertEqual(payload["processed"], 1)
        self.assertIn("/v1/orchestration/worker:loop?", captured["url"])
        self.assertIn("scheduler_max_items=4", captured["url"])
        self.assertIn("scheduler_fair=false", captured["url"])
        self.assertIn("worker_max_items=2", captured["url"])
        self.assertIn("worker_fair=false", captured["url"])
        self.assertIn("auto_start=false", captured["url"])

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

    def test_preempt_run_sends_payload_fields(self) -> None:
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
        payload = client.preempt_run(
            run_id="run-preempt-3",
            auth_token="token-1",
            reason="resource_preempted",
            cascade_children=True,
            preempted_by_run_id="run-priority-parent",
        )
        self.assertEqual(payload["event_type"], "runtime.run.status")
        self.assertIn("/v1/runs/run-preempt-3:preempt", captured["url"])
        self.assertIn('"reason":"resource_preempted"', captured["body"])
        self.assertIn('"cascade_children":true', captured["body"])
        self.assertIn('"preempted_by_run_id":"run-priority-parent"', captured["body"])

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

    def test_renew_run_lease_sends_ttl_when_provided(self) -> None:
        captured: dict[str, str] = {}

        class _Response:
            def getcode(self) -> int:
                return 200

            def read(self) -> bytes:
                return b'{"event_type":"runtime.run.status","payload":{"status":"queued"}}'

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
        payload = client.renew_run_lease(
            run_id="run-lease-renew-1",
            auth_token="token-1",
            lease_ttl_seconds=600,
        )
        self.assertEqual(payload["event_type"], "runtime.run.status")
        self.assertIn("/v1/runs/run-lease-renew-1:lease-renew", captured["url"])
        self.assertIn('"lease_ttl_seconds":600', captured["body"])

    def test_register_capability_posts_payload(self) -> None:
        captured: dict[str, str] = {}

        class _Response:
            def getcode(self) -> int:
                return 200

            def read(self) -> bytes:
                return b'{"status":"registered","capability_id":"cap.demo","version":"1.0.0"}'

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
        payload = client.register_capability(
            auth_token="token-1",
            payload={"capability": {"capability_id": "cap.demo"}},
        )
        self.assertEqual(payload["status"], "registered")
        self.assertEqual(captured["method"], "POST")
        self.assertIn("/v1/capabilities/register", captured["url"])
        self.assertIn('"capability_id":"cap.demo"', captured["body"])

    def test_list_tool_catalog_and_get_capability_use_get_method(self) -> None:
        captured: list[dict[str, str]] = []

        class _Response:
            def __init__(self, *, body: bytes) -> None:
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

        def transport(request, timeout=10.0):
            _ = timeout
            captured.append({"url": request.full_url, "method": request.get_method()})
            if request.full_url.endswith("/v1/tools/catalog"):
                return _Response(
                    body=(
                        b'{"schema_version":"tool_catalog.v1","items":[{"tool_id":"cap.demo",'
                        b'"source":{"plane":"runtime","registry":"capability_registry","kind":"app",'
                        b'"capability_id":"cap.demo","capability_version":"1.0.0"},'
                        b'"provenance":{"authority":"runtime-execution","registered_at":"2026-03-24T00:00:00+00:00"},'
                        b'"profile":{"visibility":"private"},'
                        b'"optionality":{"mode":"optional"}}]}'
                    )
                )
            if request.full_url.endswith("/v1/capabilities"):
                return _Response(body=b'{"count":1,"items":[{"capability_id":"cap.demo"}]}')
            return _Response(body=b'{"capability_id":"cap.demo","latest_version":"1.0.0","capability":{"kind":"app"}}')

        client = RuntimeExecutionClient(
            base_url="http://runtime-execution.test",
            _transport=transport,
        )
        catalog = client.list_tool_catalog(auth_token="token-1")
        listed = client.list_capabilities(auth_token="token-1")
        got = client.get_capability(capability_id="cap.demo", auth_token="token-1")

        self.assertEqual(catalog["schema_version"], "tool_catalog.v1")
        self.assertEqual(listed["count"], 1)
        self.assertEqual(got["capability_id"], "cap.demo")
        self.assertEqual(captured[0]["method"], "GET")
        self.assertIn("/v1/tools/catalog", captured[0]["url"])
        self.assertEqual(captured[1]["method"], "GET")
        self.assertIn("/v1/capabilities", captured[1]["url"])
        self.assertEqual(captured[2]["method"], "GET")
        self.assertIn("/v1/capabilities/cap.demo", captured[2]["url"])

    def test_compile_publish_invoke_capability_send_payload_fields(self) -> None:
        captured: list[dict[str, str]] = []

        class _Response:
            def __init__(self, *, body: bytes) -> None:
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

        def transport(request, timeout=10.0):
            _ = timeout
            captured.append(
                {
                    "url": request.full_url,
                    "method": request.get_method(),
                    "body": request.data.decode("utf-8"),
                }
            )
            if request.full_url.endswith("/v1/capabilities/compile"):
                return _Response(body=b'{"event_type":"app.capability.compiled.v1","payload":{"status":"compiled"}}')
            if request.full_url.endswith("/v1/capabilities/publish"):
                return _Response(body=b'{"event_type":"app.capability.published.v1","payload":{"status":"published"}}')
            return _Response(body=b'{"event_type":"app.capability.invoked.v1","payload":{"status":"invoked"}}')

        client = RuntimeExecutionClient(
            base_url="http://runtime-execution.test",
            _transport=transport,
        )
        compiled = client.compile_capability(
            auth_token="token-1",
            payload={"capability_id": "cap.demo"},
        )
        published = client.publish_capability(
            auth_token="token-1",
            payload={"capability": {"capability_id": "cap.demo"}},
        )
        invoked = client.invoke_capability(
            capability_id="cap.demo",
            auth_token="token-1",
            payload={"version": "1.0.0", "input": {"prompt": "hello"}},
        )

        self.assertEqual(compiled["event_type"], "app.capability.compiled.v1")
        self.assertEqual(published["event_type"], "app.capability.published.v1")
        self.assertEqual(invoked["event_type"], "app.capability.invoked.v1")
        self.assertEqual(captured[0]["method"], "POST")
        self.assertIn("/v1/capabilities/compile", captured[0]["url"])
        self.assertIn('"capability_id":"cap.demo"', captured[0]["body"])
        self.assertIn("/v1/capabilities/publish", captured[1]["url"])
        self.assertIn('"capability_id":"cap.demo"', captured[1]["body"])
        self.assertIn("/v1/capabilities/cap.demo:invoke", captured[2]["url"])
        self.assertIn('"version":"1.0.0"', captured[2]["body"])

    def test_contract_retirement_status_forwards_query_filters(self) -> None:
        captured: dict[str, str] = {}

        class _Response:
            def getcode(self) -> int:
                return 200

            def read(self) -> bytes:
                return (
                    b'{"schema_version":"runtime.contract_retirement_status.v1","versions":{"task_contract_version":[]}}'
                )

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
        payload = client.contract_retirement_status(
            auth_token="token-1",
            version_type="task_contract_version",
            version="task-envelope.v1",
        )
        self.assertEqual(payload["schema_version"], "runtime.contract_retirement_status.v1")
        self.assertEqual(captured["method"], "GET")
        self.assertIn("/v1/contracts/retirement:status", captured["url"])
        self.assertIn("version_type=task_contract_version", captured["url"])
        self.assertIn("version=task-envelope.v1", captured["url"])

    def test_contract_retirement_validate_posts_payload(self) -> None:
        captured: dict[str, str] = {}

        class _Response:
            def getcode(self) -> int:
                return 200

            def read(self) -> bytes:
                return b'{"schema_version":"runtime.contract_retirement_gate.v1","eligible_to_retire":false}'

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
        payload = client.contract_retirement_validate(
            auth_token="token-1",
            body={"task_contract_versions": ["task-envelope.v1"]},
        )
        self.assertEqual(payload["schema_version"], "runtime.contract_retirement_gate.v1")
        self.assertEqual(captured["method"], "POST")
        self.assertIn("/v1/contracts/retirement:validate", captured["url"])
        self.assertIn('"task_contract_versions":["task-envelope.v1"]', captured["body"])

    def test_client_pool_routes_submit_by_explicit_runtime_id_and_sticks_run(self) -> None:
        calls: list[tuple[str, str, str]] = []

        class _FakeClient:
            def __init__(self, runtime_id: str, base_url: str):
                self.runtime_id = runtime_id
                self.base_url = base_url

            def submit_command(self, *, envelope: dict[str, object], auth_token: str) -> dict[str, object]:
                _ = auth_token
                calls.append(("submit", self.runtime_id, str(envelope.get("tenant_id", ""))))
                return {
                    "event_id": "evt-submit",
                    "event_type": "runtime.run.accepted",
                    "tenant_id": "t2",
                    "app_id": "demo",
                    "session_key": "s",
                    "trace_id": "trace",
                    "correlation_id": "corr",
                    "ts": "2026-04-01T00:00:00+00:00",
                    "payload": {"run_id": "run-b"},
                }

            def get_run_status(self, *, run_id: str, auth_token: str) -> dict[str, object]:
                _ = auth_token
                calls.append(("status", self.runtime_id, run_id))
                return {"run_id": run_id, "status": "queued"}

        pool = RuntimeExecutionClientPool(
            route_table={
                "default_runtime_id": "rt-a",
                "runtimes": [
                    {"runtime_id": "rt-a", "base_url": "http://rt-a.local"},
                    {"runtime_id": "rt-b", "base_url": "http://rt-b.local"},
                ],
            },
            client_factory=lambda runtime_id, base_url: _FakeClient(runtime_id, base_url),
        )

        response = pool.submit_command(
            envelope={
                "tenant_id": "t2",
                "app_id": "demo",
                "payload": {
                    "execution_context": {
                        "runtime": {"runtime_id": "rt-b"},
                    }
                },
            },
            auth_token="token-1",
        )
        self.assertEqual(calls[0], ("submit", "rt-b", "t2"))
        payload = response["payload"]
        assert isinstance(payload, dict)
        route = payload.get("route")
        assert isinstance(route, dict)
        self.assertEqual(route.get("route_target"), "rt-b")
        self.assertEqual(route.get("routing_strategy"), "runtime_route_table")

        status = pool.get_run_status(run_id="run-b", auth_token="token-1")
        self.assertEqual(calls[1], ("status", "rt-b", "run-b"))
        self.assertEqual(status["status"], "queued")

    def test_client_pool_routes_submit_by_tenant_mapping(self) -> None:
        calls: list[tuple[str, str]] = []

        class _FakeClient:
            def __init__(self, runtime_id: str, base_url: str):
                self.runtime_id = runtime_id
                self.base_url = base_url

            def submit_command(self, *, envelope: dict[str, object], auth_token: str) -> dict[str, object]:
                _ = (auth_token, envelope)
                calls.append(("submit", self.runtime_id))
                return {
                    "event_id": "evt-submit",
                    "event_type": "runtime.run.accepted",
                    "tenant_id": "tenant-b",
                    "app_id": "demo",
                    "session_key": "s",
                    "trace_id": "trace",
                    "correlation_id": "corr",
                    "ts": "2026-04-01T00:00:00+00:00",
                    "payload": {"run_id": "run-tenant-b"},
                }

        pool = RuntimeExecutionClientPool(
            route_table={
                "default_runtime_id": "rt-a",
                "runtimes": [
                    {"runtime_id": "rt-a", "base_url": "http://rt-a.local"},
                    {"runtime_id": "rt-b", "base_url": "http://rt-b.local", "tenants": ["tenant-b"]},
                ],
            },
            client_factory=lambda runtime_id, base_url: _FakeClient(runtime_id, base_url),
        )
        pool.submit_command(
            envelope={"tenant_id": "tenant-b", "app_id": "demo", "payload": {}},
            auth_token="token-1",
        )
        self.assertEqual(calls[0], ("submit", "rt-b"))

    def test_client_pool_unknown_explicit_runtime_id_raises_client_error(self) -> None:
        class _FakeClient:
            def __init__(self, runtime_id: str, base_url: str):
                self.runtime_id = runtime_id
                self.base_url = base_url

            def submit_command(self, *, envelope: dict[str, object], auth_token: str) -> dict[str, object]:
                _ = (envelope, auth_token)
                return {}

        pool = RuntimeExecutionClientPool(
            route_table={
                "default_runtime_id": "rt-a",
                "runtimes": [
                    {"runtime_id": "rt-a", "base_url": "http://rt-a.local"},
                ],
            },
            client_factory=lambda runtime_id, base_url: _FakeClient(runtime_id, base_url),
        )
        with self.assertRaises(RuntimeExecutionClientError) as ctx:
            pool.submit_command(
                envelope={
                    "tenant_id": "t1",
                    "app_id": "demo",
                    "payload": {
                        "execution_context": {
                            "runtime": {"runtime_id": "rt-b"},
                        }
                    },
                },
                auth_token="token-1",
            )

        exc = ctx.exception
        self.assertEqual(exc.status_code, 422)
        self.assertIsInstance(exc.detail, dict)
        assert isinstance(exc.detail, dict)
        self.assertEqual(exc.detail.get("code"), "runtime_target_unknown")
        self.assertFalse(exc.retryable)


if __name__ == "__main__":
    unittest.main()
