from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime, timezone

from runtime_gateway.events.durable import (
    acknowledge_event_consumer_cursor,
    append_event_record,
    read_event_consumer_ack_cursor,
    read_event_page,
)


def _event(*, event_type: str, run_id: str, ts: str) -> dict:
    return {
        "event_id": f"evt-{run_id}-{event_type}",
        "event_type": event_type,
        "tenant_id": "t1",
        "app_id": "covernow",
        "session_key": "tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
        "trace_id": f"trace-{run_id}",
        "correlation_id": f"corr-{run_id}",
        "ts": ts,
        "payload": {
            "run_id": run_id,
            "status": "queued",
        },
    }


class DurableEventsStorageTests(unittest.TestCase):
    def setUp(self) -> None:
        os.environ.pop("OWA_PERSIST_ROOT", None)
        os.environ.pop("RUNTIME_GATEWAY_EVENT_LOG_PATH", None)
        os.environ.pop("RUNTIME_GATEWAY_EVENT_DB_PATH", None)

    def test_sqlite_durable_page_cursor_and_filters(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "events", "runtime-events.sqlite")
            os.environ["RUNTIME_GATEWAY_EVENT_DB_PATH"] = db_path

            append_event_record(
                bus_seq=10,
                event=_event(
                    event_type="runtime.run.started",
                    run_id="run-1",
                    ts="2026-03-12T00:00:00Z",
                ),
            )
            append_event_record(
                bus_seq=11,
                event=_event(
                    event_type="runtime.run.status",
                    run_id="run-1",
                    ts="2026-03-12T00:01:00Z",
                ),
            )
            append_event_record(
                bus_seq=12,
                event=_event(
                    event_type="runtime.run.completed",
                    run_id="run-2",
                    ts="2026-03-12T00:02:00Z",
                ),
            )

            latest = read_event_page(
                limit=2,
                tenant_id="t1",
                app_id="covernow",
                session_key=None,
                scope_id=None,
                scope_type=None,
                event_types=None,
                run_id=None,
                since_ts=None,
                until_ts=None,
                cursor=None,
            )
            self.assertEqual(len(latest["items"]), 2)
            self.assertTrue(latest["has_more"])
            self.assertEqual(
                [item["event"]["event_type"] for item in latest["items"]],
                ["runtime.run.status", "runtime.run.completed"],
            )
            self.assertEqual(latest["stats"]["buffered_events"], 3)
            self.assertEqual(latest["stats"]["next_seq"], 4)

            from_start = read_event_page(
                limit=2,
                tenant_id="t1",
                app_id="covernow",
                session_key=None,
                scope_id=None,
                scope_type=None,
                event_types=None,
                run_id=None,
                since_ts=None,
                until_ts=None,
                cursor=0,
            )
            self.assertEqual(len(from_start["items"]), 2)
            self.assertTrue(from_start["has_more"])
            self.assertEqual(
                [item["event"]["event_type"] for item in from_start["items"]],
                ["runtime.run.started", "runtime.run.status"],
            )
            self.assertEqual(from_start["next_cursor"], 2)

            filtered = read_event_page(
                limit=10,
                tenant_id="t1",
                app_id="covernow",
                session_key=None,
                scope_id=None,
                scope_type=None,
                event_types={"runtime.run.status", "runtime.run.completed"},
                run_id="run-1",
                since_ts=datetime(2026, 3, 12, 0, 0, 30, tzinfo=timezone.utc),
                until_ts=datetime(2026, 3, 12, 0, 1, 30, tzinfo=timezone.utc),
                cursor=0,
            )
            self.assertEqual(len(filtered["items"]), 1)
            self.assertEqual(filtered["items"][0]["event"]["event_type"], "runtime.run.status")
            self.assertEqual(filtered["items"][0]["event"]["payload"]["run_id"], "run-1")

            scope_filtered = read_event_page(
                limit=10,
                tenant_id="t1",
                app_id="covernow",
                session_key=None,
                scope_id="tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
                scope_type="session",
                event_types=None,
                run_id=None,
                since_ts=None,
                until_ts=None,
                cursor=0,
            )
            self.assertEqual(len(scope_filtered["items"]), 3)

            wrong_scope_type = read_event_page(
                limit=10,
                tenant_id="t1",
                app_id="covernow",
                session_key=None,
                scope_id="tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
                scope_type="workspace",
                event_types=None,
                run_id=None,
                since_ts=None,
                until_ts=None,
                cursor=0,
            )
            self.assertEqual(len(wrong_scope_type["items"]), 0)

    def test_sqlite_durable_uses_persist_root_default_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            os.environ["OWA_PERSIST_ROOT"] = tmp
            append_event_record(
                bus_seq=7,
                event=_event(
                    event_type="runtime.run.started",
                    run_id="run-persist-root",
                    ts="2026-03-12T00:03:00Z",
                ),
            )
            page = read_event_page(
                limit=10,
                tenant_id="t1",
                app_id="covernow",
                session_key=None,
                scope_id=None,
                scope_type=None,
                event_types=None,
                run_id="run-persist-root",
                since_ts=None,
                until_ts=None,
                cursor=0,
            )
            self.assertEqual(len(page["items"]), 1)
            self.assertEqual(page["stats"]["buffered_events"], 1)
            expected_db_path = os.path.join(tmp, "runtime-gateway", "runtime-events.sqlite")
            self.assertTrue(os.path.exists(expected_db_path))

    def test_sqlite_durable_consumer_ack_cursor_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "events", "runtime-events.sqlite")
            os.environ["RUNTIME_GATEWAY_EVENT_DB_PATH"] = db_path

            first = acknowledge_event_consumer_cursor(
                consumer_id="consumer-a",
                source="durable",
                tenant_id="t1",
                app_id="covernow",
                run_id="run-ack-1",
                cursor=7,
            )
            self.assertEqual(first["requested_cursor"], 7)
            self.assertEqual(first["ack_cursor"], 7)
            self.assertTrue(first["applied"])
            self.assertEqual(first["previous_cursor"], None)

            stored = read_event_consumer_ack_cursor(
                consumer_id="consumer-a",
                source="durable",
                tenant_id="t1",
                app_id="covernow",
                run_id="run-ack-1",
            )
            self.assertEqual(stored, 7)

            second = acknowledge_event_consumer_cursor(
                consumer_id="consumer-a",
                source="durable",
                tenant_id="t1",
                app_id="covernow",
                run_id="run-ack-1",
                cursor=3,
            )
            self.assertEqual(second["previous_cursor"], 7)
            self.assertEqual(second["ack_cursor"], 7)
            self.assertFalse(second["applied"])
            self.assertEqual(second["reason_code"], "ack_cursor_regression_ignored")

            third = acknowledge_event_consumer_cursor(
                consumer_id="consumer-a",
                source="durable",
                tenant_id="t1",
                app_id="covernow",
                run_id="run-ack-1",
                cursor=10,
            )
            self.assertEqual(third["previous_cursor"], 7)
            self.assertEqual(third["ack_cursor"], 10)
            self.assertTrue(third["applied"])


if __name__ == "__main__":
    unittest.main()
