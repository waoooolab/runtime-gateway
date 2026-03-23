#!/usr/bin/env python3
"""Minimal SQLite durable verification for gateway events and audit ledgers."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

from runtime_gateway.audit.emitter import (
    clear_audit_events,
    emit_audit_event,
    read_audit_log,
)
from runtime_gateway.events.durable import append_event_record, read_event_page


def _event(*, run_id: str, event_type: str, ts: str) -> dict[str, object]:
    return {
        "event_id": f"evt-{run_id}-{event_type}",
        "event_type": event_type,
        "tenant_id": "t1",
        "app_id": "covernow",
        "session_key": "tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
        "trace_id": f"trace-{run_id}",
        "correlation_id": f"corr-{run_id}",
        "ts": ts,
        "payload": {"run_id": run_id, "status": "queued"},
    }


def main() -> int:
    old_event_db_path = os.environ.get("RUNTIME_GATEWAY_EVENT_DB_PATH")
    old_audit_db_path = os.environ.get("RUNTIME_GATEWAY_AUDIT_DB_PATH")
    old_event_log_path = os.environ.get("RUNTIME_GATEWAY_EVENT_LOG_PATH")
    old_audit_log_path = os.environ.get("RUNTIME_GATEWAY_AUDIT_LOG_PATH")

    try:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            event_db = (base / "events" / "runtime-events.sqlite").as_posix()
            audit_db = (base / "audit" / "runtime-audit.sqlite").as_posix()

            os.environ["RUNTIME_GATEWAY_EVENT_DB_PATH"] = event_db
            os.environ["RUNTIME_GATEWAY_AUDIT_DB_PATH"] = audit_db
            os.environ.pop("RUNTIME_GATEWAY_EVENT_LOG_PATH", None)
            os.environ.pop("RUNTIME_GATEWAY_AUDIT_LOG_PATH", None)

            append_event_record(
                bus_seq=11,
                event=_event(
                    run_id="run-minimal-durable-1",
                    event_type="runtime.run.started",
                    ts="2026-03-12T00:00:00Z",
                ),
            )
            append_event_record(
                bus_seq=12,
                event=_event(
                    run_id="run-minimal-durable-1",
                    event_type="runtime.run.completed",
                    ts="2026-03-12T00:01:00Z",
                ),
            )

            page = read_event_page(
                limit=10,
                tenant_id="t1",
                app_id="covernow",
                session_key=None,
                event_types={"runtime.run.completed"},
                run_id="run-minimal-durable-1",
                since_ts=None,
                until_ts=None,
                cursor=0,
            )
            assert len(page["items"]) == 1
            assert page["items"][0]["event"]["event_type"] == "runtime.run.completed"
            assert page["stats"]["buffered_events"] == 2
            assert page["stats"]["next_seq"] == 3

            emit_audit_event(action="runs.cancel", decision="deny", actor_id="user:u2")
            clear_audit_events()
            audit_items = read_audit_log(limit=10)
            assert len(audit_items) == 1
            assert audit_items[0]["action"] == "runs.cancel"
            assert audit_items[0]["decision"] == "deny"

            print(
                "gateway_durable_sqlite_minimal_ok",
                {
                    "event_items": len(page["items"]),
                    "event_next_seq": page["stats"]["next_seq"],
                    "audit_items": len(audit_items),
                },
            )
        return 0
    finally:
        if old_event_db_path is None:
            os.environ.pop("RUNTIME_GATEWAY_EVENT_DB_PATH", None)
        else:
            os.environ["RUNTIME_GATEWAY_EVENT_DB_PATH"] = old_event_db_path
        if old_audit_db_path is None:
            os.environ.pop("RUNTIME_GATEWAY_AUDIT_DB_PATH", None)
        else:
            os.environ["RUNTIME_GATEWAY_AUDIT_DB_PATH"] = old_audit_db_path
        if old_event_log_path is None:
            os.environ.pop("RUNTIME_GATEWAY_EVENT_LOG_PATH", None)
        else:
            os.environ["RUNTIME_GATEWAY_EVENT_LOG_PATH"] = old_event_log_path
        if old_audit_log_path is None:
            os.environ.pop("RUNTIME_GATEWAY_AUDIT_LOG_PATH", None)
        else:
            os.environ["RUNTIME_GATEWAY_AUDIT_LOG_PATH"] = old_audit_log_path


if __name__ == "__main__":
    raise SystemExit(main())
