from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from runtime_gateway.audit.emitter import (
    clear_audit_events,
    emit_audit_event,
    get_audit_events,
    read_audit_log,
)

os.environ["WAOOOOLAB_PLATFORM_CONTRACTS_DIR"] = str(
    Path(__file__).resolve().parent / "fixtures" / "contracts"
)


class AuditEmitterTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_audit_events()
        os.environ.pop("RUNTIME_GATEWAY_AUDIT_LOG_PATH", None)
        os.environ.pop("RUNTIME_GATEWAY_AUDIT_DB_PATH", None)

    def test_emit_and_read_events(self) -> None:
        event = emit_audit_event(
            action="auth.token_exchange",
            decision="allow",
            actor_id="user:u1",
            trace_id="trace-1",
            metadata={"audience": "runtime-execution"},
        )
        self.assertEqual(event["decision"], "allow")
        self.assertEqual(len(get_audit_events()), 1)

    def test_invalid_decision_rejected(self) -> None:
        with self.assertRaises(ValueError):
            emit_audit_event(action="runs.create", decision="unknown", actor_id="user:u1")

    def test_emit_persists_to_durable_audit_log(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_path = os.path.join(tmp, "audit", "events.ndjson")
            os.environ["RUNTIME_GATEWAY_AUDIT_LOG_PATH"] = log_path
            emit_audit_event(action="runs.create", decision="allow", actor_id="user:u1")
            items = read_audit_log()
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["action"], "runs.create")

    def test_emit_persists_to_durable_audit_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "audit", "events.sqlite")
            os.environ["RUNTIME_GATEWAY_AUDIT_DB_PATH"] = db_path
            emit_audit_event(action="runs.cancel", decision="deny", actor_id="user:u2")
            items = read_audit_log()
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["action"], "runs.cancel")
            self.assertEqual(items[0]["decision"], "deny")


if __name__ == "__main__":
    unittest.main()
