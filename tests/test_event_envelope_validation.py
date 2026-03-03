from __future__ import annotations

import os
import unittest
from pathlib import Path

from runtime_gateway.events.envelope import build_event_envelope
from runtime_gateway.events.validation import validate_event_envelope

os.environ["WAOOOOLAB_PLATFORM_CONTRACTS_DIR"] = str(
    Path(__file__).resolve().parent / "fixtures" / "contracts"
)


class EventEnvelopeValidationTests(unittest.TestCase):
    def test_valid_event_envelope(self) -> None:
        envelope = build_event_envelope(
            event_type="run.requested",
            tenant_id="t1",
            app_id="covernow",
            session_key="tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
            payload={"run_id": "run-1"},
        )
        validate_event_envelope(envelope)

    def test_trace_id_can_be_injected(self) -> None:
        envelope = build_event_envelope(
            event_type="run.requested",
            tenant_id="t1",
            app_id="covernow",
            session_key="tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
            payload={"run_id": "run-1"},
            trace_id="trace-fixed-1",
            correlation_id="corr-fixed-1",
        )
        self.assertEqual(envelope["trace_id"], "trace-fixed-1")
        self.assertEqual(envelope["correlation_id"], "corr-fixed-1")
        validate_event_envelope(envelope)

    def test_missing_required_field(self) -> None:
        envelope = {
            "event_id": "e1",
            "event_type": "run.requested",
            "tenant_id": "t1",
            "app_id": "covernow",
            "trace_id": "tr1",
            "correlation_id": "tr1",
            "ts": "2026-03-01T10:39:00Z",
            "payload": {},
        }
        with self.assertRaises(ValueError):
            validate_event_envelope(envelope)


if __name__ == "__main__":
    unittest.main()
