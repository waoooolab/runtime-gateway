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
        self.assertEqual(
            envelope["scope_id"],
            "tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
        )
        self.assertEqual(envelope["scope_type"], "session")
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

    def test_contract_version_fields_can_be_injected(self) -> None:
        envelope = build_event_envelope(
            event_type="run.requested",
            tenant_id="t1",
            app_id="covernow",
            session_key="tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
            payload={"run_id": "run-1"},
            task_contract_version="task-envelope.v1",
            agent_contract_version="assistant-decision.v1",
            event_schema_version="event-envelope.v1",
        )
        self.assertEqual(envelope["task_contract_version"], "task-envelope.v1")
        self.assertEqual(envelope["agent_contract_version"], "assistant-decision.v1")
        self.assertEqual(envelope["event_schema_version"], "event-envelope.v1")
        validate_event_envelope(envelope)

    def test_scope_axis_can_be_overridden(self) -> None:
        envelope = build_event_envelope(
            event_type="run.requested",
            tenant_id="t1",
            app_id="covernow",
            session_key="tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
            payload={"run_id": "run-1"},
            scope_id="scope:tenant:t1:workspace:creative-lab",
            scope_type="workspace",
        )
        self.assertEqual(envelope["scope_id"], "scope:tenant:t1:workspace:creative-lab")
        self.assertEqual(envelope["scope_type"], "workspace")
        validate_event_envelope(envelope)

    def test_split_plane_blocks_can_be_projected(self) -> None:
        correlation_id = "corr-split-1"
        envelope = build_event_envelope(
            event_type="run.requested",
            tenant_id="t1",
            app_id="covernow",
            session_key="tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
            payload={"run_id": "run-1"},
            trace_id="trace-1",
            correlation_id=correlation_id,
        )
        envelope["communication_memory_trace"] = {
            "plane": "communication_memory",
            "authority": "runtime_orchestrator",
            "memory_scope": "session",
            "relay_path": ["user", "leader", "worker"],
            "correlation_id": correlation_id,
        }
        envelope["runtime_state_assembly"] = {
            "plane": "runtime_state",
            "run_id": "run-1",
            "task_id": "task-1",
            "trace_id": "trace-1",
            "session_key": "tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
            "correlation_id": correlation_id,
            "assembly_order": [
                "registry_index",
                "session_pointer",
                "event_snapshot",
                "task_snapshot",
                "context_rehydrate",
            ],
            "pointer_validity": "strict",
            "fallback_strategy": "rehydrate_context",
            "orchestration_role": "leader",
            "assistant_identity": "assistant.main",
        }
        validate_event_envelope(envelope)

    def test_split_plane_blocks_reject_partial_projection(self) -> None:
        envelope = build_event_envelope(
            event_type="run.requested",
            tenant_id="t1",
            app_id="covernow",
            session_key="tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
            payload={"run_id": "run-1"},
            trace_id="trace-1",
            correlation_id="corr-split-1",
        )
        envelope["communication_memory_trace"] = {
            "plane": "communication_memory",
            "authority": "runtime_orchestrator",
            "memory_scope": "session",
            "relay_path": ["user", "leader", "worker"],
            "correlation_id": "corr-split-1",
        }
        with self.assertRaises(ValueError):
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
