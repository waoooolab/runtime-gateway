from __future__ import annotations

import os
import unittest
from pathlib import Path

from runtime_gateway.contracts.validation import (
    ContractValidationError,
    validate_command_envelope_contract,
    validate_event_envelope_contract,
    validate_execution_context_contract,
    validate_executor_profile_catalog_contract,
    validate_orchestration_hints_contract,
    validate_template_capability_binding_contract,
    validate_runtime_worker_drain_contract,
    validate_runtime_events_page_contract,
    validate_runtime_worker_health_contract,
    validate_runtime_worker_status_contract,
    validate_tool_catalog_contract,
    validate_token_exchange_contract,
)
from runtime_gateway.events.envelope import build_event_envelope

os.environ["WAOOOOLAB_PLATFORM_CONTRACTS_DIR"] = str(
    Path(__file__).resolve().parent / "fixtures" / "contracts"
)


def _communication_memory_trace(*, correlation_id: str) -> dict:
    return {
        "plane": "communication_memory",
        "authority": "runtime_orchestrator",
        "memory_scope": "session",
        "relay_path": ["user", "leader", "worker"],
        "correlation_id": correlation_id,
        "trace_ref": "mem-trace-001",
    }


def _runtime_state_assembly(*, correlation_id: str) -> dict:
    return {
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


class ContractValidationTests(unittest.TestCase):
    def test_command_envelope_contract_allows_contract_versions(self) -> None:
        payload = {
            "command_id": "cmd-1",
            "command_type": "run.start",
            "tenant_id": "t1",
            "app_id": "covernow",
            "session_key": "tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
            "scope_id": "tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
            "scope_type": "session",
            "task_contract_version": "task-envelope.v1",
            "agent_contract_version": "assistant-decision.v1",
            "event_schema_version": "event-envelope.v1",
            "trace_id": "trace-1",
            "idempotency_key": "idempotent-key-0001",
            "retry_policy": {
                "max_attempts": 3,
                "backoff_ms": 250,
                "strategy": "fixed",
            },
            "ts": "2026-03-01T10:39:00Z",
            "payload": {"goal": "build feature"},
        }
        validate_command_envelope_contract(payload)

    def test_command_envelope_contract_rejects_empty_scope_type_when_present(self) -> None:
        payload = {
            "command_id": "cmd-1",
            "command_type": "run.start",
            "tenant_id": "t1",
            "app_id": "covernow",
            "session_key": "tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
            "scope_id": "tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
            "scope_type": "",
            "trace_id": "trace-1",
            "idempotency_key": "idempotent-key-0001",
            "retry_policy": {
                "max_attempts": 3,
                "backoff_ms": 250,
                "strategy": "fixed",
            },
            "ts": "2026-03-01T10:39:00Z",
            "payload": {"goal": "build feature"},
        }
        with self.assertRaises(ContractValidationError):
            validate_command_envelope_contract(payload)

    def test_command_envelope_contract_rejects_empty_contract_version(self) -> None:
        payload = {
            "command_id": "cmd-1",
            "command_type": "run.start",
            "tenant_id": "t1",
            "app_id": "covernow",
            "session_key": "tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
            "task_contract_version": "",
            "trace_id": "trace-1",
            "idempotency_key": "idempotent-key-0001",
            "retry_policy": {
                "max_attempts": 3,
                "backoff_ms": 250,
                "strategy": "fixed",
            },
            "ts": "2026-03-01T10:39:00Z",
            "payload": {"goal": "build feature"},
        }
        with self.assertRaises(ContractValidationError):
            validate_command_envelope_contract(payload)

    def test_command_envelope_contract_allows_split_plane_blocks(self) -> None:
        correlation_id = "corr-1"
        payload = {
            "command_id": "cmd-1",
            "command_type": "run.start",
            "tenant_id": "t1",
            "app_id": "covernow",
            "session_key": "tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
            "trace_id": "trace-1",
            "correlation_id": correlation_id,
            "idempotency_key": "idempotent-key-0001",
            "retry_policy": {
                "max_attempts": 3,
                "backoff_ms": 250,
                "strategy": "fixed",
            },
            "communication_memory_trace": _communication_memory_trace(correlation_id=correlation_id),
            "runtime_state_assembly": _runtime_state_assembly(correlation_id=correlation_id),
            "ts": "2026-03-01T10:39:00Z",
            "payload": {"goal": "build feature"},
        }
        validate_command_envelope_contract(payload)

    def test_command_envelope_contract_rejects_partial_split_plane_blocks(self) -> None:
        correlation_id = "corr-1"
        payload = {
            "command_id": "cmd-1",
            "command_type": "run.start",
            "tenant_id": "t1",
            "app_id": "covernow",
            "session_key": "tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
            "trace_id": "trace-1",
            "correlation_id": correlation_id,
            "idempotency_key": "idempotent-key-0001",
            "retry_policy": {
                "max_attempts": 3,
                "backoff_ms": 250,
                "strategy": "fixed",
            },
            "communication_memory_trace": _communication_memory_trace(correlation_id=correlation_id),
            "ts": "2026-03-01T10:39:00Z",
            "payload": {"goal": "build feature"},
        }
        with self.assertRaises(ContractValidationError):
            validate_command_envelope_contract(payload)

    def test_token_exchange_request_contract_valid(self) -> None:
        payload = {
            "kind": "request",
            "grant_type": "urn:waoooolab:params:oauth:grant-type:token-exchange",
            "subject_token": "x" * 32,
            "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "requested_token_use": "service",
            "audience": "runtime-execution",
            "scope": ["runs:write"],
            "requested_ttl_seconds": 300,
        }
        validate_token_exchange_contract(payload)

    def test_token_exchange_request_rejects_additional_properties(self) -> None:
        payload = {
            "kind": "request",
            "grant_type": "urn:waoooolab:params:oauth:grant-type:token-exchange",
            "subject_token": "x" * 32,
            "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "requested_token_use": "service",
            "audience": "runtime-execution",
            "scope": ["runs:write"],
            "unexpected": "not-allowed",
        }
        with self.assertRaises(ContractValidationError):
            validate_token_exchange_contract(payload)

    def test_token_exchange_response_contract_valid(self) -> None:
        payload = {
            "kind": "response",
            "issued_token_type": "urn:waoooolab:token-type:service_token",
            "token_type": "Bearer",
            "access_token": "x" * 32,
            "expires_in": 300,
            "scope": ["runs:write"],
            "audience": "runtime-execution",
            "token_use": "service",
            "jti": "jti-12345678",
        }
        validate_token_exchange_contract(payload)

    def test_event_envelope_contract_rejects_invalid_ts(self) -> None:
        envelope = build_event_envelope(
            event_type="run.requested",
            tenant_id="t1",
            app_id="covernow",
            session_key="tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
            payload={"run_id": "run-1"},
        )
        envelope["ts"] = "bad-time"
        with self.assertRaises(ContractValidationError):
            validate_event_envelope_contract(envelope)

    def test_event_envelope_contract_allows_contract_versions(self) -> None:
        envelope = build_event_envelope(
            event_type="run.requested",
            tenant_id="t1",
            app_id="covernow",
            session_key="tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
            payload={"run_id": "run-1"},
            scope_id="scope:tenant:t1:app:covernow",
            scope_type="workspace",
            task_contract_version="task-envelope.v1",
            agent_contract_version="assistant-decision.v1",
            event_schema_version="event-envelope.v1",
        )
        validate_event_envelope_contract(envelope)

    def test_event_envelope_contract_allows_split_plane_blocks(self) -> None:
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
        envelope["communication_memory_trace"] = _communication_memory_trace(
            correlation_id=correlation_id
        )
        envelope["runtime_state_assembly"] = _runtime_state_assembly(correlation_id=correlation_id)
        validate_event_envelope_contract(envelope)

    def test_event_envelope_contract_rejects_partial_split_plane_blocks(self) -> None:
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
        envelope["communication_memory_trace"] = _communication_memory_trace(
            correlation_id=correlation_id
        )
        with self.assertRaises(ContractValidationError):
            validate_event_envelope_contract(envelope)

    def test_execution_context_contract_valid_runtime_workload(self) -> None:
        payload = {
            "task_plane": "runtime_workload",
            "runtime": {
                "execution_mode": "compute",
            },
        }
        validate_execution_context_contract(payload)

    def test_execution_context_contract_valid_runtime_workload_with_runtime_id_hint(self) -> None:
        payload = {
            "task_plane": "runtime_workload",
            "runtime": {
                "execution_mode": "control",
                "runtime_id": "rt-b",
            },
        }
        validate_execution_context_contract(payload)

    def test_execution_context_contract_rejects_runtime_workload_with_executor(self) -> None:
        payload = {
            "task_plane": "runtime_workload",
            "executor": {
                "family": "acp",
                "engine": "claude_code",
                "adapter": "ccb",
            },
            "runtime": {
                "execution_mode": "control",
            },
        }
        with self.assertRaises(ContractValidationError):
            validate_execution_context_contract(payload)

    def test_executor_profile_catalog_contract_valid(self) -> None:
        payload = {
            "items": [
                {
                    "family": "acp",
                    "engines": ["claude_code", "codex"],
                    "adapters": ["orchestrator", "ccb"],
                    "access_modes": ["direct", "api"],
                    "window_modes": ["inline", "terminal_mux"],
                },
                {
                    "family": "workflow_runtime",
                    "engines": ["langgraph"],
                    "adapters": ["runtime_api"],
                    "access_modes": ["api"],
                    "window_modes": ["inline"],
                },
            ]
        }
        validate_executor_profile_catalog_contract(payload)

    def test_executor_profile_catalog_contract_rejects_invalid_adapter(self) -> None:
        payload = {
            "items": [
                {
                    "family": "acp",
                    "engines": ["claude_code"],
                    "adapters": ["invalid_adapter"],
                }
            ]
        }
        with self.assertRaises(ContractValidationError):
            validate_executor_profile_catalog_contract(payload)

    def test_tool_catalog_contract_valid(self) -> None:
        payload = {
            "schema_version": "tool_catalog.v1",
            "items": [
                {
                    "tool_id": "cap.demo",
                    "source": {
                        "plane": "runtime",
                        "registry": "capability_registry",
                        "kind": "app",
                        "capability_id": "cap.demo",
                        "capability_version": "1.0.0",
                        "workflow_id": "wf-demo",
                        "workflow_version": "7",
                        "mode": "node",
                    },
                    "provenance": {
                        "authority": "runtime-execution",
                        "registered_at": "2026-03-24T00:00:00Z",
                        "registration_mode": "manual",
                    },
                    "profile": {
                        "visibility": "internal",
                        "policy_profile": "internal_default",
                    },
                    "optionality": {
                        "mode": "optional",
                    },
                }
            ],
        }
        validate_tool_catalog_contract(payload)

    def test_tool_catalog_contract_rejects_missing_source(self) -> None:
        payload = {
            "schema_version": "tool_catalog.v1",
            "items": [
                {
                    "tool_id": "cap.demo",
                    "provenance": {
                        "authority": "runtime-execution",
                        "registered_at": "2026-03-24T00:00:00Z",
                    },
                    "profile": {"visibility": "private"},
                    "optionality": {"mode": "optional"},
                }
            ],
        }
        with self.assertRaises(ContractValidationError):
            validate_tool_catalog_contract(payload)

    def test_orchestration_hints_contract_valid(self) -> None:
        payload = {
            "parent_run_id": "run-parent",
            "parent_task_id": "run-parent:root",
            "priority_class": "high",
            "priority_score": 42,
            "tenant_tier": "enterprise",
            "deadline_at": "2026-03-04T08:00:00Z",
            "nested_leader_contract": {
                "delegation_mode": "nested",
                "lifecycle_ack_mode": "progress_and_completion",
                "failure_takeover_mode": "outer_failover",
                "completion_aggregation_mode": "both",
                "ack_timeout_ms": 3000,
                "max_failure_takeovers": 2,
            },
            "nested_autonomy_policy": {
                "autonomy_level": "guided",
                "suggestion_trigger_mode": "on_blocker",
                "handoff_mode": "plan_and_constraints",
                "max_inner_steps": 8,
                "allow_inner_replan": True,
                "require_outer_approval": False,
            },
        }
        validate_orchestration_hints_contract(payload)

    def test_orchestration_hints_contract_rejects_orphan_parent_task(self) -> None:
        payload = {
            "parent_task_id": "orphan:root",
        }
        with self.assertRaises(ContractValidationError):
            validate_orchestration_hints_contract(payload)

    def test_orchestration_hints_contract_rejects_invalid_nested_autonomy_mode(self) -> None:
        payload = {
            "nested_autonomy_policy": {
                "autonomy_level": "guided",
                "suggestion_trigger_mode": "on_blocker",
                "handoff_mode": "provider_native",
            }
        }
        with self.assertRaises(ContractValidationError):
            validate_orchestration_hints_contract(payload)

    def test_template_capability_binding_contract_valid(self) -> None:
        payload = {
            "schema_version": "workflow_template_capability_binding_contract.v1",
            "template_package": {
                "package_id": "pkg.visual-starter",
                "package_version": "1.0.0",
                "template_id": "tpl.visual.pipeline",
                "template_version": "2026.03.27",
                "lifecycle_stage": "draft",
            },
            "capability_bindings": [
                {
                    "binding_id": "bind-1",
                    "step_id": "phase-1:step-1",
                    "capability_id": "cap.image.upscale",
                    "capability_version": "1.2.0",
                    "binding_mode": "required",
                    "executor_profile": {
                        "family": "python",
                        "engine": "default",
                        "adapter": "runtime_api",
                    },
                }
            ],
            "resolution_policy": {
                "on_missing_capability": "deny",
                "on_version_mismatch": "deny",
                "on_compile_failure": "halt",
            },
            "acceptance_criteria": [
                "binding_references_resolvable",
                "package_lifecycle_declared",
                "version_pinning_enforced",
            ],
        }
        validate_template_capability_binding_contract(payload)

    def test_template_capability_binding_contract_rejects_missing_capability_version(self) -> None:
        payload = {
            "schema_version": "workflow_template_capability_binding_contract.v1",
            "template_package": {
                "package_id": "pkg.visual-starter",
                "package_version": "1.0.0",
                "template_id": "tpl.visual.pipeline",
                "template_version": "2026.03.27",
                "lifecycle_stage": "draft",
            },
            "capability_bindings": [
                {
                    "binding_id": "bind-1",
                    "step_id": "phase-1:step-1",
                    "capability_id": "cap.image.upscale",
                    "binding_mode": "required",
                    "executor_profile": {
                        "family": "python",
                        "engine": "default",
                        "adapter": "runtime_api",
                    },
                }
            ],
            "resolution_policy": {
                "on_missing_capability": "deny",
                "on_version_mismatch": "deny",
                "on_compile_failure": "halt",
            },
            "acceptance_criteria": [
                "binding_references_resolvable",
                "package_lifecycle_declared",
                "version_pinning_enforced",
            ],
        }
        with self.assertRaises(ContractValidationError):
            validate_template_capability_binding_contract(payload)

    def test_runtime_events_page_contract_valid(self) -> None:
        payload = {
            "items": [
                {
                    "bus_seq": 1,
                    "event": {
                        "event_id": "evt-1",
                        "event_type": "runtime.run.status",
                        "tenant_id": "t1",
                        "app_id": "covernow",
                        "session_key": "tenant:t1:app:covernow:channel:web:actor:u1:thread:main:agent:pm",
                        "trace_id": "trace-1",
                        "correlation_id": "run-1",
                        "ts": "2026-03-12T00:00:00Z",
                        "payload": {"run_id": "run-1", "status": "queued"},
                    },
                }
            ],
            "next_cursor": 1,
            "has_more": False,
            "recommended_poll_after_ms": 1500,
            "stats": {"connections": 0, "buffered_events": 1, "next_seq": 2},
        }
        validate_runtime_events_page_contract(payload)

    def test_runtime_events_page_contract_rejects_invalid_recommended_poll(self) -> None:
        payload = {
            "items": [],
            "next_cursor": 0,
            "has_more": False,
            "recommended_poll_after_ms": 50,
            "stats": {"connections": 0, "buffered_events": 0, "next_seq": 1},
        }
        with self.assertRaises(ContractValidationError):
            validate_runtime_events_page_contract(payload)

    def test_runtime_events_page_contract_rejects_missing_has_more(self) -> None:
        payload = {
            "items": [],
            "next_cursor": 0,
            "stats": {"connections": 0, "buffered_events": 0, "next_seq": 1},
        }
        with self.assertRaises(ContractValidationError):
            validate_runtime_events_page_contract(payload)

    def test_runtime_worker_health_contract_valid(self) -> None:
        payload = {
            "queue_depth": 0,
            "ticks_total": 3,
            "idle_ticks_total": 1,
            "progressed_ticks_total": 2,
            "missing_run_ticks_total": 0,
            "skipped_ticks_total": 0,
            "drain_calls_total": 1,
            "drain_processed_total": 2,
            "last_tick_at": "2026-03-12T00:00:00Z",
            "last_drain_at": "2026-03-12T00:01:00Z",
            "last_tick_outcome": "progressed",
            "last_tick_age_seconds": 1.5,
            "last_heartbeat_at": "2026-03-12T00:01:00Z",
            "last_heartbeat_age_seconds": 1.5,
            "is_heartbeat_stale": False,
            "is_tick_stale": False,
            "is_backlogged": False,
            "is_stalled": False,
            "health_state": "healthy",
            "lease_renew_signal": {
                "attempted": 1,
                "renewed": 1,
                "errors": 0,
                "expired_conflicts": 0,
                "released_conflicts": 0,
                "last_observed_at": "2026-03-12T00:01:00Z",
                "total_attempted": 2,
                "total_renewed": 2,
                "total_errors": 0,
                "total_expired_conflicts": 0,
                "total_released_conflicts": 0,
            },
            "lifecycle_state": "running",
            "is_running": True,
            "pool_last_control_counts": {"queue": 0, "defer": 0, "reject": 0},
            "pool_last_control_outcome": "dispatch",
            "pool_last_control_signal": {
                "queue_pending": False,
                "defer_pending": False,
                "reject_pending": False,
            },
            "last_transition": "start",
            "last_transition_at": "2026-03-12T00:00:00Z",
            "start_total": 1,
            "stop_total": 0,
            "restart_total": 0,
            "recommended_poll_after_ms": 5000,
        }
        validate_runtime_worker_health_contract(payload)

    def test_runtime_worker_health_contract_rejects_invalid_health_state(self) -> None:
        payload = {
            "queue_depth": 0,
            "ticks_total": 0,
            "idle_ticks_total": 0,
            "progressed_ticks_total": 0,
            "missing_run_ticks_total": 0,
            "skipped_ticks_total": 0,
            "drain_calls_total": 0,
            "drain_processed_total": 0,
            "last_tick_at": None,
            "last_drain_at": None,
            "last_tick_outcome": None,
            "last_tick_age_seconds": None,
            "last_heartbeat_at": "2026-03-12T00:00:00Z",
            "last_heartbeat_age_seconds": 0.0,
            "is_heartbeat_stale": False,
            "is_tick_stale": False,
            "is_backlogged": False,
            "is_stalled": False,
            "health_state": "unknown",
            "lease_renew_signal": {
                "attempted": 0,
                "renewed": 0,
                "errors": 0,
                "expired_conflicts": 0,
                "released_conflicts": 0,
                "last_observed_at": None,
                "total_attempted": 0,
                "total_renewed": 0,
                "total_errors": 0,
                "total_expired_conflicts": 0,
                "total_released_conflicts": 0,
            },
            "lifecycle_state": "running",
            "is_running": True,
            "pool_last_control_counts": {"queue": 0, "defer": 0, "reject": 0},
            "pool_last_control_outcome": "dispatch",
            "pool_last_control_signal": {
                "queue_pending": False,
                "defer_pending": False,
                "reject_pending": False,
            },
            "last_transition": "start",
            "last_transition_at": "2026-03-12T00:00:00Z",
            "start_total": 1,
            "stop_total": 0,
            "restart_total": 0,
            "recommended_poll_after_ms": 5000,
        }
        with self.assertRaises(ContractValidationError):
            validate_runtime_worker_health_contract(payload)

    def test_runtime_worker_drain_contract_valid(self) -> None:
        payload = {
            "fair": True,
            "auto_start": True,
            "max_items": 4,
            "processed": 2,
            "queue_depth_before": 3,
            "queue_depth_after": 1,
            "remaining": 1,
            "should_continue": False,
            "outcome_counts": {"progressed": 2, "missing_run": 0, "skipped": 0},
            "anomaly_counts": {
                "missing_run": 0,
                "skipped": 0,
                "dispatch_retry_deferred": 0,
                "dispatch_retry_failed": 0,
                "total": 0,
            },
            "retry_deferred_total": 0,
            "retry_rejected_total": 0,
            "control_counts": {"queue": 0, "defer": 0, "reject": 0},
            "control_outcome": "dispatch",
            "control_signal": {"queue_pending": False, "defer_pending": False, "reject_pending": False},
            "anomaly_ratio": 0.0,
            "progressed_ratio": 1.0,
            "stalled_signal": False,
            "recommended_poll_after_ms": 1500,
            "scheduling_signal": {
                "queue_depth_before": 3,
                "queue_depth_after": 1,
                "processed": 2,
                "max_items": 4,
                "remaining": 1,
                "should_continue": False,
                "stalled_signal": False,
                "retry_deferred_total": 0,
                "retry_rejected_total": 0,
                "control_counts": {"queue": 0, "defer": 0, "reject": 0},
                "control_outcome": "dispatch",
                "control_signal": {
                    "queue_pending": False,
                    "defer_pending": False,
                    "reject_pending": False,
                },
                "anomaly_ratio": 0.0,
                "recommended_poll_after_ms": 1500,
            },
            "outcomes": [
                {
                    "outcome": "progressed",
                    "fair": True,
                    "auto_start": True,
                    "leased_run_id": "run-1",
                    "recommended_poll_after_ms": 1500,
                }
            ],
            "lease_renew_signal": {
                "attempted": 1,
                "renewed": 1,
                "errors": 0,
                "expired_conflicts": 0,
                "released_conflicts": 0,
                "last_observed_at": "2026-03-12T00:00:00Z",
                "total_attempted": 4,
                "total_renewed": 4,
                "total_errors": 0,
                "total_expired_conflicts": 0,
                "total_released_conflicts": 0,
            },
        }
        validate_runtime_worker_drain_contract(payload)

    def test_runtime_worker_drain_contract_rejects_missing_outcomes(self) -> None:
        payload = {
            "fair": True,
            "auto_start": True,
            "max_items": 4,
            "processed": 2,
            "queue_depth_before": 3,
            "queue_depth_after": 1,
            "remaining": 1,
            "should_continue": False,
            "outcome_counts": {"progressed": 2, "missing_run": 0, "skipped": 0},
            "anomaly_counts": {
                "missing_run": 0,
                "skipped": 0,
                "dispatch_retry_deferred": 0,
                "dispatch_retry_failed": 0,
                "total": 0,
            },
            "retry_deferred_total": 0,
            "retry_rejected_total": 0,
            "control_counts": {"queue": 0, "defer": 0, "reject": 0},
            "control_outcome": "dispatch",
            "control_signal": {"queue_pending": False, "defer_pending": False, "reject_pending": False},
            "anomaly_ratio": 0.0,
            "progressed_ratio": 1.0,
            "stalled_signal": False,
            "recommended_poll_after_ms": 1500,
            "scheduling_signal": {
                "queue_depth_before": 3,
                "queue_depth_after": 1,
                "processed": 2,
                "max_items": 4,
                "remaining": 1,
                "should_continue": False,
                "stalled_signal": False,
                "retry_deferred_total": 0,
                "retry_rejected_total": 0,
                "control_counts": {"queue": 0, "defer": 0, "reject": 0},
                "control_outcome": "dispatch",
                "control_signal": {
                    "queue_pending": False,
                    "defer_pending": False,
                    "reject_pending": False,
                },
                "anomaly_ratio": 0.0,
                "recommended_poll_after_ms": 1500,
            },
        }
        with self.assertRaises(ContractValidationError):
            validate_runtime_worker_drain_contract(payload)

    def test_runtime_worker_status_contract_valid(self) -> None:
        payload = {
            "lifecycle_state": "running",
            "is_running": True,
            "lifecycle_health": "healthy",
            "queue_depth": 0,
            "last_transition": "start",
            "last_transition_at": "2026-03-12T00:00:00Z",
            "last_heartbeat_at": "2026-03-12T00:00:00Z",
            "last_heartbeat_age_seconds": 0.5,
            "is_heartbeat_stale": False,
            "pool_last_control_counts": {"queue": 0, "defer": 0, "reject": 0},
            "pool_last_control_outcome": "dispatch",
            "pool_last_control_signal": {
                "queue_pending": False,
                "defer_pending": False,
                "reject_pending": False,
            },
            "ticks_total": 3,
            "drain_calls_total": 1,
            "start_total": 1,
            "stop_total": 0,
            "restart_total": 0,
            "recommended_poll_after_ms": 5000,
        }
        validate_runtime_worker_status_contract(payload)

    def test_runtime_worker_status_contract_rejects_invalid_lifecycle_health(self) -> None:
        payload = {
            "lifecycle_state": "running",
            "is_running": True,
            "lifecycle_health": "stalled",
            "queue_depth": 0,
            "last_transition": "start",
            "last_transition_at": "2026-03-12T00:00:00Z",
            "last_heartbeat_at": "2026-03-12T00:00:00Z",
            "last_heartbeat_age_seconds": 0.5,
            "is_heartbeat_stale": False,
            "pool_last_control_counts": {"queue": 0, "defer": 0, "reject": 0},
            "pool_last_control_outcome": "dispatch",
            "pool_last_control_signal": {
                "queue_pending": False,
                "defer_pending": False,
                "reject_pending": False,
            },
            "ticks_total": 3,
            "drain_calls_total": 1,
            "start_total": 1,
            "stop_total": 0,
            "restart_total": 0,
            "recommended_poll_after_ms": 5000,
        }
        with self.assertRaises(ContractValidationError):
            validate_runtime_worker_status_contract(payload)


if __name__ == "__main__":
    unittest.main()
