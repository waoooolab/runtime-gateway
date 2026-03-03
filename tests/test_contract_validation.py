from __future__ import annotations

import os
import unittest
from pathlib import Path

from runtime_gateway.contracts.validation import (
    ContractValidationError,
    validate_event_envelope_contract,
    validate_execution_context_contract,
    validate_executor_profile_catalog_contract,
    validate_orchestration_hints_contract,
    validate_token_exchange_contract,
)
from runtime_gateway.events.envelope import build_event_envelope

os.environ["WAOOOOLAB_PLATFORM_CONTRACTS_DIR"] = str(
    Path(__file__).resolve().parent / "fixtures" / "contracts"
)


class ContractValidationTests(unittest.TestCase):
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

    def test_execution_context_contract_valid_runtime_workload(self) -> None:
        payload = {
            "task_plane": "runtime_workload",
            "runtime": {
                "execution_mode": "compute",
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
                    "adapters": ["direct", "ccb"],
                },
                {
                    "family": "workflow_runtime",
                    "engines": ["langgraph"],
                    "adapters": ["api"],
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
                    "adapters": ["tmux"],
                }
            ]
        }
        with self.assertRaises(ContractValidationError):
            validate_executor_profile_catalog_contract(payload)

    def test_orchestration_hints_contract_valid(self) -> None:
        payload = {
            "parent_run_id": "run-parent",
            "parent_task_id": "run-parent:root",
            "priority_class": "high",
            "priority_score": 42,
            "tenant_tier": "enterprise",
            "deadline_at": "2026-03-04T08:00:00Z",
        }
        validate_orchestration_hints_contract(payload)

    def test_orchestration_hints_contract_rejects_orphan_parent_task(self) -> None:
        payload = {
            "parent_task_id": "orphan:root",
        }
        with self.assertRaises(ContractValidationError):
            validate_orchestration_hints_contract(payload)


if __name__ == "__main__":
    unittest.main()
