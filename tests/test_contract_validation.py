from __future__ import annotations

import unittest

from runtime_gateway.contracts.validation import (
    ContractValidationError,
    validate_event_envelope_contract,
    validate_token_exchange_contract,
)
from runtime_gateway.events.envelope import build_event_envelope


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
            app_id="waoooo",
            session_key="tenant:t1:app:waoooo:channel:web:actor:u1:thread:main:agent:pm",
            payload={"run_id": "run-1"},
        )
        envelope["ts"] = "bad-time"
        with self.assertRaises(ContractValidationError):
            validate_event_envelope_contract(envelope)


if __name__ == "__main__":
    unittest.main()
