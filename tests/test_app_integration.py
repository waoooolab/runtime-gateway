from __future__ import annotations

import unittest
from dataclasses import dataclass, field

from runtime_gateway.audit.emitter import clear_audit_events
from runtime_gateway.auth.tokens import issue_token

try:
    from fastapi.testclient import TestClient
    from runtime_gateway import app as gateway_app_module
except ModuleNotFoundError:
    FASTAPI_STACK_AVAILABLE = False
else:
    FASTAPI_STACK_AVAILABLE = True


@dataclass
class _FakeRunStatus:
    value: str = "queued"


@dataclass
class _FakeRunRecord:
    run_id: str = "run-test-integration"
    status: _FakeRunStatus = field(default_factory=_FakeRunStatus)


class _FakeRuntimeExecutionService:
    def submit_run(self, *, session_key: str, payload: dict) -> _FakeRunRecord:
        _ = session_key, payload
        return _FakeRunRecord()


@unittest.skipUnless(FASTAPI_STACK_AVAILABLE, "fastapi stack not installed")
class AppIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_audit_events()
        self._original_service = gateway_app_module._service
        gateway_app_module._service = _FakeRuntimeExecutionService()
        self.client = TestClient(gateway_app_module.app)
        self.payload = {
            "tenant_id": "t1",
            "app_id": "waoooo",
            "session_key": "tenant:t1:app:waoooo:channel:web:actor:u1:thread:main:agent:pm",
            "payload": {"goal": "build feature"},
        }

    def tearDown(self) -> None:
        gateway_app_module._service = self._original_service

    def _token(self, audience: str, scope: list[str]) -> str:
        return issue_token(
            {
                "iss": "runtime-gateway",
                "sub": "user:u1",
                "aud": audience,
                "tenant_id": "t1",
                "app_id": "waoooo",
                "scope": scope,
                "token_use": "access",
                "trace_id": "trace-1",
            },
            ttl_seconds=300,
        )

    def test_runs_requires_bearer_token(self) -> None:
        response = self.client.post("/v1/runs", json=self.payload)
        self.assertEqual(response.status_code, 401)

    def test_runs_rejects_wrong_audience(self) -> None:
        token = self._token(audience="device-hub", scope=["runs:write"])
        response = self.client.post(
            "/v1/runs",
            json=self.payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 401)

    def test_runs_rejects_missing_scope(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:read"])
        response = self.client.post(
            "/v1/runs",
            json=self.payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 403)

    def test_runs_accepts_valid_token(self) -> None:
        token = self._token(audience="runtime-gateway", scope=["runs:write"])
        response = self.client.post(
            "/v1/runs",
            json=self.payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("run_id", data)
        self.assertEqual(data["status"], "queued")


if __name__ == "__main__":
    unittest.main()
