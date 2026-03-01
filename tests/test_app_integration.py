from __future__ import annotations

import sys
import unittest
from pathlib import Path

WAOOOOLAB_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_EXECUTION_SRC = WAOOOOLAB_ROOT / "runtime-execution" / "src"
if RUNTIME_EXECUTION_SRC.exists():
    runtime_execution_src = str(RUNTIME_EXECUTION_SRC)
    if runtime_execution_src not in sys.path:
        sys.path.insert(0, runtime_execution_src)

try:
    from fastapi.testclient import TestClient
    from runtime_gateway.app import app
except ModuleNotFoundError:
    FASTAPI_STACK_AVAILABLE = False
else:
    FASTAPI_STACK_AVAILABLE = True

from runtime_gateway.audit.emitter import clear_audit_events
from runtime_gateway.auth.tokens import issue_token


@unittest.skipUnless(FASTAPI_STACK_AVAILABLE, "fastapi stack not installed")
class AppIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_audit_events()
        self.client = TestClient(app)
        self.payload = {
            "tenant_id": "t1",
            "app_id": "waoooo",
            "session_key": "tenant:t1:app:waoooo:channel:web:actor:u1:thread:main:agent:pm",
            "payload": {"goal": "build feature"},
        }

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
