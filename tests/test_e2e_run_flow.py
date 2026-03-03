from __future__ import annotations

import io
import importlib
import os
import sys
import unittest
import urllib.error
from pathlib import Path
from urllib.parse import urlsplit

os.environ["WAOOOOLAB_PLATFORM_CONTRACTS_DIR"] = str(
    Path(__file__).resolve().parent / "fixtures" / "contracts"
)

try:
    from fastapi.testclient import TestClient
    from runtime_gateway import app as gateway_app_module
    from runtime_gateway.auth.tokens import issue_token
    from runtime_gateway.integration import RuntimeExecutionClient
except ModuleNotFoundError:
    FASTAPI_STACK_AVAILABLE = False
else:
    FASTAPI_STACK_AVAILABLE = True

_RUNTIME_EXECUTION_SRC = os.environ.get("WAOOOOLAB_RUNTIME_EXECUTION_SRC_DIR")
if _RUNTIME_EXECUTION_SRC:
    candidate_src = Path(_RUNTIME_EXECUTION_SRC)
else:
    candidate_src = Path(__file__).resolve().parents[2] / "runtime-execution" / "src"

if candidate_src.exists():
    sys.path.insert(0, str(candidate_src))

try:
    from runtime_execution.service import RuntimeExecutionService
except Exception:
    RUNTIME_EXECUTION_AVAILABLE = False
else:
    execution_app_module = importlib.import_module("runtime_execution.service_api.app")
    RUNTIME_EXECUTION_AVAILABLE = True


class _ProxyResponse:
    def __init__(self, status: int, body: bytes):
        self._status = status
        self._body = body

    def __enter__(self) -> "_ProxyResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        _ = exc_type, exc, tb

    def getcode(self) -> int:
        return self._status

    def read(self) -> bytes:
        return self._body


@unittest.skipUnless(
    FASTAPI_STACK_AVAILABLE and RUNTIME_EXECUTION_AVAILABLE,
    "fastapi/runtime-execution stack not available",
)
class EndToEndRunFlowTests(unittest.TestCase):
    def setUp(self) -> None:
        self._gateway_original_client = gateway_app_module._execution_client
        self._execution_original_runtime = execution_app_module._runtime

        execution_app_module._runtime = RuntimeExecutionService()
        self.execution_client = TestClient(execution_app_module.app)

        def transport(request, timeout=10.0):
            _ = timeout
            parsed = urlsplit(request.full_url)
            path = parsed.path
            if parsed.query:
                path = f"{path}?{parsed.query}"

            body = request.data if request.data is not None else b""
            headers = dict(request.header_items())
            response = self.execution_client.request(
                method=request.get_method(),
                url=path,
                content=body,
                headers=headers,
            )
            if response.status_code >= 400:
                raise urllib.error.HTTPError(
                    request.full_url,
                    response.status_code,
                    "upstream error",
                    hdrs=None,
                    fp=io.BytesIO(response.content),
                )
            return _ProxyResponse(response.status_code, response.content)

        gateway_app_module._execution_client = RuntimeExecutionClient(
            base_url="http://runtime-execution.local",
            _transport=transport,
        )
        self.gateway_client = TestClient(gateway_app_module.app)

    def tearDown(self) -> None:
        gateway_app_module._execution_client = self._gateway_original_client
        execution_app_module._runtime = self._execution_original_runtime

    def _gateway_token(self, scope: list[str]) -> str:
        return issue_token(
            {
                "iss": "runtime-gateway",
                "sub": "user:u-e2e",
                "aud": "runtime-gateway",
                "tenant_id": "t1",
                "app_id": "waoooo",
                "scope": scope,
                "token_use": "access",
                "trace_id": "trace-e2e-1",
                "session_key": "tenant:t1:app:waoooo:channel:web:actor:u-e2e:thread:main:agent:pm",
            },
            ttl_seconds=300,
        )

    def _execution_token(self, scope: list[str]) -> str:
        return issue_token(
            {
                "iss": "runtime-gateway",
                "sub": "svc:runtime-gateway",
                "aud": "runtime-execution",
                "tenant_id": "t1",
                "app_id": "waoooo",
                "scope": scope,
                "token_use": "service",
                "trace_id": "trace-e2e-1",
                "session_key": "tenant:t1:app:waoooo:channel:web:actor:u-e2e:thread:main:agent:pm",
            },
            ttl_seconds=300,
        )

    def test_gateway_to_execution_e2e_run_flow(self) -> None:
        token = self._gateway_token(["runs:write"])
        payload = {
            "tenant_id": "t1",
            "app_id": "waoooo",
            "session_key": "tenant:t1:app:waoooo:channel:web:actor:u-e2e:thread:main:agent:pm",
            "payload": {"goal": "verify e2e run flow"},
        }

        response = self.gateway_client.post(
            "/v1/runs",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "queued")
        run_id = data["run_id"]

        self.assertIn(run_id, execution_app_module._runtime.runs)
        run = execution_app_module._runtime.runs[run_id]
        self.assertEqual(run.trace_id, "trace-e2e-1")
        self.assertEqual(run.payload, {"goal": "verify e2e run flow"})

        read_response = self.execution_client.get(
            f"/v1/runs/{run_id}",
            headers={"Authorization": f"Bearer {self._execution_token(['runs:read'])}"},
        )
        self.assertEqual(read_response.status_code, 200)
        event = read_response.json()
        self.assertEqual(event["event_type"], "runtime.run.status")
        self.assertEqual(event["payload"]["run_id"], run_id)
        self.assertEqual(event["payload"]["status"], "queued")

    def test_gateway_and_execution_profile_catalog_are_aligned(self) -> None:
        gateway_response = self.gateway_client.get(
            "/v1/executors/profiles",
            headers={"Authorization": f"Bearer {self._gateway_token(['runs:read'])}"},
        )
        self.assertEqual(gateway_response.status_code, 200)

        execution_response = self.execution_client.get(
            "/v1/executors/profiles",
            headers={"Authorization": f"Bearer {self._execution_token(['runs:read'])}"},
        )
        self.assertEqual(execution_response.status_code, 200)

        gateway_items = gateway_response.json()["items"]
        execution_items = execution_response.json()["items"]

        normalize = lambda items: sorted(  # noqa: E731
            (
                item["family"],
                tuple(item["engines"]),
                tuple(item["adapters"]),
            )
            for item in items
        )
        self.assertEqual(normalize(gateway_items), normalize(execution_items))


if __name__ == "__main__":
    unittest.main()
