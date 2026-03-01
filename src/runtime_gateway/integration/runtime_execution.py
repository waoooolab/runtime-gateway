"""runtime-execution API boundary client for runtime-gateway."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable
from urllib.parse import urljoin

TransportCallable = Callable[..., Any]


class RuntimeExecutionClientError(RuntimeError):
    """Raised when runtime-execution boundary call fails."""


@dataclass
class RuntimeExecutionClient:
    base_url: str = field(
        default_factory=lambda: os.environ.get("RUNTIME_EXECUTION_BASE_URL", "http://localhost:8003")
    )
    timeout_seconds: float = 10.0
    _transport: TransportCallable = field(default=urllib.request.urlopen)

    def submit_command(self, *, envelope: dict[str, Any], auth_token: str) -> dict[str, Any]:
        url = urljoin(self.base_url.rstrip("/") + "/", "v1/runs")
        body = json.dumps(envelope, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {auth_token}",
        }

        request = urllib.request.Request(url=url, method="POST", data=body, headers=headers)
        try:
            with self._transport(request, timeout=self.timeout_seconds) as response:
                status = int(response.getcode())
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            text = exc.read().decode("utf-8", errors="replace")
            raise RuntimeExecutionClientError(f"HTTP {exc.code} calling {url}: {text}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeExecutionClientError(f"connection error calling {url}: {exc.reason}") from exc

        if status >= 400:
            raise RuntimeExecutionClientError(f"HTTP {status} calling {url}")

        if not raw:
            return {}

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeExecutionClientError(f"invalid json response from {url}") from exc

        if not isinstance(parsed, dict):
            raise RuntimeExecutionClientError(f"response body from {url} must be object")
        return parsed
