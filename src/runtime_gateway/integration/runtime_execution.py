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

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        response_body: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


def _try_parse_json_object(raw: str) -> dict[str, Any] | None:
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if isinstance(parsed, dict):
        return parsed
    return None


def _build_request(url: str, envelope: dict[str, Any], auth_token: str) -> urllib.request.Request:
    body = json.dumps(envelope, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {auth_token}",
    }
    return urllib.request.Request(url=url, method="POST", data=body, headers=headers)


def _parse_http_error(exc: urllib.error.HTTPError, url: str) -> RuntimeExecutionClientError:
    text = exc.read().decode("utf-8", errors="replace")
    return RuntimeExecutionClientError(
        f"HTTP {exc.code} calling {url}: {text}",
        status_code=int(exc.code),
        response_body=_try_parse_json_object(text),
    )


def _parse_response_body(raw: str, url: str) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeExecutionClientError(f"invalid json response from {url}") from exc
    if not isinstance(parsed, dict):
        raise RuntimeExecutionClientError(f"response body from {url} must be object")
    return parsed


@dataclass
class RuntimeExecutionClient:
    base_url: str = field(
        default_factory=lambda: os.environ.get("RUNTIME_EXECUTION_BASE_URL", "http://localhost:8003")
    )
    timeout_seconds: float = 10.0
    _transport: TransportCallable = field(default=urllib.request.urlopen)

    def submit_command(self, *, envelope: dict[str, Any], auth_token: str) -> dict[str, Any]:
        url = urljoin(self.base_url.rstrip("/") + "/", "v1/runs")
        request = _build_request(url, envelope, auth_token)
        try:
            with self._transport(request, timeout=self.timeout_seconds) as response:
                status = int(response.getcode())
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            raise _parse_http_error(exc, url) from exc
        except urllib.error.URLError as exc:
            raise RuntimeExecutionClientError(f"connection error calling {url}: {exc.reason}") from exc

        if status >= 400:
            raise RuntimeExecutionClientError(
                f"HTTP {status} calling {url}",
                status_code=status,
                response_body=_try_parse_json_object(raw),
            )
        return _parse_response_body(raw, url)
