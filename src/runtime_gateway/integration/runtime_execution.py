"""runtime-execution API boundary client for runtime-gateway."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable
from urllib.parse import urlencode, urljoin

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


def _build_request(
    *,
    url: str,
    auth_token: str,
    method: str,
    envelope: dict[str, Any] | None = None,
) -> urllib.request.Request:
    body: bytes | None = None
    if envelope is not None:
        body = json.dumps(envelope, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {auth_token}",
    }
    return urllib.request.Request(url=url, method=method, data=body, headers=headers)


def _build_run_control_body(
    *,
    reason: str | None = None,
    cascade_children: bool | None = None,
    by_run_key: str | None = None,
    by_run_id: str | None = None,
    success: bool | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {}
    if reason is not None:
        body["reason"] = reason
    if cascade_children is not None:
        body["cascade_children"] = cascade_children
    if success is not None:
        body["success"] = success
    if by_run_key is not None and by_run_id is not None:
        body[by_run_key] = by_run_id
    return body


def _compose_url(
    *,
    base_url: str,
    path: str,
    query: dict[str, Any] | None = None,
) -> str:
    url = urljoin(base_url.rstrip("/") + "/", path)
    if not query:
        return url
    normalized: dict[str, str] = {}
    for key, value in query.items():
        if value is None:
            continue
        if isinstance(value, bool):
            normalized[key] = "true" if value else "false"
            continue
        normalized[key] = str(value)
    if not normalized:
        return url
    return f"{url}?{urlencode(normalized)}"


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

    def _request_json(
        self,
        *,
        method: str,
        path: str,
        auth_token: str,
        body: dict[str, Any] | None = None,
        query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = _compose_url(base_url=self.base_url, path=path, query=query)
        request = _build_request(
            url=url,
            auth_token=auth_token,
            method=method,
            envelope=body,
        )
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

    def _post_json(
        self,
        *,
        path: str,
        auth_token: str,
        body: dict[str, Any] | None = None,
        query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._request_json(
            method="POST",
            path=path,
            auth_token=auth_token,
            body=body,
            query=query,
        )

    def _get_json(
        self,
        *,
        path: str,
        auth_token: str,
        query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._request_json(
            method="GET",
            path=path,
            auth_token=auth_token,
            body=None,
            query=query,
        )

    def submit_command(self, *, envelope: dict[str, Any], auth_token: str) -> dict[str, Any]:
        return self._post_json(path="v1/runs", auth_token=auth_token, body=envelope)

    def approve_run(self, *, run_id: str, auth_token: str) -> dict[str, Any]:
        return self._post_json(path=f"v1/runs/{run_id}:approve", auth_token=auth_token, body={})

    def reject_run(self, *, run_id: str, auth_token: str) -> dict[str, Any]:
        return self._post_json(path=f"v1/runs/{run_id}:reject", auth_token=auth_token, body={})

    def complete_run(
        self,
        *,
        run_id: str,
        auth_token: str,
        success: bool,
    ) -> dict[str, Any]:
        body = _build_run_control_body(success=success)
        return self._post_json(path=f"v1/runs/{run_id}:complete", auth_token=auth_token, body=body)

    def cancel_run(
        self,
        *,
        run_id: str,
        auth_token: str,
        reason: str | None = None,
        cascade_children: bool | None = None,
        canceled_by_run_id: str | None = None,
    ) -> dict[str, Any]:
        body = _build_run_control_body(
            reason=reason,
            cascade_children=cascade_children,
            by_run_key="canceled_by_run_id",
            by_run_id=canceled_by_run_id,
        )
        return self._post_json(path=f"v1/runs/{run_id}:cancel", auth_token=auth_token, body=body)

    def timeout_run(
        self,
        *,
        run_id: str,
        auth_token: str,
        reason: str | None = None,
        cascade_children: bool | None = None,
        timed_out_by_run_id: str | None = None,
    ) -> dict[str, Any]:
        body = _build_run_control_body(
            reason=reason,
            cascade_children=cascade_children,
            by_run_key="timed_out_by_run_id",
            by_run_id=timed_out_by_run_id,
        )
        return self._post_json(path=f"v1/runs/{run_id}:timeout", auth_token=auth_token, body=body)

    def worker_tick(
        self,
        *,
        auth_token: str,
        fair: bool = True,
        auto_start: bool = True,
    ) -> dict[str, Any]:
        return self._post_json(
            path="v1/orchestration/worker:tick",
            auth_token=auth_token,
            body={},
            query={"fair": fair, "auto_start": auto_start},
        )

    def worker_drain(
        self,
        *,
        auth_token: str,
        max_items: int = 16,
        fair: bool = True,
        auto_start: bool = True,
    ) -> dict[str, Any]:
        return self._post_json(
            path="v1/orchestration/worker:drain",
            auth_token=auth_token,
            body={},
            query={"max_items": max_items, "fair": fair, "auto_start": auto_start},
        )

    def worker_health(
        self,
        *,
        auth_token: str,
    ) -> dict[str, Any]:
        return self._get_json(
            path="v1/orchestration/worker:health",
            auth_token=auth_token,
        )
