"""runtime-execution API boundary client for runtime-gateway."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable
from urllib.parse import urlencode, urljoin, urlparse

from runtime_gateway.code_terms import normalize_optional_code_term

TransportCallable = Callable[..., Any]


class RuntimeExecutionClientError(RuntimeError):
    """Raised when runtime-execution boundary call fails."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        response_body: dict[str, Any] | None = None,
        detail: Any = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body
        self.detail = detail

    @property
    def downstream_failure(self) -> dict[str, Any] | None:
        if not isinstance(self.response_body, dict):
            return None
        payload = self.response_body.get("payload")
        if not isinstance(payload, dict):
            return None
        failure = payload.get("failure")
        if not isinstance(failure, dict):
            return None
        return failure

    @property
    def retryable(self) -> bool:
        if isinstance(self.detail, dict):
            detail_retryable = self.detail.get("retryable")
            if isinstance(detail_retryable, bool):
                return detail_retryable
        failure = self.downstream_failure
        if isinstance(failure, dict):
            normalized_classification = normalize_optional_code_term(failure.get("classification"))
            if isinstance(normalized_classification, str) and normalized_classification.startswith("capacity"):
                return True
            if isinstance(normalized_classification, str) and normalized_classification.startswith("policy"):
                return False
        return self.status_code in {408, 425, 429, 500, 502, 503, 504}


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


def _summarize_detail(detail: Any, fallback_text: str) -> str:
    if isinstance(detail, str) and detail.strip():
        return detail.strip()
    if isinstance(detail, dict):
        category = detail.get("category")
        code = detail.get("code")
        message = detail.get("message")
        parts = [str(item) for item in (category, code, message) if isinstance(item, str) and item]
        if parts:
            return " | ".join(parts)
    if fallback_text:
        return fallback_text
    return "unknown error"


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
    failure_reason_code: str | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {}
    if reason is not None:
        body["reason"] = reason
    if cascade_children is not None:
        body["cascade_children"] = cascade_children
    if success is not None:
        body["success"] = success
    if failure_reason_code is not None:
        body["failure_reason_code"] = failure_reason_code
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
    response_body = _try_parse_json_object(text)
    detail = response_body.get("detail") if isinstance(response_body, dict) else None
    return RuntimeExecutionClientError(
        f"HTTP {exc.code} calling {url}: {_summarize_detail(detail, text)}",
        status_code=int(exc.code),
        response_body=response_body,
        detail=detail,
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


def _env_truthy(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _normalize_base_url(base_url: str, *, require_https: bool) -> str:
    normalized = base_url.strip()
    if not normalized:
        raise ValueError("RuntimeExecutionClient base_url must not be empty")
    parsed = urlparse(normalized)
    if not parsed.netloc:
        raise ValueError("RuntimeExecutionClient base_url must include host")
    if parsed.query or parsed.fragment:
        raise ValueError("RuntimeExecutionClient base_url must not include query or fragment")
    if parsed.path not in {"", "/"}:
        raise ValueError("RuntimeExecutionClient base_url must not include path")
    if require_https:
        scheme = parsed.scheme.strip().lower()
        if scheme != "https":
            raise ValueError("RuntimeExecutionClient base_url must use https when WAOOOOLAB_REQUIRE_INTERNAL_TLS=true")
    return normalized.rstrip("/")


@dataclass
class RuntimeExecutionClient:
    base_url: str = field(
        default_factory=lambda: os.environ.get("RUNTIME_EXECUTION_BASE_URL", "http://localhost:8003")
    )
    require_https: bool = field(
        default_factory=lambda: _env_truthy("WAOOOOLAB_REQUIRE_INTERNAL_TLS", default=False)
    )
    timeout_seconds: float = 10.0
    _transport: TransportCallable = field(default=urllib.request.urlopen)

    def __post_init__(self) -> None:
        self.base_url = _normalize_base_url(self.base_url, require_https=self.require_https)

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
            reason = str(exc.reason)
            raise RuntimeExecutionClientError(
                f"connection error calling {url}: {reason}",
                status_code=503,
                detail={
                    "error": "upstream_transport_error",
                    "category": "upstream_unavailable",
                    "code": "upstream_connection_error",
                    "retryable": True,
                    "message": reason,
                },
            ) from exc

        if status >= 400:
            response_body = _try_parse_json_object(raw)
            detail = response_body.get("detail") if isinstance(response_body, dict) else None
            raise RuntimeExecutionClientError(
                f"HTTP {status} calling {url}: {_summarize_detail(detail, raw)}",
                status_code=status,
                response_body=response_body,
                detail=detail,
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
        failure_reason_code: str | None = None,
    ) -> dict[str, Any]:
        body = _build_run_control_body(
            success=success,
            failure_reason_code=failure_reason_code,
        )
        return self._post_json(path=f"v1/runs/{run_id}:complete", auth_token=auth_token, body=body)

    def renew_run_lease(
        self,
        *,
        run_id: str,
        auth_token: str,
        lease_ttl_seconds: int | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if lease_ttl_seconds is not None:
            body["lease_ttl_seconds"] = lease_ttl_seconds
        return self._post_json(path=f"v1/runs/{run_id}:lease-renew", auth_token=auth_token, body=body)

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

    def preempt_run(
        self,
        *,
        run_id: str,
        auth_token: str,
        reason: str | None = None,
        cascade_children: bool | None = None,
        preempted_by_run_id: str | None = None,
    ) -> dict[str, Any]:
        body = _build_run_control_body(
            reason=reason,
            cascade_children=cascade_children,
            by_run_key="preempted_by_run_id",
            by_run_id=preempted_by_run_id,
        )
        return self._post_json(path=f"v1/runs/{run_id}:preempt", auth_token=auth_token, body=body)

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

    def worker_loop(
        self,
        *,
        auth_token: str,
        scheduler_max_items: int = 32,
        scheduler_fair: bool = True,
        worker_max_items: int = 16,
        worker_fair: bool = True,
        auto_start: bool = True,
    ) -> dict[str, Any]:
        return self._post_json(
            path="v1/orchestration/worker:loop",
            auth_token=auth_token,
            body={},
            query={
                "scheduler_max_items": scheduler_max_items,
                "scheduler_fair": scheduler_fair,
                "worker_max_items": worker_max_items,
                "worker_fair": worker_fair,
                "auto_start": auto_start,
            },
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

    def worker_start(
        self,
        *,
        auth_token: str,
        reason: str | None = None,
    ) -> dict[str, Any]:
        body = {"reason": reason} if reason is not None else {}
        return self._post_json(
            path="v1/orchestration/worker:start",
            auth_token=auth_token,
            body=body,
        )

    def worker_stop(
        self,
        *,
        auth_token: str,
        reason: str | None = None,
    ) -> dict[str, Any]:
        body = {"reason": reason} if reason is not None else {}
        return self._post_json(
            path="v1/orchestration/worker:stop",
            auth_token=auth_token,
            body=body,
        )

    def worker_restart(
        self,
        *,
        auth_token: str,
        reason: str | None = None,
    ) -> dict[str, Any]:
        body = {"reason": reason} if reason is not None else {}
        return self._post_json(
            path="v1/orchestration/worker:restart",
            auth_token=auth_token,
            body=body,
        )

    def worker_status(
        self,
        *,
        auth_token: str,
    ) -> dict[str, Any]:
        return self._get_json(
            path="v1/orchestration/worker:status",
            auth_token=auth_token,
        )

    def scheduler_enqueue(
        self,
        *,
        auth_token: str,
        run_id: str,
        due_at: str | None = None,
        delay_ms: int | None = None,
        reason: str | None = None,
        misfire_policy: str | None = None,
        misfire_grace_ms: int | None = None,
        cron_interval_ms: int | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"run_id": run_id}
        if due_at is not None:
            body["due_at"] = due_at
        if delay_ms is not None:
            body["delay_ms"] = delay_ms
        if reason is not None:
            body["reason"] = reason
        if misfire_policy is not None:
            body["misfire_policy"] = misfire_policy
        if misfire_grace_ms is not None:
            body["misfire_grace_ms"] = misfire_grace_ms
        if cron_interval_ms is not None:
            body["cron_interval_ms"] = cron_interval_ms
        return self._post_json(
            path="v1/orchestration/scheduler:enqueue",
            auth_token=auth_token,
            body=body,
        )

    def scheduler_tick(
        self,
        *,
        auth_token: str,
        max_items: int = 32,
        fair: bool = True,
    ) -> dict[str, Any]:
        return self._post_json(
            path="v1/orchestration/scheduler:tick",
            auth_token=auth_token,
            body={},
            query={"max_items": max_items, "fair": fair},
        )

    def scheduler_health(
        self,
        *,
        auth_token: str,
    ) -> dict[str, Any]:
        return self._get_json(
            path="v1/orchestration/scheduler:health",
            auth_token=auth_token,
        )

    def get_run_status(
        self,
        *,
        run_id: str,
        auth_token: str,
    ) -> dict[str, Any]:
        return self._get_json(
            path=f"v1/runs/{run_id}",
            auth_token=auth_token,
        )

    def get_run_lease(
        self,
        *,
        run_id: str,
        auth_token: str,
    ) -> dict[str, Any]:
        return self._get_json(
            path=f"v1/runs/{run_id}/lease",
            auth_token=auth_token,
        )

    def register_capability(
        self,
        *,
        auth_token: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return self._post_json(
            path="v1/capabilities/register",
            auth_token=auth_token,
            body=payload,
        )

    def list_capabilities(
        self,
        *,
        auth_token: str,
    ) -> dict[str, Any]:
        return self._get_json(
            path="v1/capabilities",
            auth_token=auth_token,
        )

    def get_capability(
        self,
        *,
        capability_id: str,
        auth_token: str,
    ) -> dict[str, Any]:
        return self._get_json(
            path=f"v1/capabilities/{capability_id}",
            auth_token=auth_token,
        )

    def resolve_capability(
        self,
        *,
        auth_token: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return self._post_json(
            path="v1/capabilities/resolve",
            auth_token=auth_token,
            body=payload,
        )

    def compile_capability(
        self,
        *,
        auth_token: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return self._post_json(
            path="v1/capabilities/compile",
            auth_token=auth_token,
            body=payload,
        )

    def publish_capability(
        self,
        *,
        auth_token: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return self._post_json(
            path="v1/capabilities/publish",
            auth_token=auth_token,
            body=payload,
        )

    def invoke_capability(
        self,
        *,
        capability_id: str,
        auth_token: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return self._post_json(
            path=f"v1/capabilities/{capability_id}:invoke",
            auth_token=auth_token,
            body=payload,
        )
