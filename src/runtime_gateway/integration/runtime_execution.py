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
ClientFactory = Callable[[str, str], "RuntimeExecutionClient"]
_REQUIRE_INTERNAL_TLS_ENV = "OWA_REQUIRE_INTERNAL_TLS"
_REQUIRE_INTERNAL_TLS_ENV_LEGACY = "WAOOOOLAB_REQUIRE_INTERNAL_TLS"
_RUNTIME_ROUTE_TABLE_ENV = "RUNTIME_EXECUTION_ROUTE_TABLE_JSON"
_RUNTIME_ROUTE_TABLE_ENV_LEGACY = "OWA_RUNTIME_EXECUTION_ROUTE_TABLE_JSON"
_DEFAULT_RUNTIME_ID = "runtime-default"


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
    def failure_classification(self) -> str | None:
        if isinstance(self.detail, dict):
            detail_failure = self.detail.get("failure")
            if isinstance(detail_failure, dict):
                normalized = normalize_optional_code_term(detail_failure.get("classification"))
                if isinstance(normalized, str):
                    return normalized
            normalized = normalize_optional_code_term(self.detail.get("failure_classification"))
            if isinstance(normalized, str):
                return normalized
        failure = self.downstream_failure
        if isinstance(failure, dict):
            normalized = normalize_optional_code_term(failure.get("classification"))
            if isinstance(normalized, str):
                return normalized
        if isinstance(self.response_body, dict):
            payload = self.response_body.get("payload")
            if isinstance(payload, dict):
                normalized = normalize_optional_code_term(payload.get("failure_classification"))
                if isinstance(normalized, str):
                    return normalized
        return None

    @property
    def retryable(self) -> bool:
        if isinstance(self.detail, dict):
            detail_retryable = self.detail.get("retryable")
            if isinstance(detail_retryable, bool):
                return detail_retryable
        classification = self.failure_classification
        if isinstance(classification, str) and classification.startswith("capacity"):
            return True
        if isinstance(classification, str) and classification.startswith("policy"):
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


def _env_truthy_with_alias(*, canonical_name: str, legacy_name: str, default: bool = False) -> bool:
    if canonical_name in os.environ:
        return _env_truthy(canonical_name, default=default)
    return _env_truthy(legacy_name, default=default)


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
            raise ValueError(
                "RuntimeExecutionClient base_url must use https when "
                f"{_REQUIRE_INTERNAL_TLS_ENV}=true "
                f"(legacy alias: {_REQUIRE_INTERNAL_TLS_ENV_LEGACY})"
            )
    return normalized.rstrip("/")


@dataclass
class RuntimeExecutionClient:
    base_url: str = field(
        default_factory=lambda: os.environ.get("RUNTIME_EXECUTION_BASE_URL", "http://localhost:8003")
    )
    require_https: bool = field(
        default_factory=lambda: _env_truthy_with_alias(
            canonical_name=_REQUIRE_INTERNAL_TLS_ENV,
            legacy_name=_REQUIRE_INTERNAL_TLS_ENV_LEGACY,
            default=False,
        )
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

    def worker_pool_status(
        self,
        *,
        auth_token: str,
    ) -> dict[str, Any]:
        return self._get_json(
            path="v1/orchestration/worker:pool",
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

    def scheduler_registry(
        self,
        *,
        auth_token: str,
        limit: int = 100,
        cursor: int = 0,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        query: dict[str, Any] = {
            "limit": limit,
            "cursor": cursor,
        }
        if run_id is not None:
            query["run_id"] = run_id
        return self._get_json(
            path="v1/orchestration/scheduler:registry",
            auth_token=auth_token,
            query=query,
        )

    def scheduler_cancel(
        self,
        *,
        auth_token: str,
        run_id: str,
        reason: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"run_id": run_id}
        if reason is not None:
            body["reason"] = reason
        return self._post_json(
            path="v1/orchestration/scheduler:cancel",
            auth_token=auth_token,
            body=body,
        )

    def contract_retirement_status(
        self,
        *,
        auth_token: str,
        version_type: str | None = None,
        version: str | None = None,
    ) -> dict[str, Any]:
        query: dict[str, Any] = {
            "version_type": version_type,
            "version": version,
        }
        return self._get_json(
            path="v1/contracts/retirement:status",
            auth_token=auth_token,
            query=query,
        )

    def contract_retirement_validate(
        self,
        *,
        auth_token: str,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = body if isinstance(body, dict) else {}
        return self._post_json(
            path="v1/contracts/retirement:validate",
            auth_token=auth_token,
            body=payload,
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

    def list_tool_catalog(
        self,
        *,
        auth_token: str,
    ) -> dict[str, Any]:
        return self._get_json(
            path="v1/tools/catalog",
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


def _normalize_runtime_id(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip().lower()


def _normalize_label_values(values: Any) -> set[str]:
    if not isinstance(values, list):
        return set()
    normalized: set[str] = set()
    for item in values:
        token = _normalize_runtime_id(item)
        if token:
            normalized.add(token)
    return normalized


def _normalize_route_entry(raw: Any, *, index: int) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError(f"runtime route entry[{index}] must be object")
    runtime_id = _normalize_runtime_id(raw.get("runtime_id"))
    if not runtime_id:
        raise ValueError(f"runtime route entry[{index}] missing runtime_id")
    base_url = str(raw.get("base_url", "")).strip()
    if not base_url:
        raise ValueError(f"runtime route entry[{index}] missing base_url")
    return {
        "runtime_id": runtime_id,
        "base_url": base_url,
        "default": bool(raw.get("default", False)),
        "tenants": _normalize_label_values(raw.get("tenants")),
        "apps": _normalize_label_values(raw.get("apps")),
    }


def _parse_runtime_route_table(raw_route_table: Any) -> tuple[list[dict[str, Any]], str | None]:
    default_runtime_id: str | None = None
    entries_raw: list[Any] = []
    if isinstance(raw_route_table, list):
        entries_raw = raw_route_table
    elif isinstance(raw_route_table, dict):
        explicit_default = _normalize_runtime_id(
            raw_route_table.get("default_runtime_id") or raw_route_table.get("default")
        )
        if explicit_default:
            default_runtime_id = explicit_default
        runtimes = raw_route_table.get("runtimes")
        if isinstance(runtimes, list):
            entries_raw = runtimes
        elif runtimes is None:
            mapping_entries: list[dict[str, Any]] = []
            for key, value in raw_route_table.items():
                if key in {"default", "default_runtime_id", "runtimes"}:
                    continue
                runtime_id = _normalize_runtime_id(key)
                if not runtime_id:
                    continue
                if isinstance(value, str):
                    mapping_entries.append(
                        {
                            "runtime_id": runtime_id,
                            "base_url": value,
                        }
                    )
                elif isinstance(value, dict):
                    merged = dict(value)
                    merged.setdefault("runtime_id", runtime_id)
                    mapping_entries.append(merged)
            entries_raw = mapping_entries
        else:
            raise ValueError("runtime route table field 'runtimes' must be array")
    elif raw_route_table is None:
        entries_raw = []
    else:
        raise ValueError("runtime route table must be array or object")

    entries = [_normalize_route_entry(item, index=index) for index, item in enumerate(entries_raw)]
    if not default_runtime_id:
        for entry in entries:
            if entry["default"]:
                default_runtime_id = str(entry["runtime_id"])
                break
    return entries, default_runtime_id


def _load_runtime_route_table_from_env() -> tuple[list[dict[str, Any]], str | None]:
    raw = os.environ.get(_RUNTIME_ROUTE_TABLE_ENV)
    if raw is None:
        raw = os.environ.get(_RUNTIME_ROUTE_TABLE_ENV_LEGACY)
    if raw is None or not raw.strip():
        return [], None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("runtime route table env is invalid JSON") from exc
    return _parse_runtime_route_table(parsed)


def _resolve_runtime_id_from_command(command: dict[str, Any]) -> str:
    payload = command.get("payload")
    if not isinstance(payload, dict):
        return ""
    execution_context = payload.get("execution_context")
    if isinstance(execution_context, dict):
        runtime = execution_context.get("runtime")
        if isinstance(runtime, dict):
            candidate = _normalize_runtime_id(runtime.get("runtime_id") or runtime.get("id"))
            if candidate:
                return candidate
    runtime_payload = payload.get("runtime")
    if isinstance(runtime_payload, dict):
        candidate = _normalize_runtime_id(runtime_payload.get("runtime_id") or runtime_payload.get("id"))
        if candidate:
            return candidate
    return _normalize_runtime_id(payload.get("runtime_id"))


def _normalize_runtime_route_labels(command: dict[str, Any]) -> tuple[str | None, str | None]:
    tenant_id = _normalize_runtime_id(command.get("tenant_id"))
    app_id = _normalize_runtime_id(command.get("app_id"))
    return tenant_id or None, app_id or None


def _record_run_route_from_event(*, event: dict[str, Any], runtime_id: str, index: dict[str, str]) -> None:
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return
    run_id = payload.get("run_id")
    if isinstance(run_id, str) and run_id.strip():
        index[run_id.strip()] = runtime_id


def _project_route_target(*, event: dict[str, Any], runtime_id: str) -> None:
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return
    route = payload.get("route")
    if not isinstance(route, dict):
        route = {}
        payload["route"] = route
    if not isinstance(route.get("route_target"), str) or not str(route.get("route_target")).strip():
        route["route_target"] = runtime_id
    if not isinstance(route.get("routing_strategy"), str) or not str(route.get("routing_strategy")).strip():
        route["routing_strategy"] = "runtime_route_table"


class RuntimeExecutionClientPool:
    """Multi-runtime router with run-id sticky mapping on top of RuntimeExecutionClient."""

    def __init__(
        self,
        *,
        route_table: dict[str, Any] | list[dict[str, Any]] | None = None,
        default_runtime_id: str | None = None,
        timeout_seconds: float = 10.0,
        require_https: bool | None = None,
        client_factory: ClientFactory | None = None,
    ) -> None:
        if route_table is None:
            entries, env_default_runtime_id = _load_runtime_route_table_from_env()
        else:
            entries, env_default_runtime_id = _parse_runtime_route_table(route_table)
        configured_default = _normalize_runtime_id(default_runtime_id) or env_default_runtime_id
        fallback_base_url = str(os.environ.get("RUNTIME_EXECUTION_BASE_URL", "http://localhost:8003")).strip()

        if not entries:
            entries = [
                {
                    "runtime_id": _DEFAULT_RUNTIME_ID,
                    "base_url": fallback_base_url,
                    "default": True,
                    "tenants": set(),
                    "apps": set(),
                }
            ]

        if not configured_default:
            configured_default = _normalize_runtime_id(entries[0].get("runtime_id"))
        if configured_default not in {str(item["runtime_id"]) for item in entries}:
            raise ValueError(f"default runtime_id '{configured_default}' not found in route table")

        self._routes = entries
        self._default_runtime_id = configured_default
        self._run_route_index: dict[str, str] = {}

        resolved_require_https = (
            require_https
            if isinstance(require_https, bool)
            else _env_truthy_with_alias(
                canonical_name=_REQUIRE_INTERNAL_TLS_ENV,
                legacy_name=_REQUIRE_INTERNAL_TLS_ENV_LEGACY,
                default=False,
            )
        )
        if client_factory is None:
            client_factory = lambda _runtime_id, base_url: RuntimeExecutionClient(
                base_url=base_url,
                require_https=resolved_require_https,
                timeout_seconds=timeout_seconds,
            )
        self._clients: dict[str, RuntimeExecutionClient] = {}
        for entry in entries:
            runtime_id = str(entry["runtime_id"])
            self._clients[runtime_id] = client_factory(runtime_id, str(entry["base_url"]))
        self._default_client = self._clients[self._default_runtime_id]

    @property
    def base_url(self) -> str:
        return self._default_client.base_url

    @property
    def default_runtime_id(self) -> str:
        return self._default_runtime_id

    def list_runtime_routes(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for entry in self._routes:
            rows.append(
                {
                    "runtime_id": str(entry["runtime_id"]),
                    "base_url": str(entry["base_url"]),
                    "default": str(entry["runtime_id"]) == self._default_runtime_id,
                    "tenants": sorted(str(item) for item in set(entry["tenants"])),
                    "apps": sorted(str(item) for item in set(entry["apps"])),
                }
            )
        return rows

    def _client_for_runtime_id(self, runtime_id: str) -> RuntimeExecutionClient:
        client = self._clients.get(runtime_id)
        if client is None:
            raise RuntimeExecutionClientError(
                f"unknown runtime target: {runtime_id}",
                status_code=422,
                detail={
                    "error": "runtime_target_unknown",
                    "category": "validation",
                    "code": "runtime_target_unknown",
                    "retryable": False,
                    "runtime_id": runtime_id,
                },
            )
        return client

    def _resolve_runtime_id_for_command(self, command: dict[str, Any]) -> str:
        explicit_runtime_id = _resolve_runtime_id_from_command(command)
        if explicit_runtime_id:
            if explicit_runtime_id in self._clients:
                return explicit_runtime_id
            raise RuntimeExecutionClientError(
                f"unknown runtime target: {explicit_runtime_id}",
                status_code=422,
                detail={
                    "error": "runtime_target_unknown",
                    "category": "validation",
                    "code": "runtime_target_unknown",
                    "retryable": False,
                    "runtime_id": explicit_runtime_id,
                },
            )

        tenant_id, app_id = _normalize_runtime_route_labels(command)
        if tenant_id:
            for entry in self._routes:
                if tenant_id in set(entry["tenants"]):
                    return str(entry["runtime_id"])
        if app_id:
            for entry in self._routes:
                if app_id in set(entry["apps"]):
                    return str(entry["runtime_id"])
        return self._default_runtime_id

    def _resolve_runtime_id_for_run(self, run_id: str) -> str:
        normalized_run_id = run_id.strip()
        if not normalized_run_id:
            return self._default_runtime_id
        return self._run_route_index.get(normalized_run_id, self._default_runtime_id)

    def _run_scoped_call(self, *, run_id: str, method_name: str, **kwargs: Any) -> dict[str, Any]:
        runtime_id = self._resolve_runtime_id_for_run(run_id)
        client = self._client_for_runtime_id(runtime_id)
        method = getattr(client, method_name)
        response = method(run_id=run_id, **kwargs)
        if isinstance(response, dict):
            _record_run_route_from_event(event=response, runtime_id=runtime_id, index=self._run_route_index)
        return response

    def submit_command(self, *, envelope: dict[str, Any], auth_token: str) -> dict[str, Any]:
        runtime_id = self._resolve_runtime_id_for_command(envelope)
        client = self._client_for_runtime_id(runtime_id)
        try:
            response = client.submit_command(envelope=envelope, auth_token=auth_token)
        except RuntimeExecutionClientError as exc:
            if isinstance(exc.response_body, dict):
                _project_route_target(event=exc.response_body, runtime_id=runtime_id)
                _record_run_route_from_event(
                    event=exc.response_body,
                    runtime_id=runtime_id,
                    index=self._run_route_index,
                )
            raise
        _project_route_target(event=response, runtime_id=runtime_id)
        _record_run_route_from_event(event=response, runtime_id=runtime_id, index=self._run_route_index)
        return response

    def approve_run(self, *, run_id: str, auth_token: str) -> dict[str, Any]:
        return self._run_scoped_call(run_id=run_id, method_name="approve_run", auth_token=auth_token)

    def reject_run(self, *, run_id: str, auth_token: str) -> dict[str, Any]:
        return self._run_scoped_call(run_id=run_id, method_name="reject_run", auth_token=auth_token)

    def complete_run(
        self,
        *,
        run_id: str,
        auth_token: str,
        success: bool,
        failure_reason_code: str | None = None,
    ) -> dict[str, Any]:
        return self._run_scoped_call(
            run_id=run_id,
            method_name="complete_run",
            auth_token=auth_token,
            success=success,
            failure_reason_code=failure_reason_code,
        )

    def renew_run_lease(
        self,
        *,
        run_id: str,
        auth_token: str,
        lease_ttl_seconds: int | None = None,
    ) -> dict[str, Any]:
        return self._run_scoped_call(
            run_id=run_id,
            method_name="renew_run_lease",
            auth_token=auth_token,
            lease_ttl_seconds=lease_ttl_seconds,
        )

    def cancel_run(
        self,
        *,
        run_id: str,
        auth_token: str,
        reason: str | None = None,
        cascade_children: bool | None = None,
        canceled_by_run_id: str | None = None,
    ) -> dict[str, Any]:
        return self._run_scoped_call(
            run_id=run_id,
            method_name="cancel_run",
            auth_token=auth_token,
            reason=reason,
            cascade_children=cascade_children,
            canceled_by_run_id=canceled_by_run_id,
        )

    def timeout_run(
        self,
        *,
        run_id: str,
        auth_token: str,
        reason: str | None = None,
        cascade_children: bool | None = None,
        timed_out_by_run_id: str | None = None,
    ) -> dict[str, Any]:
        return self._run_scoped_call(
            run_id=run_id,
            method_name="timeout_run",
            auth_token=auth_token,
            reason=reason,
            cascade_children=cascade_children,
            timed_out_by_run_id=timed_out_by_run_id,
        )

    def preempt_run(
        self,
        *,
        run_id: str,
        auth_token: str,
        reason: str | None = None,
        cascade_children: bool | None = None,
        preempted_by_run_id: str | None = None,
    ) -> dict[str, Any]:
        return self._run_scoped_call(
            run_id=run_id,
            method_name="preempt_run",
            auth_token=auth_token,
            reason=reason,
            cascade_children=cascade_children,
            preempted_by_run_id=preempted_by_run_id,
        )

    def get_run_status(self, *, run_id: str, auth_token: str) -> dict[str, Any]:
        return self._run_scoped_call(run_id=run_id, method_name="get_run_status", auth_token=auth_token)

    def get_run_lease(self, *, run_id: str, auth_token: str) -> dict[str, Any]:
        return self._run_scoped_call(run_id=run_id, method_name="get_run_lease", auth_token=auth_token)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._default_client, name)
