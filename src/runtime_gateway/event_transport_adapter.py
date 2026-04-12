"""Adapter seam for runtime-gateway external event transport integrations."""

from __future__ import annotations

import json
import os
from typing import Any, Protocol
import urllib.error
import urllib.request

EVENT_TRANSPORT_ADAPTER_HTTP = "http"
DEFAULT_EVENT_TRANSPORT_ADAPTER = EVENT_TRANSPORT_ADAPTER_HTTP
SUPPORTED_EVENT_TRANSPORT_ADAPTERS = frozenset(
    {
        EVENT_TRANSPORT_ADAPTER_HTTP,
    }
)

EVENT_TRANSPORT_ADAPTER_ENV = "RUNTIME_GATEWAY_EVENT_TRANSPORT_ADAPTER"
EVENT_TRANSPORT_BRIDGE_URL_ENV = "RUNTIME_GATEWAY_EVENT_TRANSPORT_BRIDGE_URL"
EVENT_TRANSPORT_BRIDGE_TIMEOUT_MS_ENV = "RUNTIME_GATEWAY_EVENT_TRANSPORT_BRIDGE_TIMEOUT_MS"
EVENT_TRANSPORT_BRIDGE_REQUIRED_ENV = "RUNTIME_GATEWAY_EVENT_TRANSPORT_BRIDGE_REQUIRED"
EVENT_TRANSPORT_BRIDGE_BEARER_TOKEN_ENV = "RUNTIME_GATEWAY_EVENT_TRANSPORT_BRIDGE_BEARER_TOKEN"

# Legacy aliases kept for compatibility with existing runtime-gateway deployment envs.
JETSTREAM_TRANSPORT_ADAPTER_HTTP = EVENT_TRANSPORT_ADAPTER_HTTP
DEFAULT_JETSTREAM_TRANSPORT_ADAPTER = DEFAULT_EVENT_TRANSPORT_ADAPTER
SUPPORTED_JETSTREAM_TRANSPORT_ADAPTERS = SUPPORTED_EVENT_TRANSPORT_ADAPTERS
JETSTREAM_TRANSPORT_ADAPTER_ENV = "RUNTIME_GATEWAY_JETSTREAM_ADAPTER"
JETSTREAM_BRIDGE_URL_ENV = "RUNTIME_GATEWAY_JETSTREAM_BRIDGE_URL"
JETSTREAM_BRIDGE_TIMEOUT_MS_ENV = "RUNTIME_GATEWAY_JETSTREAM_BRIDGE_TIMEOUT_MS"
JETSTREAM_BRIDGE_REQUIRED_ENV = "RUNTIME_GATEWAY_JETSTREAM_BRIDGE_REQUIRED"
JETSTREAM_BRIDGE_BEARER_TOKEN_ENV = "RUNTIME_GATEWAY_JETSTREAM_BRIDGE_BEARER_TOKEN"
_JETSTREAM_BRIDGE_TIMEOUT_SECONDS_DEFAULT = 3.0
_JETSTREAM_BRIDGE_TIMEOUT_SECONDS_MIN = 0.05


class EventTransportAdapter(Protocol):
    adapter_id: str

    def publish_event(self, *, bus_seq: int, event: dict[str, Any]) -> None:
        ...


JetStreamTransportAdapter = EventTransportAdapter


class HttpEventTransportAdapter:
    adapter_id = EVENT_TRANSPORT_ADAPTER_HTTP

    def __init__(
        self,
        *,
        bridge_url: str,
        timeout_seconds: float,
        bearer_token: str | None = None,
    ) -> None:
        self._bridge_url = bridge_url
        self._timeout_seconds = _normalize_timeout_seconds(timeout_seconds)
        self._bearer_token = _normalize_optional_str(bearer_token)

    def publish_event(self, *, bus_seq: int, event: dict[str, Any]) -> None:
        payload = {
            "schema_version": "runtime_gateway.jetstream_bridge_event.v1",
            "adapter_id": self.adapter_id,
            "bus_seq": bus_seq,
            "event": event,
        }
        encoded_body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        request = urllib.request.Request(
            self._bridge_url,
            data=encoded_body,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method="POST",
        )
        if self._bearer_token:
            request.add_header("Authorization", f"Bearer {self._bearer_token}")
        try:
            with urllib.request.urlopen(request, timeout=self._timeout_seconds) as response:
                status = int(getattr(response, "status", 0) or 0)
                if status < 200 or status >= 300:
                    raise RuntimeError(
                        "event transport adapter HTTP bridge rejected event "
                        f"with status {status}"
                    )
        except urllib.error.HTTPError as exc:
            detail = ""
            try:
                detail = exc.read().decode("utf-8").strip()
            except Exception:
                detail = ""
            suffix = f": {detail}" if detail else ""
            raise RuntimeError(f"event transport adapter HTTP error {exc.code}{suffix}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(
                "event transport adapter bridge unavailable: "
                f"{exc.reason}"
            ) from exc


HttpJetStreamTransportAdapter = HttpEventTransportAdapter


def normalize_event_transport_adapter_name(raw_name: str | None) -> str:
    if raw_name is None:
        return DEFAULT_EVENT_TRANSPORT_ADAPTER
    normalized = raw_name.strip().lower()
    if not normalized:
        return DEFAULT_EVENT_TRANSPORT_ADAPTER
    if normalized not in SUPPORTED_EVENT_TRANSPORT_ADAPTERS:
        supported = ", ".join(sorted(SUPPORTED_EVENT_TRANSPORT_ADAPTERS))
        raise ValueError(
            f"unsupported event transport adapter '{raw_name}'; expected one of: {supported}"
        )
    return normalized


def normalize_jetstream_transport_adapter_name(raw_name: str | None) -> str:
    return normalize_event_transport_adapter_name(raw_name)


def event_transport_adapter_name_from_env() -> str:
    return normalize_event_transport_adapter_name(
        _first_env_value(EVENT_TRANSPORT_ADAPTER_ENV, JETSTREAM_TRANSPORT_ADAPTER_ENV)
    )


def jetstream_transport_adapter_name_from_env() -> str:
    return event_transport_adapter_name_from_env()


def event_transport_bridge_url_from_env() -> str | None:
    return _normalize_optional_str(
        _first_env_value(EVENT_TRANSPORT_BRIDGE_URL_ENV, JETSTREAM_BRIDGE_URL_ENV)
    )


def jetstream_bridge_url_from_env() -> str | None:
    return event_transport_bridge_url_from_env()


def event_transport_bridge_timeout_seconds_from_env() -> float:
    return _parse_timeout_seconds_from_millis(
        _first_env_value(EVENT_TRANSPORT_BRIDGE_TIMEOUT_MS_ENV, JETSTREAM_BRIDGE_TIMEOUT_MS_ENV)
    )


def jetstream_bridge_timeout_seconds_from_env() -> float:
    return event_transport_bridge_timeout_seconds_from_env()


def event_transport_bridge_required_from_env() -> bool:
    return _parse_bool(
        _first_env_value(EVENT_TRANSPORT_BRIDGE_REQUIRED_ENV, JETSTREAM_BRIDGE_REQUIRED_ENV),
        default=False,
    )


def jetstream_bridge_required_from_env() -> bool:
    return event_transport_bridge_required_from_env()


def event_transport_bridge_bearer_token_from_env() -> str | None:
    return _normalize_optional_str(
        _first_env_value(
            EVENT_TRANSPORT_BRIDGE_BEARER_TOKEN_ENV,
            JETSTREAM_BRIDGE_BEARER_TOKEN_ENV,
        )
    )


def jetstream_bridge_bearer_token_from_env() -> str | None:
    return event_transport_bridge_bearer_token_from_env()


def build_event_transport_adapter(
    *,
    name: str,
    bridge_url: str,
    timeout_seconds: float,
    bearer_token: str | None = None,
) -> EventTransportAdapter:
    normalized = normalize_event_transport_adapter_name(name)
    if normalized == EVENT_TRANSPORT_ADAPTER_HTTP:
        return HttpEventTransportAdapter(
            bridge_url=bridge_url,
            timeout_seconds=timeout_seconds,
            bearer_token=bearer_token,
        )
    raise ValueError(f"unsupported event transport adapter: {name}")


def build_jetstream_transport_adapter(
    *,
    name: str,
    bridge_url: str,
    timeout_seconds: float,
    bearer_token: str | None = None,
) -> JetStreamTransportAdapter:
    return build_event_transport_adapter(
        name=name,
        bridge_url=bridge_url,
        timeout_seconds=timeout_seconds,
        bearer_token=bearer_token,
    )


def resolve_event_transport_adapter(
    *,
    adapter: EventTransportAdapter | None,
    adapter_name: str | None,
    bridge_url: str | None,
    bridge_timeout_seconds: float | None,
    bridge_bearer_token: str | None,
) -> EventTransportAdapter | None:
    if adapter is not None:
        return adapter
    resolved_url = _normalize_optional_str(bridge_url)
    if resolved_url is None:
        resolved_url = event_transport_bridge_url_from_env()
    if resolved_url is None:
        return None
    resolved_timeout_seconds = (
        _normalize_timeout_seconds(bridge_timeout_seconds)
        if bridge_timeout_seconds is not None
        else event_transport_bridge_timeout_seconds_from_env()
    )
    resolved_bearer_token = (
        _normalize_optional_str(bridge_bearer_token)
        if bridge_bearer_token is not None
        else event_transport_bridge_bearer_token_from_env()
    )
    resolved_name = (
        normalize_event_transport_adapter_name(adapter_name)
        if adapter_name is not None
        else event_transport_adapter_name_from_env()
    )
    return build_event_transport_adapter(
        name=resolved_name,
        bridge_url=resolved_url,
        timeout_seconds=resolved_timeout_seconds,
        bearer_token=resolved_bearer_token,
    )


def resolve_jetstream_transport_adapter(
    *,
    adapter: JetStreamTransportAdapter | None,
    adapter_name: str | None,
    bridge_url: str | None,
    bridge_timeout_seconds: float | None,
    bridge_bearer_token: str | None,
) -> JetStreamTransportAdapter | None:
    return resolve_event_transport_adapter(
        adapter=adapter,
        adapter_name=adapter_name,
        bridge_url=bridge_url,
        bridge_timeout_seconds=bridge_timeout_seconds,
        bridge_bearer_token=bridge_bearer_token,
    )


def _normalize_optional_str(raw: str | None) -> str | None:
    if raw is None:
        return None
    normalized = raw.strip()
    if not normalized:
        return None
    return normalized


def _first_env_value(*keys: str) -> str | None:
    for key in keys:
        value = os.environ.get(key)
        if value is not None:
            return value
    return None


def _parse_bool(raw: str | None, *, default: bool) -> bool:
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _parse_timeout_seconds_from_millis(raw: str | None) -> float:
    if raw is None:
        return _JETSTREAM_BRIDGE_TIMEOUT_SECONDS_DEFAULT
    try:
        parsed = float(raw.strip())
    except (TypeError, ValueError):
        return _JETSTREAM_BRIDGE_TIMEOUT_SECONDS_DEFAULT
    if parsed <= 0:
        return _JETSTREAM_BRIDGE_TIMEOUT_SECONDS_DEFAULT
    return max(_JETSTREAM_BRIDGE_TIMEOUT_SECONDS_MIN, parsed / 1000.0)


def _normalize_timeout_seconds(value: float) -> float:
    if value <= 0:
        return _JETSTREAM_BRIDGE_TIMEOUT_SECONDS_DEFAULT
    return max(_JETSTREAM_BRIDGE_TIMEOUT_SECONDS_MIN, value)
