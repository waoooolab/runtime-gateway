"""Event transport backend seam for runtime-gateway."""

from __future__ import annotations

from typing import Any, Protocol

from .event_transport_adapter import (
    EVENT_TRANSPORT_BRIDGE_URL_ENV,
    EventTransportAdapter,
    event_transport_bridge_required_from_env,
    resolve_event_transport_adapter,
)
from .events.bus import InMemoryEventBus
from .events.durable import append_event_record

DEFAULT_EVENT_TRANSPORT_BACKEND = "local"
# Canonical adapter-backed transport backend.
ADAPTER_EVENT_TRANSPORT_BACKEND = "adapter"
# Backward-compat alias; normalized to ADAPTER_EVENT_TRANSPORT_BACKEND.
LEGACY_EVENT_TRANSPORT_BACKEND_ALIAS = "jetstream"
SUPPORTED_EVENT_TRANSPORT_BACKENDS = frozenset(
    {
        DEFAULT_EVENT_TRANSPORT_BACKEND,
        ADAPTER_EVENT_TRANSPORT_BACKEND,
        LEGACY_EVENT_TRANSPORT_BACKEND_ALIAS,
    }
)


class EventTransportBackend(Protocol):
    name: str

    def publish(self, *, event: dict[str, Any]) -> int:
        ...


class LocalEventTransportBackend:
    name = DEFAULT_EVENT_TRANSPORT_BACKEND

    def __init__(self, *, event_bus: InMemoryEventBus) -> None:
        self._event_bus = event_bus

    def publish(self, *, event: dict[str, Any]) -> int:
        bus_seq = self._event_bus.publish(event)
        append_event_record(bus_seq=bus_seq, event=event)
        return bus_seq


class AdapterEventTransportBackend:
    name = ADAPTER_EVENT_TRANSPORT_BACKEND

    def __init__(
        self,
        *,
        event_bus: InMemoryEventBus,
        adapter: EventTransportAdapter | None = None,
        adapter_name: str | None = None,
        bridge_url: str | None = None,
        bridge_timeout_seconds: float | None = None,
        bridge_required: bool | None = None,
        bridge_bearer_token: str | None = None,
    ) -> None:
        self._event_bus = event_bus
        self._bridge_required = (
            bool(bridge_required)
            if bridge_required is not None
            else event_transport_bridge_required_from_env()
        )
        self._adapter = resolve_event_transport_adapter(
            adapter=adapter,
            adapter_name=adapter_name,
            bridge_url=bridge_url,
            bridge_timeout_seconds=bridge_timeout_seconds,
            bridge_bearer_token=bridge_bearer_token,
        )

    def publish(self, *, event: dict[str, Any]) -> int:
        bus_seq = self._event_bus.publish(event)
        append_event_record(bus_seq=bus_seq, event=event)
        if self._adapter is None:
            if self._bridge_required:
                raise RuntimeError(
                    "event transport backend 'adapter' requires "
                    f"{EVENT_TRANSPORT_BRIDGE_URL_ENV}"
                )
            return bus_seq
        self._adapter.publish_event(bus_seq=bus_seq, event=event)
        return bus_seq


def normalize_event_transport_backend_name(raw_name: str | None) -> str:
    if raw_name is None:
        return DEFAULT_EVENT_TRANSPORT_BACKEND
    normalized = raw_name.strip().lower()
    if not normalized:
        return DEFAULT_EVENT_TRANSPORT_BACKEND
    if normalized == LEGACY_EVENT_TRANSPORT_BACKEND_ALIAS:
        return ADAPTER_EVENT_TRANSPORT_BACKEND
    if normalized not in SUPPORTED_EVENT_TRANSPORT_BACKENDS:
        supported = ", ".join(sorted(SUPPORTED_EVENT_TRANSPORT_BACKENDS))
        raise ValueError(
            f"unsupported event transport backend '{raw_name}'; expected one of: {supported}"
        )
    return normalized


def build_event_transport_backend(
    *,
    name: str,
    event_bus: InMemoryEventBus,
    adapter: EventTransportAdapter | None = None,
    adapter_name: str | None = None,
    bridge_url: str | None = None,
    bridge_timeout_seconds: float | None = None,
    bridge_required: bool | None = None,
    bridge_bearer_token: str | None = None,
) -> EventTransportBackend:
    normalized = normalize_event_transport_backend_name(name)
    if normalized == ADAPTER_EVENT_TRANSPORT_BACKEND:
        return AdapterEventTransportBackend(
            event_bus=event_bus,
            adapter=adapter,
            adapter_name=adapter_name,
            bridge_url=bridge_url,
            bridge_timeout_seconds=bridge_timeout_seconds,
            bridge_required=bridge_required,
            bridge_bearer_token=bridge_bearer_token,
        )
    return LocalEventTransportBackend(event_bus=event_bus)


def resolve_event_transport_backend(
    *,
    backend: EventTransportBackend | None,
    backend_name: str | None,
    event_bus: InMemoryEventBus,
    adapter: EventTransportAdapter | None = None,
    adapter_name: str | None = None,
    bridge_url: str | None = None,
    bridge_timeout_seconds: float | None = None,
    bridge_required: bool | None = None,
    bridge_bearer_token: str | None = None,
) -> EventTransportBackend:
    if backend is not None:
        return backend
    resolved_name = normalize_event_transport_backend_name(backend_name)
    return build_event_transport_backend(
        name=resolved_name,
        event_bus=event_bus,
        adapter=adapter,
        adapter_name=adapter_name,
        bridge_url=bridge_url,
        bridge_timeout_seconds=bridge_timeout_seconds,
        bridge_required=bridge_required,
        bridge_bearer_token=bridge_bearer_token,
    )
