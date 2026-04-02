"""Event transport backend seam for runtime-gateway."""

from __future__ import annotations

from typing import Any, Protocol

from .events.bus import InMemoryEventBus
from .events.durable import append_event_record

DEFAULT_EVENT_TRANSPORT_BACKEND = "local"
JETSTREAM_EVENT_TRANSPORT_BACKEND = "jetstream"
SUPPORTED_EVENT_TRANSPORT_BACKENDS = frozenset(
    {
        DEFAULT_EVENT_TRANSPORT_BACKEND,
        JETSTREAM_EVENT_TRANSPORT_BACKEND,
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


class JetStreamEventTransportBackend:
    name = JETSTREAM_EVENT_TRANSPORT_BACKEND

    def __init__(self, *, event_bus: InMemoryEventBus) -> None:
        self._event_bus = event_bus

    def publish(self, *, event: dict[str, Any]) -> int:
        _ = self._event_bus, event
        _raise_jetstream_not_ready("publish")


def normalize_event_transport_backend_name(raw_name: str | None) -> str:
    if raw_name is None:
        return DEFAULT_EVENT_TRANSPORT_BACKEND
    normalized = raw_name.strip().lower()
    if not normalized:
        return DEFAULT_EVENT_TRANSPORT_BACKEND
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
) -> EventTransportBackend:
    normalized = normalize_event_transport_backend_name(name)
    if normalized == JETSTREAM_EVENT_TRANSPORT_BACKEND:
        return JetStreamEventTransportBackend(event_bus=event_bus)
    return LocalEventTransportBackend(event_bus=event_bus)


def resolve_event_transport_backend(
    *,
    backend: EventTransportBackend | None,
    backend_name: str | None,
    event_bus: InMemoryEventBus,
) -> EventTransportBackend:
    if backend is not None:
        return backend
    resolved_name = normalize_event_transport_backend_name(backend_name)
    return build_event_transport_backend(name=resolved_name, event_bus=event_bus)


def _raise_jetstream_not_ready(method_name: str) -> None:
    raise NotImplementedError(
        "event transport backend 'jetstream' is not implemented yet; "
        f"cannot call {method_name}. Use event transport backend 'local' for now."
    )
