from __future__ import annotations

import pytest

from runtime_gateway.event_transport_adapter import EVENT_TRANSPORT_BRIDGE_URL_ENV
from runtime_gateway.event_transport_backend import (
    ADAPTER_EVENT_TRANSPORT_BACKEND,
    DEFAULT_EVENT_TRANSPORT_BACKEND,
    EVENT_TRANSPORT_BACKEND_ENV,
    LEGACY_EVENT_TRANSPORT_BACKEND_ENV,
    LocalEventTransportBackend,
    build_event_transport_backend,
    event_transport_backend_name_from_env,
    normalize_event_transport_backend_name,
    resolve_event_transport_backend,
)
from runtime_gateway.events.bus import InMemoryEventBus


class _RecordingAdapter:
    adapter_id = "recording"

    def __init__(self) -> None:
        self.calls: list[tuple[int, dict[str, object]]] = []

    def publish_event(self, *, bus_seq: int, event: dict[str, object]) -> None:
        self.calls.append((bus_seq, dict(event)))


def test_normalize_event_transport_backend_name_defaults_to_local() -> None:
    assert normalize_event_transport_backend_name(None) == DEFAULT_EVENT_TRANSPORT_BACKEND
    assert normalize_event_transport_backend_name(" ") == DEFAULT_EVENT_TRANSPORT_BACKEND
    assert normalize_event_transport_backend_name("LOCAL") == DEFAULT_EVENT_TRANSPORT_BACKEND
    assert (
        normalize_event_transport_backend_name("JetStream")
        == ADAPTER_EVENT_TRANSPORT_BACKEND
    )
    assert normalize_event_transport_backend_name("ADAPTER") == ADAPTER_EVENT_TRANSPORT_BACKEND


def test_normalize_event_transport_backend_name_rejects_unsupported_value() -> None:
    with pytest.raises(ValueError, match="unsupported event transport backend"):
        normalize_event_transport_backend_name("nats")


def test_event_transport_backend_name_from_env_defaults_to_local(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(EVENT_TRANSPORT_BACKEND_ENV, raising=False)
    monkeypatch.delenv(LEGACY_EVENT_TRANSPORT_BACKEND_ENV, raising=False)
    assert event_transport_backend_name_from_env() == DEFAULT_EVENT_TRANSPORT_BACKEND


def test_event_transport_backend_name_from_env_uses_legacy_when_canonical_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(EVENT_TRANSPORT_BACKEND_ENV, raising=False)
    monkeypatch.setenv(LEGACY_EVENT_TRANSPORT_BACKEND_ENV, "jetstream")
    assert event_transport_backend_name_from_env() == ADAPTER_EVENT_TRANSPORT_BACKEND


def test_event_transport_backend_name_from_env_prefers_canonical_over_legacy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(EVENT_TRANSPORT_BACKEND_ENV, "local")
    monkeypatch.setenv(LEGACY_EVENT_TRANSPORT_BACKEND_ENV, "jetstream")
    assert event_transport_backend_name_from_env() == DEFAULT_EVENT_TRANSPORT_BACKEND


def test_local_event_transport_backend_publishes_to_memory_and_durable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event_bus = InMemoryEventBus()
    backend = build_event_transport_backend(name="local", event_bus=event_bus)
    captured: list[tuple[int, dict[str, object]]] = []

    def _append_event_record(*, bus_seq: int, event: dict[str, object]) -> None:
        captured.append((bus_seq, dict(event)))

    monkeypatch.setattr(
        "runtime_gateway.event_transport_backend.append_event_record",
        _append_event_record,
    )
    event = {"event_type": "runtime.run.status", "payload": {"run_id": "run-1"}}
    bus_seq = backend.publish(event=event)
    assert bus_seq == 1
    assert captured == [(1, event)]
    stats = event_bus.stats()
    assert stats["buffered_events"] == 1
    assert stats["next_seq"] == 2


def test_resolve_event_transport_backend_prefers_explicit_backend() -> None:
    event_bus = InMemoryEventBus()
    explicit = LocalEventTransportBackend(event_bus=event_bus)
    resolved = resolve_event_transport_backend(
        backend=explicit,
        backend_name="jetstream",
        event_bus=event_bus,
    )
    assert resolved is explicit


def test_adapter_backend_publishes_with_injected_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event_bus = InMemoryEventBus()
    adapter = _RecordingAdapter()
    backend = build_event_transport_backend(
        name="adapter",
        event_bus=event_bus,
        adapter=adapter,
    )
    captured: list[tuple[int, dict[str, object]]] = []

    def _append_event_record(*, bus_seq: int, event: dict[str, object]) -> None:
        captured.append((bus_seq, dict(event)))

    monkeypatch.setattr(
        "runtime_gateway.event_transport_backend.append_event_record",
        _append_event_record,
    )
    event = {"event_type": "runtime.run.status", "payload": {"run_id": "run-adapter"}}
    bus_seq = backend.publish(event=event)
    assert bus_seq == 1
    assert captured == [(1, event)]
    assert adapter.calls == [(1, event)]
    stats = event_bus.stats()
    assert stats["buffered_events"] == 1
    assert stats["next_seq"] == 2


def test_adapter_backend_without_adapter_is_local_when_bridge_not_required(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event_bus = InMemoryEventBus()
    backend = build_event_transport_backend(
        name="adapter",
        event_bus=event_bus,
        bridge_required=False,
    )
    captured: list[tuple[int, dict[str, object]]] = []

    def _append_event_record(*, bus_seq: int, event: dict[str, object]) -> None:
        captured.append((bus_seq, dict(event)))

    monkeypatch.setattr(
        "runtime_gateway.event_transport_backend.append_event_record",
        _append_event_record,
    )
    event = {"event_type": "runtime.run.status", "payload": {"run_id": "run-local-fallback"}}
    bus_seq = backend.publish(event=event)
    assert bus_seq == 1
    assert captured == [(1, event)]
    stats = event_bus.stats()
    assert stats["buffered_events"] == 1


def test_adapter_backend_requires_bridge_when_flag_enabled() -> None:
    event_bus = InMemoryEventBus()
    backend = build_event_transport_backend(
        name="adapter",
        event_bus=event_bus,
        bridge_required=True,
    )
    with pytest.raises(RuntimeError, match=EVENT_TRANSPORT_BRIDGE_URL_ENV):
        backend.publish(event={"event_type": "runtime.run.status", "payload": {"run_id": "run-1"}})
