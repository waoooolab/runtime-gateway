"""In-memory runtime event bus for gateway baseline."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from threading import Lock
from typing import Any


@dataclass
class InMemoryEventBus:
    """Small event bus with cursor-based reads for websocket fan-out."""

    max_events: int = 2000
    _records: deque[tuple[int, dict[str, Any]]] = field(default_factory=deque, init=False)
    _next_seq: int = field(default=1, init=False)
    _connections: int = field(default=0, init=False)
    _lock: Lock = field(default_factory=Lock, init=False)

    def publish(self, event: dict[str, Any]) -> int:
        with self._lock:
            seq = self._next_seq
            self._next_seq += 1
            self._records.append((seq, dict(event)))
            while len(self._records) > self.max_events:
                self._records.popleft()
            return seq

    def clear(self) -> None:
        with self._lock:
            self._records.clear()
            self._next_seq = 1
            self._connections = 0

    def recent(
        self,
        *,
        limit: int = 50,
        tenant_id: str | None = None,
        app_id: str | None = None,
        event_types: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        with self._lock:
            snapshot = list(self._records)
        filtered = [
            {"bus_seq": seq, "event": event}
            for seq, event in snapshot
            if _matches(event, tenant_id=tenant_id, app_id=app_id, event_types=event_types)
        ]
        if limit <= 0:
            return []
        return filtered[-limit:]

    def since(
        self,
        *,
        cursor: int,
        tenant_id: str | None = None,
        app_id: str | None = None,
        event_types: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        with self._lock:
            snapshot = list(self._records)
        return [
            {"bus_seq": seq, "event": event}
            for seq, event in snapshot
            if seq > cursor
            and _matches(event, tenant_id=tenant_id, app_id=app_id, event_types=event_types)
        ]

    def open_connection(self) -> int:
        with self._lock:
            self._connections += 1
            return self._connections

    def close_connection(self) -> int:
        with self._lock:
            self._connections = max(0, self._connections - 1)
            return self._connections

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "connections": self._connections,
                "buffered_events": len(self._records),
                "next_seq": self._next_seq,
            }


def _matches(
    event: dict[str, Any],
    *,
    tenant_id: str | None,
    app_id: str | None,
    event_types: set[str] | None,
) -> bool:
    if tenant_id and str(event.get("tenant_id")) != tenant_id:
        return False
    if app_id and str(event.get("app_id")) != app_id:
        return False
    if event_types:
        return str(event.get("event_type")) in event_types
    return True
