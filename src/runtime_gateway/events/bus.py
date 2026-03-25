"""In-memory runtime event bus for gateway baseline."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import Any

from runtime_gateway.code_terms import normalize_optional_code_term


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
        session_key: str | None = None,
        scope_id: str | None = None,
        scope_type: str | None = None,
        event_types: set[str] | None = None,
        run_statuses: set[str] | None = None,
        reason_codes: set[str] | None = None,
        run_id: str | None = None,
        since_ts: datetime | None = None,
        until_ts: datetime | None = None,
    ) -> list[dict[str, Any]]:
        with self._lock:
            snapshot = list(self._records)
        filtered = [
            {"bus_seq": seq, "event": event}
            for seq, event in snapshot
            if _matches(
                event,
                tenant_id=tenant_id,
                app_id=app_id,
                session_key=session_key,
                scope_id=scope_id,
                scope_type=scope_type,
                event_types=event_types,
                run_statuses=run_statuses,
                reason_codes=reason_codes,
                run_id=run_id,
                since_ts=since_ts,
                until_ts=until_ts,
            )
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
        session_key: str | None = None,
        scope_id: str | None = None,
        scope_type: str | None = None,
        event_types: set[str] | None = None,
        run_statuses: set[str] | None = None,
        reason_codes: set[str] | None = None,
        run_id: str | None = None,
        since_ts: datetime | None = None,
        until_ts: datetime | None = None,
    ) -> list[dict[str, Any]]:
        with self._lock:
            snapshot = list(self._records)
        return [
            {"bus_seq": seq, "event": event}
            for seq, event in snapshot
            if seq > cursor
            and _matches(
                event,
                tenant_id=tenant_id,
                app_id=app_id,
                session_key=session_key,
                scope_id=scope_id,
                scope_type=scope_type,
                event_types=event_types,
                run_statuses=run_statuses,
                reason_codes=reason_codes,
                run_id=run_id,
                since_ts=since_ts,
                until_ts=until_ts,
            )
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
    session_key: str | None,
    scope_id: str | None,
    scope_type: str | None,
    event_types: set[str] | None,
    run_statuses: set[str] | None,
    reason_codes: set[str] | None,
    run_id: str | None,
    since_ts: datetime | None,
    until_ts: datetime | None,
) -> bool:
    if tenant_id and str(event.get("tenant_id")) != tenant_id:
        return False
    if app_id and str(event.get("app_id")) != app_id:
        return False
    if session_key and str(event.get("session_key")) != session_key:
        return False
    if scope_id:
        event_scope_id, _ = _resolve_event_scope_axis(event)
        if event_scope_id != scope_id:
            return False
    if scope_type:
        _, event_scope_type = _resolve_event_scope_axis(event)
        if event_scope_type != scope_type:
            return False
    if event_types:
        if str(event.get("event_type")) not in event_types:
            return False
    if run_statuses:
        event_run_status = _extract_run_status(event)
        if event_run_status is None or event_run_status not in run_statuses:
            return False
    if reason_codes:
        event_reason_code = _extract_failure_reason_code(event)
        if event_reason_code is None or event_reason_code not in reason_codes:
            return False
    if run_id:
        payload = event.get("payload")
        if not isinstance(payload, dict):
            return False
        if str(payload.get("run_id")) != run_id:
            return False
    if since_ts is not None:
        event_ts = _parse_event_ts(event.get("ts"))
        if event_ts is None:
            return False
        if event_ts < since_ts:
            return False
    if until_ts is not None:
        event_ts = _parse_event_ts(event.get("ts"))
        if event_ts is None:
            return False
        if event_ts > until_ts:
            return False
    return True


def _parse_event_ts(raw_ts: Any) -> datetime | None:
    if not isinstance(raw_ts, str) or not raw_ts:
        return None
    try:
        parsed = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _extract_run_status(event: dict[str, Any]) -> str | None:
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return None
    raw_status = payload.get("status")
    if not isinstance(raw_status, str):
        return None
    normalized = raw_status.strip().lower()
    return normalized or None


def _extract_failure_reason_code(event: dict[str, Any]) -> str | None:
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return None
    orchestration = payload.get("orchestration")
    if isinstance(orchestration, dict):
        normalized_orchestration_code = normalize_optional_code_term(orchestration.get("failure_reason_code"))
        if normalized_orchestration_code is not None:
            return normalized_orchestration_code
    normalized_failure_code = normalize_optional_code_term(payload.get("failure_reason_code"))
    if normalized_failure_code is not None:
        return normalized_failure_code
    route = payload.get("route")
    if isinstance(route, dict):
        normalized_placement_reason = normalize_optional_code_term(route.get("placement_reason_code"))
        if normalized_placement_reason is not None:
            return normalized_placement_reason
        normalized_route_reason = normalize_optional_code_term(route.get("reason_code"))
        if normalized_route_reason is not None:
            return normalized_route_reason
    return normalize_optional_code_term(payload.get("reason_code"))


def _resolve_event_scope_axis(event: dict[str, Any]) -> tuple[str | None, str | None]:
    raw_scope_id = str(event.get("scope_id") or "").strip() or None
    raw_scope_type = str(event.get("scope_type") or "").strip() or None
    if raw_scope_id is not None:
        return raw_scope_id, raw_scope_type
    fallback_session_key = str(event.get("session_key") or "").strip() or None
    if fallback_session_key is not None:
        return fallback_session_key, raw_scope_type or "session"
    return None, raw_scope_type
