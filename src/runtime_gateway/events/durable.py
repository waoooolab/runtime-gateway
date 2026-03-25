"""Durable runtime event log helpers for gateway event reads."""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any

from runtime_gateway.code_terms import normalize_optional_code_term
from runtime_gateway.persistence_paths import resolve_event_db_path

_FILE_LOCK = Lock()


def _event_log_path() -> Path | None:
    raw = os.environ.get("RUNTIME_GATEWAY_EVENT_LOG_PATH")
    if not raw:
        return None
    return Path(raw).expanduser()


def _event_db_path() -> Path | None:
    return resolve_event_db_path()


def _connect_event_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path.as_posix())
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _ensure_event_db_schema(path: Path) -> None:
    with closing(_connect_event_db(path)) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runtime_gateway_events (
                bus_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                memory_bus_seq INTEGER NOT NULL,
                event_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.commit()


def append_event_record(*, bus_seq: int, event: dict[str, Any]) -> None:
    db_path = _event_db_path()
    if db_path is not None:
        _append_event_record_sqlite(
            db_path=db_path,
            bus_seq=bus_seq,
            event=event,
        )
        return
    path = _event_log_path()
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with _FILE_LOCK:
        durable_seq = _next_record_seq(path)
        line = json.dumps(
            {
                "bus_seq": durable_seq,
                "memory_bus_seq": int(bus_seq),
                "event": dict(event),
            },
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.write("\n")


def _append_event_record_sqlite(*, db_path: Path, bus_seq: int, event: dict[str, Any]) -> None:
    payload = json.dumps(
        {
            "memory_bus_seq": int(bus_seq),
            "event": dict(event),
        },
        ensure_ascii=True,
        separators=(",", ":"),
    )
    created_at = datetime.now(timezone.utc).isoformat()
    with _FILE_LOCK:
        _ensure_event_db_schema(db_path)
        with closing(_connect_event_db(db_path)) as conn:
            conn.execute(
                """
                INSERT INTO runtime_gateway_events (
                    memory_bus_seq,
                    event_json,
                    created_at
                )
                VALUES (?, ?, ?)
                """,
                (int(bus_seq), payload, created_at),
            )
            conn.commit()


def read_event_page(
    *,
    limit: int,
    tenant_id: str | None,
    app_id: str | None,
    session_key: str | None,
    scope_id: str | None,
    scope_type: str | None,
    event_types: set[str] | None,
    run_statuses: set[str] | None = None,
    reason_codes: set[str] | None = None,
    run_id: str | None,
    since_ts: datetime | None,
    until_ts: datetime | None,
    cursor: int | None,
) -> dict[str, Any]:
    if limit <= 0:
        return {
            "items": [],
            "next_cursor": cursor or 0,
            "has_more": False,
            "stats": {"connections": 0, "buffered_events": 0, "next_seq": 1},
        }

    db_path = _event_db_path()
    if db_path is not None:
        records = _read_records_from_db(db_path)
    else:
        path = _event_log_path()
        records = _read_records_from_file(path)

    filtered = [
        record
        for record in records
        if _matches(
            record["event"],
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

    if cursor is None:
        has_more = len(filtered) > limit
        items = filtered[-limit:]
        next_cursor = int(items[-1]["bus_seq"]) if items else 0
    else:
        since_items = [record for record in filtered if int(record["bus_seq"]) > cursor]
        has_more = len(since_items) > limit
        items = since_items[:limit]
        next_cursor = int(items[-1]["bus_seq"]) if items else cursor

    max_seq = max((int(record["bus_seq"]) for record in records), default=0)
    stats = {
        "connections": 0,
        "buffered_events": len(records),
        "next_seq": max_seq + 1 if max_seq >= 1 else 1,
    }
    return {
        "items": items,
        "next_cursor": next_cursor,
        "has_more": has_more,
        "stats": stats,
    }


def _read_records_from_db(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with _FILE_LOCK:
        _ensure_event_db_schema(path)
        with closing(_connect_event_db(path)) as conn:
            rows = conn.execute(
                """
                SELECT bus_seq, event_json
                FROM runtime_gateway_events
                ORDER BY bus_seq ASC
                """
            ).fetchall()
    records: list[dict[str, Any]] = []
    for row in rows:
        bus_seq = row[0]
        raw_payload = row[1]
        if not isinstance(bus_seq, int) or bus_seq < 1:
            continue
        if not isinstance(raw_payload, str) or not raw_payload:
            continue
        try:
            value = json.loads(raw_payload)
        except json.JSONDecodeError:
            continue
        if not isinstance(value, dict):
            continue
        event = value.get("event")
        if not isinstance(event, dict):
            continue
        records.append({"bus_seq": int(bus_seq), "event": event})
    return records


def _read_records_from_file(path: Path | None = None) -> list[dict[str, Any]]:
    if path is None:
        path = _event_log_path()
    if path is None or not path.exists():
        return []
    lines = path.read_text(encoding="utf-8").splitlines()
    records: list[dict[str, Any]] = []
    line_seq = 0
    for raw in lines:
        line_seq += 1
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(value, dict):
            continue
        event = value.get("event")
        if not isinstance(event, dict):
            continue
        seq = value.get("bus_seq")
        if not (isinstance(seq, int) and seq >= 1):
            seq = line_seq
        records.append({"bus_seq": int(seq), "event": event})
    return records


def _next_record_seq(path: Path) -> int:
    records = _read_records_from_file(path)
    max_seq = max((int(record["bus_seq"]) for record in records), default=0)
    return max_seq + 1 if max_seq >= 1 else 1


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
    if event_types and str(event.get("event_type")) not in event_types:
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
        if event_ts is None or event_ts < since_ts:
            return False
    if until_ts is not None:
        event_ts = _parse_event_ts(event.get("ts"))
        if event_ts is None or event_ts > until_ts:
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
