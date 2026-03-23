"""Audit emitter with in-memory and optional durable file sink."""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any
from uuid import uuid4

_AUDIT_EVENTS: list[dict[str, Any]] = []
_FILE_LOCK = Lock()


def _audit_log_path() -> Path | None:
    raw = os.environ.get("RUNTIME_GATEWAY_AUDIT_LOG_PATH")
    if not raw:
        return None
    return Path(raw).expanduser()


def _audit_db_path() -> Path | None:
    raw = os.environ.get("RUNTIME_GATEWAY_AUDIT_DB_PATH")
    if not raw:
        return None
    return Path(raw).expanduser()


def _connect_audit_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path.as_posix())
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _ensure_audit_db_schema(path: Path) -> None:
    with _connect_audit_db(path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runtime_gateway_audit_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.commit()


def _append_to_audit_log(event: dict[str, Any]) -> None:
    db_path = _audit_db_path()
    if db_path is not None:
        _append_to_audit_db(db_path=db_path, event=event)
        return

    path = _audit_log_path()
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(event, ensure_ascii=True, separators=(",", ":"))
    with _FILE_LOCK:
        with path.open("a", encoding="utf-8") as f:
            f.write(line)
            f.write("\n")


def _append_to_audit_db(*, db_path: Path, event: dict[str, Any]) -> None:
    payload = json.dumps(event, ensure_ascii=True, separators=(",", ":"))
    created_at = datetime.now(timezone.utc).isoformat()
    with _FILE_LOCK:
        _ensure_audit_db_schema(db_path)
        with _connect_audit_db(db_path) as conn:
            conn.execute(
                """
                INSERT INTO runtime_gateway_audit_events (
                    event_json,
                    created_at
                )
                VALUES (?, ?)
                """,
                (payload, created_at),
            )
            conn.commit()


def emit_audit_event(
    *,
    action: str,
    decision: str,
    actor_id: str,
    resource: str | None = None,
    trace_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Emit one audit event to memory and optional file sink."""
    if decision not in {"allow", "deny"}:
        raise ValueError("decision must be allow or deny")

    event = {
        "event_id": str(uuid4()),
        "event_type": "audit.gateway",
        "action": action,
        "decision": decision,
        "actor_id": actor_id,
        "resource": resource,
        "trace_id": trace_id,
        "metadata": metadata or {},
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    _AUDIT_EVENTS.append(event)
    _append_to_audit_log(event)
    return event


def get_audit_events(limit: int = 100) -> list[dict[str, Any]]:
    """Read recent in-memory audit events."""
    if limit <= 0:
        return []
    return list(_AUDIT_EVENTS[-limit:])


def read_audit_log(limit: int = 100) -> list[dict[str, Any]]:
    """Read recent events from the durable audit log if configured."""
    db_path = _audit_db_path()
    if db_path is not None:
        return _read_audit_db(db_path=db_path, limit=limit)

    path = _audit_log_path()
    if limit <= 0 or path is None or not path.exists():
        return []

    lines = path.read_text(encoding="utf-8").splitlines()
    selected = lines[-limit:]
    items: list[dict[str, Any]] = []
    for line in selected:
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            items.append(value)
    return items


def _read_audit_db(*, db_path: Path, limit: int) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    if not db_path.exists():
        return []
    with _FILE_LOCK:
        _ensure_audit_db_schema(db_path)
        with _connect_audit_db(db_path) as conn:
            rows = conn.execute(
                """
                SELECT event_json
                FROM runtime_gateway_audit_events
                ORDER BY id DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
    items: list[dict[str, Any]] = []
    for row in reversed(rows):
        raw_payload = row[0]
        if not isinstance(raw_payload, str) or not raw_payload:
            continue
        try:
            value = json.loads(raw_payload)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            items.append(value)
    return items


def clear_audit_events() -> None:
    """Clear in-memory audit events (for tests/dev only)."""
    _AUDIT_EVENTS.clear()
