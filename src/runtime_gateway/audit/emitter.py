"""Audit emitter with in-memory and optional durable file sink."""

from __future__ import annotations

import json
import os
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


def _append_to_audit_log(event: dict[str, Any]) -> None:
    path = _audit_log_path()
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(event, ensure_ascii=True, separators=(",", ":"))
    with _FILE_LOCK:
        with path.open("a", encoding="utf-8") as f:
            f.write(line)
            f.write("\n")


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


def clear_audit_events() -> None:
    """Clear in-memory audit events (for tests/dev only)."""
    _AUDIT_EVENTS.clear()
