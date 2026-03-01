"""Event envelope model."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4


def build_event_envelope(event_type: str, tenant_id: str, app_id: str, session_key: str, payload: dict) -> dict:
    trace_id = str(uuid4())
    return {
        "event_id": str(uuid4()),
        "event_type": event_type,
        "tenant_id": tenant_id,
        "app_id": app_id,
        "session_key": session_key,
        "trace_id": trace_id,
        "correlation_id": trace_id,
        "ts": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }
