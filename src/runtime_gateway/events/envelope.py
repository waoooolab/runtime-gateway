"""Event envelope model."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4


def build_event_envelope(
    event_type: str,
    tenant_id: str,
    app_id: str,
    session_key: str,
    payload: dict,
    trace_id: str | None = None,
    correlation_id: str | None = None,
) -> dict:
    final_trace_id = trace_id or str(uuid4())
    final_correlation_id = correlation_id or final_trace_id
    return {
        "event_id": str(uuid4()),
        "event_type": event_type,
        "tenant_id": tenant_id,
        "app_id": app_id,
        "session_key": session_key,
        "trace_id": final_trace_id,
        "correlation_id": final_correlation_id,
        "ts": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }
