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
    task_contract_version: str | None = None,
    agent_contract_version: str | None = None,
    event_schema_version: str | None = None,
) -> dict:
    final_trace_id = trace_id or str(uuid4())
    final_correlation_id = correlation_id or final_trace_id
    envelope = {
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
    if isinstance(task_contract_version, str) and task_contract_version.strip():
        envelope["task_contract_version"] = task_contract_version
    if isinstance(agent_contract_version, str) and agent_contract_version.strip():
        envelope["agent_contract_version"] = agent_contract_version
    if isinstance(event_schema_version, str) and event_schema_version.strip():
        envelope["event_schema_version"] = event_schema_version
    return envelope
