"""Durable runtime event log helpers for gateway event reads."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any

_FILE_LOCK = Lock()


def _event_log_path() -> Path | None:
    raw = os.environ.get("RUNTIME_GATEWAY_EVENT_LOG_PATH")
    if not raw:
        return None
    return Path(raw).expanduser()


def append_event_record(*, bus_seq: int, event: dict[str, Any]) -> None:
    path = _event_log_path()
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(
        {"bus_seq": int(bus_seq), "event": dict(event)},
        ensure_ascii=True,
        separators=(",", ":"),
    )
    with _FILE_LOCK:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.write("\n")


def read_event_page(
    *,
    limit: int,
    tenant_id: str | None,
    app_id: str | None,
    event_types: set[str] | None,
    run_id: str | None,
    since_ts: datetime | None,
    cursor: int | None,
) -> dict[str, Any]:
    if limit <= 0:
        return {
            "items": [],
            "next_cursor": cursor or 0,
            "has_more": False,
            "stats": {"connections": 0, "buffered_events": 0, "next_seq": 1},
        }

    records = _read_records_from_file()
    filtered = [
        record
        for record in records
        if _matches(
            record["event"],
            tenant_id=tenant_id,
            app_id=app_id,
            event_types=event_types,
            run_id=run_id,
            since_ts=since_ts,
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


def _read_records_from_file() -> list[dict[str, Any]]:
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


def _matches(
    event: dict[str, Any],
    *,
    tenant_id: str | None,
    app_id: str | None,
    event_types: set[str] | None,
    run_id: str | None,
    since_ts: datetime | None,
) -> bool:
    if tenant_id and str(event.get("tenant_id")) != tenant_id:
        return False
    if app_id and str(event.get("app_id")) != app_id:
        return False
    if event_types and str(event.get("event_type")) not in event_types:
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

