"""WebSocket streaming for gateway event bus."""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import WebSocket, WebSocketDisconnect, status

from .auth.tokens import TokenError, verify_token
from .events.bus import InMemoryEventBus
from .events.durable import read_event_page
from .security import EVENT_SCOPE_READ, scope_contains


def _websocket_token(websocket: WebSocket) -> str | None:
    token = websocket.query_params.get("access_token")
    auth_header = websocket.headers.get("authorization", "")
    if token:
        return token
    if auth_header.startswith("Bearer "):
        return auth_header.split(" ", 1)[1].strip()
    return None


def _parse_event_types(raw: str | None) -> set[str] | None:
    if not raw:
        return None
    return {item.strip() for item in raw.split(",") if item.strip()}


def _parse_optional_str(raw: str | None) -> str | None:
    if raw is None:
        return None
    value = raw.strip()
    return value if value else None


def _parse_cursor(raw: str) -> int:
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


def _parse_source(raw: str | None) -> str:
    value = (raw or "memory").strip().lower()
    if value not in {"memory", "durable"}:
        raise ValueError("source must be one of: memory, durable")
    return value


def _resolve_query_scope(
    *,
    field: str,
    query_value: str | None,
    claim_value: str | None,
) -> str:
    query = _parse_optional_str(query_value)
    claim = _parse_optional_str(claim_value)
    if claim is None:
        raise ValueError(f"{field} claim is required")
    if query is not None and query != claim:
        raise ValueError(f"{field} query must match token claim")
    return claim


def _websocket_filters(
    websocket: WebSocket, claims: dict[str, Any]
) -> tuple[str, str, set[str] | None, str | None, int, str]:
    tenant_id = _resolve_query_scope(
        field="tenant_id",
        query_value=websocket.query_params.get("tenant_id"),
        claim_value=str(claims.get("tenant_id", "")),
    )
    app_id = _resolve_query_scope(
        field="app_id",
        query_value=websocket.query_params.get("app_id"),
        claim_value=str(claims.get("app_id", "")),
    )
    event_types = _parse_event_types(websocket.query_params.get("event_types"))
    run_id = _parse_optional_str(websocket.query_params.get("run_id"))
    cursor = _parse_cursor(websocket.query_params.get("cursor", "0"))
    source = _parse_source(websocket.query_params.get("source"))
    return tenant_id, app_id, event_types, run_id, cursor, source


async def _send_ready(
    websocket: WebSocket,
    event_bus: InMemoryEventBus,
    cursor: int,
    tenant_id: str,
    app_id: str,
    run_id: str | None,
    source: str,
) -> None:
    payload: dict[str, Any] = {
        "kind": "ws.ready",
        "cursor": cursor,
        "tenant_id": tenant_id,
        "app_id": app_id,
        "source": source,
        "stats": event_bus.stats(),
    }
    if run_id is not None:
        payload["run_id"] = run_id
    await websocket.send_json(
        payload
    )


async def _push_records(
    websocket: WebSocket,
    event_bus: InMemoryEventBus,
    cursor: int,
    tenant_id: str,
    app_id: str,
    event_types: set[str] | None,
    run_id: str | None,
) -> int:
    for item in event_bus.since(
        cursor=cursor,
        tenant_id=tenant_id or None,
        app_id=app_id or None,
        event_types=event_types,
        run_id=run_id,
    ):
        cursor = max(cursor, int(item["bus_seq"]))
        await websocket.send_json(item)
    return cursor


async def _wait_keepalive(websocket: WebSocket, cursor: int) -> bool:
    try:
        incoming = await asyncio.wait_for(websocket.receive_text(), timeout=2.0)
    except asyncio.TimeoutError:
        return True
    except WebSocketDisconnect:
        return False
    if incoming.strip().lower() == "ping":
        await websocket.send_json({"kind": "ws.pong", "cursor": cursor})
    return True


async def _replay_durable_records(
    *,
    websocket: WebSocket,
    cursor: int,
    tenant_id: str,
    app_id: str,
    event_types: set[str] | None,
    run_id: str | None,
) -> int:
    next_cursor = cursor
    while True:
        page = read_event_page(
            limit=200,
            tenant_id=tenant_id,
            app_id=app_id,
            event_types=event_types,
            run_id=run_id,
            since_ts=None,
            cursor=next_cursor,
        )
        items = page.get("items", [])
        if not isinstance(items, list) or not items:
            return next_cursor
        for item in items:
            if not isinstance(item, dict):
                continue
            bus_seq = item.get("bus_seq")
            if isinstance(bus_seq, int):
                next_cursor = max(next_cursor, bus_seq)
            await websocket.send_json(item)
        if not bool(page.get("has_more", False)):
            return next_cursor


async def handle_websocket_events(websocket: WebSocket, event_bus: InMemoryEventBus) -> None:
    token = _websocket_token(websocket)
    if not token:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="missing bearer token")
        return

    try:
        claims = verify_token(token, audience="runtime-gateway")
    except TokenError:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="invalid bearer token")
        return

    if not scope_contains(claims, EVENT_SCOPE_READ):
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="missing read scope")
        return

    try:
        tenant_id, app_id, event_types, run_id, cursor, source = _websocket_filters(websocket, claims)
    except ValueError as exc:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason=str(exc))
        return

    await websocket.accept()
    event_bus.open_connection()
    try:
        await _send_ready(websocket, event_bus, cursor, tenant_id, app_id, run_id, source)
        if source == "durable":
            cursor = await _replay_durable_records(
                websocket=websocket,
                cursor=cursor,
                tenant_id=tenant_id,
                app_id=app_id,
                event_types=event_types,
                run_id=run_id,
            )
            stats = event_bus.stats()
            next_seq = int(stats.get("next_seq", 1))
            cursor = max(0, next_seq - 1)
        while True:
            cursor = await _push_records(
                websocket,
                event_bus,
                cursor,
                tenant_id,
                app_id,
                event_types,
                run_id,
            )
            if not await _wait_keepalive(websocket, cursor):
                break
    finally:
        event_bus.close_connection()
