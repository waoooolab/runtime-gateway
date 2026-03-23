"""Persistence path resolution helpers for runtime-gateway."""

from __future__ import annotations

import os
from pathlib import Path


def _normalized_env(name: str) -> str | None:
    raw = os.environ.get(name)
    if raw is None:
        return None
    value = raw.strip()
    return value or None


def _persist_root() -> Path | None:
    raw = _normalized_env("OWA_PERSIST_ROOT")
    if raw is None:
        return None
    return Path(raw).expanduser()


def resolve_event_db_path() -> Path | None:
    explicit = _normalized_env("RUNTIME_GATEWAY_EVENT_DB_PATH")
    if explicit is not None:
        return Path(explicit).expanduser()
    root = _persist_root()
    if root is None:
        return None
    return root / "runtime-gateway" / "runtime-events.sqlite"


def resolve_audit_db_path() -> Path | None:
    explicit = _normalized_env("RUNTIME_GATEWAY_AUDIT_DB_PATH")
    if explicit is not None:
        return Path(explicit).expanduser()
    root = _persist_root()
    if root is None:
        return None
    return root / "runtime-gateway" / "runtime-audit.sqlite"
