"""Canonical run-status terms loaded from platform contracts.

This module centralizes run-status vocabulary usage in runtime-gateway so
status semantics stay aligned with platform-contracts/runtime-state.v1.json.
"""

from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path

_DEFAULT_RUN_STATUSES = frozenset(
    {
        "requested",
        "queued",
        "dispatching",
        "running",
        "waiting_approval",
        "retrying",
        "succeeded",
        "failed",
        "dlq",
        "canceled",
        "timed_out",
    }
)
_NON_TERMINAL_RUN_STATUSES = frozenset(
    {
        "requested",
        "queued",
        "dispatching",
        "running",
        "waiting_approval",
        "retrying",
    }
)
_LEGACY_TERMINAL_STATUSES = frozenset({"rejected"})
_PLATFORM_CONTRACTS_DIR_ENV = "OWA_PLATFORM_CONTRACTS_DIR"
_PLATFORM_CONTRACTS_DIR_ENV_LEGACY = "WAOOOOLAB_PLATFORM_CONTRACTS_DIR"


def _contracts_root() -> Path:
    configured = os.environ.get(_PLATFORM_CONTRACTS_DIR_ENV)
    if configured is None:
        configured = os.environ.get(_PLATFORM_CONTRACTS_DIR_ENV_LEGACY)
    if configured:
        return Path(configured).expanduser().resolve()
    return Path(__file__).resolve().parents[3] / "platform-contracts"


def _runtime_state_schema_path() -> Path:
    root = _contracts_root()
    jsonschema_root = root / "jsonschema"
    if jsonschema_root.exists():
        return jsonschema_root / "runtime" / "runtime-state.v1.json"
    return root / "runtime" / "runtime-state.v1.json"


@lru_cache(maxsize=1)
def canonical_run_statuses() -> frozenset[str]:
    path = _runtime_state_schema_path()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _DEFAULT_RUN_STATUSES
    properties = raw.get("properties")
    if not isinstance(properties, dict):
        return _DEFAULT_RUN_STATUSES
    run_status = properties.get("run_status")
    if not isinstance(run_status, dict):
        return _DEFAULT_RUN_STATUSES
    enum_values = run_status.get("enum")
    if not isinstance(enum_values, list):
        return _DEFAULT_RUN_STATUSES
    normalized = {
        str(item).strip().lower()
        for item in enum_values
        if isinstance(item, str) and item.strip()
    }
    if not normalized:
        return _DEFAULT_RUN_STATUSES
    return frozenset(normalized)


def is_terminal_run_status(status: str | None) -> bool:
    normalized = (status or "").strip().lower()
    if not normalized:
        return False
    if normalized in _LEGACY_TERMINAL_STATUSES:
        return True
    canonical = canonical_run_statuses()
    if normalized not in canonical:
        return False
    return normalized not in _NON_TERMINAL_RUN_STATUSES


def recommended_poll_after_ms_for_run_status(status: str | None) -> int:
    normalized = (status or "").strip().lower()
    if is_terminal_run_status(normalized):
        return 10000
    if normalized in {"running", "dispatching", "retrying"}:
        return 1000
    if normalized in {"requested", "queued"}:
        return 1500
    return 3000
