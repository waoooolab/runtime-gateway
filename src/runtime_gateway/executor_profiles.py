"""Executor profile compatibility checks for gateway ingress."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
import os
from pathlib import Path

from .contracts.validation import ContractValidationError


@dataclass(frozen=True)
class ExecutorProfile:
    family: str
    engines: tuple[str, ...]
    adapters: tuple[str, ...]
    access_modes: tuple[str, ...]
    window_modes: tuple[str, ...]


_PLATFORM_CONTRACTS_DIR_ENV = "OWA_PLATFORM_CONTRACTS_DIR"
_PLATFORM_CONTRACTS_DIR_ENV_LEGACY = "WAOOOOLAB_PLATFORM_CONTRACTS_DIR"
_EXECUTOR_PROFILE_CATALOG_DATA_PATH = "catalog/runtime/executor-profile-catalog.data.v1.json"

_DEFAULT_PROFILES: dict[str, ExecutorProfile] = {
    "acp": ExecutorProfile(
        family="acp",
        engines=("codex", "claude_code", "gemini_cli", "opencode", "droid"),
        adapters=("orchestrator", "ccb"),
        access_modes=("direct", "api"),
        window_modes=("inline", "terminal_mux"),
    ),
    "native_agent": ExecutorProfile(
        family="native_agent",
        engines=("pi_ai",),
        adapters=("native",),
        access_modes=("direct",),
        window_modes=("inline",),
    ),
    "workflow_runtime": ExecutorProfile(
        family="workflow_runtime",
        engines=("langgraph",),
        adapters=("runtime_api",),
        access_modes=("api",),
        window_modes=("inline",),
    ),
    "compute_runtime": ExecutorProfile(
        family="compute_runtime",
        engines=("device_hub",),
        adapters=("runtime_api",),
        access_modes=("api",),
        window_modes=("inline",),
    ),
}


def _contracts_root() -> Path:
    configured = os.environ.get(_PLATFORM_CONTRACTS_DIR_ENV)
    if configured is None:
        configured = os.environ.get(_PLATFORM_CONTRACTS_DIR_ENV_LEGACY)
    if configured:
        return Path(configured).expanduser().resolve()
    # runtime-gateway/src/runtime_gateway/executor_profiles.py -> workspace root
    return Path(__file__).resolve().parents[3] / "platform-contracts"


def _catalog_data_path() -> Path:
    root = _contracts_root()
    if (root / "jsonschema").exists():
        return root / _EXECUTOR_PROFILE_CATALOG_DATA_PATH
    if root.name == "jsonschema":
        return root.parent / _EXECUTOR_PROFILE_CATALOG_DATA_PATH
    return root / _EXECUTOR_PROFILE_CATALOG_DATA_PATH


def _coerce_profile(raw: object) -> ExecutorProfile | None:
    if not isinstance(raw, dict):
        return None
    family = str(raw.get("family", "")).strip()
    if not family:
        return None
    engines = _coerce_str_tuple(raw.get("engines"))
    adapters = _coerce_str_tuple(raw.get("adapters"))
    access_modes = _coerce_str_tuple(raw.get("access_modes"))
    window_modes = _coerce_str_tuple(raw.get("window_modes"))
    if (
        engines is None
        or adapters is None
        or access_modes is None
        or window_modes is None
    ):
        return None
    return ExecutorProfile(
        family=family,
        engines=engines,
        adapters=adapters,
        access_modes=access_modes,
        window_modes=window_modes,
    )


def _coerce_str_tuple(raw: object) -> tuple[str, ...] | None:
    if not isinstance(raw, list):
        return None
    values: list[str] = []
    for item in raw:
        if not isinstance(item, str):
            return None
        normalized = item.strip()
        if not normalized:
            return None
        values.append(normalized)
    if not values:
        return None
    return tuple(values)


@lru_cache(maxsize=1)
def _load_profiles_from_catalog_data() -> dict[str, ExecutorProfile] | None:
    path = _catalog_data_path()
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    items = payload.get("items")
    if not isinstance(items, list):
        return None
    profiles: dict[str, ExecutorProfile] = {}
    for item in items:
        profile = _coerce_profile(item)
        if profile is None:
            return None
        profiles[profile.family] = profile
    if not profiles:
        return None
    return profiles


def _build_profiles() -> dict[str, ExecutorProfile]:
    loaded = _load_profiles_from_catalog_data()
    if loaded is not None:
        return loaded
    return dict(_DEFAULT_PROFILES)


PROFILES = _build_profiles()


def list_executor_profiles() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for family in sorted(PROFILES.keys()):
        profile = PROFILES[family]
        rows.append(
            {
                "family": profile.family,
                "engines": list(profile.engines),
                "adapters": list(profile.adapters),
                "access_modes": list(profile.access_modes),
                "window_modes": list(profile.window_modes),
            }
        )
    return rows


def validate_executor_profile(*, family: str, engine: str, adapter: str) -> None:
    profile = PROFILES.get(family)
    if profile is None:
        return
    if engine not in profile.engines:
        raise ContractValidationError(
            f"execution_context.executor.engine '{engine}' is unsupported for family "
            f"'{family}'; expected one of: {', '.join(profile.engines)}"
        )
    if adapter not in profile.adapters:
        raise ContractValidationError(
            f"execution_context.executor.adapter '{adapter}' is unsupported for family "
            f"'{family}'; expected one of: {', '.join(profile.adapters)}"
        )
