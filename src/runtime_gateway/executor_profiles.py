"""Executor profile compatibility checks for gateway ingress."""

from __future__ import annotations

from dataclasses import dataclass

from .contracts.validation import ContractValidationError


@dataclass(frozen=True)
class ExecutorProfile:
    family: str
    engines: tuple[str, ...]
    adapters: tuple[str, ...]
    access_modes: tuple[str, ...]
    window_modes: tuple[str, ...]


PROFILES: dict[str, ExecutorProfile] = {
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
