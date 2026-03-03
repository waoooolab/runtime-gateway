"""Executor profile compatibility checks for gateway ingress."""

from __future__ import annotations

from dataclasses import dataclass

from .contracts.validation import ContractValidationError


@dataclass(frozen=True)
class ExecutorProfile:
    family: str
    engines: tuple[str, ...]
    adapters: tuple[str, ...]


PROFILES: dict[str, ExecutorProfile] = {
    "acp": ExecutorProfile(
        family="acp",
        engines=("codex", "claude_code", "gemini_cli", "opencode", "droid"),
        adapters=("direct", "ccb"),
    ),
    "native_agent": ExecutorProfile(
        family="native_agent",
        engines=("pi_ai",),
        adapters=("direct",),
    ),
    "workflow_runtime": ExecutorProfile(
        family="workflow_runtime",
        engines=("langgraph",),
        adapters=("api",),
    ),
    "compute_runtime": ExecutorProfile(
        family="compute_runtime",
        engines=("device_hub",),
        adapters=("api",),
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
