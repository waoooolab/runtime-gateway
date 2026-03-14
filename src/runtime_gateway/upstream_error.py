"""Shared mapping helpers for upstream integration failures."""

from __future__ import annotations


def resolve_upstream_status_code(
    *,
    status_code: int | None,
    retryable: bool,
    message: str,
) -> int:
    if isinstance(status_code, int):
        return status_code
    if retryable:
        return 503
    lowered = message.strip().lower()
    if "connection error" in lowered or "timeout" in lowered:
        return 503
    return 502

