"""Shared mapping helpers for upstream integration failures."""

from __future__ import annotations

from typing import Any

from .code_terms import normalize_optional_code_term

_RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}


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


def extract_upstream_failure_classification(
    *,
    message: str,
    detail: Any,
) -> str:
    if isinstance(detail, dict):
        category = detail.get("category")
        normalized_category = normalize_optional_code_term(category)
        if normalized_category is not None:
            return normalized_category
        classification = detail.get("classification")
        normalized_classification = normalize_optional_code_term(classification)
        if normalized_classification is not None:
            return normalized_classification
    lowered = message.strip().lower()
    if "connection error" in lowered or "timeout" in lowered:
        return "upstream_unavailable"
    return "upstream_error"


def resolve_upstream_retryable(
    *,
    status_code: int | None,
    retryable: bool,
    message: str,
    detail: Any,
) -> bool:
    if isinstance(detail, dict):
        detail_retryable = detail.get("retryable")
        if isinstance(detail_retryable, bool):
            return detail_retryable
    if retryable:
        return True
    if isinstance(status_code, int):
        return status_code in _RETRYABLE_STATUS_CODES
    lowered = message.strip().lower()
    if "connection error" in lowered or "timeout" in lowered:
        return True
    return False


def build_upstream_error_detail(
    *,
    message: str,
    status_code: int,
    retryable: bool,
    failure_classification: str,
    detail: Any,
    retry_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "message": message,
        "status_code": status_code,
        "retryable": retryable,
        "failure_classification": failure_classification,
    }
    if isinstance(retry_policy, dict) and retry_policy:
        payload["retry_policy"] = dict(retry_policy)
    if isinstance(detail, dict):
        payload["upstream_detail"] = detail
        upstream_category = detail.get("category")
        if isinstance(upstream_category, str) and upstream_category.strip():
            payload["upstream_category"] = upstream_category.strip()
        upstream_code = detail.get("code")
        if isinstance(upstream_code, str) and upstream_code.strip():
            payload["upstream_code"] = upstream_code.strip()
        upstream_message = detail.get("message")
        if isinstance(upstream_message, str) and upstream_message.strip():
            payload["upstream_message"] = upstream_message.strip()
    return payload
