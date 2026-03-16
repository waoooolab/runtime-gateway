"""Shared mapping helpers for upstream integration failures."""

from __future__ import annotations

from typing import Any

from .code_terms import normalize_optional_code_term

_RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}
_UPSTREAM_ERROR_CLASSES = {"timeout", "unavailable", "retryable", "error"}
_TIMEOUT_HINT_TERMS = (
    "timeout",
    "timed out",
    "timed_out",
    "deadline exceeded",
    "deadline_exceeded",
)
_UNAVAILABLE_HINT_TERMS = (
    "connection error",
    "connection refused",
    "connection reset",
    "network unreachable",
    "name or service not known",
    "dns",
    "socket",
    "econn",
    "transport error",
    "upstream unavailable",
)


def _normalize_upstream_error_class(value: Any) -> str | None:
    normalized = normalize_optional_code_term(value)
    if normalized is None:
        return None
    alias_map = {
        "timeout": "timeout",
        "timed_out": "timeout",
        "deadline_exceeded": "timeout",
        "unavailable": "unavailable",
        "upstream_unavailable": "unavailable",
        "upstream_connection_error": "unavailable",
        "connection_error": "unavailable",
        "retryable": "retryable",
        "retry": "retryable",
        "error": "error",
        "upstream_error": "error",
    }
    resolved = alias_map.get(normalized)
    if resolved in _UPSTREAM_ERROR_CLASSES:
        return resolved
    return None


def _contains_hint_term(text: str, *, terms: tuple[str, ...]) -> bool:
    lowered = text.strip().lower()
    if not lowered:
        return False
    return any(term in lowered for term in terms)


def _collect_detail_hint_text(detail: Any) -> str:
    if not isinstance(detail, dict):
        return ""
    parts: list[str] = []
    for key in (
        "category",
        "code",
        "message",
        "classification",
        "failure_classification",
    ):
        value = detail.get(key)
        if isinstance(value, str) and value.strip():
            parts.append(value.strip())
    nested_failure = detail.get("failure")
    if isinstance(nested_failure, dict):
        for key in ("classification", "code", "message"):
            value = nested_failure.get(key)
            if isinstance(value, str) and value.strip():
                parts.append(value.strip())
    return " ".join(parts)


def resolve_upstream_error_class(
    *,
    message: str,
    detail: Any,
    status_code: int | None,
    retryable: bool,
    failure_classification: str | None = None,
) -> str:
    if isinstance(detail, dict):
        explicit = _normalize_upstream_error_class(detail.get("upstream_error_class"))
        if explicit is not None:
            return explicit

    hint_text = " ".join(
        part
        for part in (
            message.strip(),
            _collect_detail_hint_text(detail),
        )
        if part
    )
    if _contains_hint_term(hint_text, terms=_UNAVAILABLE_HINT_TERMS):
        return "unavailable"
    if _contains_hint_term(hint_text, terms=_TIMEOUT_HINT_TERMS):
        return "timeout"

    normalized_failure_classification = normalize_optional_code_term(failure_classification)
    if isinstance(normalized_failure_classification, str):
        if normalized_failure_classification == "upstream_unavailable":
            return "unavailable"
        if _contains_hint_term(normalized_failure_classification, terms=_TIMEOUT_HINT_TERMS):
            return "timeout"
    if retryable or (isinstance(status_code, int) and status_code in _RETRYABLE_STATUS_CODES):
        return "retryable"
    return "error"


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
    upstream_error_class: str | None = None,
    retry_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_upstream_error_class = _normalize_upstream_error_class(upstream_error_class)
    if normalized_upstream_error_class is None:
        normalized_upstream_error_class = resolve_upstream_error_class(
            message=message,
            detail=detail,
            status_code=status_code,
            retryable=retryable,
            failure_classification=failure_classification,
        )
    payload: dict[str, Any] = {
        "message": message,
        "status_code": status_code,
        "retryable": retryable,
        "failure_classification": failure_classification,
        "upstream_error_class": normalized_upstream_error_class,
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
