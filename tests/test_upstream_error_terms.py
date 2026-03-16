from __future__ import annotations

from runtime_gateway.upstream_error import (
    build_upstream_error_detail,
    extract_upstream_failure_classification,
    resolve_upstream_error_class,
)


def test_extract_upstream_failure_classification_normalizes_category_term() -> None:
    classification = extract_upstream_failure_classification(
        message="upstream failed",
        detail={"category": "Upstream.Unavailable"},
    )
    assert classification == "upstream_unavailable"


def test_extract_upstream_failure_classification_normalizes_classification_term() -> None:
    classification = extract_upstream_failure_classification(
        message="upstream failed",
        detail={"classification": "Capacity-Exhausted"},
    )
    assert classification == "capacity_exhausted"


def test_extract_upstream_failure_classification_uses_message_fallback() -> None:
    classification = extract_upstream_failure_classification(
        message="connection error calling upstream",
        detail={"classification": "###"},
    )
    assert classification == "upstream_unavailable"


def test_resolve_upstream_error_class_resolves_timeout() -> None:
    upstream_error_class = resolve_upstream_error_class(
        message="upstream request timeout after 10.0s",
        detail={"code": "UPSTREAM_TIMEOUT"},
        status_code=503,
        retryable=True,
        failure_classification="upstream_unavailable",
    )
    assert upstream_error_class == "timeout"


def test_resolve_upstream_error_class_resolves_unavailable() -> None:
    upstream_error_class = resolve_upstream_error_class(
        message="connection error calling runtime execution",
        detail={"category": "Upstream.Unavailable"},
        status_code=503,
        retryable=True,
        failure_classification="upstream_unavailable",
    )
    assert upstream_error_class == "unavailable"


def test_resolve_upstream_error_class_resolves_retryable_from_status() -> None:
    upstream_error_class = resolve_upstream_error_class(
        message="HTTP 503 calling run status endpoint",
        detail={"category": "Internal"},
        status_code=503,
        retryable=False,
        failure_classification="internal",
    )
    assert upstream_error_class == "retryable"


def test_build_upstream_error_detail_includes_resolved_upstream_error_class() -> None:
    detail = build_upstream_error_detail(
        message="connection error calling timeout endpoint",
        status_code=503,
        retryable=True,
        failure_classification="upstream_unavailable",
        detail={"code": "UPSTREAM_CONNECTION_ERROR"},
    )
    assert detail["upstream_error_class"] == "unavailable"
