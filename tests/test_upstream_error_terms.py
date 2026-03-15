from __future__ import annotations

from runtime_gateway.upstream_error import extract_upstream_failure_classification


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
