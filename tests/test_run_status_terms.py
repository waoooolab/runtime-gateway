from __future__ import annotations

from runtime_gateway.run_status_terms import (
    canonical_run_statuses,
    is_terminal_run_status,
    recommended_poll_after_ms_for_run_status,
)


def test_canonical_run_statuses_loaded_from_contract_schema() -> None:
    statuses = canonical_run_statuses()
    assert "queued" in statuses
    assert "running" in statuses
    assert "succeeded" in statuses
    assert "timed_out" in statuses
    assert "rejected" not in statuses


def test_is_terminal_run_status_uses_canonical_terms_with_legacy_compat() -> None:
    assert is_terminal_run_status("queued") is False
    assert is_terminal_run_status("running") is False
    assert is_terminal_run_status("succeeded") is True
    assert is_terminal_run_status("failed") is True
    assert is_terminal_run_status("dlq") is True
    assert is_terminal_run_status("rejected") is True
    assert is_terminal_run_status("unknown_status") is False


def test_recommended_poll_after_ms_for_run_status() -> None:
    assert recommended_poll_after_ms_for_run_status("queued") == 1500
    assert recommended_poll_after_ms_for_run_status("running") == 1000
    assert recommended_poll_after_ms_for_run_status("succeeded") == 10000
    assert recommended_poll_after_ms_for_run_status("dlq") == 10000
    assert recommended_poll_after_ms_for_run_status("rejected") == 10000
    assert recommended_poll_after_ms_for_run_status("new_future_status") == 3000
