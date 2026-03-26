"""Error-budget policy contract projection for runtime surfaces."""

from __future__ import annotations

from typing import Any, Iterable, Literal

ErrorBudgetLevel = Literal["green", "yellow", "red"]
ErrorBudgetAction = Literal["allow_new_dispatch", "defer_new_dispatch", "pause_new_dispatch"]

WARNING_ANOMALY_RATIO_THRESHOLD = 0.7
HARD_STOP_ANOMALY_RATIO_THRESHOLD = 0.9
WARNING_REASON_CODES = frozenset(
    {
        "drain_anomaly_ratio_exceeded",
        "lease_renew_errors_present",
        "lease_renew_expired_conflicts_present",
        "lease_renew_released_conflicts_present",
    }
)
HARD_STOP_REASON_CODES = frozenset({"worker_stalled"})

_SATURATION_TO_ERROR_BUDGET_LEVEL: dict[str, ErrorBudgetLevel] = {
    "normal": "green",
    "warning": "yellow",
    "hard_stop": "red",
}

_ERROR_BUDGET_ACTIONS: dict[ErrorBudgetLevel, ErrorBudgetAction] = {
    "green": "allow_new_dispatch",
    "yellow": "defer_new_dispatch",
    "red": "pause_new_dispatch",
}


def _normalize_reason_codes(reason_codes: Iterable[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for code in reason_codes:
        token = str(code).strip()
        if not token or token in seen:
            continue
        seen.add(token)
        normalized.append(token)
    return normalized


def _classify_saturation(
    *,
    reason_codes: Iterable[str],
    anomaly_ratio: float | None,
) -> str:
    reason_set = set(_normalize_reason_codes(reason_codes))
    if reason_set.intersection(HARD_STOP_REASON_CODES):
        return "hard_stop"
    if isinstance(anomaly_ratio, (int, float)) and float(anomaly_ratio) >= HARD_STOP_ANOMALY_RATIO_THRESHOLD:
        return "hard_stop"
    if reason_set.intersection(WARNING_REASON_CODES):
        return "warning"
    if isinstance(anomaly_ratio, (int, float)) and float(anomaly_ratio) >= WARNING_ANOMALY_RATIO_THRESHOLD:
        return "warning"
    return "normal"


def parse_reason_codes_query(raw_reason_codes: str | None) -> list[str]:
    if raw_reason_codes is None:
        return []
    return _normalize_reason_codes(raw_reason_codes.split(","))


def error_budget_level_from_saturation(saturation_level: str | None) -> ErrorBudgetLevel | None:
    normalized = str(saturation_level or "").strip().lower()
    if not normalized:
        return None
    return _SATURATION_TO_ERROR_BUDGET_LEVEL.get(normalized)


def classify_error_budget_level(
    *,
    reason_codes: Iterable[str],
    anomaly_ratio: float | None,
    saturation_level: str | None = None,
) -> ErrorBudgetLevel:
    explicit = error_budget_level_from_saturation(saturation_level)
    if explicit is not None:
        return explicit
    inferred_saturation = _classify_saturation(
        reason_codes=reason_codes,
        anomaly_ratio=anomaly_ratio,
    )
    return _SATURATION_TO_ERROR_BUDGET_LEVEL[inferred_saturation]


def resolve_error_budget_policy_snapshot() -> dict[str, Any]:
    warning_reasons = sorted(WARNING_REASON_CODES)
    hard_stop_reasons = sorted(HARD_STOP_REASON_CODES)
    return {
        "schema_version": "runtime.error_budget_policy.v1",
        "levels": {
            "green": {
                "action": _ERROR_BUDGET_ACTIONS["green"],
                "saturation_aliases": ["normal"],
                "reason_codes": [],
            },
            "yellow": {
                "action": _ERROR_BUDGET_ACTIONS["yellow"],
                "saturation_aliases": ["warning"],
                "anomaly_ratio_threshold": WARNING_ANOMALY_RATIO_THRESHOLD,
                "reason_codes": warning_reasons,
            },
            "red": {
                "action": _ERROR_BUDGET_ACTIONS["red"],
                "saturation_aliases": ["hard_stop"],
                "anomaly_ratio_threshold": HARD_STOP_ANOMALY_RATIO_THRESHOLD,
                "reason_codes": hard_stop_reasons,
            },
        },
        "thresholds": {
            "yellow_anomaly_ratio": WARNING_ANOMALY_RATIO_THRESHOLD,
            "red_anomaly_ratio": HARD_STOP_ANOMALY_RATIO_THRESHOLD,
        },
        "compatibility_aliases": dict(_SATURATION_TO_ERROR_BUDGET_LEVEL),
        "actions": dict(_ERROR_BUDGET_ACTIONS),
        "reason_taxonomy": {
            "yellow": warning_reasons,
            "red": hard_stop_reasons,
        },
    }


def evaluate_error_budget_decision(
    *,
    reason_codes: Iterable[str],
    anomaly_ratio: float | None,
    saturation_level: str | None = None,
) -> dict[str, Any]:
    normalized_reason_codes = _normalize_reason_codes(reason_codes)
    resolved_level = classify_error_budget_level(
        reason_codes=normalized_reason_codes,
        anomaly_ratio=anomaly_ratio,
        saturation_level=saturation_level,
    )
    matched_reason_codes: list[str] = []
    if resolved_level == "yellow":
        matched_reason_codes = sorted(set(normalized_reason_codes).intersection(WARNING_REASON_CODES))
    elif resolved_level == "red":
        matched_reason_codes = sorted(set(normalized_reason_codes).intersection(HARD_STOP_REASON_CODES))
    inferred_saturation = _classify_saturation(
        reason_codes=normalized_reason_codes,
        anomaly_ratio=anomaly_ratio,
    )
    return {
        "schema_version": "runtime.error_budget_decision.v1",
        "level": resolved_level,
        "action": _ERROR_BUDGET_ACTIONS[resolved_level],
        "saturation_level": str(saturation_level).strip().lower() if str(saturation_level or "").strip() else inferred_saturation,
        "anomaly_ratio": float(anomaly_ratio) if isinstance(anomaly_ratio, (int, float)) else None,
        "reason_codes": normalized_reason_codes,
        "matched_reason_codes": matched_reason_codes,
    }
