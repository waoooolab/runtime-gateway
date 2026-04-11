"""Canonical execution-ingress surfaces owned by runtime-gateway."""

from __future__ import annotations

from typing import Any

INGRESS_AUTHORITY = "runtime-gateway"
RUN_SUBMIT_SURFACE = "/v1/runs"
SCHEDULER_ENQUEUE_SURFACE = "/v1/orchestration/scheduler:enqueue"
SCHEDULER_TICK_SURFACE = "/v1/orchestration/scheduler:tick"
SCHEDULER_HEALTH_SURFACE = "/v1/orchestration/scheduler:health"
SCHEDULER_REGISTRY_SURFACE = "/v1/orchestration/scheduler:registry"
SCHEDULER_CANCEL_SURFACE = "/v1/orchestration/scheduler:cancel"


def execution_ingress_contract_payload() -> dict[str, Any]:
    """Return a stable execution-ingress contract projection for API responses."""
    return {
        "authority": INGRESS_AUTHORITY,
        "run_submit_surface": RUN_SUBMIT_SURFACE,
        "scheduler": {
            "enqueue_surface": SCHEDULER_ENQUEUE_SURFACE,
            "tick_surface": SCHEDULER_TICK_SURFACE,
            "health_surface": SCHEDULER_HEALTH_SURFACE,
            "registry_surface": SCHEDULER_REGISTRY_SURFACE,
            "cancel_surface": SCHEDULER_CANCEL_SURFACE,
        },
    }
