from __future__ import annotations

from runtime_gateway.run_dispatch import (
    _extract_route_failure_metadata,
    _extract_route_success_metadata,
)


def test_extract_route_failure_metadata_normalizes_code_terms() -> None:
    downstream_event = {
        "payload": {
            "status": "failed",
            "failure": {
                "code": "Capacity-Exhausted",
                "classification": "Capacity-Exhausted",
                "message": "capacity exhausted",
            },
            "decision": {
                "reason_code": "NoEligibleDevice",
                "placement_event_type": "device.route.rejected",
                "placement_audit": {
                    "candidate_device_count": 2,
                    "candidate_execution_sites": ["local", "cloud"],
                    "selected_device_id": "gpu-node-1",
                    "selected_execution_site": "cloud",
                    "fallback_applied": True,
                    "fallback_reason_code": "Local.Preference.Fallback",
                    "failure_domain": "Execution.Site",
                },
            },
        }
    }

    metadata = _extract_route_failure_metadata(downstream_event)
    assert metadata["run_status"] == "failed"
    assert metadata["failure_code"] == "capacity_exhausted"
    assert metadata["failure_classification"] == "capacity_exhausted"
    assert metadata["placement_reason_code"] == "no_eligible_device"
    assert metadata["placement_audit"] == {
        "candidate_device_count": 2,
        "candidate_execution_sites": ["local", "cloud"],
        "selected_device_id": "gpu-node-1",
        "selected_execution_site": "cloud",
        "fallback_applied": True,
        "fallback_reason_code": "local_preference_fallback",
        "failure_domain": "execution_site",
    }


def test_extract_route_success_metadata_normalizes_placement_reason_code() -> None:
    execution_event = {
        "payload": {
            "route": {
                "execution_mode": "compute",
                "route_target": "device-hub",
                "placement_reason_code": "NoEligibleDevice",
                "placement_event_type": "device.route.decided",
                "placement_audit": {
                    "candidate_device_count": 1,
                    "selected_device_id": "gpu-node-2",
                    "selected_execution_site": "local",
                    "fallback_applied": False,
                    "failure_domain": "Capability.Context",
                },
            }
        }
    }

    metadata = _extract_route_success_metadata(execution_event)
    assert metadata["execution_mode"] == "compute"
    assert metadata["route_target"] == "device-hub"
    assert metadata["placement_reason_code"] == "no_eligible_device"
    assert metadata["placement_event_type"] == "device.route.decided"
    assert metadata["placement_audit"] == {
        "candidate_device_count": 1,
        "selected_device_id": "gpu-node-2",
        "selected_execution_site": "local",
        "fallback_applied": False,
        "failure_domain": "capability_context",
    }


def test_extract_route_failure_metadata_uses_flat_failure_fields_when_nested_missing() -> None:
    downstream_event = {
        "payload": {
            "status": "failed",
            "failure_code": "No-Eligible-Device",
            "failure_classification": "Capacity-Exhausted",
            "failure_message": "no eligible device",
            "decision": {
                "reason_code": "NoEligibleDevice",
                "placement_event_type": "device.route.rejected",
            },
        }
    }

    metadata = _extract_route_failure_metadata(downstream_event)
    assert metadata["run_status"] == "failed"
    assert metadata["failure_code"] == "no_eligible_device"
    assert metadata["failure_classification"] == "capacity_exhausted"
    assert metadata["failure_message"] == "no eligible device"
    assert metadata["placement_reason_code"] == "no_eligible_device"
    assert metadata["placement_event_type"] == "device.route.rejected"


def test_extract_route_failure_metadata_uses_flat_placement_fields_when_decision_missing() -> None:
    downstream_event = {
        "payload": {
            "status": "failed",
            "failure_code": "Route.Unavailable",
            "failure_classification": "Internal",
            "failure_message": "unable to select device from eligible route set",
            "placement_reason_code": "Route.Unavailable",
            "placement_event_type": "device.route.rejected",
            "placement_resource_snapshot": {
                "eligible_devices": 1,
                "active_leases": 0,
                "available_slots": 1,
                "tenant_id": "t1",
                "tenant_active_leases": 0,
                "queue_depth": 0,
            },
            "placement_audit": {
                "candidate_device_count": 1,
                "selected_device_id": "gpu-node-3",
                "selected_execution_site": "cloud",
                "fallback_applied": True,
                "fallback_reason_code": "node-pool-fallback",
                "failure_domain": "Node.Pool",
            },
        }
    }

    metadata = _extract_route_failure_metadata(downstream_event)
    assert metadata["run_status"] == "failed"
    assert metadata["failure_code"] == "route_unavailable"
    assert metadata["failure_classification"] == "internal"
    assert metadata["failure_message"] == "unable to select device from eligible route set"
    assert metadata["placement_reason_code"] == "route_unavailable"
    assert metadata["placement_event_type"] == "device.route.rejected"
    assert metadata["placement_resource_snapshot"] == {
        "queue_depth": 0,
        "eligible_devices": 1,
        "active_leases": 0,
        "available_slots": 1,
        "tenant_active_leases": 0,
        "tenant_id": "t1",
    }
    assert metadata["placement_audit"] == {
        "candidate_device_count": 1,
        "selected_device_id": "gpu-node-3",
        "selected_execution_site": "cloud",
        "fallback_applied": True,
        "fallback_reason_code": "node_pool_fallback",
        "failure_domain": "node_pool",
    }
