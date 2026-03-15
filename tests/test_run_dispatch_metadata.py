from __future__ import annotations

from runtime_gateway.run_dispatch import _extract_route_failure_metadata


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
            },
        }
    }

    metadata = _extract_route_failure_metadata(downstream_event)
    assert metadata["run_status"] == "failed"
    assert metadata["failure_code"] == "capacity_exhausted"
    assert metadata["failure_classification"] == "capacity_exhausted"
    assert metadata["placement_reason_code"] == "no_eligible_device"
