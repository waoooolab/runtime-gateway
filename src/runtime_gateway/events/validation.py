"""Runtime event envelope validation helpers."""

from __future__ import annotations

from runtime_gateway.contracts.validation import (
    ContractValidationError,
    validate_event_envelope_contract,
)


def validate_event_envelope(envelope: dict) -> None:
    """Validate event envelope against platform contracts schema."""
    try:
        validate_event_envelope_contract(envelope)
    except ContractValidationError as exc:
        raise ValueError(str(exc)) from exc
