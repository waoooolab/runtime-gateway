"""JSON Schema contract validators backed by platform-contracts."""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any

from jsonschema import Draft202012Validator

TOKEN_EXCHANGE_SCHEMA = "auth/token-exchange.v1.json"
EVENT_ENVELOPE_SCHEMA = "event-envelope.v1.json"
COMMAND_ENVELOPE_SCHEMA = "command-envelope.v1.json"
EXECUTION_CONTEXT_SCHEMA = "runtime/execution-context.v1.json"
EXECUTOR_PROFILE_CATALOG_SCHEMA = "runtime/executor-profile-catalog.v1.json"
TOOL_CATALOG_SCHEMA = "runtime/tool-catalog.v1.json"
ORCHESTRATION_HINTS_SCHEMA = "runtime/orchestration-hints.v1.json"
TEMPLATE_CAPABILITY_BINDING_SCHEMA = "runtime/workflow-template-capability-binding-contract.v1.json"
RUNTIME_EVENTS_PAGE_SCHEMA = "runtime/runtime-events-page.v1.json"
RUNTIME_RUN_LEASE_SCHEMA = "runtime/runtime-run-lease.v1.json"
RUNTIME_WORKER_HEALTH_SCHEMA = "runtime/runtime-worker-health.v1.json"
RUNTIME_WORKER_STATUS_SCHEMA = "runtime/runtime-worker-status.v1.json"
RUNTIME_WORKER_DRAIN_SCHEMA = "runtime/runtime-worker-drain.v1.json"

_VALIDATOR_CACHE: dict[str, Draft202012Validator] = {}
_CACHE_LOCK = Lock()
_PLATFORM_CONTRACTS_DIR_ENV = "OWA_PLATFORM_CONTRACTS_DIR"
_PLATFORM_CONTRACTS_DIR_ENV_LEGACY = "WAOOOOLAB_PLATFORM_CONTRACTS_DIR"


class ContractValidationError(ValueError):
    """Raised when payload does not satisfy platform contract schema."""


def _contracts_root() -> Path:
    configured = os.environ.get(_PLATFORM_CONTRACTS_DIR_ENV)
    if configured is None:
        configured = os.environ.get(_PLATFORM_CONTRACTS_DIR_ENV_LEGACY)
    if configured:
        return Path(configured).expanduser().resolve()
    # runtime-gateway/src/runtime_gateway/contracts/validation.py -> repository root
    return Path(__file__).resolve().parents[4] / "platform-contracts"


def _schema_path(schema_relative_path: str) -> Path:
    root = _contracts_root()
    jsonschema_root = root / "jsonschema"
    if jsonschema_root.exists():
        return jsonschema_root / schema_relative_path
    # Compatibility mode: allow OWA_PLATFORM_CONTRACTS_DIR (or legacy alias)
    # to point directly at the jsonschema root.
    return root / schema_relative_path


def _load_validator(schema_relative_path: str) -> Draft202012Validator:
    with _CACHE_LOCK:
        cached = _VALIDATOR_CACHE.get(schema_relative_path)
        if cached is not None:
            return cached

        path = _schema_path(schema_relative_path)
        if not path.exists():
            raise ContractValidationError(f"contract schema not found: {path}")
        try:
            schema = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ContractValidationError(f"invalid schema json: {path}") from exc

        try:
            validator = Draft202012Validator(
                schema,
                format_checker=Draft202012Validator.FORMAT_CHECKER,
            )
        except Exception as exc:  # pragma: no cover - defensive for malformed schemas
            raise ContractValidationError(f"schema compilation failed: {path}") from exc

        _VALIDATOR_CACHE[schema_relative_path] = validator
        return validator


def validate_contract(schema_relative_path: str, payload: dict[str, Any]) -> None:
    """Validate payload against a platform contract schema."""
    if not isinstance(payload, dict):
        raise ContractValidationError("payload must be object")

    validator = _load_validator(schema_relative_path)
    errors = sorted(validator.iter_errors(payload), key=lambda err: list(err.path))
    if errors:
        first = errors[0]
        location = ".".join(str(part) for part in first.path) or "$"
        raise ContractValidationError(
            f"{schema_relative_path} validation failed at {location}: {first.message}"
        )

    if "ts" in payload:
        ts = payload["ts"]
        if not isinstance(ts, str):
            raise ContractValidationError(
                f"{schema_relative_path} validation failed at ts: ts must be string"
            )
        try:
            datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ContractValidationError(
                f"{schema_relative_path} validation failed at ts: invalid date-time format"
            ) from exc


def validate_token_exchange_contract(payload: dict[str, Any]) -> None:
    validate_contract(TOKEN_EXCHANGE_SCHEMA, payload)


def validate_event_envelope_contract(payload: dict[str, Any]) -> None:
    validate_contract(EVENT_ENVELOPE_SCHEMA, payload)


def validate_command_envelope_contract(payload: dict[str, Any]) -> None:
    validate_contract(COMMAND_ENVELOPE_SCHEMA, payload)


def validate_execution_context_contract(payload: dict[str, Any]) -> None:
    validate_contract(EXECUTION_CONTEXT_SCHEMA, payload)


def validate_executor_profile_catalog_contract(payload: dict[str, Any]) -> None:
    validate_contract(EXECUTOR_PROFILE_CATALOG_SCHEMA, payload)


def validate_tool_catalog_contract(payload: dict[str, Any]) -> None:
    validate_contract(TOOL_CATALOG_SCHEMA, payload)


def validate_orchestration_hints_contract(payload: dict[str, Any]) -> None:
    validate_contract(ORCHESTRATION_HINTS_SCHEMA, payload)


def validate_template_capability_binding_contract(payload: dict[str, Any]) -> None:
    validate_contract(TEMPLATE_CAPABILITY_BINDING_SCHEMA, payload)


def validate_runtime_events_page_contract(payload: dict[str, Any]) -> None:
    validate_contract(RUNTIME_EVENTS_PAGE_SCHEMA, payload)


def validate_runtime_run_lease_contract(payload: dict[str, Any]) -> None:
    validate_contract(RUNTIME_RUN_LEASE_SCHEMA, payload)


def validate_runtime_worker_health_contract(payload: dict[str, Any]) -> None:
    validate_contract(RUNTIME_WORKER_HEALTH_SCHEMA, payload)


def validate_runtime_worker_status_contract(payload: dict[str, Any]) -> None:
    validate_contract(RUNTIME_WORKER_STATUS_SCHEMA, payload)


def validate_runtime_worker_drain_contract(payload: dict[str, Any]) -> None:
    validate_contract(RUNTIME_WORKER_DRAIN_SCHEMA, payload)
