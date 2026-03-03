from runtime_gateway.contracts.validation import ContractValidationError
from runtime_gateway.executor_profiles import validate_executor_profile


def test_validate_executor_profile_accepts_supported_profile() -> None:
    validate_executor_profile(
        family="acp",
        engine="claude_code",
        adapter="ccb",
    )


def test_validate_executor_profile_rejects_unsupported_engine() -> None:
    try:
        validate_executor_profile(
            family="acp",
            engine="unknown",
            adapter="ccb",
        )
    except ContractValidationError as exc:
        assert "unsupported for family 'acp'" in str(exc)
    else:
        raise AssertionError("expected ContractValidationError")


def test_validate_executor_profile_rejects_unsupported_adapter() -> None:
    try:
        validate_executor_profile(
            family="workflow_runtime",
            engine="langgraph",
            adapter="direct",
        )
    except ContractValidationError as exc:
        assert "unsupported for family 'workflow_runtime'" in str(exc)
    else:
        raise AssertionError("expected ContractValidationError")
