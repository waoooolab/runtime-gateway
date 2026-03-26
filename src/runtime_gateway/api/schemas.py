from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class RetryPolicyInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_attempts: int = Field(ge=1, le=20)
    backoff_ms: int = Field(ge=0)
    strategy: str = Field(pattern="^(fixed|exponential)$")


class ContractVersionsInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_contract_version: str | None = Field(default=None, min_length=1)
    agent_contract_version: str | None = Field(default=None, min_length=1)
    event_schema_version: str | None = Field(default=None, min_length=1)


class CreateRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: str
    app_id: str
    session_key: str
    scope_id: str | None = Field(default=None, min_length=1)
    scope_type: str | None = Field(default=None, min_length=1)
    payload: dict
    retry_policy: RetryPolicyInput | None = None
    contract_versions: ContractVersionsInput | None = None


class CreateRunResponse(BaseModel):
    run_id: str
    status: str


class TokenExchangeRequest(BaseModel):
    kind: str = "request"
    grant_type: str = "urn:waoooolab:params:oauth:grant-type:token-exchange"
    subject_token: str = Field(min_length=16)
    subject_token_type: str = "urn:ietf:params:oauth:token-type:access_token"
    requested_token_use: str = "service"
    audience: str
    scope: list[str]
    tenant_id: str | None = None
    app_id: str | None = None
    session_key: str | None = None
    run_id: str | None = None
    task_id: str | None = None
    trace_id: str | None = None
    requested_ttl_seconds: int = Field(default=300, ge=30, le=3600)


class TokenExchangeResponse(BaseModel):
    kind: str = "response"
    issued_token_type: str
    token_type: str = "Bearer"
    access_token: str
    expires_in: int
    scope: list[str]
    audience: str
    token_use: str
    jti: str
