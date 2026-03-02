"""Token exchange endpoint orchestration for runtime-gateway."""

from __future__ import annotations

import uuid

from fastapi import HTTPException

from .api.schemas import TokenExchangeRequest, TokenExchangeResponse
from .audit.emitter import emit_audit_event
from .auth.exchange import ExchangeError, exchange_subject_token
from .auth.tokens import TokenError, verify_token
from .contracts.validation import ContractValidationError, validate_token_exchange_contract


def _validate_exchange_payload(payload: dict) -> None:
    try:
        validate_token_exchange_contract(payload)
    except ContractValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _resolve_actor_id(subject_token: str) -> str:
    try:
        claims = verify_token(subject_token)
    except TokenError:
        return "unknown"
    return str(claims.get("sub", "unknown"))


def _exchange_or_raise(req: TokenExchangeRequest, actor_id: str) -> dict:
    try:
        return exchange_subject_token(
            subject_token=req.subject_token,
            requested_token_use=req.requested_token_use,
            audience=req.audience,
            scope=req.scope,
            requested_ttl_seconds=req.requested_ttl_seconds,
            tenant_id=req.tenant_id,
            app_id=req.app_id,
            session_key=req.session_key,
            run_id=req.run_id,
            task_id=req.task_id,
            trace_id=req.trace_id or str(uuid.uuid4()),
        )
    except ExchangeError as exc:
        emit_audit_event(
            action="auth.token_exchange",
            decision="deny",
            actor_id=actor_id,
            trace_id=req.trace_id,
            metadata={"reason": exc.detail, "audience": req.audience},
        )
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


def token_exchange_response(req: TokenExchangeRequest) -> TokenExchangeResponse:
    _validate_exchange_payload(req.model_dump())
    actor_id = _resolve_actor_id(req.subject_token)
    exchange_result = _exchange_or_raise(req, actor_id)
    response = TokenExchangeResponse(**exchange_result)
    try:
        validate_token_exchange_contract(response.model_dump())
    except ContractValidationError as exc:
        raise HTTPException(status_code=500, detail=f"token exchange response invalid: {exc}") from exc
    emit_audit_event(
        action="auth.token_exchange",
        decision="allow",
        actor_id=actor_id,
        trace_id=req.trace_id,
        metadata={
            "audience": req.audience,
            "token_use": req.requested_token_use,
            "scope": req.scope,
        },
    )
    return response
