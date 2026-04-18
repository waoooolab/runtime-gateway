# runtime-gateway AGENTS.md
Status: active
Owner: runtime-gateway-maintainers

## Service Description
Runtime gateway HTTP boundary that validates contracts, exchanges delegated tokens, and dispatches run/control requests to runtime-execution.

## Entrypoints
- `src/runtime_gateway/app.py` (`runtime_gateway.app:app`) - FastAPI gateway entry.
- `src/runtime_gateway/routes_runs.py` - `/v1/runs` and run lifecycle route assembly.
- `src/runtime_gateway/token_exchange_api.py` - delegated token exchange endpoint.

## Key Modules
- `src/runtime_gateway/run_dispatch.py` and `src/runtime_gateway/integration/runtime_execution.py` - downstream dispatch and runtime-execution integration.
- `src/runtime_gateway/contracts/validation.py` and `src/runtime_gateway/events/validation.py` - schema validation boundaries.
- `src/runtime_gateway/auth/tokens.py` and `src/runtime_gateway/security.py` - auth and scope checks.
- `src/runtime_gateway/events/bus.py` and `src/runtime_gateway/events/durable.py` - event bus and durable event storage.

## Common Commands
- `uv run pytest -q`
- `uv run uvicorn runtime_gateway.app:app --host 0.0.0.0 --port 8002`
- `python3 scripts/check_boundary_imports.py --own-package runtime_gateway --src src/runtime_gateway`
- `python3 scripts/check_code_shape.py`

## ACP Provider Playbook

### Claude Code (claude)
- Start/Connection: Route requests with provider id `claude` and inject Anthropic credentials through gateway-managed secret config.
- Best Practices: Keep contract checks ahead of provider invocation and include explicit tool/response schemas.
- Known Limitations/Notes: Large payloads can increase end-to-end latency; monitor throttling and backoff policies.
- Suitable Scenarios: Policy-rich runtime decisions, human-readable root-cause analysis, and context-heavy triage.

### Codex
- Start/Connection: Route with provider id `codex` via configured OpenAI-compatible endpoint.
- Best Practices: Enforce strict JSON shape at gateway boundary and use bounded retries with idempotency keys.
- Known Limitations/Notes: Tool call verbosity can grow without concise system prompts; maintain response size limits.
- Suitable Scenarios: Structured remediation plans, code-focused diagnostics, and deterministic command composition.

### Gemini
- Start/Connection: Route with provider id `gemini` and load Google AI credentials from runtime configuration.
- Best Practices: Keep instruction blocks segmented by intent and define explicit output schema requirements.
- Known Limitations/Notes: Capability/quotas vary by model and region; validate fallback chain behavior.
- Suitable Scenarios: Long-context summarization, semantic grouping, and broad retrieval-assisted reasoning.

### OpenCode (droid)
- Start/Connection: Route with provider id `droid` and establish ACP session metadata (workspace, task, user scope).
- Best Practices: Require command/tool allowlists, short-lived sessions, and explicit cancellation semantics.
- Known Limitations/Notes: Session state may reset after transport reconnect; partial stream handling must be resilient.
- Suitable Scenarios: Interactive tool orchestration, runtime command execution, and operator-guided automation.

### Other Mainstream ACP Providers
- Start/Connection: Integrate additional providers through OpenAI-compatible paths (Azure OpenAI, enterprise proxy, self-hosted models).
- Best Practices: Keep compatibility adapters thin and test contract conformance per provider profile.
- Known Limitations/Notes: Token limits and function-calling formats differ; do not assume one canonical provider behavior.
- Suitable Scenarios: Cost-tier routing, sovereign/private deployments, and failover across provider pools.
