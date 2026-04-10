# runtime-gateway AGENTS.md

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
