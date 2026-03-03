# runtime-gateway
[![CI](https://github.com/waoooolab/runtime-gateway/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/waoooolab/runtime-gateway/actions/workflows/ci.yml)

Runtime gateway service.

P0 scope:
- HTTP/REST API entrypoints
- Event envelope generation
- Delegated token exchange and dispatch to runtime-execution boundary API

Current modules (src layout):
- `src/runtime_gateway/api`
- `src/runtime_gateway/auth`
- `src/runtime_gateway/contracts`
- `src/runtime_gateway/events`
- `src/runtime_gateway/audit`
- `src/runtime_gateway/integration`

Current endpoints:
- `GET /healthz`
- `GET /v1/audit/events` (dev baseline read endpoint)
- `POST /v1/auth/token/exchange` (baseline delegated token exchange)
- `POST /v1/runs` (protected by Bearer token middleware)
- `GET /v1/executors/profiles` (protected by Bearer token middleware)
- `POST /v1/runs/{run_id}:approve` (protected by Bearer token middleware)
- `POST /v1/runs/{run_id}:reject` (protected by Bearer token middleware)
- `POST /v1/orchestration/worker:tick` (protected by Bearer token middleware)
- `POST /v1/orchestration/worker:drain` (protected by Bearer token middleware)

Validation and auth notes:
- `auth/exchange.py` contains framework-agnostic token exchange logic
- `contracts/validation.py` validates payloads against `platform-contracts/jsonschema`
- `events/validation.py` validates event envelope via contract schema
- `/v1/runs` requires `aud=runtime-gateway` and `runs:write` scope
- `/v1/runs` exchanges caller token into delegated `aud=runtime-execution` service token
- `/v1/runs` sends `command-envelope.v1` to `runtime-execution` via HTTP boundary
- `/v1/runs` validates downstream event envelope and returns normalized run response
- `/v1/runs` validates executor profile compatibility for `execution_context.executor`
  (`family/engine/adapter`, optional `access_mode/window_mode`) before dispatching downstream
- `/v1/executors/profiles` returns the gateway-side supported executor profile catalog
  and requires `runs:read`
- executor profile catalog fields:
  - `adapters`: execution program axis (`orchestrator|ccb|runtime_api|native`)
  - `access_modes`: transport access axis (`direct|api`)
  - `window_modes`: presentation axis (`inline|terminal_mux`)
- `/v1/runs/{run_id}:approve` and `:reject` forward to runtime-execution and publish downstream events to gateway event bus
- `/v1/orchestration/worker:tick` and `/v1/orchestration/worker:drain` forward
  to runtime-execution through delegated service tokens (`runs:write`)
- auth and run actions emit audit events in memory, and optionally to file via `RUNTIME_GATEWAY_AUDIT_LOG_PATH`
- gateway currently forwards runtime workload route semantics
  (`execution_mode=control|compute`) and consumes `execution_context.executor`
  as an ingress compatibility gate (not as dispatch target selector)

Required environment:
- `RUNTIME_EXECUTION_BASE_URL` (default: `http://localhost:8003`)

Testing:
- `tests/test_app_integration.py` covers HTTP auth rejections and valid run creation path.
- `tests/test_e2e_run_flow.py` covers cross-service run flow (`runtime-gateway -> runtime-execution`).
- `scripts/check_code_shape.py` reports file/function size guardrails
  (target fail: file>300/function>40; CI enforces this threshold).
- `.github/workflows/ci.yml` runs tests on PR/push across
  Ubuntu/Windows/macOS with Python 3.11 and 3.12.
- `.github/workflows/ci.yml` also runs a cross-repo E2E job on Ubuntu by
  checking out `runtime-execution` and running `tests/test_e2e_run_flow.py`.
- Cross-repo E2E requires repository secret `CROSS_REPO_READ_TOKEN` to clone
  `waoooolab/runtime-execution` in GitHub Actions.
- If `CROSS_REPO_READ_TOKEN` is not configured, the cross-repo E2E job is
  marked successful with test steps skipped, while the main test matrix still
  executes normally.
