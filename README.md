# runtime-gateway
[![CI](https://github.com/waoooolab/runtime-gateway/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/waoooolab/runtime-gateway/actions/workflows/ci.yml)

Runtime gateway service.

P0 scope:
- HTTP/REST API entrypoints
- Event envelope generation
- In-process adapter to runtime-execution (temporary)

Current modules (src layout):
- `src/runtime_gateway/api`
- `src/runtime_gateway/auth`
- `src/runtime_gateway/contracts`
- `src/runtime_gateway/events`
- `src/runtime_gateway/audit`

Current endpoints:
- `GET /healthz`
- `GET /v1/audit/events` (dev baseline read endpoint)
- `POST /v1/auth/token/exchange` (baseline delegated token exchange)
- `POST /v1/runs` (protected by Bearer token middleware)

Validation and auth notes:
- `auth/exchange.py` contains framework-agnostic token exchange logic
- `contracts/validation.py` validates payloads against `platform-contracts/jsonschema`
- `events/validation.py` validates event envelope via contract schema
- `/v1/runs` requires `aud=runtime-gateway` and `runs:write` scope
- `/v1/runs` propagates claim `trace_id` into runtime-execution run submission and event envelope
- auth and run actions emit audit events in memory, and optionally to file via `RUNTIME_GATEWAY_AUDIT_LOG_PATH`

Testing:
- `tests/test_app_integration.py` covers HTTP auth rejections and valid run creation path.
- `.github/workflows/ci.yml` runs tests on PR/push across
  Ubuntu/Windows/macOS with Python 3.11 and 3.12.
