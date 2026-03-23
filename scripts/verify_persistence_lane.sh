#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

echo "[runtime-gateway] run persistence path + durable tests"
PYTHONPATH=src python3 -m unittest \
  tests.test_persistence_paths \
  tests.test_audit_emitter \
  tests.test_events_durable \
  -v

echo "[runtime-gateway] boundary import check"
python3 scripts/check_boundary_imports.py --own-package runtime_gateway --src src/runtime_gateway

echo "[runtime-gateway] durable sqlite minimal verification"
PYTHONPATH=src python3 scripts/verify_durable_sqlite_minimal.py

echo "[runtime-gateway] persistence lane verify: OK"
