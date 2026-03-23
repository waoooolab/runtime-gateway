from __future__ import annotations

import os
import tempfile
import unittest

from runtime_gateway.persistence_paths import (
    resolve_audit_db_path,
    resolve_event_db_path,
)

_ENV_KEYS = (
    "OWA_PERSIST_ROOT",
    "RUNTIME_GATEWAY_AUDIT_DB_PATH",
    "RUNTIME_GATEWAY_EVENT_DB_PATH",
)


class PersistencePathsTests(unittest.TestCase):
    def setUp(self) -> None:
        self._old_env = {key: os.environ.get(key) for key in _ENV_KEYS}
        for key in _ENV_KEYS:
            os.environ.pop(key, None)

    def tearDown(self) -> None:
        for key, value in self._old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_resolve_paths_none_when_no_env(self) -> None:
        self.assertIsNone(resolve_audit_db_path())
        self.assertIsNone(resolve_event_db_path())

    def test_resolve_paths_from_persist_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            os.environ["OWA_PERSIST_ROOT"] = tmp
            audit_path = resolve_audit_db_path()
            event_path = resolve_event_db_path()
            expected_audit = os.path.join(tmp, "runtime-gateway", "runtime-audit.sqlite")
            expected_event = os.path.join(tmp, "runtime-gateway", "runtime-events.sqlite")
            self.assertIsNotNone(audit_path)
            self.assertIsNotNone(event_path)
            self.assertEqual(audit_path.as_posix(), expected_audit)
            self.assertEqual(event_path.as_posix(), expected_event)

    def test_explicit_paths_override_persist_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            os.environ["OWA_PERSIST_ROOT"] = os.path.join(tmp, "persist-root")
            audit_db = os.path.join(tmp, "explicit", "audit.sqlite")
            event_db = os.path.join(tmp, "explicit", "events.sqlite")
            os.environ["RUNTIME_GATEWAY_AUDIT_DB_PATH"] = audit_db
            os.environ["RUNTIME_GATEWAY_EVENT_DB_PATH"] = event_db
            audit_path = resolve_audit_db_path()
            event_path = resolve_event_db_path()
            self.assertIsNotNone(audit_path)
            self.assertIsNotNone(event_path)
            self.assertEqual(audit_path.as_posix(), audit_db)
            self.assertEqual(event_path.as_posix(), event_db)


if __name__ == "__main__":
    unittest.main()
