from __future__ import annotations

import os
import unittest
from pathlib import Path
from unittest.mock import patch

from runtime_gateway.auth.exchange import ExchangeError, exchange_subject_token
from runtime_gateway.auth.tokens import TokenError, issue_token, verify_token

os.environ["WAOOOOLAB_PLATFORM_CONTRACTS_DIR"] = str(
    Path(__file__).resolve().parent / "fixtures" / "contracts"
)


class TokenExchangeTests(unittest.TestCase):
    def _subject_token(
        self,
        scope: list[str],
        *,
        token_use: str | None = "access",
    ) -> str:
        claims: dict[str, object] = {
            "iss": "runtime-gateway",
            "sub": "user:u1",
            "aud": "runtime-gateway",
            "tenant_id": "t1",
            "app_id": "covernow",
            "scope": scope,
            "trace_id": "trace-root",
        }
        if token_use is not None:
            claims["token_use"] = token_use
        return issue_token(claims, ttl_seconds=600)

    def test_exchange_success(self) -> None:
        subject_token = self._subject_token(["runs:write", "runs:read"])
        result = exchange_subject_token(
            subject_token=subject_token,
            requested_token_use="service",
            audience="runtime-execution",
            scope=["runs:write"],
            requested_ttl_seconds=300,
        )
        claims = verify_token(result["access_token"], audience="runtime-execution")
        self.assertEqual(claims["aud"], "runtime-execution")
        self.assertEqual(claims["scope"], ["runs:write"])

    def test_exchange_rejects_scope_escalation(self) -> None:
        subject_token = self._subject_token(["runs:read"])
        with self.assertRaises(ExchangeError) as exc:
            exchange_subject_token(
                subject_token=subject_token,
                requested_token_use="service",
                audience="runtime-execution",
                scope=["runs:write"],
                requested_ttl_seconds=300,
            )
        self.assertEqual(exc.exception.status_code, 403)

    def test_exchange_rejects_missing_subject_token_use_claim(self) -> None:
        subject_token = self._subject_token(["runs:write"], token_use=None)
        with self.assertRaises(ExchangeError) as exc:
            exchange_subject_token(
                subject_token=subject_token,
                requested_token_use="service",
                audience="runtime-execution",
                scope=["runs:write"],
                requested_ttl_seconds=300,
            )
        self.assertEqual(exc.exception.status_code, 401)
        self.assertIn("missing token_use", exc.exception.detail)

    def test_exchange_rejects_unsupported_subject_token_use_claim(self) -> None:
        subject_token = self._subject_token(["runs:write"], token_use="device")
        with self.assertRaises(ExchangeError) as exc:
            exchange_subject_token(
                subject_token=subject_token,
                requested_token_use="service",
                audience="runtime-execution",
                scope=["runs:write"],
                requested_ttl_seconds=300,
            )
        self.assertEqual(exc.exception.status_code, 401)
        self.assertIn("unsupported token_use", exc.exception.detail)

    def test_verify_rejects_wrong_audience(self) -> None:
        subject_token = self._subject_token(["runs:write"])
        result = exchange_subject_token(
            subject_token=subject_token,
            requested_token_use="service",
            audience="runtime-execution",
            scope=["runs:write"],
            requested_ttl_seconds=300,
        )
        with self.assertRaises(TokenError):
            verify_token(result["access_token"], audience="device-hub")

    def test_issue_token_rejects_default_secret_in_strict_mode(self) -> None:
        with patch.dict(
            os.environ,
            {"WAOOOOLAB_STRICT_TOKEN_SECRET": "true"},
            clear=True,
        ):
            with self.assertRaises(TokenError) as exc:
                issue_token(
                    {
                        "iss": "runtime-gateway",
                        "sub": "user:u1",
                        "aud": "runtime-gateway",
                        "tenant_id": "t1",
                        "app_id": "covernow",
                        "scope": ["runs:read"],
                        "token_use": "access",
                        "trace_id": "trace-root",
                    },
                    ttl_seconds=60,
                )
        self.assertIn("insecure default token secret", str(exc.exception))


if __name__ == "__main__":
    unittest.main()
