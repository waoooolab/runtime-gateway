from __future__ import annotations

import io
import json
import urllib.error

import pytest

from runtime_gateway.event_transport_adapter import (
    EVENT_TRANSPORT_ADAPTER_ENV,
    EVENT_TRANSPORT_BRIDGE_TIMEOUT_MS_ENV,
    EVENT_TRANSPORT_BRIDGE_URL_ENV,
    HttpJetStreamTransportAdapter,
    build_event_transport_adapter,
    normalize_event_transport_adapter_name,
    resolve_event_transport_adapter,
)


def test_normalize_event_transport_adapter_name_defaults_to_http() -> None:
    assert normalize_event_transport_adapter_name(None) == "http"
    assert normalize_event_transport_adapter_name(" ") == "http"
    assert normalize_event_transport_adapter_name("HTTP") == "http"


def test_normalize_event_transport_adapter_name_rejects_unsupported_value() -> None:
    with pytest.raises(ValueError, match="unsupported jetstream transport adapter"):
        normalize_event_transport_adapter_name("nats")


def test_build_event_transport_adapter_returns_http_adapter() -> None:
    adapter = build_event_transport_adapter(
        name="http",
        bridge_url="http://localhost:3900/runtime/events",
        timeout_seconds=1.5,
        bearer_token=None,
    )
    assert isinstance(adapter, HttpJetStreamTransportAdapter)
    assert adapter.adapter_id == "http"


def test_resolve_event_transport_adapter_returns_none_without_bridge_url() -> None:
    adapter = resolve_event_transport_adapter(
        adapter=None,
        adapter_name=None,
        bridge_url=None,
        bridge_timeout_seconds=None,
        bridge_bearer_token=None,
    )
    assert adapter is None


def test_resolve_event_transport_adapter_uses_env_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(EVENT_TRANSPORT_ADAPTER_ENV, "http")
    monkeypatch.setenv(EVENT_TRANSPORT_BRIDGE_URL_ENV, "http://bridge.test/runtime/events")
    monkeypatch.setenv(EVENT_TRANSPORT_BRIDGE_TIMEOUT_MS_ENV, "1200")

    adapter = resolve_event_transport_adapter(
        adapter=None,
        adapter_name=None,
        bridge_url=None,
        bridge_timeout_seconds=None,
        bridge_bearer_token=None,
    )
    assert isinstance(adapter, HttpJetStreamTransportAdapter)
    assert adapter.adapter_id == "http"


def test_http_jetstream_transport_adapter_publish_event_posts_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class _Response:
        status = 202

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            _ = (exc_type, exc, tb)
            return None

    def _urlopen(request, timeout):  # noqa: ANN001
        captured["url"] = request.full_url
        captured["method"] = request.get_method()
        captured["timeout"] = timeout
        captured["authorization"] = request.get_header("Authorization")
        body = request.data.decode("utf-8")
        captured["body"] = json.loads(body)
        return _Response()

    monkeypatch.setattr("runtime_gateway.event_transport_adapter.urllib.request.urlopen", _urlopen)
    adapter = HttpJetStreamTransportAdapter(
        bridge_url="http://bridge.test/runtime/events",
        timeout_seconds=2.0,
        bearer_token="token-1",
    )

    event = {"event_type": "runtime.run.status", "payload": {"run_id": "run-42"}}
    adapter.publish_event(bus_seq=7, event=event)

    assert captured["url"] == "http://bridge.test/runtime/events"
    assert captured["method"] == "POST"
    assert captured["timeout"] == 2.0
    assert captured["authorization"] == "Bearer token-1"
    assert captured["body"] == {
        "schema_version": "runtime_gateway.jetstream_bridge_event.v1",
        "adapter_id": "http",
        "bus_seq": 7,
        "event": event,
    }


def test_http_jetstream_transport_adapter_publish_event_surfaces_http_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _urlopen(request, timeout):  # noqa: ANN001
        _ = (request, timeout)
        raise urllib.error.HTTPError(
            url="http://bridge.test/runtime/events",
            code=502,
            msg="bad gateway",
            hdrs=None,
            fp=io.BytesIO(b"upstream unavailable"),
        )

    monkeypatch.setattr("runtime_gateway.event_transport_adapter.urllib.request.urlopen", _urlopen)
    adapter = HttpJetStreamTransportAdapter(
        bridge_url="http://bridge.test/runtime/events",
        timeout_seconds=2.0,
        bearer_token=None,
    )

    with pytest.raises(RuntimeError, match="HTTP error 502"):
        adapter.publish_event(bus_seq=1, event={"event_type": "runtime.run.status", "payload": {}})


def test_http_jetstream_transport_adapter_publish_event_surfaces_url_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _urlopen(request, timeout):  # noqa: ANN001
        _ = (request, timeout)
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr("runtime_gateway.event_transport_adapter.urllib.request.urlopen", _urlopen)
    adapter = HttpJetStreamTransportAdapter(
        bridge_url="http://bridge.test/runtime/events",
        timeout_seconds=2.0,
        bearer_token=None,
    )

    with pytest.raises(RuntimeError, match="bridge unavailable"):
        adapter.publish_event(bus_seq=1, event={"event_type": "runtime.run.status", "payload": {}})
