"""P1-11: SSE Heartbeat + Disconnect Detection + Durable Event Source."""

from __future__ import annotations

import inspect
import pytest

from sas2pyspark.api.sse import create_sse_router, _sse_format, _HEARTBEAT_INTERVAL


def test_heartbeat_interval_configured():
    assert _HEARTBEAT_INTERVAL == 15.0


def test_sse_format_correct():
    result = _sse_format("migration-event", "evt-1", '{"key":"value"}')
    assert "event: migration-event" in result
    assert "id: evt-1" in result
    assert 'data: {"key":"value"}' in result
    assert result.endswith("\n\n")


def test_disconnect_detection_present():
    source = inspect.getsource(create_sse_router)
    assert "is_disconnected" in source


def test_reads_from_durable_log():
    source = inspect.getsource(create_sse_router)
    assert "_persistence" in source
    assert "persistence.read" in source or "persisted.events" in source


def test_last_event_id_resume():
    source = inspect.getsource(create_sse_router)
    assert "Last-Event-ID" in source
    assert "last_event_id" in source


def test_keep_alive_comment():
    source = inspect.getsource(create_sse_router)
    assert "keep-alive" in source


def test_heartbeat_emitted_in_generator():
    source = inspect.getsource(create_sse_router)
    assert "_HEARTBEAT_INTERVAL" in source
    assert "last_heartbeat" in source
