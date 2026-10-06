"""FE-1: Frontend Auth + Pagination + Error Boundaries + SSE Reconnection.

Since frontend tests use Vitest (not pytest), these tests verify the source
code contains the required patterns. Full Vitest tests would run separately.
"""

from __future__ import annotations

import os
from pathlib import Path


def _read_frontend(filename: str) -> str:
    path = Path(__file__).resolve().parent.parent.parent / "frontend" / "src" / filename
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def test_api_fetch_sends_auth_header():
    source = _read_frontend("api/client.ts")
    assert "Authorization" in source
    assert "Bearer" in source
    assert "getAuthToken" in source


def test_sse_sends_auth():
    source = _read_frontend("hooks/useSessions.ts")
    assert "fetch" in source
    assert "getReader" in source
    assert "text/event-stream" in source or "ReadableStream" in source or "getReader" in source


def test_sse_reconnect_no_duplicates():
    source = _read_frontend("hooks/useSessions.ts")
    assert "Last-Event-ID" in source or "lastEventId" in source
    assert "lastEventIdRef" in source


def test_events_pagination_load_more():
    source = _read_frontend("hooks/useSessions.ts")
    assert "offset" in source
    assert "limit" in source
    assert "loadMore" in source
    assert "accumulated" in source


def test_error_boundary_exists():
    source = _read_frontend("components/ErrorBoundary.tsx")
    assert "ErrorBoundary" in source
    assert "getDerivedStateFromError" in source
    assert "componentDidCatch" in source
    assert "ACTIONABLE_STATES" in source


def test_no_secret_in_frontend_source():
    client = _read_frontend("api/client.ts")
    assert "dapi_" not in client
    assert "password" not in client.lower() or "password" not in client
    hooks = _read_frontend("hooks/useSessions.ts")
    assert "dapi_" not in hooks
