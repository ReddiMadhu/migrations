"""P0-4: Persistence flush() durability + truncated_tail honored."""

from __future__ import annotations

import json
import os

import pytest
import pytest_asyncio

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.session.persistence import JsonlSessionPersistence, SessionHeader, PersistedSession
from sas2pyspark.workbench import WorkbenchConfig, create_workbench

from .conftest import make_scopes, make_session


def _make_header(session_id: str = "sess-test") -> SessionHeader:
    return SessionHeader(session_id=session_id, migration_id="mig-test", scope_id="tenant/tenant-a/migration/migration-a", created_at="2026-01-01T00:00:01.000Z")


def test_flush_fsyncs_events(test_dir):
    persistence = JsonlSessionPersistence(str(test_dir))
    persistence.create(header=_make_header())
    events_path = persistence._events_path("sess-test")
    with open(events_path, "a") as f:
        f.write('{"eventId":"e1","migrationId":"mig","scopeId":"s","type":"migration/created","sequence":1,"createdAt":"2026-01-01T00:00:01.000Z","data":{},"previousHash":"0".repeat(64),"hash":"a".repeat(64)}\n')
    persistence.flush("sess-test")
    assert os.path.exists(events_path)


def test_append_is_durable_before_return(test_dir):
    persistence = JsonlSessionPersistence(str(test_dir))
    persistence.create(header=_make_header())
    events_path = persistence._events_path("sess-test")
    from sas2pyspark.core.events import MigrationEvent, MigrationEventType
    event = MigrationEvent(
        eventId="sess-test:1", migrationId="mig-test", scopeId="tenant/tenant-a/migration/migration-a",
        type=MigrationEventType.MIGRATION_CREATED, sequence=1, createdAt="2026-01-01T00:00:01.000Z",
        data={}, previousHash="0" * 64, hash="a" * 64,
    )
    persistence.append("sess-test", event)
    with open(events_path) as f:
        content = f.read()
    assert "sess-test:1" in content


def test_concurrent_appends_serialize(test_dir):
    persistence = JsonlSessionPersistence(str(test_dir))
    persistence.create(header=_make_header())
    events_path = persistence._events_path("sess-test")
    from sas2pyspark.core.events import MigrationEvent, MigrationEventType
    for i in range(1, 4):
        event = MigrationEvent(
            eventId=f"sess-test:{i}", migrationId="mig-test", scopeId="tenant/tenant-a/migration/migration-a",
            type=MigrationEventType.MIGRATION_CREATED, sequence=i, createdAt="2026-01-01T00:00:01.000Z",
            data={}, previousHash="0" * 64, hash="a" * 64,
        )
        persistence.append("sess-test", event)
    with open(events_path) as f:
        lines = [l for l in f.read().splitlines() if l.strip()]
    assert len(lines) == 3


def test_unparseable_line_surfaces_corruption(test_dir):
    persistence = JsonlSessionPersistence(str(test_dir))
    persistence.create(header=_make_header())
    events_path = persistence._events_path("sess-test")
    with open(events_path, "a") as f:
        f.write('{"valid": true}\n')
        f.write('THIS IS NOT JSON\n')
        f.write('{"also_valid": true}\n')
    result = persistence.read("sess-test")
    assert result.truncated_tail is True
    assert 2 in result.corrupt_lines


def test_strict_mode_raises_on_corruption(test_dir):
    persistence = JsonlSessionPersistence(str(test_dir), strict=True)
    persistence.create(header=_make_header())
    events_path = persistence._events_path("sess-test")
    with open(events_path, "a") as f:
        f.write('THIS IS NOT JSON\n')
    with pytest.raises(WorkbenchError) as exc_info:
        persistence.read("sess-test")
    assert exc_info.value.code == WorkbenchErrorCode.SESSION_CORRUPTION
    assert "corruptLines" in exc_info.value.details


def test_lenient_mode_warns_on_corruption(test_dir):
    persistence = JsonlSessionPersistence(str(test_dir), strict=False)
    persistence.create(header=_make_header())
    events_path = persistence._events_path("sess-test")
    with open(events_path, "a") as f:
        f.write('THIS IS NOT JSON\n')
    result = persistence.read("sess-test")
    assert result.truncated_tail is True
    assert len(result.corrupt_lines) == 1


@pytest.mark.asyncio
async def test_truncated_tail_honored_on_open(test_dir, fixed_clock):
    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        _, _, _, candidate = make_scopes(runtime)
        session = await make_session(runtime)
        events_path = runtime.persistence._events_path(session.id)
        with open(events_path, "a") as f:
            f.write('CORRUPT_LINE\n')
        reopened = await runtime.sessions.replay(session.id)
        assert reopened.truncated_tail is True
        assert len(reopened.corrupt_lines) > 0
    finally:
        await runtime.dispose()


def test_crlf_line_endings_parsed(test_dir):
    persistence = JsonlSessionPersistence(str(test_dir))
    persistence.create(header=_make_header())
    events_path = persistence._events_path("sess-test")
    with open(events_path, "a") as f:
        f.write('{"eventId":"e1","migrationId":"mig","scopeId":"s","type":"migration/created","sequence":1,"createdAt":"2026-01-01T00:00:01.000Z","data":{},"previousHash":"0000000000000000000000000000000000000000000000000000000000000000","hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}\r\n')
    result = persistence.read("sess-test")
    assert len(result.events) == 1
    assert result.truncated_tail is False
