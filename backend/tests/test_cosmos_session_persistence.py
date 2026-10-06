"""Unit tests for CosmosDbSessionPersistence and CheckpointLifecycleManager."""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock
from typing import Any

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.core.events import MigrationEvent, MigrationEventType
from sas2pyspark.core.hash import GENESIS_HASH, sha256_json
from sas2pyspark.graph.checkpoint_lifecycle import (
    CheckpointLifecycleManager,
    CheckpointRetentionPolicy,
)
from sas2pyspark.graph.cosmos_checkpointer import CosmosDbSaver
from sas2pyspark.session.cosmos_persistence import CosmosDbSessionPersistence
from sas2pyspark.session.persistence import SessionHeader
from tests.test_cosmos_checkpointer import FakeContainer


class SessionFakeContainer:
    """In-memory fake Cosmos container supporting session partition queries, ETags, and access conditions."""

    def __init__(self) -> None:
        self._items: dict[str, dict[str, Any]] = {}
        self._etags: dict[str, int] = {}
        self.simulate_etag_conflict_once = False

    def create_item(self, body: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        item_id = body["id"]
        if item_id in self._items:
            err = Exception("Resource with specified id already exists")
            setattr(err, "status_code", 409)
            raise err
        self._items[item_id] = dict(body)
        self._etags[item_id] = 1
        body["_etag"] = f"etag_{self._etags[item_id]}"
        return body

    def upsert_item(self, body: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        item_id = body["id"]
        self._items[item_id] = dict(body)
        etag_num = self._etags.get(item_id, 0) + 1
        self._etags[item_id] = etag_num
        body["_etag"] = f"etag_{etag_num}"
        return body

    def read_item(self, item: str, partition_key: str, **kwargs: Any) -> dict[str, Any]:
        if item not in self._items:
            err = Exception("Not found")
            setattr(err, "status_code", 404)
            raise err
        doc = dict(self._items[item])
        doc["_etag"] = f"etag_{self._etags.get(item, 1)}"
        return doc

    def replace_item(self, item: str, body: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        if self.simulate_etag_conflict_once:
            self.simulate_etag_conflict_once = False
            err = Exception("Precondition failed")
            setattr(err, "status_code", 412)
            raise err

        access_cond = kwargs.get("access_conditions", {})
        if_match = access_cond.get("if_match")
        current_etag = f"etag_{self._etags.get(item, 1)}"
        if if_match and if_match != current_etag:
            err = Exception("Precondition failed")
            setattr(err, "status_code", 412)
            raise err

        self._items[item] = dict(body)
        etag_num = self._etags.get(item, 1) + 1
        self._etags[item] = etag_num
        body["_etag"] = f"etag_{etag_num}"
        return body

    def delete_item(self, item: str, partition_key: str, **kwargs: Any) -> None:
        self._items.pop(item, None)
        self._etags.pop(item, None)

    def query_items(
        self,
        query: str,
        parameters: list[dict[str, Any]] | None = None,
        partition_key: str | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        param_map = {p["name"]: p["value"] for p in (parameters or [])}
        sid = param_map.get("@sid")
        results = []
        for doc in self._items.values():
            if sid and doc.get("session_id") != sid:
                continue
            if "c.type = 'event'" in query and doc.get("type") != "event":
                continue
            if "c.type = 'header'" in query and doc.get("type") != "header":
                continue
            results.append(doc)

        if "ORDER BY c.sequence ASC" in query:
            results.sort(key=lambda d: d.get("sequence", 0))

        return results


def make_session_provider(container):
    provider = MagicMock()
    provider.get_container.return_value = container
    return provider


def _make_event(sequence: int, prev_hash: str, session_id: str = "sess-1") -> MigrationEvent:
    data = {"step": f"step-{sequence}"}
    content = {
        "eventId": f"ev-{sequence}",
        "sequence": sequence,
        "migrationId": "mig-1",
        "scopeId": "scope-1",
        "type": MigrationEventType.WORK_UNIT_CREATED.value,
        "createdAt": "2026-01-01T00:00:00Z",
        "data": data,
        "previousHash": prev_hash,
    }
    h = sha256_json({
        "sequence": sequence,
        "type": MigrationEventType.WORK_UNIT_CREATED.value,
        "createdAt": "2026-01-01T00:00:00Z",
        "data": data,
        "previousHash": prev_hash,
    })
    return MigrationEvent(
        event_id=f"ev-{sequence}",
        sequence=sequence,
        migration_id="mig-1",
        scope_id="scope-1",
        type=MigrationEventType.WORK_UNIT_CREATED,
        created_at="2026-01-01T00:00:00Z",
        data=data,
        previous_hash=prev_hash,
        hash=h,
    )


def test_session_lifecycle_roundtrip():
    container = SessionFakeContainer()
    provider = make_session_provider(container)
    persistence = CosmosDbSessionPersistence(provider)

    header = SessionHeader(
        session_id="sess-roundtrip",
        migration_id="mig-1",
        scope_id="scope-1",
        created_at="2026-01-01T00:00:00Z",
    )

    ev1 = _make_event(1, GENESIS_HASH, "sess-roundtrip")
    persistence.create(header=header, initial_events=[ev1])

    # Reading session back
    session = persistence.read("sess-roundtrip")
    assert session.header.session_id == "sess-roundtrip"
    assert len(session.events) == 1
    assert session.events[0].sequence == 1
    assert session.events[0].hash == ev1.hash

    # Append second event
    ev2 = _make_event(2, ev1.hash, "sess-roundtrip")
    persistence.append("sess-roundtrip", ev2)

    session_updated = persistence.read("sess-roundtrip")
    assert len(session_updated.events) == 2
    assert session_updated.events[1].sequence == 2
    assert session_updated.events[1].previous_hash == ev1.hash
    assert session_updated.events[1].hash == ev2.hash


def test_create_session_already_exists_fails_closed():
    container = SessionFakeContainer()
    provider = make_session_provider(container)
    persistence = CosmosDbSessionPersistence(provider)

    header = SessionHeader(
        session_id="sess-dup",
        migration_id="mig-1",
        scope_id="scope-1",
        created_at="2026-01-01T00:00:00Z",
    )
    persistence.create(header=header)

    with pytest.raises(WorkbenchError) as exc_info:
        persistence.create(header=header)
    assert exc_info.value.code == WorkbenchErrorCode.SESSION_ALREADY_EXISTS


def test_read_nonexistent_session_raises_not_found():
    container = SessionFakeContainer()
    provider = make_session_provider(container)
    persistence = CosmosDbSessionPersistence(provider)

    with pytest.raises(WorkbenchError) as exc_info:
        persistence.read("does-not-exist")
    assert exc_info.value.code == WorkbenchErrorCode.SESSION_NOT_FOUND


def test_append_etag_conflict_retries_and_succeeds():
    container = SessionFakeContainer()
    provider = make_session_provider(container)
    persistence = CosmosDbSessionPersistence(provider)

    header = SessionHeader(
        session_id="sess-conflict",
        migration_id="mig-1",
        scope_id="scope-1",
        created_at="2026-01-01T00:00:00Z",
    )
    ev1 = _make_event(1, GENESIS_HASH, "sess-conflict")
    persistence.create(header=header, initial_events=[ev1])

    # Trigger a 412 ETag conflict on the first attempt of append
    container.simulate_etag_conflict_once = True

    ev2 = _make_event(2, ev1.hash, "sess-conflict")
    persistence.append("sess-conflict", ev2)

    session = persistence.read("sess-conflict")
    assert len(session.events) == 2
    assert session.events[1].sequence == 2


def test_list_sessions():
    container = SessionFakeContainer()
    provider = make_session_provider(container)
    persistence = CosmosDbSessionPersistence(provider)

    h1 = SessionHeader(session_id="sess-a", migration_id="m1", scope_id="s1", created_at="2026-01-01T00:00:00Z")
    h2 = SessionHeader(session_id="sess-b", migration_id="m2", scope_id="s2", created_at="2026-01-01T00:00:00Z")
    persistence.create(header=h1)
    persistence.create(header=h2)

    sessions = persistence.list_sessions()
    assert sessions == ["sess-a", "sess-b"]


# ── Checkpoint Lifecycle Tests ───────────────────────────────────────────────


def test_checkpoint_lifecycle_cleanup_completed():
    saver = MagicMock()
    saver.prune.return_value = 5
    mgr = CheckpointLifecycleManager(saver, policy=CheckpointRetentionPolicy(success_retention="final"))

    pruned = mgr.cleanup_thread("thread-100", status="COMPLETED")
    assert pruned == 5
    saver.prune.assert_called_once_with({"configurable": {"thread_id": "thread-100", "checkpoint_ns": ""}}, keep_last=1)


def test_checkpoint_lifecycle_cleanup_purge():
    saver = MagicMock()
    mgr = CheckpointLifecycleManager(saver)

    result = mgr.cleanup_thread("thread-100", status="PURGE")
    assert result == 1
    saver.delete_thread.assert_called_once_with("thread-100")


@pytest.mark.asyncio
async def test_async_checkpoint_lifecycle_cleanup():
    saver = MagicMock()
    saver.aprune = AsyncMock(return_value=3)
    mgr = CheckpointLifecycleManager(saver)

    pruned = await mgr.acleanup_thread("thread-async", status="FAILED")
    assert pruned == 3
    saver.aprune.assert_called_once_with({"configurable": {"thread_id": "thread-async", "checkpoint_ns": ""}}, keep_last=2)
