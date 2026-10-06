"""Unit tests for CosmosDbSaver — mock-based, no live Cosmos DB required.

Tests verify that the saver correctly:
- Stores and retrieves checkpoints via Cosmos container operations
- Handles put_writes with idempotent deduplication
- Lists checkpoints in descending order
- Deletes all documents for a thread
- Integrates with resolve_checkpointer(mode="cosmos")
- Fails closed when Cosmos config is missing
"""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock
from typing import Any

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.graph.checkpointer import resolve_checkpointer


# ── Fixtures ─────────────────────────────────────────────────────────────────

class FakeContainer:
    """In-memory fake of a Cosmos container for testing."""

    def __init__(self) -> None:
        self._items: dict[str, dict[str, Any]] = {}

    def upsert_item(self, body: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        self._items[body["id"]] = body
        return body

    def read_item(self, item: str, partition_key: str, **kwargs: Any) -> dict[str, Any]:
        if item not in self._items:
            from azure.cosmos.exceptions import CosmosResourceNotFoundError
            raise CosmosResourceNotFoundError(status_code=404, message="Not found")
        doc = self._items[item]
        if doc.get("thread_id") != partition_key:
            from azure.cosmos.exceptions import CosmosResourceNotFoundError
            raise CosmosResourceNotFoundError(status_code=404, message="Not found")
        return doc

    def query_items(
        self,
        query: str,
        parameters: list[dict[str, Any]] | None = None,
        partition_key: str | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        # Simple filter: match thread_id and type from parameters
        param_map = {p["name"]: p["value"] for p in (parameters or [])}
        tid = param_map.get("@tid")
        ns = param_map.get("@ns")
        doc_type = None
        before_id = param_map.get("@before")

        results = []
        for doc in self._items.values():
            if tid and doc.get("thread_id") != tid:
                continue
            if partition_key and doc.get("thread_id") != partition_key:
                continue
            # Determine type filter from query
            if "c.type = 'checkpoint'" in query and doc.get("type") != "checkpoint":
                continue
            if "c.type = 'write'" in query and doc.get("type") != "write":
                continue
            if ns is not None and doc.get("checkpoint_ns") != ns:
                continue
            cpid = param_map.get("@cpid")
            if cpid is not None and doc.get("checkpoint_id") != cpid:
                continue
            if before_id and doc.get("checkpoint_id", "") >= before_id:
                continue
            results.append(doc)

        # Sort descending by checkpoint_id for checkpoint queries
        if "ORDER BY c.checkpoint_id DESC" in query:
            results.sort(key=lambda d: d.get("checkpoint_id", ""), reverse=True)

        # Handle LIMIT
        if "LIMIT 1" in query:
            results = results[:1]

        return results

    def delete_item(self, item: str, partition_key: str, **kwargs: Any) -> None:
        if item in self._items:
            del self._items[item]


class AsyncFakeContainer:
    """Async version of FakeContainer wrapping a sync FakeContainer."""

    def __init__(self, sync_container: FakeContainer) -> None:
        self._sync = sync_container

    async def upsert_item(self, body: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        return self._sync.upsert_item(body, **kwargs)

    async def read_item(self, item: str, partition_key: str, **kwargs: Any) -> dict[str, Any]:
        return self._sync.read_item(item, partition_key, **kwargs)

    async def query_items(
        self,
        query: str,
        parameters: list[dict[str, Any]] | None = None,
        partition_key: str | None = None,
        **kwargs: Any,
    ):
        results = self._sync.query_items(query, parameters, partition_key, **kwargs)
        for item in results:
            yield item

    async def delete_item(self, item: str, partition_key: str, **kwargs: Any) -> None:
        self._sync.delete_item(item, partition_key, **kwargs)


@pytest.fixture
def fake_container():
    return FakeContainer()


@pytest.fixture
def async_fake_container(fake_container):
    return AsyncFakeContainer(fake_container)


@pytest.fixture
def mock_provider(fake_container, async_fake_container):
    provider = MagicMock()
    provider.get_container.return_value = fake_container
    provider.aget_container = AsyncMock(return_value=async_fake_container)
    provider.dispose = MagicMock()
    provider.adispose = AsyncMock()
    return provider


@pytest.fixture
def cosmos_saver(mock_provider):
    from sas2pyspark.graph.cosmos_checkpointer import CosmosDbSaver
    return CosmosDbSaver(mock_provider, container_name="test_checkpoints")


# ── Test helpers ─────────────────────────────────────────────────────────────

def _make_config(thread_id: str = "thread-1", checkpoint_ns: str = "", checkpoint_id: str | None = None) -> dict:
    cfg: dict[str, Any] = {
        "configurable": {
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns,
        }
    }
    if checkpoint_id is not None:
        cfg["configurable"]["checkpoint_id"] = checkpoint_id
    return cfg


def _make_checkpoint(checkpoint_id: str = "cp-1", channel_values: dict | None = None) -> dict:
    return {
        "v": 1,
        "id": checkpoint_id,
        "ts": "2026-01-01T00:00:00Z",
        "channel_values": channel_values or {"counter": 42},
        "channel_versions": {"counter": "1"},
        "versions_seen": {},
    }


def _make_metadata(source: str = "loop", step: int = 0) -> dict:
    return {
        "source": source,
        "step": step,
        "parents": {},
    }


# ── Sync Tests ───────────────────────────────────────────────────────────────

class TestCosmosDbSaverSync:

    def test_put_and_get_tuple(self, cosmos_saver, fake_container):
        """put() stores a checkpoint, get_tuple() retrieves it."""
        config = _make_config(checkpoint_ns="")
        checkpoint = _make_checkpoint("cp-001")
        metadata = _make_metadata()
        new_versions = {"counter": "1"}

        result_config = cosmos_saver.put(config, checkpoint, metadata, new_versions)

        assert result_config["configurable"]["checkpoint_id"] == "cp-001"
        assert result_config["configurable"]["thread_id"] == "thread-1"

        # Retrieve by specific checkpoint_id
        fetch_config = _make_config(checkpoint_id="cp-001")
        tup = cosmos_saver.get_tuple(fetch_config)
        assert tup is not None
        assert tup.checkpoint["id"] == "cp-001"
        assert tup.checkpoint["channel_values"]["counter"] == 42
        assert tup.metadata["source"] == "loop"
        assert tup.parent_config is None

    def test_get_tuple_latest(self, cosmos_saver):
        """get_tuple() without checkpoint_id returns the latest checkpoint."""
        config_base = _make_config()

        # Put two checkpoints
        cosmos_saver.put(config_base, _make_checkpoint("cp-001"), _make_metadata(step=0), {"counter": "1"})

        config_with_parent = _make_config(checkpoint_id="cp-001")
        cosmos_saver.put(config_with_parent, _make_checkpoint("cp-002", {"counter": 99}), _make_metadata(step=1), {"counter": "2"})

        # Fetch latest (no checkpoint_id)
        tup = cosmos_saver.get_tuple(_make_config())
        assert tup is not None
        assert tup.checkpoint["id"] == "cp-002"

    def test_get_tuple_not_found(self, cosmos_saver):
        """get_tuple() returns None for nonexistent checkpoint."""
        config = _make_config(checkpoint_id="nonexistent")
        assert cosmos_saver.get_tuple(config) is None

    def test_get_tuple_empty_thread(self, cosmos_saver):
        """get_tuple() returns None for a thread with no checkpoints."""
        config = _make_config(thread_id="empty-thread")
        assert cosmos_saver.get_tuple(config) is None

    def test_put_records_parent(self, cosmos_saver):
        """put() correctly records the parent checkpoint id."""
        cosmos_saver.put(_make_config(), _make_checkpoint("cp-001"), _make_metadata(), {"counter": "1"})

        parent_config = _make_config(checkpoint_id="cp-001")
        cosmos_saver.put(parent_config, _make_checkpoint("cp-002"), _make_metadata(step=1), {"counter": "2"})

        tup = cosmos_saver.get_tuple(_make_config(checkpoint_id="cp-002"))
        assert tup is not None
        assert tup.parent_config is not None
        assert tup.parent_config["configurable"]["checkpoint_id"] == "cp-001"

    def test_put_writes_and_retrieve(self, cosmos_saver):
        """put_writes() stores pending writes, retrievable via get_tuple()."""
        config = _make_config()
        cosmos_saver.put(config, _make_checkpoint("cp-001"), _make_metadata(), {"counter": "1"})

        write_config = _make_config(checkpoint_id="cp-001")
        cosmos_saver.put_writes(write_config, [("messages", "hello"), ("counter", 99)], task_id="task-A")

        tup = cosmos_saver.get_tuple(_make_config(checkpoint_id="cp-001"))
        assert tup is not None
        assert len(tup.pending_writes) == 2
        channels = {w[1] for w in tup.pending_writes}
        assert "messages" in channels
        assert "counter" in channels

    def test_put_writes_idempotent(self, cosmos_saver, fake_container):
        """put_writes() skips existing non-negative index writes."""
        config = _make_config()
        cosmos_saver.put(config, _make_checkpoint("cp-001"), _make_metadata(), {"counter": "1"})

        write_config = _make_config(checkpoint_id="cp-001")
        cosmos_saver.put_writes(write_config, [("counter", 42)], task_id="task-A")
        count_before = len(fake_container._items)

        # Writing again with same task_id and channel should be idempotent
        cosmos_saver.put_writes(write_config, [("counter", 42)], task_id="task-A")
        count_after = len(fake_container._items)
        assert count_after == count_before

    def test_list_checkpoints(self, cosmos_saver):
        """list() returns checkpoints in descending order."""
        config = _make_config()
        cosmos_saver.put(config, _make_checkpoint("cp-001"), _make_metadata(step=0), {"counter": "1"})
        cosmos_saver.put(
            _make_config(checkpoint_id="cp-001"),
            _make_checkpoint("cp-002"),
            _make_metadata(step=1),
            {"counter": "2"},
        )

        results = list(cosmos_saver.list(_make_config()))
        assert len(results) == 2
        assert results[0].checkpoint["id"] == "cp-002"
        assert results[1].checkpoint["id"] == "cp-001"

    def test_list_with_limit(self, cosmos_saver):
        """list() respects the limit parameter."""
        config = _make_config()
        for i in range(5):
            parent_config = _make_config(checkpoint_id=f"cp-{i:03d}") if i > 0 else config
            cosmos_saver.put(parent_config, _make_checkpoint(f"cp-{i+1:03d}"), _make_metadata(step=i), {"counter": str(i+1)})

        results = list(cosmos_saver.list(_make_config(), limit=2))
        assert len(results) == 2

    def test_list_with_before(self, cosmos_saver):
        """list() filters checkpoints before a given checkpoint_id."""
        config = _make_config()
        cosmos_saver.put(config, _make_checkpoint("cp-001"), _make_metadata(step=0), {"counter": "1"})
        cosmos_saver.put(
            _make_config(checkpoint_id="cp-001"),
            _make_checkpoint("cp-002"),
            _make_metadata(step=1),
            {"counter": "2"},
        )
        cosmos_saver.put(
            _make_config(checkpoint_id="cp-002"),
            _make_checkpoint("cp-003"),
            _make_metadata(step=2),
            {"counter": "3"},
        )

        before_config = _make_config(checkpoint_id="cp-003")
        results = list(cosmos_saver.list(_make_config(), before=before_config))
        assert len(results) == 2
        assert all(r.checkpoint["id"] < "cp-003" for r in results)

    def test_list_none_config(self, cosmos_saver):
        """list(None) returns no results."""
        results = list(cosmos_saver.list(None))
        assert results == []

    def test_delete_thread(self, cosmos_saver, fake_container):
        """delete_thread() removes all documents for a thread."""
        config = _make_config()
        cosmos_saver.put(config, _make_checkpoint("cp-001"), _make_metadata(), {"counter": "1"})
        cosmos_saver.put_writes(_make_config(checkpoint_id="cp-001"), [("x", 1)], task_id="t1")

        assert len(fake_container._items) > 0

        cosmos_saver.delete_thread("thread-1")
        # All docs for thread-1 should be deleted
        remaining = [d for d in fake_container._items.values() if d.get("thread_id") == "thread-1"]
        assert len(remaining) == 0

    def test_context_manager(self, cosmos_saver):
        """CosmosDbSaver supports context manager protocol."""
        with cosmos_saver as s:
            assert s is cosmos_saver


# ── Async Tests ──────────────────────────────────────────────────────────────

class TestCosmosDbSaverAsync:

    @pytest.mark.asyncio
    async def test_aput_and_aget_tuple(self, cosmos_saver):
        """aput() stores a checkpoint, aget_tuple() retrieves it."""
        config = _make_config()
        checkpoint = _make_checkpoint("cp-001")
        metadata = _make_metadata()

        result_config = await cosmos_saver.aput(config, checkpoint, metadata, {"counter": "1"})
        assert result_config["configurable"]["checkpoint_id"] == "cp-001"

        tup = await cosmos_saver.aget_tuple(_make_config(checkpoint_id="cp-001"))
        assert tup is not None
        assert tup.checkpoint["id"] == "cp-001"
        assert tup.checkpoint["channel_values"]["counter"] == 42

    @pytest.mark.asyncio
    async def test_aget_tuple_latest(self, cosmos_saver):
        """aget_tuple() without checkpoint_id returns the latest checkpoint."""
        await cosmos_saver.aput(_make_config(), _make_checkpoint("cp-001"), _make_metadata(), {"counter": "1"})
        await cosmos_saver.aput(_make_config(checkpoint_id="cp-001"), _make_checkpoint("cp-002"), _make_metadata(step=1), {"counter": "2"})

        tup = await cosmos_saver.aget_tuple(_make_config())
        assert tup is not None
        assert tup.checkpoint["id"] == "cp-002"

    @pytest.mark.asyncio
    async def test_aget_tuple_not_found(self, cosmos_saver):
        """aget_tuple() returns None for nonexistent checkpoint."""
        assert await cosmos_saver.aget_tuple(_make_config(checkpoint_id="nope")) is None

    @pytest.mark.asyncio
    async def test_aput_writes(self, cosmos_saver):
        """aput_writes() stores pending writes."""
        await cosmos_saver.aput(_make_config(), _make_checkpoint("cp-001"), _make_metadata(), {"counter": "1"})
        await cosmos_saver.aput_writes(_make_config(checkpoint_id="cp-001"), [("msg", "hi")], task_id="t1")

        tup = await cosmos_saver.aget_tuple(_make_config(checkpoint_id="cp-001"))
        assert tup is not None
        assert len(tup.pending_writes) == 1

    @pytest.mark.asyncio
    async def test_adelete_thread(self, cosmos_saver, fake_container):
        """adelete_thread() removes all documents for a thread."""
        await cosmos_saver.aput(_make_config(), _make_checkpoint("cp-001"), _make_metadata(), {"counter": "1"})
        assert len(fake_container._items) > 0

        await cosmos_saver.adelete_thread("thread-1")
        remaining = [d for d in fake_container._items.values() if d.get("thread_id") == "thread-1"]
        assert len(remaining) == 0

    @pytest.mark.asyncio
    async def test_async_context_manager(self, cosmos_saver):
        """CosmosDbSaver supports async context manager protocol."""
        async with cosmos_saver as s:
            assert s is cosmos_saver


# ── resolve_checkpointer Integration Tests ───────────────────────────────────

class TestResolveCheckpointerCosmos:

    def test_cosmos_mode_fails_closed_without_endpoint(self, monkeypatch):
        """mode=cosmos fails closed when AZURE_COSMOS_ENDPOINT is missing."""
        monkeypatch.delenv("AZURE_COSMOS_ENDPOINT", raising=False)
        monkeypatch.delenv("AZURE_COSMOS_KEY", raising=False)

        with pytest.raises(WorkbenchError) as exc_info:
            resolve_checkpointer(mode="cosmos")
        assert exc_info.value.code == WorkbenchErrorCode.CONFIG_ERROR

    def test_cosmos_mode_fails_closed_without_key(self, monkeypatch):
        """mode=cosmos fails closed when AZURE_COSMOS_KEY is missing."""
        monkeypatch.setenv("AZURE_COSMOS_ENDPOINT", "https://test.documents.azure.com:443/")
        monkeypatch.delenv("AZURE_COSMOS_KEY", raising=False)

        with pytest.raises(WorkbenchError) as exc_info:
            resolve_checkpointer(mode="cosmos")
        assert exc_info.value.code == WorkbenchErrorCode.CONFIG_ERROR

    def test_azure_cosmos_mode_alias(self, monkeypatch):
        """mode=azure_cosmos is treated identically to mode=cosmos."""
        monkeypatch.delenv("AZURE_COSMOS_ENDPOINT", raising=False)
        monkeypatch.delenv("AZURE_COSMOS_KEY", raising=False)

        with pytest.raises(WorkbenchError) as exc_info:
            resolve_checkpointer(mode="azure_cosmos")
        assert exc_info.value.code == WorkbenchErrorCode.CONFIG_ERROR

    def test_explicit_checkpointer_bypasses_cosmos(self):
        """An explicitly provided checkpointer is always returned, regardless of mode."""
        sentinel = object()
        result = resolve_checkpointer(checkpointer=sentinel, mode="cosmos")
        assert result is sentinel

    def test_development_mode_unchanged(self):
        """Development mode still returns MemorySaver."""
        cp = resolve_checkpointer(mode="development")
        assert type(cp).__name__ in ("MemorySaver", "InMemorySaver")
