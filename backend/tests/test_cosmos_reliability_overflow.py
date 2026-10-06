"""Tests for Phase 1b: Reliability layer, payload overflow to blob store, telemetry, and pruning."""

from __future__ import annotations

import time
import pytest
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import Checkpoint, CheckpointMetadata

from unittest.mock import AsyncMock, MagicMock

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.core.immutable_blob_store import BlobRef, InMemoryBlobStore
from sas2pyspark.graph.cosmos_checkpointer import CosmosDbSaver
from sas2pyspark.graph.cosmos_reliability import (
    CircuitBreaker,
    CircuitState,
    arun_with_retry,
    is_transient_error,
    run_with_retry,
)
from sas2pyspark.graph.cosmos_telemetry import (
    get_telemetry_snapshot,
    record_circuit_breaker_state,
    reset_telemetry_snapshot,
)
from sas2pyspark.services.artifacts import FileSystemBlobStore
from tests.test_cosmos_checkpointer import AsyncFakeContainer, FakeContainer


def make_mock_provider(sync_container=None, async_container=None):
    provider = MagicMock()
    if sync_container is not None:
        provider.get_container.return_value = sync_container
    if async_container is not None:
        provider.aget_container = AsyncMock(return_value=async_container)
    return provider


# ── 1. ImmutableBlobStore Tests ──────────────────────────────────────────────


def test_in_memory_blob_store_roundtrip():
    store = InMemoryBlobStore()
    payload = b"test payload content 12345"
    ref = store.put(payload, namespace="checkpoints")

    assert ref.content_hash.startswith("sha256:")
    assert ref.size == len(payload)
    assert ref.namespace == "checkpoints"
    assert store.has(ref)
    assert store.count() == 1

    retrieved = store.get(ref)
    assert retrieved == payload

    store.delete(ref)
    assert not store.has(ref)
    assert store.count() == 0


def test_filesystem_blob_store_roundtrip(tmp_path):
    store = FileSystemBlobStore(str(tmp_path))
    payload = b"large binary data to persist"
    ref = store.put(payload, namespace="checkpoints")

    assert ref.size == len(payload)
    retrieved = store.get(ref)
    assert retrieved == payload

    # Deduplication test: putting exact same content produces same ref
    ref2 = store.put(payload, namespace="checkpoints")
    assert ref.content_hash == ref2.content_hash

    # Delete
    store.delete(ref)
    with pytest.raises(WorkbenchError) as exc_info:
        store.get(ref)
    assert exc_info.value.code == WorkbenchErrorCode.EVIDENCE_TAMPERED


def test_filesystem_blob_store_tamper_detection(tmp_path):
    store = FileSystemBlobStore(str(tmp_path))
    payload = b"authentic data"
    ref = store.put(payload, namespace="checkpoints")

    # Corrupt the file on disk
    target_path = store._resolve_path(ref.namespace, ref.content_hash)
    with open(target_path, "wb") as f:
        f.write(b"tampered data!")

    with pytest.raises(WorkbenchError) as exc_info:
        store.get(ref)
    assert exc_info.value.code == WorkbenchErrorCode.EVIDENCE_TAMPERED


# ── 2. Checkpoint Overflow Tests ─────────────────────────────────────────────


def test_small_checkpoint_stays_inline():
    reset_telemetry_snapshot()
    container = FakeContainer()
    provider = make_mock_provider(sync_container=container)
    blob_store = InMemoryBlobStore()
    saver = CosmosDbSaver(provider, blob_store=blob_store, max_inline_bytes=1000)

    config: RunnableConfig = {"configurable": {"thread_id": "t-inline"}}
    checkpoint: Checkpoint = {
        "v": 1,
        "id": "cp-small",
        "ts": "2026-01-01T00:00:00Z",
        "channel_values": {"msg": "hello"},
        "channel_versions": {"msg": 1},
        "versions_seen": {},
        "pending_sends": [],
    }
    saver.put(config, checkpoint, {}, {"msg": 1})

    # Assert no blobs stored in blob_store
    assert blob_store.count() == 0
    snapshot = get_telemetry_snapshot()
    assert snapshot["overflow_count"] == 0

    # Read back
    tup = saver.get_tuple(config)
    assert tup is not None
    assert tup.checkpoint["channel_values"]["msg"] == "hello"


def test_large_checkpoint_overflows_to_blob_store():
    reset_telemetry_snapshot()
    container = FakeContainer()
    provider = make_mock_provider(sync_container=container)
    blob_store = InMemoryBlobStore()
    # Threshold set very low (50 bytes) to trigger overflow
    saver = CosmosDbSaver(provider, blob_store=blob_store, max_inline_bytes=50)

    large_payload = "x" * 200
    config: RunnableConfig = {"configurable": {"thread_id": "t-overflow"}}
    checkpoint: Checkpoint = {
        "v": 1,
        "id": "cp-large",
        "ts": "2026-01-01T00:00:00Z",
        "channel_values": {"big_channel": large_payload},
        "channel_versions": {"big_channel": 1},
        "versions_seen": {},
        "pending_sends": [],
    }
    saver.put(config, checkpoint, {}, {"big_channel": 1})

    # Assert blob was spilled to blob_store
    assert blob_store.count() >= 1
    snapshot = get_telemetry_snapshot()
    assert snapshot["overflow_count"] >= 1

    # Inspect doc in fake Cosmos container to ensure overflow_ref is saved and blob_data is empty
    blob_doc_key = "blob#t-overflow##big_channel#1"
    assert blob_doc_key in container._items
    blob_doc = container._items[blob_doc_key]
    assert "overflow_ref" in blob_doc
    assert blob_doc["blob_data"] == ""

    # Read back and verify channel values match original
    tup = saver.get_tuple(config)
    assert tup is not None
    assert tup.checkpoint["channel_values"]["big_channel"] == large_payload


@pytest.mark.asyncio
async def test_async_checkpoint_overflow_roundtrip():
    reset_telemetry_snapshot()
    sync_c = FakeContainer()
    container = AsyncFakeContainer(sync_c)
    provider = make_mock_provider(async_container=container)
    blob_store = InMemoryBlobStore()
    saver = CosmosDbSaver(provider, blob_store=blob_store, max_inline_bytes=50)

    large_payload = "async-overflow-data-" * 10
    config: RunnableConfig = {"configurable": {"thread_id": "t-async-overflow"}}
    checkpoint: Checkpoint = {
        "v": 1,
        "id": "cp-async-large",
        "ts": "2026-01-01T00:00:00Z",
        "channel_values": {"data": large_payload},
        "channel_versions": {"data": 1},
        "versions_seen": {},
        "pending_sends": [],
    }
    await saver.aput(config, checkpoint, {}, {"data": 1})
    assert blob_store.count() >= 1

    tup = await saver.aget_tuple(config)
    assert tup is not None
    assert tup.checkpoint["channel_values"]["data"] == large_payload


# ── 3. Reliability & Circuit Breaker Tests ────────────────────────────────────


class DummyCosmosError(Exception):
    def __init__(self, status_code: int, message: str = ""):
        super().__init__(message)
        self.status_code = status_code


def test_error_classification():
    assert is_transient_error(DummyCosmosError(429))
    assert is_transient_error(DummyCosmosError(503))
    assert is_transient_error(DummyCosmosError(408))
    assert is_transient_error(ConnectionError("network reset"))
    assert is_transient_error(TimeoutError("request timeout"))

    assert not is_transient_error(DummyCosmosError(400))
    assert not is_transient_error(DummyCosmosError(401))
    assert not is_transient_error(DummyCosmosError(403))
    assert not is_transient_error(DummyCosmosError(404))
    assert not is_transient_error(DummyCosmosError(409))
    assert not is_transient_error(DummyCosmosError(412))


def test_retry_success_after_transient_failures():
    attempts = 0

    def flaky_func():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise DummyCosmosError(503, "temporary glitch")
        return "success"

    cb = CircuitBreaker()
    result = run_with_retry(
        flaky_func,
        operation="test_flaky",
        circuit_breaker=cb,
        max_attempts=4,
        base_backoff=0.01,
        max_backoff=0.05,
    )
    assert result == "success"
    assert attempts == 3
    assert cb.state == CircuitState.CLOSED


def test_permanent_error_fails_immediately():
    attempts = 0

    def bad_func():
        nonlocal attempts
        attempts += 1
        raise DummyCosmosError(400, "bad request")

    cb = CircuitBreaker()
    with pytest.raises(WorkbenchError) as exc_info:
        run_with_retry(
            bad_func,
            operation="test_bad",
            circuit_breaker=cb,
            max_attempts=4,
            base_backoff=0.01,
        )
    assert attempts == 1
    assert exc_info.value.code == WorkbenchErrorCode.PERSISTENCE_UNAVAILABLE


def test_circuit_breaker_transitions():
    cb = CircuitBreaker(failure_threshold=3, cooldown_seconds=0.1)
    assert cb.state == CircuitState.CLOSED
    assert cb.can_execute()

    # Failures trip to OPEN
    cb.record_failure()
    cb.record_failure()
    assert cb.state == CircuitState.CLOSED
    cb.record_failure()
    assert cb.state == CircuitState.OPEN
    assert not cb.can_execute()

    # Wait for cooldown
    time.sleep(0.12)
    assert cb.can_execute()
    assert cb.state == CircuitState.HALF_OPEN

    # Success in HALF_OPEN resets to CLOSED
    cb.record_success()
    assert cb.state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_async_retry_success():
    attempts = 0

    async def async_flaky():
        nonlocal attempts
        attempts += 1
        if attempts < 2:
            raise DummyCosmosError(429, "rate limited")
        return "async_success"

    cb = CircuitBreaker()
    result = await arun_with_retry(
        async_flaky,
        operation="test_async_flaky",
        circuit_breaker=cb,
        max_attempts=3,
        base_backoff=0.01,
        max_backoff=0.05,
    )
    assert result == "async_success"
    assert attempts == 2


# ── 4. Prune Tests ────────────────────────────────────────────────────────────


def test_sync_prune_retains_keep_last():
    container = FakeContainer()
    provider = make_mock_provider(sync_container=container)
    blob_store = InMemoryBlobStore()
    saver = CosmosDbSaver(provider, blob_store=blob_store, max_inline_bytes=50)

    config: RunnableConfig = {"configurable": {"thread_id": "t-prune"}}

    # Create 4 sequential checkpoints
    for i in range(1, 5):
        cp: Checkpoint = {
            "v": 1,
            "id": f"cp-000{i}",
            "ts": f"2026-01-01T00:0{i}:00Z",
            "channel_values": {"data": f"content-{i}" * 10},
            "channel_versions": {"data": i},
            "versions_seen": {},
            "pending_sends": [],
        }
        saver.put(config, cp, {}, {"data": i})

    # Prune keeping last 2
    pruned_count = saver.prune(config, keep_last=2)
    assert pruned_count == 2

    # Listing should now only yield 2 checkpoints
    tuples = list(saver.list(config))
    assert len(tuples) == 2
    ids = [t.checkpoint["id"] for t in tuples]
    assert ids == ["cp-0004", "cp-0003"]


@pytest.mark.asyncio
async def test_async_prune_retains_keep_last():
    sync_c = FakeContainer()
    container = AsyncFakeContainer(sync_c)
    provider = make_mock_provider(async_container=container)
    blob_store = InMemoryBlobStore()
    saver = CosmosDbSaver(provider, blob_store=blob_store, max_inline_bytes=50)

    config: RunnableConfig = {"configurable": {"thread_id": "t-async-prune"}}

    for i in range(1, 4):
        cp: Checkpoint = {
            "v": 1,
            "id": f"cp-000{i}",
            "ts": f"2026-01-01T00:0{i}:00Z",
            "channel_values": {"data": f"content-{i}" * 10},
            "channel_versions": {"data": i},
            "versions_seen": {},
            "pending_sends": [],
        }
        await saver.aput(config, cp, {}, {"data": i})

    pruned_count = await saver.aprune(config, keep_last=1)
    assert pruned_count == 2

    tuples = [t async for t in saver.alist(config)]
    assert len(tuples) == 1
    assert tuples[0].checkpoint["id"] == "cp-0003"
