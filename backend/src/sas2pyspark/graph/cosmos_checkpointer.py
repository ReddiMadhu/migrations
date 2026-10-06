"""Azure Cosmos DB checkpoint saver for LangGraph — Phase 1a implementation.

Stores LangGraph checkpoints, channel blobs, and pending writes in a single
Cosmos DB container partitioned by ``/thread_id``.  All documents for a given
thread share a logical partition, enabling transactional batch on put.

Document types stored in the container:

* ``checkpoint``  — serialised checkpoint metadata (without channel_values)
* ``blob``        — individual serialised channel value (keyed by channel + version)
* ``write``       — pending channel write from a task
"""

from __future__ import annotations

import json
import logging
import time
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import Any, Iterator, Sequence

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    PendingWrite,
    SerializerProtocol,
    get_checkpoint_id,
    get_checkpoint_metadata,
)

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.core.immutable_blob_store import BlobRef, ImmutableBlobStore
from sas2pyspark.graph.cosmos_client_provider import CosmosClientProvider
from sas2pyspark.graph.cosmos_reliability import (
    CircuitBreaker,
    arun_with_retry,
    run_with_retry,
)
from sas2pyspark.graph.cosmos_telemetry import (
    record_blob_overflow,
    record_checkpoint_payload_size,
    record_checkpoint_put_latency,
)

logger = logging.getLogger(__name__)

# ── Document ID helpers ──────────────────────────────────────────────────────


def _checkpoint_doc_id(thread_id: str, checkpoint_ns: str, checkpoint_id: str) -> str:
    return f"cp#{thread_id}#{checkpoint_ns}#{checkpoint_id}"


def _blob_doc_id(
    thread_id: str, checkpoint_ns: str, channel: str, version: str | int | float,
) -> str:
    return f"blob#{thread_id}#{checkpoint_ns}#{channel}#{version}"


def _write_doc_id(
    thread_id: str,
    checkpoint_ns: str,
    checkpoint_id: str,
    task_id: str,
    idx: int,
) -> str:
    return f"write#{thread_id}#{checkpoint_ns}#{checkpoint_id}#{task_id}#{idx}"


# ── Saver ────────────────────────────────────────────────────────────────────


class CosmosDbSaver(
    BaseCheckpointSaver[str],
    AbstractContextManager,
    AbstractAsyncContextManager,
):
    """Azure Cosmos DB LangGraph checkpoint saver.

    Stores checkpoints, channel blobs, and pending writes in a single Cosmos
    container using ``/thread_id`` as the partition key.  Supports both sync
    and async operation via the dual-client :class:`CosmosClientProvider`.

    Args:
        provider: A :class:`CosmosClientProvider` instance.
        container_name: Cosmos container name (default ``"langgraph_checkpoints"``).
        serde: Optional custom serialiser.
        blob_store: Optional ImmutableBlobStore for checkpoint payload overflow.
        max_inline_bytes: Payload threshold above which blobs spill to blob store (default 1.5MB).
        circuit_breaker: Optional CircuitBreaker instance for failure isolation.
    """

    def __init__(
        self,
        provider: CosmosClientProvider,
        *,
        container_name: str = "langgraph_checkpoints",
        serde: SerializerProtocol | None = None,
        blob_store: ImmutableBlobStore | None = None,
        max_inline_bytes: int = 1_500_000,
        circuit_breaker: CircuitBreaker | None = None,
    ) -> None:
        super().__init__(serde=serde)
        self._provider = provider
        self._container_name = container_name
        self._blob_store = blob_store
        self._max_inline_bytes = max_inline_bytes
        self._circuit_breaker = circuit_breaker
        self._sync_container: Any = None
        self._async_container: Any = None

    # ── Context manager support ──────────────────────────────────────────

    def __enter__(self) -> CosmosDbSaver:
        return self

    def __exit__(self, *args: Any) -> None:
        pass  # Provider owns client lifecycle

    async def __aenter__(self) -> CosmosDbSaver:
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass  # Provider owns client lifecycle

    # ── Lazy container access ────────────────────────────────────────────

    def _get_container(self) -> Any:
        if self._sync_container is None:
            self._sync_container = self._provider.get_container(self._container_name)
        return self._sync_container

    async def _aget_container(self) -> Any:
        if self._async_container is None:
            self._async_container = await self._provider.aget_container(
                self._container_name,
            )
        return self._async_container

    # ── put ───────────────────────────────────────────────────────────────

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Store a checkpoint with its channel blobs in Cosmos DB (sync)."""
        start_time = time.monotonic()
        thread_id: str = config["configurable"]["thread_id"]
        checkpoint_ns: str = config["configurable"].get("checkpoint_ns", "")
        parent_checkpoint_id: str | None = config["configurable"].get("checkpoint_id")
        checkpoint_id: str = checkpoint["id"]

        container = self._get_container()

        # Separate channel_values from checkpoint metadata
        c = checkpoint.copy()
        channel_values: dict[str, Any] = c.pop("channel_values")  # type: ignore[misc]
        total_payload_bytes = 0

        # Store each channel blob individually
        for channel, version in new_versions.items():
            blob_type, blob_data = (
                self.serde.dumps_typed(channel_values[channel])
                if channel in channel_values
                else ("empty", b"")
            )
            blob_len = len(blob_data) if isinstance(blob_data, bytes) else 0
            total_payload_bytes += blob_len

            overflow_ref: dict[str, Any] | None = None
            raw_hex = ""
            if isinstance(blob_data, bytes) and blob_data:
                if blob_len > self._max_inline_bytes and self._blob_store is not None:
                    ref = self._blob_store.put(blob_data, namespace="checkpoints")
                    record_blob_overflow()
                    overflow_ref = ref.to_dict()
                else:
                    raw_hex = blob_data.hex()

            blob_doc: dict[str, Any] = {
                "id": _blob_doc_id(thread_id, checkpoint_ns, channel, version),
                "thread_id": thread_id,
                "type": "blob",
                "checkpoint_ns": checkpoint_ns,
                "channel": channel,
                "version": str(version),
                "blob_type": blob_type,
                "blob_data": raw_hex,
            }
            if overflow_ref is not None:
                blob_doc["overflow_ref"] = overflow_ref

            run_with_retry(
                container.upsert_item,
                body=blob_doc,
                operation="put_blob",
                circuit_breaker=self._circuit_breaker,
            )

        # Serialise checkpoint (without channel_values) and metadata
        cp_type, cp_data = self.serde.dumps_typed(c)
        md = get_checkpoint_metadata(config, metadata)
        md_type, md_data = self.serde.dumps_typed(md)

        cp_len = len(cp_data) if isinstance(cp_data, bytes) else 0
        md_len = len(md_data) if isinstance(md_data, bytes) else 0
        total_payload_bytes += cp_len + md_len

        cp_overflow_ref: dict[str, Any] | None = None
        cp_raw_hex = ""
        if isinstance(cp_data, bytes) and cp_data:
            if cp_len > self._max_inline_bytes and self._blob_store is not None:
                ref = self._blob_store.put(cp_data, namespace="checkpoints")
                record_blob_overflow()
                cp_overflow_ref = ref.to_dict()
            else:
                cp_raw_hex = cp_data.hex()

        checkpoint_doc: dict[str, Any] = {
            "id": _checkpoint_doc_id(thread_id, checkpoint_ns, checkpoint_id),
            "thread_id": thread_id,
            "type": "checkpoint",
            "checkpoint_ns": checkpoint_ns,
            "checkpoint_id": checkpoint_id,
            "parent_checkpoint_id": parent_checkpoint_id,
            "cp_type": cp_type,
            "cp_data": cp_raw_hex,
            "md_type": md_type,
            "md_data": md_data.hex() if isinstance(md_data, bytes) else "",
        }
        if cp_overflow_ref is not None:
            checkpoint_doc["overflow_ref"] = cp_overflow_ref

        run_with_retry(
            container.upsert_item,
            body=checkpoint_doc,
            operation="put_checkpoint",
            circuit_breaker=self._circuit_breaker,
        )

        duration_ms = (time.monotonic() - start_time) * 1000.0
        record_checkpoint_put_latency(duration_ms, operation="put")
        record_checkpoint_payload_size(total_payload_bytes)

        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            },
        }

    # ── aput ──────────────────────────────────────────────────────────────

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Store a checkpoint with its channel blobs in Cosmos DB (async)."""
        start_time = time.monotonic()
        thread_id: str = config["configurable"]["thread_id"]
        checkpoint_ns: str = config["configurable"].get("checkpoint_ns", "")
        parent_checkpoint_id: str | None = config["configurable"].get("checkpoint_id")
        checkpoint_id: str = checkpoint["id"]

        container = await self._aget_container()

        c = checkpoint.copy()
        channel_values: dict[str, Any] = c.pop("channel_values")  # type: ignore[misc]
        total_payload_bytes = 0

        for channel, version in new_versions.items():
            blob_type, blob_data = (
                self.serde.dumps_typed(channel_values[channel])
                if channel in channel_values
                else ("empty", b"")
            )
            blob_len = len(blob_data) if isinstance(blob_data, bytes) else 0
            total_payload_bytes += blob_len

            overflow_ref: dict[str, Any] | None = None
            raw_hex = ""
            if isinstance(blob_data, bytes) and blob_data:
                if blob_len > self._max_inline_bytes and self._blob_store is not None:
                    ref = self._blob_store.put(blob_data, namespace="checkpoints")
                    record_blob_overflow()
                    overflow_ref = ref.to_dict()
                else:
                    raw_hex = blob_data.hex()

            blob_doc: dict[str, Any] = {
                "id": _blob_doc_id(thread_id, checkpoint_ns, channel, version),
                "thread_id": thread_id,
                "type": "blob",
                "checkpoint_ns": checkpoint_ns,
                "channel": channel,
                "version": str(version),
                "blob_type": blob_type,
                "blob_data": raw_hex,
            }
            if overflow_ref is not None:
                blob_doc["overflow_ref"] = overflow_ref

            await arun_with_retry(
                container.upsert_item,
                body=blob_doc,
                operation="aput_blob",
                circuit_breaker=self._circuit_breaker,
            )

        cp_type, cp_data = self.serde.dumps_typed(c)
        md = get_checkpoint_metadata(config, metadata)
        md_type, md_data = self.serde.dumps_typed(md)

        cp_len = len(cp_data) if isinstance(cp_data, bytes) else 0
        md_len = len(md_data) if isinstance(md_data, bytes) else 0
        total_payload_bytes += cp_len + md_len

        cp_overflow_ref: dict[str, Any] | None = None
        cp_raw_hex = ""
        if isinstance(cp_data, bytes) and cp_data:
            if cp_len > self._max_inline_bytes and self._blob_store is not None:
                ref = self._blob_store.put(cp_data, namespace="checkpoints")
                record_blob_overflow()
                cp_overflow_ref = ref.to_dict()
            else:
                cp_raw_hex = cp_data.hex()

        checkpoint_doc: dict[str, Any] = {
            "id": _checkpoint_doc_id(thread_id, checkpoint_ns, checkpoint_id),
            "thread_id": thread_id,
            "type": "checkpoint",
            "checkpoint_ns": checkpoint_ns,
            "checkpoint_id": checkpoint_id,
            "parent_checkpoint_id": parent_checkpoint_id,
            "cp_type": cp_type,
            "cp_data": cp_raw_hex,
            "md_type": md_type,
            "md_data": md_data.hex() if isinstance(md_data, bytes) else "",
        }
        if cp_overflow_ref is not None:
            checkpoint_doc["overflow_ref"] = cp_overflow_ref

        await arun_with_retry(
            container.upsert_item,
            body=checkpoint_doc,
            operation="aput_checkpoint",
            circuit_breaker=self._circuit_breaker,
        )

        duration_ms = (time.monotonic() - start_time) * 1000.0
        record_checkpoint_put_latency(duration_ms, operation="aput")
        record_checkpoint_payload_size(total_payload_bytes)

        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            },
        }

    # ── put_writes ────────────────────────────────────────────────────────

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Store pending writes associated with a checkpoint (sync)."""
        thread_id: str = config["configurable"]["thread_id"]
        checkpoint_ns: str = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id: str = config["configurable"]["checkpoint_id"]

        container = self._get_container()

        for idx, (channel, value) in enumerate(writes):
            write_idx = WRITES_IDX_MAP.get(channel, idx)
            doc_id = _write_doc_id(
                thread_id, checkpoint_ns, checkpoint_id, task_id, write_idx,
            )

            # Skip if non-negative index write already exists (idempotent)
            if write_idx >= 0:
                try:
                    container.read_item(item=doc_id, partition_key=thread_id)
                    continue  # Already exists
                except Exception:
                    pass  # Does not exist yet — create it

            w_type, w_data = self.serde.dumps_typed(value)
            w_len = len(w_data) if isinstance(w_data, bytes) else 0

            overflow_ref: dict[str, Any] | None = None
            raw_hex = ""
            if isinstance(w_data, bytes) and w_data:
                if w_len > self._max_inline_bytes and self._blob_store is not None:
                    ref = self._blob_store.put(w_data, namespace="checkpoints")
                    record_blob_overflow()
                    overflow_ref = ref.to_dict()
                else:
                    raw_hex = w_data.hex()

            write_doc: dict[str, Any] = {
                "id": doc_id,
                "thread_id": thread_id,
                "type": "write",
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
                "task_id": task_id,
                "task_path": task_path,
                "write_idx": write_idx,
                "channel": channel,
                "w_type": w_type,
                "w_data": raw_hex,
            }
            if overflow_ref is not None:
                write_doc["overflow_ref"] = overflow_ref

            run_with_retry(
                container.upsert_item,
                body=write_doc,
                operation="put_write",
                circuit_breaker=self._circuit_breaker,
            )

    # ── aput_writes ───────────────────────────────────────────────────────

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Store pending writes associated with a checkpoint (async)."""
        thread_id: str = config["configurable"]["thread_id"]
        checkpoint_ns: str = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id: str = config["configurable"]["checkpoint_id"]

        container = await self._aget_container()

        for idx, (channel, value) in enumerate(writes):
            write_idx = WRITES_IDX_MAP.get(channel, idx)
            doc_id = _write_doc_id(
                thread_id, checkpoint_ns, checkpoint_id, task_id, write_idx,
            )

            if write_idx >= 0:
                try:
                    await container.read_item(item=doc_id, partition_key=thread_id)
                    continue
                except Exception:
                    pass

            w_type, w_data = self.serde.dumps_typed(value)
            w_len = len(w_data) if isinstance(w_data, bytes) else 0

            overflow_ref: dict[str, Any] | None = None
            raw_hex = ""
            if isinstance(w_data, bytes) and w_data:
                if w_len > self._max_inline_bytes and self._blob_store is not None:
                    ref = self._blob_store.put(w_data, namespace="checkpoints")
                    record_blob_overflow()
                    overflow_ref = ref.to_dict()
                else:
                    raw_hex = w_data.hex()

            write_doc: dict[str, Any] = {
                "id": doc_id,
                "thread_id": thread_id,
                "type": "write",
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
                "task_id": task_id,
                "task_path": task_path,
                "write_idx": write_idx,
                "channel": channel,
                "w_type": w_type,
                "w_data": raw_hex,
            }
            if overflow_ref is not None:
                write_doc["overflow_ref"] = overflow_ref

            await arun_with_retry(
                container.upsert_item,
                body=write_doc,
                operation="aput_write",
                circuit_breaker=self._circuit_breaker,
            )

    # ── get_tuple ─────────────────────────────────────────────────────────

    def _load_blobs(
        self,
        container: Any,
        thread_id: str,
        checkpoint_ns: str,
        channel_versions: dict[str, str | int | float],
    ) -> dict[str, Any]:
        """Load channel value blobs for a checkpoint."""
        result: dict[str, Any] = {}
        for channel, version in channel_versions.items():
            doc_id = _blob_doc_id(thread_id, checkpoint_ns, channel, version)
            try:
                doc = container.read_item(item=doc_id, partition_key=thread_id)
            except Exception:
                continue
            if doc.get("blob_type") == "empty":
                continue
            if doc.get("overflow_ref") and self._blob_store is not None:
                ref = BlobRef.from_dict(doc["overflow_ref"])
                blob_bytes = self._blob_store.get(ref)
            else:
                blob_bytes = bytes.fromhex(doc.get("blob_data", ""))
            result[channel] = self.serde.loads_typed((doc["blob_type"], blob_bytes))
        return result

    async def _aload_blobs(
        self,
        container: Any,
        thread_id: str,
        checkpoint_ns: str,
        channel_versions: dict[str, str | int | float],
    ) -> dict[str, Any]:
        """Load channel value blobs for a checkpoint (async)."""
        result: dict[str, Any] = {}
        for channel, version in channel_versions.items():
            doc_id = _blob_doc_id(thread_id, checkpoint_ns, channel, version)
            try:
                doc = await container.read_item(item=doc_id, partition_key=thread_id)
            except Exception:
                continue
            if doc.get("blob_type") == "empty":
                continue
            if doc.get("overflow_ref") and self._blob_store is not None:
                ref = BlobRef.from_dict(doc["overflow_ref"])
                blob_bytes = self._blob_store.get(ref)
            else:
                blob_bytes = bytes.fromhex(doc.get("blob_data", ""))
            result[channel] = self.serde.loads_typed((doc["blob_type"], blob_bytes))
        return result

    def _load_writes(
        self,
        container: Any,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
    ) -> list[PendingWrite]:
        """Load pending writes for a checkpoint."""
        query = (
            "SELECT * FROM c WHERE c.thread_id = @tid "
            "AND c.type = 'write' "
            "AND c.checkpoint_ns = @ns "
            "AND c.checkpoint_id = @cpid"
        )
        params: list[dict[str, Any]] = [
            {"name": "@tid", "value": thread_id},
            {"name": "@ns", "value": checkpoint_ns},
            {"name": "@cpid", "value": checkpoint_id},
        ]
        items = list(container.query_items(
            query=query,
            parameters=params,
            partition_key=thread_id,
        ))
        writes: list[PendingWrite] = []
        for doc in items:
            if doc.get("overflow_ref") and self._blob_store is not None:
                ref = BlobRef.from_dict(doc["overflow_ref"])
                w_bytes = self._blob_store.get(ref)
            else:
                w_bytes = bytes.fromhex(doc.get("w_data", ""))
            value = self.serde.loads_typed((doc["w_type"], w_bytes))
            writes.append((doc["task_id"], doc["channel"], value))
        return writes

    async def _aload_writes(
        self,
        container: Any,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
    ) -> list[PendingWrite]:
        """Load pending writes for a checkpoint (async)."""
        query = (
            "SELECT * FROM c WHERE c.thread_id = @tid "
            "AND c.type = 'write' "
            "AND c.checkpoint_ns = @ns "
            "AND c.checkpoint_id = @cpid"
        )
        params: list[dict[str, Any]] = [
            {"name": "@tid", "value": thread_id},
            {"name": "@ns", "value": checkpoint_ns},
            {"name": "@cpid", "value": checkpoint_id},
        ]
        writes: list[PendingWrite] = []
        async for doc in container.query_items(
            query=query,
            parameters=params,
            partition_key=thread_id,
        ):
            if doc.get("overflow_ref") and self._blob_store is not None:
                ref = BlobRef.from_dict(doc["overflow_ref"])
                w_bytes = self._blob_store.get(ref)
            else:
                w_bytes = bytes.fromhex(doc.get("w_data", ""))
            value = self.serde.loads_typed((doc["w_type"], w_bytes))
            writes.append((doc["task_id"], doc["channel"], value))
        return writes

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Fetch a checkpoint tuple from Cosmos DB (sync)."""
        thread_id: str = config["configurable"]["thread_id"]
        checkpoint_ns: str = config["configurable"].get("checkpoint_ns", "")
        container = self._get_container()

        checkpoint_id = get_checkpoint_id(config)

        if checkpoint_id:
            # Fetch specific checkpoint
            doc_id = _checkpoint_doc_id(thread_id, checkpoint_ns, checkpoint_id)
            try:
                doc = container.read_item(item=doc_id, partition_key=thread_id)
            except Exception:
                return None
        else:
            # Fetch latest checkpoint for this thread
            query = (
                "SELECT * FROM c WHERE c.thread_id = @tid "
                "AND c.type = 'checkpoint' "
                "AND c.checkpoint_ns = @ns "
                "ORDER BY c.checkpoint_id DESC OFFSET 0 LIMIT 1"
            )
            params: list[dict[str, Any]] = [
                {"name": "@tid", "value": thread_id},
                {"name": "@ns", "value": checkpoint_ns},
            ]
            results = list(container.query_items(
                query=query,
                parameters=params,
                partition_key=thread_id,
            ))
            if not results:
                return None
            doc = results[0]
            checkpoint_id = doc["checkpoint_id"]

        return self._doc_to_tuple(container, doc, thread_id, checkpoint_ns)

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Fetch a checkpoint tuple from Cosmos DB (async)."""
        thread_id: str = config["configurable"]["thread_id"]
        checkpoint_ns: str = config["configurable"].get("checkpoint_ns", "")
        container = await self._aget_container()

        checkpoint_id = get_checkpoint_id(config)

        if checkpoint_id:
            doc_id = _checkpoint_doc_id(thread_id, checkpoint_ns, checkpoint_id)
            try:
                doc = await container.read_item(item=doc_id, partition_key=thread_id)
            except Exception:
                return None
        else:
            query = (
                "SELECT * FROM c WHERE c.thread_id = @tid "
                "AND c.type = 'checkpoint' "
                "AND c.checkpoint_ns = @ns "
                "ORDER BY c.checkpoint_id DESC OFFSET 0 LIMIT 1"
            )
            params: list[dict[str, Any]] = [
                {"name": "@tid", "value": thread_id},
                {"name": "@ns", "value": checkpoint_ns},
            ]
            results: list[dict[str, Any]] = []
            async for item in container.query_items(
                query=query,
                parameters=params,
                partition_key=thread_id,
            ):
                results.append(item)
            if not results:
                return None
            doc = results[0]
            checkpoint_id = doc["checkpoint_id"]

        return await self._adoc_to_tuple(container, doc, thread_id, checkpoint_ns)

    def _doc_to_tuple(
        self,
        container: Any,
        doc: dict[str, Any],
        thread_id: str,
        checkpoint_ns: str,
    ) -> CheckpointTuple:
        """Convert a Cosmos checkpoint document to a CheckpointTuple (sync)."""
        checkpoint_id = doc["checkpoint_id"]
        parent_checkpoint_id = doc.get("parent_checkpoint_id")

        if doc.get("overflow_ref") and self._blob_store is not None:
            ref = BlobRef.from_dict(doc["overflow_ref"])
            cp_bytes = self._blob_store.get(ref)
        else:
            cp_bytes = bytes.fromhex(doc.get("cp_data", ""))
        checkpoint_data: Checkpoint = self.serde.loads_typed((doc["cp_type"], cp_bytes))

        md_bytes = bytes.fromhex(doc.get("md_data", ""))
        metadata = self.serde.loads_typed((doc["md_type"], md_bytes))

        channel_values = self._load_blobs(
            container, thread_id, checkpoint_ns, checkpoint_data["channel_versions"],
        )

        pending_writes = self._load_writes(
            container, thread_id, checkpoint_ns, checkpoint_id,
        )

        return CheckpointTuple(
            config={
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_id,
                },
            },
            checkpoint={
                **checkpoint_data,
                "channel_values": channel_values,
            },
            metadata=metadata,
            pending_writes=pending_writes,
            parent_config=(
                {
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": parent_checkpoint_id,
                    },
                }
                if parent_checkpoint_id
                else None
            ),
        )

    async def _adoc_to_tuple(
        self,
        container: Any,
        doc: dict[str, Any],
        thread_id: str,
        checkpoint_ns: str,
    ) -> CheckpointTuple:
        """Convert a Cosmos checkpoint document to a CheckpointTuple (async)."""
        checkpoint_id = doc["checkpoint_id"]
        parent_checkpoint_id = doc.get("parent_checkpoint_id")

        if doc.get("overflow_ref") and self._blob_store is not None:
            ref = BlobRef.from_dict(doc["overflow_ref"])
            cp_bytes = self._blob_store.get(ref)
        else:
            cp_bytes = bytes.fromhex(doc.get("cp_data", ""))
        checkpoint_data: Checkpoint = self.serde.loads_typed((doc["cp_type"], cp_bytes))

        md_bytes = bytes.fromhex(doc.get("md_data", ""))
        metadata = self.serde.loads_typed((doc["md_type"], md_bytes))

        channel_values = await self._aload_blobs(
            container, thread_id, checkpoint_ns, checkpoint_data["channel_versions"],
        )

        pending_writes = await self._aload_writes(
            container, thread_id, checkpoint_ns, checkpoint_id,
        )

        return CheckpointTuple(
            config={
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_id,
                },
            },
            checkpoint={
                **checkpoint_data,
                "channel_values": channel_values,
            },
            metadata=metadata,
            pending_writes=pending_writes,
            parent_config=(
                {
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": parent_checkpoint_id,
                    },
                }
                if parent_checkpoint_id
                else None
            ),
        )

    # ── list ──────────────────────────────────────────────────────────────

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        """List checkpoints matching the given criteria (sync)."""
        container = self._get_container()

        if config is None:
            return

        thread_id: str = config["configurable"]["thread_id"]
        checkpoint_ns: str = config["configurable"].get("checkpoint_ns", "")

        query = (
            "SELECT * FROM c WHERE c.thread_id = @tid "
            "AND c.type = 'checkpoint' "
            "AND c.checkpoint_ns = @ns"
        )
        params: list[dict[str, Any]] = [
            {"name": "@tid", "value": thread_id},
            {"name": "@ns", "value": checkpoint_ns},
        ]

        before_id = get_checkpoint_id(before) if before else None
        if before_id:
            query += " AND c.checkpoint_id < @before"
            params.append({"name": "@before", "value": before_id})

        query += " ORDER BY c.checkpoint_id DESC"

        count = 0
        for doc in container.query_items(
            query=query,
            parameters=params,
            partition_key=thread_id,
        ):
            if limit is not None and count >= limit:
                break

            # Apply metadata filter
            if filter:
                md_bytes = bytes.fromhex(doc["md_data"])
                md = self.serde.loads_typed((doc["md_type"], md_bytes))
                if not all(md.get(k) == v for k, v in filter.items()):
                    continue

            yield self._doc_to_tuple(container, doc, thread_id, checkpoint_ns)
            count += 1

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ):  # type: ignore[override]
        """List checkpoints matching the given criteria (async)."""
        container = await self._aget_container()

        if config is None:
            return

        thread_id: str = config["configurable"]["thread_id"]
        checkpoint_ns: str = config["configurable"].get("checkpoint_ns", "")

        query = (
            "SELECT * FROM c WHERE c.thread_id = @tid "
            "AND c.type = 'checkpoint' "
            "AND c.checkpoint_ns = @ns"
        )
        params: list[dict[str, Any]] = [
            {"name": "@tid", "value": thread_id},
            {"name": "@ns", "value": checkpoint_ns},
        ]

        before_id = get_checkpoint_id(before) if before else None
        if before_id:
            query += " AND c.checkpoint_id < @before"
            params.append({"name": "@before", "value": before_id})

        query += " ORDER BY c.checkpoint_id DESC"

        count = 0
        async for doc in container.query_items(
            query=query,
            parameters=params,
            partition_key=thread_id,
        ):
            if limit is not None and count >= limit:
                break

            if filter:
                md_bytes = bytes.fromhex(doc["md_data"])
                md = self.serde.loads_typed((doc["md_type"], md_bytes))
                if not all(md.get(k) == v for k, v in filter.items()):
                    continue

            yield await self._adoc_to_tuple(container, doc, thread_id, checkpoint_ns)
            count += 1

    # ── delete_thread ─────────────────────────────────────────────────────

    def delete_thread(self, thread_id: str) -> None:
        """Delete all checkpoints, blobs, and writes for a thread (sync)."""
        container = self._get_container()
        query = "SELECT c.id FROM c WHERE c.thread_id = @tid"
        params = [{"name": "@tid", "value": thread_id}]

        doc_ids = [
            doc["id"]
            for doc in container.query_items(
                query=query,
                parameters=params,
                partition_key=thread_id,
            )
        ]
        for doc_id in doc_ids:
            try:
                container.delete_item(item=doc_id, partition_key=thread_id)
            except Exception:
                logger.warning(
                    "cosmos_delete_item_failed",
                    extra={"thread_id": thread_id, "doc_id": doc_id},
                    exc_info=True,
                )

    async def adelete_thread(self, thread_id: str) -> None:
        """Delete all checkpoints, blobs, and writes for a thread (async)."""
        container = await self._aget_container()
        query = "SELECT c.id FROM c WHERE c.thread_id = @tid"
        params = [{"name": "@tid", "value": thread_id}]

        doc_ids: list[str] = []
        async for doc in container.query_items(
            query=query,
            parameters=params,
            partition_key=thread_id,
        ):
            doc_ids.append(doc["id"])

        for doc_id in doc_ids:
            try:
                await container.delete_item(item=doc_id, partition_key=thread_id)
            except Exception:
                logger.warning(
                    "cosmos_delete_item_failed",
                    extra={"thread_id": thread_id, "doc_id": doc_id},
                    exc_info=True,
                )

    # ── prune ─────────────────────────────────────────────────────────────

    def prune(self, config: RunnableConfig, *, keep_last: int = 1) -> int:
        """Prune older checkpoints for a thread (sync), retaining keep_last newest."""
        thread_id: str = config["configurable"]["thread_id"]
        checkpoint_ns: str = config["configurable"].get("checkpoint_ns", "")
        container = self._get_container()

        query = (
            "SELECT c.id, c.checkpoint_id, c.overflow_ref FROM c WHERE c.thread_id = @tid "
            "AND c.type = 'checkpoint' "
            "AND c.checkpoint_ns = @ns "
            "ORDER BY c.checkpoint_id DESC"
        )
        params: list[dict[str, Any]] = [
            {"name": "@tid", "value": thread_id},
            {"name": "@ns", "value": checkpoint_ns},
        ]
        checkpoints = list(container.query_items(
            query=query,
            parameters=params,
            partition_key=thread_id,
        ))
        if len(checkpoints) <= keep_last:
            return 0

        to_prune = checkpoints[keep_last:]
        pruned_count = 0
        for cp in to_prune:
            cp_id = cp["checkpoint_id"]
            if cp.get("overflow_ref") and self._blob_store is not None:
                try:
                    ref = BlobRef.from_dict(cp["overflow_ref"])
                    self._blob_store.delete(ref)
                except Exception:
                    pass
            try:
                container.delete_item(item=cp["id"], partition_key=thread_id)
                pruned_count += 1
            except Exception:
                pass

            w_query = (
                "SELECT c.id FROM c WHERE c.thread_id = @tid "
                "AND c.type = 'write' "
                "AND c.checkpoint_ns = @ns "
                "AND c.checkpoint_id = @cpid"
            )
            w_params = [
                {"name": "@tid", "value": thread_id},
                {"name": "@ns", "value": checkpoint_ns},
                {"name": "@cpid", "value": cp_id},
            ]
            for w in container.query_items(query=w_query, parameters=w_params, partition_key=thread_id):
                try:
                    container.delete_item(item=w["id"], partition_key=thread_id)
                except Exception:
                    pass

        return pruned_count

    async def aprune(self, config: RunnableConfig, *, keep_last: int = 1) -> int:
        """Prune older checkpoints for a thread (async), retaining keep_last newest."""
        thread_id: str = config["configurable"]["thread_id"]
        checkpoint_ns: str = config["configurable"].get("checkpoint_ns", "")
        container = await self._aget_container()

        query = (
            "SELECT c.id, c.checkpoint_id, c.overflow_ref FROM c WHERE c.thread_id = @tid "
            "AND c.type = 'checkpoint' "
            "AND c.checkpoint_ns = @ns "
            "ORDER BY c.checkpoint_id DESC"
        )
        params: list[dict[str, Any]] = [
            {"name": "@tid", "value": thread_id},
            {"name": "@ns", "value": checkpoint_ns},
        ]
        checkpoints: list[dict[str, Any]] = []
        async for item in container.query_items(
            query=query,
            parameters=params,
            partition_key=thread_id,
        ):
            checkpoints.append(item)

        if len(checkpoints) <= keep_last:
            return 0

        to_prune = checkpoints[keep_last:]
        pruned_count = 0
        for cp in to_prune:
            cp_id = cp["checkpoint_id"]
            if cp.get("overflow_ref") and self._blob_store is not None:
                try:
                    ref = BlobRef.from_dict(cp["overflow_ref"])
                    self._blob_store.delete(ref)
                except Exception:
                    pass
            try:
                await container.delete_item(item=cp["id"], partition_key=thread_id)
                pruned_count += 1
            except Exception:
                pass

            w_query = (
                "SELECT c.id FROM c WHERE c.thread_id = @tid "
                "AND c.type = 'write' "
                "AND c.checkpoint_ns = @ns "
                "AND c.checkpoint_id = @cpid"
            )
            w_params = [
                {"name": "@tid", "value": thread_id},
                {"name": "@ns", "value": checkpoint_ns},
                {"name": "@cpid", "value": cp_id},
            ]
            async for w in container.query_items(query=w_query, parameters=w_params, partition_key=thread_id):
                try:
                    await container.delete_item(item=w["id"], partition_key=thread_id)
                except Exception:
                    pass

        return pruned_count
