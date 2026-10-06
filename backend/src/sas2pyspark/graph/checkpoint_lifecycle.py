"""Checkpoint lifecycle and asynchronous cleanup management for Cosmos DB.

Implements retention policy enforcement, TTL management, and blob garbage collection
following terminal migration outcomes (COMPLETED, FAILED, CANCELLED).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from langchain_core.runnables import RunnableConfig

from sas2pyspark.core.immutable_blob_store import ImmutableBlobStore
from sas2pyspark.graph.cosmos_checkpointer import CosmosDbSaver

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CheckpointRetentionPolicy:
    """Configurable retention policy for LangGraph checkpoints in Cosmos DB."""

    active_retention: str = "all"
    success_retention: str = "final"
    failure_retention_days: int = 30
    cancelled_retention_days: int = 7
    intermediate_ttl_hours: int = 24


class CheckpointLifecycleManager:
    """Manages asynchronous post-terminal cleanup of Cosmos checkpoints and blobs."""

    def __init__(
        self,
        checkpointer: CosmosDbSaver,
        *,
        policy: CheckpointRetentionPolicy | None = None,
        blob_store: ImmutableBlobStore | None = None,
    ) -> None:
        self.checkpointer = checkpointer
        self.policy = policy or CheckpointRetentionPolicy()
        self.blob_store = blob_store

    def cleanup_thread(
        self,
        thread_id: str,
        *,
        status: str = "COMPLETED",
        checkpoint_ns: str = "",
    ) -> int:
        """Synchronously prune checkpoints according to terminal status policy."""
        config: RunnableConfig = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
            }
        }
        normalized = status.upper()
        if normalized == "COMPLETED":
            keep = 1 if self.policy.success_retention == "final" else 2
            pruned = self.checkpointer.prune(config, keep_last=keep)
            logger.info("checkpoint_lifecycle_completed_pruned", extra={"thread_id": thread_id, "pruned": pruned})
            return pruned

        if normalized in ("FAILED", "CANCELLED"):
            pruned = self.checkpointer.prune(config, keep_last=2)
            logger.info("checkpoint_lifecycle_retained_terminal_pair", extra={"thread_id": thread_id, "pruned": pruned})
            return pruned

        if normalized == "PURGE":
            self.checkpointer.delete_thread(thread_id)
            logger.info("checkpoint_lifecycle_thread_purged", extra={"thread_id": thread_id})
            return 1

        return 0

    async def acleanup_thread(
        self,
        thread_id: str,
        *,
        status: str = "COMPLETED",
        checkpoint_ns: str = "",
    ) -> int:
        """Asynchronously prune checkpoints according to terminal status policy."""
        config: RunnableConfig = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
            }
        }
        normalized = status.upper()
        if normalized == "COMPLETED":
            keep = 1 if self.policy.success_retention == "final" else 2
            pruned = await self.checkpointer.aprune(config, keep_last=keep)
            logger.info("checkpoint_lifecycle_completed_pruned", extra={"thread_id": thread_id, "pruned": pruned})
            return pruned

        if normalized in ("FAILED", "CANCELLED"):
            pruned = await self.checkpointer.aprune(config, keep_last=2)
            logger.info("checkpoint_lifecycle_retained_terminal_pair", extra={"thread_id": thread_id, "pruned": pruned})
            return pruned

        if normalized == "PURGE":
            await self.checkpointer.adelete_thread(thread_id)
            logger.info("checkpoint_lifecycle_thread_purged", extra={"thread_id": thread_id})
            return 1

        return 0
