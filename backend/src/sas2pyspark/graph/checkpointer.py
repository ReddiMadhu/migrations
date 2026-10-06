"""LangGraph checkpoint selection — dev uses in-memory state, production requires durable storage."""

from __future__ import annotations

import os
from typing import Any

from langgraph.checkpoint.memory import MemorySaver

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode


def _make_sqlite_checkpointer(checkpoint_path: str, *, fail_closed: bool = False) -> Any:
    try:
        import sqlite3
        from langgraph.checkpoint.sqlite import SqliteSaver
    except ModuleNotFoundError:
        if fail_closed:
            raise WorkbenchError(
                WorkbenchErrorCode.CONFIG_ERROR,
                "production requires a durable checkpointer but langgraph sqlite module is not installed",
                {"checkpointPath": checkpoint_path},
            )
        return MemorySaver()

    try:
        conn = sqlite3.connect(checkpoint_path)
        return SqliteSaver(conn=conn)
    except TypeError:
        try:
            return SqliteSaver(checkpoint_path)
        except Exception:
            if fail_closed:
                raise WorkbenchError(
                    WorkbenchErrorCode.CONFIG_ERROR,
                    "production requires a durable checkpointer but SqliteSaver could not be constructed",
                    {"checkpointPath": checkpoint_path},
                )
            return MemorySaver()
    except Exception:
        if fail_closed:
            raise WorkbenchError(
                WorkbenchErrorCode.CONFIG_ERROR,
                "production requires a durable checkpointer but sqlite connection failed",
                {"checkpointPath": checkpoint_path},
            )
        return MemorySaver()


def _make_cosmos_checkpointer(
    *,
    fail_closed: bool = False,
    auto_create: bool = False,
    blob_store: Any = None,
) -> Any:
    """Create a Cosmos DB checkpoint saver.

    Reads connection details from environment variables:
    - AZURE_COSMOS_ENDPOINT
    - AZURE_COSMOS_KEY
    - AZURE_COSMOS_DATABASE (default: sas2pyspark)
    """
    try:
        from sas2pyspark.graph.cosmos_client_provider import CosmosClientProvider
        from sas2pyspark.graph.cosmos_checkpointer import CosmosDbSaver
    except ImportError:
        if fail_closed:
            raise WorkbenchError(
                WorkbenchErrorCode.CONFIG_ERROR,
                "cosmos checkpointer requires azure-cosmos package but it is not installed",
            )
        return MemorySaver()

    try:
        provider = CosmosClientProvider(auto_create=auto_create)
        return CosmosDbSaver(provider, blob_store=blob_store)
    except WorkbenchError:
        if fail_closed:
            raise
        return MemorySaver()
    except Exception as exc:
        if fail_closed:
            raise WorkbenchError(
                WorkbenchErrorCode.CONFIG_ERROR,
                "cosmos checkpointer could not be constructed",
                {"error": str(exc)},
            ) from exc
        return MemorySaver()


def resolve_checkpointer(
    checkpointer: Any = None,
    *,
    mode: str | None = None,
    checkpoint_path: str | None = None,
    environment: str = "development",
    blob_store: Any = None,
) -> Any:
    """Resolve a graph checkpointer.

    Development mode always uses ``MemorySaver`` for local test runs.
    Production mode requires a durable SQLite-backed saver; if the sqlite
    checkpoint module is unavailable or the connection fails, the call
    raises ``WorkbenchError(CONFIG_ERROR)`` instead of silently falling
    back to an in-memory saver.
    """
    if checkpointer is not None:
        return checkpointer

    normalized_mode = (mode or os.getenv("SAS2PYSPARK_CHECKPOINTER_MODE") or "development").lower()
    is_production = normalized_mode == "production" or environment == "production"
    is_cosmos = normalized_mode in ("cosmos", "azure_cosmos")

    if is_cosmos:
        auto_create = not is_production and environment != "production"
        return _make_cosmos_checkpointer(
            fail_closed=True,
            auto_create=auto_create,
            blob_store=blob_store,
        )

    if is_production:
        durable_path = checkpoint_path or os.getenv("SAS2PYSPARK_CHECKPOINT_PATH")
        if durable_path is None:
            durable_path = os.path.join(os.getcwd(), "sessions", "migration-checkpoints.sqlite")
        return _make_sqlite_checkpointer(durable_path, fail_closed=True)
    return MemorySaver()
