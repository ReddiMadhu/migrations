"""Cosmos DB client provider — singleton sync + async client pair with lifecycle management.

The provider creates and manages Azure Cosmos DB clients for the workbench.
Both sync and async clients connect to the same account and are created at
application startup, then closed on dispose.

Usage::

    provider = CosmosClientProvider(
        endpoint="https://account.documents.azure.com:443/",
        credential="<api-key>",
        database_name="sas2pyspark",
    )
    container = provider.get_container("langgraph_checkpoints")
    # ... use container ...
    provider.dispose()  # or await provider.adispose()
"""

from __future__ import annotations

import logging
import os
from typing import Any

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode

logger = logging.getLogger(__name__)


class CosmosClientProvider:
    """Manages singleton sync + async Cosmos DB clients and container references.

    The provider is intentionally thin: it owns client lifecycle and container
    lookup, but has no knowledge of checkpoints, sessions, or serialization.
    """

    __slots__ = (
        "_endpoint",
        "_credential",
        "_database_name",
        "_sync_client",
        "_async_client",
        "_sync_database",
        "_async_database",
        "_auto_create",
        "_disposed",
    )

    def __init__(
        self,
        *,
        endpoint: str | None = None,
        credential: str | None = None,
        database_name: str | None = None,
        auto_create: bool = False,
    ) -> None:
        self._endpoint = endpoint or os.environ.get("AZURE_COSMOS_ENDPOINT", "")
        self._credential = credential or os.environ.get("AZURE_COSMOS_KEY", "")
        self._database_name = database_name or os.environ.get("AZURE_COSMOS_DATABASE", "sas2pyspark")
        self._auto_create = auto_create
        self._sync_client: Any = None
        self._async_client: Any = None
        self._sync_database: Any = None
        self._async_database: Any = None
        self._disposed = False

        if not self._endpoint:
            raise WorkbenchError(
                WorkbenchErrorCode.CONFIG_ERROR,
                "AZURE_COSMOS_ENDPOINT is required for Cosmos DB checkpointer",
                {"endpoint": self._endpoint},
            )
        if not self._credential:
            raise WorkbenchError(
                WorkbenchErrorCode.CONFIG_ERROR,
                "AZURE_COSMOS_KEY is required for Cosmos DB checkpointer",
            )

    def _ensure_sync_client(self) -> None:
        """Lazily create the sync Cosmos client on first use."""
        if self._sync_client is not None:
            return
        try:
            from azure.cosmos import CosmosClient
        except ImportError as exc:
            raise WorkbenchError(
                WorkbenchErrorCode.CONFIG_ERROR,
                "azure-cosmos package is required for Cosmos DB checkpointer but is not installed",
            ) from exc

        self._sync_client = CosmosClient(
            url=self._endpoint,
            credential=self._credential,
        )
        if self._auto_create:
            self._sync_database = self._sync_client.create_database_if_not_exists(
                id=self._database_name,
            )
        else:
            self._sync_database = self._sync_client.get_database_client(
                database=self._database_name,
            )
        logger.info(
            "cosmos_sync_client_created",
            extra={"database": self._database_name},
        )

    async def _ensure_async_client(self) -> None:
        """Lazily create the async Cosmos client on first use."""
        if self._async_client is not None:
            return
        try:
            from azure.cosmos.aio import CosmosClient as AsyncCosmosClient
        except ImportError as exc:
            raise WorkbenchError(
                WorkbenchErrorCode.CONFIG_ERROR,
                "azure-cosmos[aio] package is required for async Cosmos DB operations",
            ) from exc

        self._async_client = AsyncCosmosClient(
            url=self._endpoint,
            credential=self._credential,
        )
        if self._auto_create:
            self._async_database = await self._async_client.create_database_if_not_exists(
                id=self._database_name,
            )
        else:
            self._async_database = self._async_client.get_database_client(
                database=self._database_name,
            )
        logger.info(
            "cosmos_async_client_created",
            extra={"database": self._database_name},
        )

    def get_container(self, container_name: str) -> Any:
        """Get a sync container client, creating the container in dev mode if needed."""
        self._ensure_sync_client()
        if self._auto_create:
            return self._sync_database.create_container_if_not_exists(
                id=container_name,
                partition_key={"paths": ["/thread_id"], "kind": "Hash"},
            )
        return self._sync_database.get_container_client(container=container_name)

    async def aget_container(self, container_name: str) -> Any:
        """Get an async container client, creating the container in dev mode if needed."""
        await self._ensure_async_client()
        if self._auto_create:
            return await self._async_database.create_container_if_not_exists(
                id=container_name,
                partition_key={"paths": ["/thread_id"], "kind": "Hash"},
            )
        return self._async_database.get_container_client(container=container_name)

    def dispose(self) -> None:
        """Close the sync client and release resources."""
        if self._disposed:
            return
        self._disposed = True
        if self._sync_client is not None:
            try:
                self._sync_client.close()
            except Exception:
                logger.warning("cosmos_sync_client_close_error", exc_info=True)
            self._sync_client = None
        logger.info("cosmos_client_provider_disposed")

    async def adispose(self) -> None:
        """Close both sync and async clients and release resources."""
        if self._disposed:
            return
        self._disposed = True
        if self._async_client is not None:
            try:
                await self._async_client.close()
            except Exception:
                logger.warning("cosmos_async_client_close_error", exc_info=True)
            self._async_client = None
        if self._sync_client is not None:
            try:
                self._sync_client.close()
            except Exception:
                logger.warning("cosmos_sync_client_close_error", exc_info=True)
            self._sync_client = None
        logger.info("cosmos_client_provider_disposed")
