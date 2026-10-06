"""FastAPI application — migration workbench REST API + SSE streaming.

Authentication: bearer token (configurable via WorkbenchApiConfig).
Static file serving: built React bundle from backend/static/.
Graceful shutdown: SIGTERM handler drains in-flight nodes.
"""

from __future__ import annotations

import asyncio
import os
import signal
import sys
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from sas2pyspark.api.routes import create_router
from sas2pyspark.api.sse import create_sse_router
from sas2pyspark.workbench import WorkbenchConfig, WorkbenchRuntime, create_workbench


class WorkbenchApiConfig:
    __slots__ = ("root_dir", "auth_token", "cors_origins")

    def __init__(self, *, root_dir: str, auth_token: str | None = None, cors_origins: tuple[str, ...] = ()) -> None:
        self.root_dir = root_dir
        self.auth_token = auth_token
        self.cors_origins = cors_origins


class ShutdownCoordinator:
    __slots__ = ("drain_event", "in_flight", "_lock")

    def __init__(self) -> None:
        self.drain_event = asyncio.Event()
        self.in_flight: set[asyncio.Task] = set()
        self._lock = asyncio.Lock()

    async def track(self, task: asyncio.Task) -> None:
        async with self._lock:
            self.in_flight.add(task)

    async def untrack(self, task: asyncio.Task) -> None:
        async with self._lock:
            self.in_flight.discard(task)

    async def drain(self, timeout: float = 15.0) -> None:
        self.drain_event.set()
        async with self._lock:
            tasks = list(self.in_flight)
        if not tasks:
            return
        try:
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=timeout)
        except asyncio.TimeoutError:
            for t in tasks:
                t.cancel()


async def create_app(api_config: WorkbenchApiConfig, runtime: WorkbenchRuntime | None = None, *, environment: str | None = None, checkpointer: Any = None) -> FastAPI:
    env = environment or "development"
    model_provider = os.environ.get("WORKBENCH_MODEL_PROVIDER")
    model_name = os.environ.get("WORKBENCH_MODEL_NAME")
    model_kwargs: dict[str, Any] | None = None
    if model_provider == "azure_openai":
        model_kwargs = {
            "azure_endpoint": os.environ.get("AZURE_OPENAI_ENDPOINT", ""),
            "api_key": os.environ.get("AZURE_OPENAI_API_KEY", ""),
            "api_version": os.environ.get("AZURE_OPENAI_API_VERSION", "2024-10-21"),
        }
    if runtime is None:
        if env == "production":
            from sas2pyspark.services.clock import SystemClock
            runtime = await create_workbench(WorkbenchConfig(root_dir=api_config.root_dir, clock=SystemClock(), environment=env, checkpointer=checkpointer, model_provider=model_provider, model_name=model_name, model_kwargs=model_kwargs))
        else:
            from sas2pyspark.services.clock import FixedClock
            runtime = await create_workbench(WorkbenchConfig(root_dir=api_config.root_dir, clock=FixedClock(), environment=env, checkpointer=checkpointer, model_provider=model_provider, model_name=model_name, model_kwargs=model_kwargs))
    elif env == "production":
        from sas2pyspark.services.clock import FixedClock
        from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
        if isinstance(runtime.clock, FixedClock):
            raise WorkbenchError(WorkbenchErrorCode.CONFIG_ERROR, "production requires SystemClock; FixedClock is non-deterministic")

    from sas2pyspark.graph.migration_graph import build_migration_graph
    graph = build_migration_graph(runtime)
    shutdown = ShutdownCoordinator()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if sys.platform != "win32":
            loop = asyncio.get_event_loop()

            def _handle_sigterm() -> None:
                asyncio.create_task(shutdown.drain(timeout=15.0))

            loop.add_signal_handler(signal.SIGTERM, _handle_sigterm)
        yield
        await shutdown.drain(timeout=15.0)
        await runtime.dispose()

    app = FastAPI(
        title="SAS→PySpark Migration Workbench",
        version="1.0.0",
        lifespan=lifespan,
    )

    cors_origins = list(api_config.cors_origins) if api_config.cors_origins else ["*"]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(create_router(runtime, graph, api_config))
    app.include_router(create_sse_router(runtime, graph, api_config))

    app.state.runtime = runtime
    app.state.graph = graph
    app.state.shutdown = shutdown

    return app
