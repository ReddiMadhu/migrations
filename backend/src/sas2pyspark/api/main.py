"""Uvicorn-compatible ASGI entry point."""

from __future__ import annotations

import os

from sas2pyspark.api.app import WorkbenchApiConfig, create_app
from starlette.types import ASGIApp, Receive, Scope, Send


class LazyWorkbenchApp:
    """Create the async workbench inside the server's ASGI event loop."""

    def __init__(self) -> None:
        self._app: ASGIApp | None = None

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        inner_app = self._app
        if inner_app is None:
            inner_app = await create_app(
                WorkbenchApiConfig(
                    root_dir=os.environ.get("WORKBENCH_ROOT", ".workbench"),
                    auth_token=os.environ.get("WORKBENCH_AUTH_TOKEN"),
                ),
                environment=os.environ.get("WORKBENCH_ENV", "development"),
            )
            self._app = inner_app
        await inner_app(scope, receive, send)


app = LazyWorkbenchApp()
