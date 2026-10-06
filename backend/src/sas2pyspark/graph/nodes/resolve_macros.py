"""resolve_macros node — deterministic static macro-resolution evidence."""

from __future__ import annotations

from typing import Any

from sas2pyspark.core.events import MigrationEventType
from sas2pyspark.graph.state import MigrationGraphState
from sas2pyspark.services.workspace_discovery import dependency_graph_from_event


def make_resolve_macros_node(runtime: Any):
    async def resolve_macros(state: MigrationGraphState) -> dict[str, Any]:
        ir_snapshot = state.get("ir_snapshot")
        raw_graph = state.get("dependency_graph")
        if ir_snapshot is None or raw_graph is None:
            return {"state": "HUMAN_REQUIRED", "error": "IR snapshot or dependency graph is unavailable"}
        scope = runtime.scopes.require_by_id(state.get("scope_id", ""))
        session = await runtime.sessions.open(state.get("session_id", ""))
        graph = dependency_graph_from_event(raw_graph)
        root_path = (graph.root_paths or ["main.sas"])[0]
        snapshot = runtime.macro_resolution.create(
            session=session,
            scope=scope,
            graph=graph,
            root_path=root_path,
        )
        await session.append(MigrationEventType.MACRO_RESOLUTION_CREATED, {"resolution": snapshot.to_dict()})
        await session.flush()
        return {
            "state": "MACROS_RESOLVED",
            "macro_resolution": snapshot.to_dict(),
        }
    return resolve_macros
