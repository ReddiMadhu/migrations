"""create_work_unit node — deterministic bounded repair context from IR + closure."""

from __future__ import annotations

from typing import Any

from sas2pyspark.graph.state import MigrationGraphState
from sas2pyspark.services.workspace_discovery import dependency_closure, dependency_graph_from_event


def make_work_unit_node(runtime: Any):
    async def create_work_unit(state: MigrationGraphState) -> dict[str, Any]:
        raw_graph = state.get("dependency_graph")
        ir = state.get("ir_snapshot") or {}
        macros = state.get("macro_resolution") or {}
        if raw_graph is None or not ir or not macros:
            return {"state": "HUMAN_REQUIRED", "error": "workspace, IR, or macro evidence is unavailable"}
        session = await runtime.sessions.open(state.get("session_id", ""))
        scope = runtime.scopes.require_by_id(state.get("scope_id", ""))
        graph = dependency_graph_from_event(raw_graph)
        root_path = (graph.root_paths or ["main.sas"])[0]
        closure_paths = dependency_closure(graph, root_path)["paths"]
        dependencies = [path for path in closure_paths if path != root_path]
        selected_nodes = [
            node["nodeId"]
            for node in ir.get("nodes", [])
            if node.get("kind") in ("data-step", "procedure", "macro", "operation")
            and node.get("span", {}).get("path") in closure_paths
        ]
        work_unit = runtime.work_units.create(
            session=session,
            scope=scope,
            sas_module=root_path,
            dependency_closure=dependencies,
            ir_node_ids=selected_nodes[:512],
            ir_snapshot_id=ir.get("irId"),
            macro_resolution_id=macros.get("resolutionId"),
            input_snapshot_id=state.get("input_snapshot_id"),
            required_skills=["sas-to-pyspark"],
        )
        return {
            "state": "WORK_UNIT_CREATED",
            "work_unit": work_unit.to_dict(),
        }
    return create_work_unit
