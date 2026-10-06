"""parse_sas node — deterministic SAS parser → semantic IR + source spans."""

from __future__ import annotations

from typing import Any

from sas2pyspark.core.events import MigrationEventType
from sas2pyspark.graph.state import MigrationGraphState
from sas2pyspark.sas.parser import parse_sas
from sas2pyspark.services.workspace_discovery import dependency_graph_from_event


def make_parse_node(runtime: Any):
    async def parse_sas_node(state: MigrationGraphState) -> dict[str, Any]:
        scope_id = state.get("scope_id", "")
        sas_artifact = state.get("sas_source_artifact")
        raw_graph = state.get("dependency_graph")
        if sas_artifact is None or raw_graph is None:
            return {"error": "source artifact or dependency graph is missing"}
        scope = runtime.scopes.require_by_id(scope_id)
        session = await runtime.sessions.open(state.get("session_id", ""))
        graph = dependency_graph_from_event(raw_graph)
        if graph.unresolved or graph.cycles:
            return {"state": "HUMAN_REQUIRED", "error": "SAS dependency graph is incomplete or cyclic"}
        root_path = (graph.root_paths or ["main.sas"])[0]
        snapshot = runtime.semantic_ir.create(
            session=session,
            scope=scope,
            graph=graph,
            root_path=root_path,
        )
        parser_results: dict[str, Any] = {}
        parser_errors: list[dict[str, Any]] = []
        for file_entry in graph.files:
            if not file_entry.is_sas:
                continue
            source_text = runtime.artifacts.read_text(scope, file_entry.artifact)
            parsed = parse_sas(source_text, path=file_entry.path)
            parser_results[file_entry.path] = {
                "nodes": [node.to_dict() for node in parsed.nodes],
                "diagnostics": [diagnostic.to_dict() for diagnostic in parsed.diagnostics],
            }
            parser_errors.extend(
                diagnostic.to_dict()
                for diagnostic in parsed.diagnostics
                if diagnostic.code not in ("UNRESOLVED_MACRO", "UNRESOLVED_MACRO_VARIABLE")
            )
        await session.append(MigrationEventType.SEMANTIC_IR_CREATED, {"ir": snapshot.to_dict()})
        await session.flush()
        if snapshot.diagnostics or parser_errors:
            return {
                "state": "HUMAN_REQUIRED",
                "ir_snapshot": snapshot.to_dict(),
                "parser_results": parser_results,
                "error": "SAS source contains dynamic or unsupported constructs requiring review",
            }
        return {
            "state": "PARSED",
            "ir_snapshot": snapshot.to_dict(),
            "parser_results": parser_results,
        }
    return parse_sas_node
