"""discover_workspace node — deterministic SAS %include graph + closure discovery."""

from __future__ import annotations

from typing import Any

from sas2pyspark.core.events import MigrationEventType
from sas2pyspark.graph.state import MigrationGraphState
from sas2pyspark.services.artifacts import ArtifactReferenceData


def make_discover_node(runtime: Any):
    async def discover_workspace(state: MigrationGraphState) -> dict[str, Any]:
        sas_artifact = state.get("sas_source_artifact")
        if sas_artifact is None:
            return {"error": "no SAS source artifact in state"}
        scope = runtime.scopes.require_by_id(state.get("scope_id", ""))
        session = await runtime.sessions.open(state.get("session_id", ""))
        reference = ArtifactReferenceData(**sas_artifact)
        source = runtime.artifacts.read_reference(scope, reference)
        artifact = runtime.artifacts.create(
            scope=scope,
            data=source,
            media_type=reference.media_type,
            created_by="migration-graph-discovery",
        )
        path = state.get("sas_source_path", "main.sas")
        files = [(path, artifact)]
        for dependency in state.get("sas_dependency_artifacts", []):
            dependency_ref = ArtifactReferenceData(**dependency["artifact"])
            dependency_artifact = runtime.artifacts.create(
                scope=scope,
                data=runtime.artifacts.read_reference(scope, dependency_ref),
                media_type=dependency_ref.media_type,
                created_by="migration-graph-discovery",
                label=dependency["path"],
            )
            files.append((dependency["path"], dependency_artifact))
        discovery, graph = runtime.workspace_discovery.discover(
            session=session,
            scope=scope,
            files=files,
            root_paths=[path],
        )
        await session.append(MigrationEventType.WORKSPACE_DISCOVERED, discovery.to_dict())
        await session.append(MigrationEventType.DEPENDENCY_GRAPH_CREATED, {"graph": graph.to_dict()})
        await session.flush()
        if graph.unresolved or graph.cycles:
            return {
                "state": "HUMAN_REQUIRED",
                "dependency_graph": graph.to_dict(),
                "discovery": discovery.to_dict(),
                "error": "SAS dependency graph has unresolved includes or cycles",
            }
        return {
            "state": "DISCOVERED",
            "dependency_graph": graph.to_dict(),
            "discovery": discovery.to_dict(),
        }
    return discover_workspace
