"""Macro resolution tests — definitions, call sites, unresolved references."""

from __future__ import annotations

import pytest

from sas2pyspark.services.macro_resolution import RESOLVER_VERSION, MacroResolutionService

from .conftest import make_scopes, make_session


async def test_resolver_version_is_stable():
    assert RESOLVER_VERSION == "sas-static-macro-v1"


async def test_macro_resolution_finds_definitions(workbench):
    from sas2pyspark.services.workspace_discovery import DependencyGraph, WorkspaceFileData
    from sas2pyspark.services.artifacts import artifact_reference
    _, _, _, candidate = make_scopes(workbench)
    session = await make_session(workbench)
    artifact = workbench.artifacts.create(
        scope=candidate,
        data="%macro mymacro(x);\n  &x\n%mend;\n%mymacro(1)",
        media_type="text/x-sas", created_by="test",
    )
    file_data = WorkspaceFileData(path="main.sas", artifact=artifact_reference(artifact), is_sas=True)
    graph = DependencyGraph(
        graph_id="g1", discovery_id="d1", migration_id="migration-a",
        scope_id=candidate.id, root_paths=["main.sas"],
        files=[file_data], edges=[], unresolved=[], cycles=[],
    )
    resolution = workbench.macro_resolution.create(
        session=session, scope=candidate, graph=graph, root_path="main.sas",
    )
    assert resolution.resolver_version == RESOLVER_VERSION
    assert len(resolution.definitions) >= 1
    assert resolution.definitions[0].name == "mymacro"


async def test_macro_resolution_reports_unresolved(workbench):
    from sas2pyspark.services.workspace_discovery import DependencyGraph, WorkspaceFileData
    from sas2pyspark.services.artifacts import artifact_reference
    _, _, _, candidate = make_scopes(workbench)
    session = await make_session(workbench)
    artifact = workbench.artifacts.create(
        scope=candidate, data="%unknownmacro(1)",
        media_type="text/x-sas", created_by="test",
    )
    file_data = WorkspaceFileData(path="main.sas", artifact=artifact_reference(artifact), is_sas=True)
    graph = DependencyGraph(
        graph_id="g1", discovery_id="d1", migration_id="migration-a",
        scope_id=candidate.id, root_paths=["main.sas"],
        files=[file_data], edges=[], unresolved=[], cycles=[],
    )
    resolution = workbench.macro_resolution.create(
        session=session, scope=candidate, graph=graph, root_path="main.sas",
    )
    assert len(resolution.unresolved) >= 1
    assert resolution.unresolved[0].reason_code == "NO_DEFINITION"
