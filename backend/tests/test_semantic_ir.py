"""Semantic IR tests — IR nodes, source spans, parser version."""

from __future__ import annotations

import pytest

from sas2pyspark.services.semantic_ir import PARSER_VERSION, SemanticIrService, _parse_file

from .conftest import make_scopes, make_session


async def test_parser_version_is_stable():
    assert PARSER_VERSION == "sas-structural-ir-v1"


async def test_parser_handles_multiple_statements_and_quoted_semicolons():
    program, diagnostics = _parse_file(
        "inline.sas",
        'data out; set source; if label="left;right"; run; '
        'proc sql; create table result as select * from out; quit;',
    )

    data_step = next(node for node in program.children if node.kind == "data-step")
    procedure = next(node for node in program.children if node.kind == "procedure")
    assert data_step.name == "out"
    assert [node.name.lower() for node in data_step.children] == ["set", "if"]
    assert procedure.name == "sql"
    assert [node.name.lower() for node in procedure.children] == ["create"]
    assert diagnostics == []


async def test_parser_reports_unterminated_data_step():
    program, diagnostics = _parse_file("broken.sas", "data out; set source;")

    assert program.children[0].kind == "data-step"
    assert [diagnostic.code for diagnostic in diagnostics] == ["UNTERMINATED_BLOCK"]


async def test_semantic_ir_parses_data_step(workbench):
    from sas2pyspark.services.workspace_discovery import DependencyGraph, WorkspaceFileData, DependencyEdgeData
    _, _, _, candidate = make_scopes(workbench)
    session = await make_session(workbench)
    artifact = workbench.artifacts.create(
        scope=candidate, data="data temp;\n  set in;\nrun;",
        media_type="text/x-sas", created_by="test",
    )
    from sas2pyspark.services.artifacts import artifact_reference
    file_data = WorkspaceFileData(path="main.sas", artifact=artifact_reference(artifact), is_sas=True)
    graph = DependencyGraph(
        graph_id="g1", discovery_id="d1", migration_id="migration-a",
        scope_id=candidate.id, root_paths=["main.sas"],
        files=[file_data], edges=[], unresolved=[], cycles=[],
    )
    ir = workbench.semantic_ir.create(session=session, scope=candidate, graph=graph, root_path="main.sas")
    assert ir.parser_version == PARSER_VERSION
    assert len(ir.nodes) > 0
    assert any(n.kind == "data-step" for n in ir.nodes)
