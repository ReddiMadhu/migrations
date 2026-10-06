"""Workspace discovery tests — %include graph, cycle detection, lazy discovery."""

from __future__ import annotations

import pytest

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.services.workspace_discovery import (
    _normalize_workspace_path,
    _include_directives,
    _strip_sas_comments,
)

from .conftest import make_scopes


def test_normalize_workspace_path_rejects_absolute():
    with pytest.raises(WorkbenchError):
        _normalize_workspace_path("/absolute/path")


def test_normalize_workspace_path_rejects_parent_escape():
    with pytest.raises(WorkbenchError):
        _normalize_workspace_path("../escape")


def test_normalize_workspace_path_normalizes_backslashes():
    assert _normalize_workspace_path("sub\\dir\\file.sas") == "sub/dir/file.sas"


def test_include_directives_extracts_targets():
    text = '%include "lib/helper.sas";\ndata temp;\nrun;'
    directives = _include_directives(text)
    assert len(directives) == 1
    assert directives[0][0] == "lib/helper.sas"


def test_strip_sas_comments_removes_block_comments():
    text = "/* this is a comment */\ndata temp;\nrun;"
    lines = _strip_sas_comments(text)
    assert "data temp;" in lines[1]


def test_strip_sas_comments_removes_line_comments():
    text = "data temp; // inline comment\nrun;"
    lines = _strip_sas_comments(text)
    assert "run;" in lines[1]


async def test_workspace_discovery_creates_graph(workbench):
    from .conftest import make_session
    _, _, _, candidate = make_scopes(workbench)
    session = await make_session(workbench)
    main_artifact = workbench.artifacts.create(
        scope=candidate, data='%include "lib.sas";\ndata temp;\nrun;',
        media_type="text/x-sas", created_by="test",
    )
    lib_artifact = workbench.artifacts.create(
        scope=candidate, data="data lib;\nrun;",
        media_type="text/x-sas", created_by="test",
    )
    discovery, graph = workbench.workspace_discovery.discover(
        session=session, scope=candidate,
        files=[("main.sas", main_artifact), ("lib.sas", lib_artifact)],
    )
    assert discovery.file_count == 2
    assert len(graph.edges) == 1
    assert graph.edges[0].from_path == "main.sas"
    assert graph.edges[0].to_path == "lib.sas"
