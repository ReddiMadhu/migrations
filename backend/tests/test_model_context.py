"""Model context tests — bounded context assembly, projection of durable data."""

from __future__ import annotations

import pytest

from sas2pyspark.services.model_context import (
    MAX_MODEL_CONTEXT_NODES,
    MAX_MODEL_CONTEXT_EXCERPT_BYTES,
    MAX_MODEL_CONTEXT_EVIDENCE,
    CONTEXT_VERSION,
)

from .conftest import make_scopes, make_session


async def test_context_version_is_stable():
    assert CONTEXT_VERSION == "bounded-model-context-v1"


async def test_model_context_assembles_for_work_unit(workbench):
    _, _, _, candidate = make_scopes(workbench)
    session = await make_session(workbench)
    wu = workbench.work_units.create(
        session=session, scope=candidate, sas_module="main.sas",
        dependency_closure=[], work_unit_id="wu-1",
    )
    ctx = workbench.model_context.assemble(
        session=session, work_unit_id="wu-1",
    )
    assert ctx.context_version == CONTEXT_VERSION
    assert ctx.session_id == session.id
    assert ctx.work_unit_id == "wu-1"
    assert ctx.sas_module == "main.sas"
    assert ctx.context_digest is not None


async def test_model_context_rejects_missing_work_unit(workbench):
    from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
    _, _, _, candidate = make_scopes(workbench)
    session = await make_session(workbench)
    with pytest.raises(WorkbenchError) as exc_info:
        workbench.model_context.assemble(session=session, work_unit_id="nonexistent")
    assert exc_info.value.code == WorkbenchErrorCode.INVALID_INPUT


async def test_model_context_limits_are_bounded():
    assert MAX_MODEL_CONTEXT_NODES == 128
    assert MAX_MODEL_CONTEXT_EXCERPT_BYTES == 64 * 1024
    assert MAX_MODEL_CONTEXT_EVIDENCE == 32
