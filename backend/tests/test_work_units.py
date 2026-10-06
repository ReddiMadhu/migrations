"""Work unit tests — bounded repair context, create/update/close, transitions."""

from __future__ import annotations

import pytest

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.core.events import MigrationEventType

from .conftest import make_scopes, make_session


async def test_create_work_unit(workbench):
    _, _, _, candidate = make_scopes(workbench)
    session = await make_session(workbench)
    wu = workbench.work_units.create(
        session=session, scope=candidate, sas_module="main.sas",
        dependency_closure=["lib.sas"], work_unit_id="wu-1",
    )
    assert wu.work_unit_id == "wu-1"
    assert wu.status == "OPEN"
    assert wu.sas_module == "main.sas"


async def test_update_work_unit_status(workbench):
    _, _, _, candidate = make_scopes(workbench)
    session = await make_session(workbench)
    workbench.work_units.create(
        session=session, scope=candidate, sas_module="main.sas",
        dependency_closure=[], work_unit_id="wu-1",
    )
    updated = workbench.work_units.update(session=session, work_unit_id="wu-1", status="ACTIVE")
    assert updated.status == "ACTIVE"


async def test_close_work_unit(workbench):
    _, _, _, candidate = make_scopes(workbench)
    session = await make_session(workbench)
    workbench.work_units.create(
        session=session, scope=candidate, sas_module="main.sas",
        dependency_closure=[], work_unit_id="wu-1",
    )
    closed = workbench.work_units.close(session=session, work_unit_id="wu-1", reason="done")
    assert closed.status == "CLOSED"
    assert closed.closed_reason == "done"


async def test_closed_work_unit_cannot_be_updated(workbench):
    _, _, _, candidate = make_scopes(workbench)
    session = await make_session(workbench)
    workbench.work_units.create(
        session=session, scope=candidate, sas_module="main.sas",
        dependency_closure=[], work_unit_id="wu-1",
    )
    workbench.work_units.close(session=session, work_unit_id="wu-1", reason="done")
    with pytest.raises(WorkbenchError) as exc_info:
        workbench.work_units.update(session=session, work_unit_id="wu-1", status="ACTIVE")
    assert exc_info.value.code == WorkbenchErrorCode.INVALID_INPUT


async def test_duplicate_work_unit_rejected(workbench):
    _, _, _, candidate = make_scopes(workbench)
    session = await make_session(workbench)
    workbench.work_units.create(
        session=session, scope=candidate, sas_module="main.sas",
        dependency_closure=[], work_unit_id="wu-1",
    )
    with pytest.raises(WorkbenchError) as exc_info:
        workbench.work_units.create(
            session=session, scope=candidate, sas_module="main.sas",
            dependency_closure=[], work_unit_id="wu-1",
        )
    assert exc_info.value.code == WorkbenchErrorCode.INVALID_INPUT
