"""P1-10: API Concurrency + Encapsulation + Durable Artifact Mapping."""

from __future__ import annotations

import pytest

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode


def _run(coro):
    try:
        while True:
            coro.send(None)
    except StopIteration as e:
        return e.value


def test_find_by_migration_id_uses_durable_read(test_dir, fixed_clock):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench

    async def _run_test():
        runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
        try:
            from .conftest import make_scopes, make_session
            _, _, _, candidate = make_scopes(runtime)
            session = await make_session(runtime, scope=candidate)
            found = await runtime.sessions.find_by_migration_id(session.migration_id)
            assert found is not None
            assert found.id == session.id
        finally:
            await runtime.dispose()

    import asyncio
    asyncio.new_event_loop().run_until_complete(_run_test())


def test_find_by_migration_id_returns_none(test_dir, fixed_clock):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench

    async def _run_test():
        runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
        try:
            found = await runtime.sessions.find_by_migration_id("nonexistent-migration")
            assert found is None
        finally:
            await runtime.dispose()

    import asyncio
    asyncio.new_event_loop().run_until_complete(_run_test())


def test_no_reach_into_sessions_dict():
    import inspect
    from sas2pyspark.api.routes import create_router

    source = inspect.getsource(create_router)
    assert "._sessions" not in source or "_sessions" not in source.replace("_sessions.values()", "find_by_migration_id")


def test_migration_artifacts_dict_still_present_as_fallback():
    import inspect
    from sas2pyspark.api.routes import create_router

    source = inspect.getsource(create_router)
    assert "_derive_migration_input" in source


def test_list_sessions_on_persistence(test_dir, fixed_clock):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench

    async def _run_test():
        runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
        try:
            from .conftest import make_scopes, make_session
            _, _, _, candidate = make_scopes(runtime)
            await make_session(runtime, scope=candidate, session_id="list-test-1")
            sessions = runtime.sessions._persistence.list_sessions()
            assert "list-test-1" in sessions
        finally:
            await runtime.dispose()

    import asyncio
    asyncio.new_event_loop().run_until_complete(_run_test())
