"""P0-3: Production clock + FixedClock overflow."""

from __future__ import annotations

import pytest
import pytest_asyncio

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.services.clock import FixedClock, SystemClock
from sas2pyspark.workbench import WorkbenchConfig, create_workbench


def test_fixed_clock_seconds_wrap_past_59():
    clock = FixedClock()
    for _ in range(59):
        clock.now()
    t60 = clock.now()
    assert t60 == "2026-01-01T00:01:00.000Z"
    for _ in range(64):
        clock.now()
    t125 = clock.now()
    assert t125 == "2026-01-01T00:02:05.000Z"


def test_fixed_clock_first_tick():
    clock = FixedClock()
    t1 = clock.now()
    assert t1 == "2026-01-01T00:00:01.000Z"


def test_fixed_clock_hours_wrap():
    clock = FixedClock()
    for _ in range(3600):
        clock.now()
    t3601 = clock.now()
    assert t3601 == "2026-01-01T01:00:01.000Z"


def test_fixed_clock_uuid_wraps():
    clock = FixedClock()
    u1 = clock.uuid()
    assert u1 == "00000000-0000-4000-8000-000000000001"
    for _ in range(100):
        clock.uuid()
    u102 = clock.uuid()
    assert u102 == "00000000-0000-4000-8000-000000000102"


@pytest.mark.asyncio
async def test_create_app_production_uses_system_clock(test_dir):
    from langgraph.checkpoint.memory import MemorySaver
    from sas2pyspark.api.app import WorkbenchApiConfig, create_app
    app = await create_app(WorkbenchApiConfig(root_dir=str(test_dir)), environment="production", checkpointer=MemorySaver())
    try:
        assert isinstance(app.state.runtime.clock, SystemClock)
    finally:
        await app.state.runtime.dispose()


@pytest.mark.asyncio
async def test_create_app_production_rejects_fixed_clock(test_dir):
    from sas2pyspark.api.app import WorkbenchApiConfig, create_app
    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=FixedClock(), environment="development"))
    try:
        with pytest.raises(WorkbenchError) as exc_info:
            await create_app(WorkbenchApiConfig(root_dir=str(test_dir)), runtime=runtime, environment="production")
        assert exc_info.value.code == WorkbenchErrorCode.CONFIG_ERROR
    finally:
        await runtime.dispose()


@pytest.mark.asyncio
async def test_create_workbench_production_rejects_fixed_clock(test_dir):
    with pytest.raises(WorkbenchError) as exc_info:
        await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=FixedClock(), environment="production"))
    assert exc_info.value.code == WorkbenchErrorCode.CONFIG_ERROR


@pytest.mark.asyncio
async def test_create_workbench_production_uses_system_clock(test_dir):
    from langgraph.checkpoint.memory import MemorySaver
    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), environment="production", checkpointer=MemorySaver()))
    try:
        assert isinstance(runtime.clock, SystemClock)
    finally:
        await runtime.dispose()


@pytest.mark.asyncio
async def test_create_app_development_allows_fixed_clock(test_dir):
    from sas2pyspark.api.app import WorkbenchApiConfig, create_app
    app = await create_app(WorkbenchApiConfig(root_dir=str(test_dir)), environment="development")
    try:
        assert isinstance(app.state.runtime.clock, FixedClock)
    finally:
        await app.state.runtime.dispose()


@pytest.mark.asyncio
async def test_create_app_defaults_to_development(test_dir):
    from sas2pyspark.api.app import WorkbenchApiConfig, create_app
    app = await create_app(WorkbenchApiConfig(root_dir=str(test_dir)))
    try:
        assert isinstance(app.state.runtime.clock, FixedClock)
    finally:
        await app.state.runtime.dispose()
