"""Tests for ToolDispatchPipeline — four-phase tool execution."""

from __future__ import annotations

from typing import Any

import pytest
import pytest_asyncio

from sas2pyspark.agent.cancellation import ManualAbortSignal
from sas2pyspark.agent.dispatch import ToolCall, ToolDispatchPipeline, ToolOutcomeStatus
from sas2pyspark.agent.registry import ScopedToolRegistry, ToolClassification
from sas2pyspark.core.events import MigrationEventType
from sas2pyspark.services.clock import FixedClock


class MockSession:
    def __init__(self) -> None:
        self.events: list[tuple[MigrationEventType, dict]] = []

    async def append(self, event_type: MigrationEventType, data: dict) -> None:
        self.events.append((event_type, data))


class MockScope:
    @property
    def id(self) -> str:
        return "scope/test"


def _simple_tool():
    def tool(**kwargs):
        return {"result": "ok", "args": kwargs}
    return tool


@pytest.fixture
def registry() -> ScopedToolRegistry:
    r = ScopedToolRegistry()
    r.register_global(
        name="read_file", tool=_simple_tool(), classification=ToolClassification.PARALLEL,
    )
    r.register_global(
        name="write_file", tool=_simple_tool(), classification=ToolClassification.EXCLUSIVE,
    )
    return r


@pytest.fixture
def pipeline(registry: ScopedToolRegistry, fixed_clock: FixedClock) -> ToolDispatchPipeline:
    return ToolDispatchPipeline(registry=registry, clock=fixed_clock)


class TestToolDispatchPipeline:
    @pytest.mark.asyncio
    async def test_successful_dispatch(self, pipeline: ToolDispatchPipeline):
        session = MockSession()
        call = ToolCall(toolName="read_file", args={"path": "/test"}, callId="c1")
        outcome = await pipeline.execute(call=call, scope=MockScope(), session=session)
        assert outcome.status == ToolOutcomeStatus.COMPLETED
        assert outcome.result == {"result": "ok", "args": {"path": "/test"}}
        assert len(session.events) == 2
        assert session.events[0][0] == MigrationEventType.TOOL_STARTED
        assert session.events[1][0] == MigrationEventType.TOOL_COMPLETED

    @pytest.mark.asyncio
    async def test_tool_not_found(self, pipeline: ToolDispatchPipeline):
        session = MockSession()
        call = ToolCall(toolName="nonexistent", args={}, callId="c1")
        outcome = await pipeline.execute(call=call, scope=MockScope(), session=session)
        assert outcome.status == ToolOutcomeStatus.DENIED
        assert len(session.events) == 2
        assert session.events[1][0] == MigrationEventType.TOOL_DENIED

    @pytest.mark.asyncio
    async def test_cancelled_before_dispatch(self, pipeline: ToolDispatchPipeline):
        session = MockSession()
        abort = ManualAbortSignal()
        abort.cancel()
        call = ToolCall(toolName="read_file", args={}, callId="c1")
        outcome = await pipeline.execute(call=call, scope=MockScope(), session=session, abort=abort)
        assert outcome.status == ToolOutcomeStatus.NOT_STARTED
        assert len(session.events) == 2
        assert session.events[1][0] == MigrationEventType.TOOL_NOT_STARTED

    @pytest.mark.asyncio
    async def test_dispatch_error_blocked(self, registry: ScopedToolRegistry, fixed_clock: FixedClock):
        def bad_tool(**kwargs):
            raise RuntimeError("boom")

        registry.register_global(
            name="bad", tool=bad_tool, classification=ToolClassification.PARALLEL,
        )
        pipeline = ToolDispatchPipeline(registry=registry, clock=fixed_clock)
        session = MockSession()
        call = ToolCall(toolName="bad", args={}, callId="c1")
        outcome = await pipeline.execute(call=call, scope=MockScope(), session=session)
        assert outcome.status == ToolOutcomeStatus.BLOCKED
        assert "boom" in outcome.reason
        assert len(session.events) == 2
        assert session.events[1][0] == MigrationEventType.TOOL_BLOCKED

    @pytest.mark.asyncio
    async def test_no_session_no_events(self, pipeline: ToolDispatchPipeline):
        call = ToolCall(toolName="read_file", args={}, callId="c1")
        outcome = await pipeline.execute(call=call, scope=MockScope(), session=None)
        assert outcome.status == ToolOutcomeStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_tool_call_model(self):
        call = ToolCall(toolName="test", args={"x": 1}, callId="c1")
        assert call.tool_name == "test"
        assert call.call_id == "c1"
        assert call.args == {"x": 1}

    @pytest.mark.asyncio
    async def test_tool_outcome_model(self):
        from sas2pyspark.agent.dispatch import ToolOutcome
        outcome = ToolOutcome(callId="c1", result={"ok": True}, status=ToolOutcomeStatus.COMPLETED)
        assert outcome.call_id == "c1"
        assert outcome.status == ToolOutcomeStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_events_hash_chained_order(self, pipeline: ToolDispatchPipeline):
        session = MockSession()
        call = ToolCall(toolName="read_file", args={}, callId="c1")
        await pipeline.execute(call=call, scope=MockScope(), session=session)
        assert session.events[0][0] == MigrationEventType.TOOL_STARTED
        assert session.events[1][0] == MigrationEventType.TOOL_COMPLETED
