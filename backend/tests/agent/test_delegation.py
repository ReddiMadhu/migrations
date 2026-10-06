"""Tests for DelegationController — multi-agent delegation with depth budget."""

from __future__ import annotations

import pytest

from sas2pyspark.agent.cancellation import ManualAbortSignal
from sas2pyspark.agent.delegation import DelegationController, DepthExceededError
from sas2pyspark.agent.dispatch import ToolCall, ToolDispatchPipeline
from sas2pyspark.agent.driver import AgentDriver, AgentResult, StepRunner, TurnRunner
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


@pytest.fixture
def pipeline(fixed_clock: FixedClock) -> ToolDispatchPipeline:
    registry = ScopedToolRegistry()
    registry.register_global(
        name="echo", tool=lambda **kw: kw, classification=ToolClassification.PARALLEL,
    )
    return ToolDispatchPipeline(registry=registry, clock=fixed_clock)


def _make_driver_factory(pipeline: ToolDispatchPipeline, fixed_clock: FixedClock):
    def factory(*, depth: int, depth_cap: int) -> AgentDriver:
        step_runner = StepRunner(dispatch=pipeline)
        turn_runner = TurnRunner(step_runner=step_runner)
        return AgentDriver(
            turn_runner=turn_runner, clock=fixed_clock, depth=depth, depth_cap=depth_cap,
        )
    return factory


class TestDelegationController:
    @pytest.mark.asyncio
    async def test_delegate_child_depth_increments(self, pipeline: ToolDispatchPipeline, fixed_clock: FixedClock):
        factory = _make_driver_factory(pipeline, fixed_clock)
        parent = factory(depth=0, depth_cap=3)
        controller = DelegationController(driver_factory=factory, depth_cap=3)
        session = MockSession()
        result = await controller.delegate(
            parent=parent, scope=MockScope(), session=session, inbox=[],
        )
        assert result.advisory is True
        started = next(e for e in session.events if e[0] == MigrationEventType.DELEGATION_STARTED)
        assert started[1]["childDepth"] == 1
        assert started[1]["parentDepth"] == 0

    @pytest.mark.asyncio
    async def test_delegate_depth_exceeded(self, pipeline: ToolDispatchPipeline, fixed_clock: FixedClock):
        factory = _make_driver_factory(pipeline, fixed_clock)
        parent = factory(depth=3, depth_cap=3)
        controller = DelegationController(driver_factory=factory, depth_cap=3)
        session = MockSession()
        with pytest.raises(DepthExceededError) as exc_info:
            await controller.delegate(
                parent=parent, scope=MockScope(), session=session, inbox=[],
            )
        assert exc_info.value.depth == 4
        assert exc_info.value.cap == 3

    @pytest.mark.asyncio
    async def test_delegate_appended_events(self, pipeline: ToolDispatchPipeline, fixed_clock: FixedClock):
        factory = _make_driver_factory(pipeline, fixed_clock)
        parent = factory(depth=0, depth_cap=3)
        controller = DelegationController(driver_factory=factory, depth_cap=3)
        session = MockSession()
        await controller.delegate(
            parent=parent, scope=MockScope(), session=session, inbox=[],
        )
        types = [e[0] for e in session.events]
        assert MigrationEventType.DELEGATION_STARTED in types
        assert MigrationEventType.DELEGATION_COMPLETED in types

    @pytest.mark.asyncio
    async def test_delegate_with_tools(self, pipeline: ToolDispatchPipeline, fixed_clock: FixedClock):
        factory = _make_driver_factory(pipeline, fixed_clock)
        parent = factory(depth=0, depth_cap=3)
        controller = DelegationController(driver_factory=factory, depth_cap=3)
        session = MockSession()
        inbox = [ToolCall(toolName="echo", args={"x": 1}, callId="c1")]
        result = await controller.delegate(
            parent=parent, scope=MockScope(), session=session, inbox=inbox,
        )
        assert result.step_count == 1

    @pytest.mark.asyncio
    async def test_delegate_cancellation(self, pipeline: ToolDispatchPipeline, fixed_clock: FixedClock):
        factory = _make_driver_factory(pipeline, fixed_clock)
        parent = factory(depth=0, depth_cap=3)
        controller = DelegationController(driver_factory=factory, depth_cap=3)
        session = MockSession()
        abort = ManualAbortSignal()
        abort.cancel()
        result = await controller.delegate(
            parent=parent, scope=MockScope(), session=session, inbox=[], abort=abort,
        )
        assert result.interrupted is True
