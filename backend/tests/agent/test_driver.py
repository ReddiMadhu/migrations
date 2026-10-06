"""Tests for AgentDriver — phase machine, turn runner, step runner."""

from __future__ import annotations

import pytest

from sas2pyspark.agent.cancellation import ManualAbortSignal
from sas2pyspark.agent.dispatch import ToolCall, ToolDispatchPipeline, ToolOutcomeStatus
from sas2pyspark.agent.driver import AgentDriver, AgentResult, DriverPhase, StepRunner, TurnRunner
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


def _echo_tool():
    def tool(**kwargs):
        return {"echo": kwargs}
    return tool


@pytest.fixture
def pipeline(fixed_clock: FixedClock) -> ToolDispatchPipeline:
    registry = ScopedToolRegistry()
    registry.register_global(
        name="echo", tool=_echo_tool(), classification=ToolClassification.PARALLEL,
    )
    return ToolDispatchPipeline(registry=registry, clock=fixed_clock)


@pytest.fixture
def driver(pipeline: ToolDispatchPipeline, fixed_clock: FixedClock) -> AgentDriver:
    step_runner = StepRunner(dispatch=pipeline)
    turn_runner = TurnRunner(step_runner=step_runner)
    return AgentDriver(turn_runner=turn_runner, clock=fixed_clock)


class TestAgentDriver:
    @pytest.mark.asyncio
    async def test_kick_executes_inbox(self, driver: AgentDriver):
        session = MockSession()
        inbox = [ToolCall(toolName="echo", args={"x": 1}, callId="c1")]
        result = await driver.kick(session=session, scope=MockScope(), inbox=inbox)
        assert result.interrupted is False
        assert result.budget_exceeded is False
        assert result.step_count == 1
        assert result.advisory is True

    @pytest.mark.asyncio
    async def test_kick_appends_events(self, driver: AgentDriver):
        session = MockSession()
        inbox = [ToolCall(toolName="echo", args={}, callId="c1")]
        await driver.kick(session=session, scope=MockScope(), inbox=inbox)
        types = [e[0] for e in session.events]
        assert MigrationEventType.AGENT_STARTED in types
        assert MigrationEventType.AGENT_TURN_STARTED in types
        assert MigrationEventType.AGENT_TURN_ENDED in types
        assert MigrationEventType.AGENT_IDLE in types

    @pytest.mark.asyncio
    async def test_kick_empty_inbox(self, driver: AgentDriver):
        session = MockSession()
        result = await driver.kick(session=session, scope=MockScope(), inbox=[])
        assert result.step_count == 0
        assert result.interrupted is False

    @pytest.mark.asyncio
    async def test_kick_cancellation(self, driver: AgentDriver):
        session = MockSession()
        abort = ManualAbortSignal()
        abort.cancel()
        inbox = [ToolCall(toolName="echo", args={}, callId="c1")]
        result = await driver.kick(
            session=session, scope=MockScope(), inbox=inbox, abort=abort,
        )
        assert result.interrupted is True

    @pytest.mark.asyncio
    async def test_kick_multiple_tools(self, driver: AgentDriver):
        session = MockSession()
        inbox = [
            ToolCall(toolName="echo", args={"i": 1}, callId="c1"),
            ToolCall(toolName="echo", args={"i": 2}, callId="c2"),
            ToolCall(toolName="echo", args={"i": 3}, callId="c3"),
        ]
        result = await driver.kick(session=session, scope=MockScope(), inbox=inbox)
        assert result.step_count == 3

    @pytest.mark.asyncio
    async def test_driver_phase_enum(self):
        assert DriverPhase.IDLE == "idle"
        assert DriverPhase.MAINTENANCE == "maintenance"
        assert DriverPhase.RUNNING == "running"

    @pytest.mark.asyncio
    async def test_agent_result_model(self):
        result = AgentResult(content="test", turnCount=2, stepCount=5)
        assert result.turn_count == 2
        assert result.step_count == 5
        assert result.advisory is True

    @pytest.mark.asyncio
    async def test_turn_end_reason_completed(self, driver: AgentDriver):
        session = MockSession()
        inbox = [ToolCall(toolName="echo", args={}, callId="c1")]
        await driver.kick(session=session, scope=MockScope(), inbox=inbox)
        turn_end = next(
            e for e in session.events if e[0] == MigrationEventType.AGENT_TURN_ENDED
        )
        assert turn_end[1]["reason"] == "completed"

    @pytest.mark.asyncio
    async def test_turn_end_reason_interrupted(self, driver: AgentDriver):
        session = MockSession()
        abort = ManualAbortSignal()
        abort.cancel()
        inbox = [ToolCall(toolName="echo", args={}, callId="c1")]
        await driver.kick(session=session, scope=MockScope(), inbox=inbox, abort=abort)
        turn_end = next(
            e for e in session.events if e[0] == MigrationEventType.AGENT_TURN_ENDED
        )
        assert turn_end[1]["reason"] == "interrupted"

    @pytest.mark.asyncio
    async def test_depth_tracking(self, fixed_clock: FixedClock, pipeline: ToolDispatchPipeline):
        step_runner = StepRunner(dispatch=pipeline)
        turn_runner = TurnRunner(step_runner=step_runner)
        d = AgentDriver(turn_runner=turn_runner, clock=fixed_clock, depth=2, depth_cap=5)
        assert d.depth == 2
        assert d.depth_cap == 5
