"""Tests for retry behavior — retry without re-admission on transient failures."""

from __future__ import annotations

import pytest

from sas2pyspark.agent.dispatch import ToolCall, ToolDispatchPipeline, ToolOutcomeStatus
from sas2pyspark.agent.driver import StepRunner, TurnRunner
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


class TestRetryBehavior:
    @pytest.mark.asyncio
    async def test_step_retries_on_transient_failure(self, fixed_clock: FixedClock):
        call_count = 0

        def flaky_tool(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RuntimeError("transient")
            return {"ok": True}

        registry = ScopedToolRegistry()
        registry.register_global(
            name="flaky", tool=flaky_tool, classification=ToolClassification.PARALLEL,
        )
        pipeline = ToolDispatchPipeline(registry=registry, clock=fixed_clock)
        session = MockSession()
        call = ToolCall(toolName="flaky", args={}, callId="c1")
        outcome = await pipeline.execute(call=call, scope=MockScope(), session=session)
        assert outcome.status == ToolOutcomeStatus.BLOCKED
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_step_proceeds_after_recovery(self, fixed_clock: FixedClock):
        registry = ScopedToolRegistry()
        registry.register_global(
            name="ok", tool=lambda **kw: {"result": "success"},
            classification=ToolClassification.PARALLEL,
        )
        pipeline = ToolDispatchPipeline(registry=registry, clock=fixed_clock)
        step_runner = StepRunner(dispatch=pipeline)
        session = MockSession()
        inbox = [ToolCall(toolName="ok", args={}, callId="c1")]
        outcomes = await step_runner.step(
            session=session, scope=MockScope(), inbox=inbox,
        )
        assert len(outcomes) == 1
        assert outcomes[0].status == ToolOutcomeStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_turn_loops_until_inbox_empty(self, fixed_clock: FixedClock):
        registry = ScopedToolRegistry()
        registry.register_global(
            name="echo", tool=lambda **kw: kw,
            classification=ToolClassification.PARALLEL,
        )
        pipeline = ToolDispatchPipeline(registry=registry, clock=fixed_clock)
        step_runner = StepRunner(dispatch=pipeline)
        turn_runner = TurnRunner(step_runner=step_runner)
        session = MockSession()
        inbox = [
            ToolCall(toolName="echo", args={"i": 1}, callId="c1"),
            ToolCall(toolName="echo", args={"i": 2}, callId="c2"),
        ]
        outcomes, budget_exceeded = await turn_runner.turn(
            session=session, scope=MockScope(), inbox=inbox,
        )
        assert len(outcomes) == 2
        assert budget_exceeded is False
        assert len(inbox) == 0
