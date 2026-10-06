"""Tests for CrashRepairSynthesizer — conservative closers for incomplete runs."""

from __future__ import annotations

import pytest

from sas2pyspark.agent.crash_repair import CrashRepairSynthesizer
from sas2pyspark.core.events import MigrationEventType
from sas2pyspark.services.clock import FixedClock


class MockEvent:
    def __init__(self, event_type: MigrationEventType, data: dict) -> None:
        self.type = event_type
        self.data = data


class MockSession:
    def __init__(self, events: list[MockEvent] | None = None) -> None:
        self._events = events or []
        self.appended: list[tuple[MigrationEventType, dict]] = []

    @property
    def events(self) -> list[MockEvent]:
        return self._events

    async def append(self, event_type: MigrationEventType, data: dict) -> None:
        self._events.append(MockEvent(event_type, data))
        self.appended.append((event_type, data))


class TestCrashRepairSynthesizer:
    @pytest.mark.asyncio
    async def test_no_repair_needed(self, fixed_clock: FixedClock):
        session = MockSession()
        repairer = CrashRepairSynthesizer(clock=fixed_clock)
        count = await repairer.repair(session)
        assert count == 0

    @pytest.mark.asyncio
    async def test_synthesize_tool_outcome_unknown(self, fixed_clock: FixedClock):
        events = [
            MockEvent(MigrationEventType.TOOL_STARTED, {"callId": "c1", "toolName": "read"}),
        ]
        session = MockSession(events)
        repairer = CrashRepairSynthesizer(clock=fixed_clock)
        count = await repairer.repair(session)
        assert count == 1
        assert session.appended[0][0] == MigrationEventType.TOOL_OUTCOME_UNKNOWN
        assert session.appended[0][1]["callId"] == "c1"

    @pytest.mark.asyncio
    async def test_completed_tool_not_repaired(self, fixed_clock: FixedClock):
        events = [
            MockEvent(MigrationEventType.TOOL_STARTED, {"callId": "c1", "toolName": "read"}),
            MockEvent(MigrationEventType.TOOL_COMPLETED, {"callId": "c1", "toolName": "read"}),
        ]
        session = MockSession(events)
        repairer = CrashRepairSynthesizer(clock=fixed_clock)
        count = await repairer.repair(session)
        assert count == 0

    @pytest.mark.asyncio
    async def test_synthesize_step_end(self, fixed_clock: FixedClock):
        events = [
            MockEvent(MigrationEventType.AGENT_STEP_STARTED, {}),
        ]
        session = MockSession(events)
        repairer = CrashRepairSynthesizer(clock=fixed_clock)
        count = await repairer.repair(session)
        assert count >= 1
        types = [e[0] for e in session.appended]
        assert MigrationEventType.AGENT_STEP_ENDED in types

    @pytest.mark.asyncio
    async def test_synthesize_turn_end(self, fixed_clock: FixedClock):
        events = [
            MockEvent(MigrationEventType.AGENT_TURN_STARTED, {}),
        ]
        session = MockSession(events)
        repairer = CrashRepairSynthesizer(clock=fixed_clock)
        count = await repairer.repair(session)
        assert count >= 1
        types = [e[0] for e in session.appended]
        assert MigrationEventType.AGENT_TURN_ENDED in types

    @pytest.mark.asyncio
    async def test_synthesize_all_closers(self, fixed_clock: FixedClock):
        events = [
            MockEvent(MigrationEventType.AGENT_TURN_STARTED, {}),
            MockEvent(MigrationEventType.AGENT_STEP_STARTED, {}),
            MockEvent(MigrationEventType.TOOL_STARTED, {"callId": "c1", "toolName": "read"}),
        ]
        session = MockSession(events)
        repairer = CrashRepairSynthesizer(clock=fixed_clock)
        count = await repairer.repair(session)
        assert count == 3

    @pytest.mark.asyncio
    async def test_synthesized_events_marked(self, fixed_clock: FixedClock):
        events = [
            MockEvent(MigrationEventType.TOOL_STARTED, {"callId": "c1", "toolName": "read"}),
        ]
        session = MockSession(events)
        repairer = CrashRepairSynthesizer(clock=fixed_clock)
        await repairer.repair(session)
        assert session.appended[0][1]["synthesized"] is True
        assert session.appended[0][1]["reason"] == "interrupted"
