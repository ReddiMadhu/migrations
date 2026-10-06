"""CrashRepairSynthesizer — synthesizes conservative closers for incomplete agent runs."""

from __future__ import annotations

from typing import Any

from sas2pyspark.core.events import MigrationEventType
from sas2pyspark.services.clock import Clock


class CrashRepairSynthesizer:
    """Scan event log for incomplete tool calls and synthesize conservative closers."""

    __slots__ = ("_clock",)

    def __init__(self, *, clock: Clock) -> None:
        self._clock = clock

    async def repair(self, session: Any) -> int:
        events = session.events
        started_tool_calls: dict[str, dict[str, Any]] = {}
        for event in events:
            if event.type == MigrationEventType.TOOL_STARTED:
                call_id = event.data.get("callId", "")
                started_tool_calls[call_id] = event.data
            elif event.type in (
                MigrationEventType.TOOL_COMPLETED,
                MigrationEventType.TOOL_DENIED,
                MigrationEventType.TOOL_BLOCKED,
                MigrationEventType.TOOL_OUTCOME_UNKNOWN,
                MigrationEventType.TOOL_NOT_STARTED,
            ):
                call_id = event.data.get("callId", "")
                started_tool_calls.pop(call_id, None)

        closers_appended = 0
        for call_id, started_data in started_tool_calls.items():
            await session.append(
                MigrationEventType.TOOL_OUTCOME_UNKNOWN,
                {
                    "callId": call_id,
                    "toolName": started_data.get("toolName", ""),
                    "reason": "interrupted",
                    "synthesized": True,
                },
            )
            closers_appended += 1

        step_started = any(e.type == MigrationEventType.AGENT_STEP_STARTED for e in events)
        step_ended = any(e.type == MigrationEventType.AGENT_STEP_ENDED for e in events)
        if step_started and not step_ended:
            await session.append(
                MigrationEventType.AGENT_STEP_ENDED,
                {"reason": "interrupted", "synthesized": True},
            )
            closers_appended += 1

        turn_started = any(e.type == MigrationEventType.AGENT_TURN_STARTED for e in events)
        turn_ended = any(e.type == MigrationEventType.AGENT_TURN_ENDED for e in events)
        if turn_started and not turn_ended:
            await session.append(
                MigrationEventType.AGENT_TURN_ENDED,
                {"reason": "interrupted", "synthesized": True},
            )
            closers_appended += 1

        return closers_appended
