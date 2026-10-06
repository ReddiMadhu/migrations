"""Tests for StreamingDurabilityUnifier — compact stream in durable event."""

from __future__ import annotations

from typing import AsyncIterator

import pytest
import pytest_asyncio

from sas2pyspark.agent.cancellation import ManualAbortSignal
from sas2pyspark.agent.streaming import StreamingDurabilityUnifier
from sas2pyspark.core.events import MigrationEventType


class MockSession:
    def __init__(self) -> None:
        self.events: list[tuple[MigrationEventType, dict]] = []

    async def append(self, event_type: MigrationEventType, data: dict) -> None:
        self.events.append((event_type, data))


async def _async_frames(frames: list) -> AsyncIterator:
    for f in frames:
        yield f


class TestStreamingDurabilityUnifier:
    @pytest.mark.asyncio
    async def test_stream_collects_and_persists(self):
        session = MockSession()
        unifier = StreamingDurabilityUnifier(sessions=None)
        result = await unifier.stream(
            session=session,
            event_type=MigrationEventType.AGENT_STEP_ENDED,
            live_frames=_async_frames(["chunk1", "chunk2", "chunk3"]),
        )
        assert result["content"] == "chunk1chunk2chunk3"
        assert result["frameCount"] == 3
        assert result["interrupted"] is False
        assert len(session.events) == 1
        assert session.events[0][0] == MigrationEventType.AGENT_STEP_ENDED

    @pytest.mark.asyncio
    async def test_stream_dict_frames(self):
        session = MockSession()
        unifier = StreamingDurabilityUnifier(sessions=None)
        result = await unifier.stream(
            session=session,
            event_type=MigrationEventType.AGENT_STEP_ENDED,
            live_frames=_async_frames([{"content": "a"}, {"text": "b"}, {"content": "c"}]),
        )
        assert result["content"] == "abc"

    @pytest.mark.asyncio
    async def test_stream_cancellation_commits_safe_prefix(self):
        session = MockSession()
        unifier = StreamingDurabilityUnifier(sessions=None)
        abort = ManualAbortSignal()
        abort.cancel()
        result = await unifier.stream(
            session=session,
            event_type=MigrationEventType.AGENT_STEP_ENDED,
            live_frames=_async_frames(["chunk1", "chunk2"]),
            abort=abort,
        )
        assert result["interrupted"] is True
        assert len(session.events) == 1

    @pytest.mark.asyncio
    async def test_stream_empty_frames(self):
        session = MockSession()
        unifier = StreamingDurabilityUnifier(sessions=None)
        result = await unifier.stream(
            session=session,
            event_type=MigrationEventType.AGENT_STEP_ENDED,
            live_frames=_async_frames([]),
        )
        assert result["content"] == ""
        assert result["frameCount"] == 0
        assert result["interrupted"] is False

    @pytest.mark.asyncio
    async def test_stream_mixed_frame_types(self):
        session = MockSession()
        unifier = StreamingDurabilityUnifier(sessions=None)
        result = await unifier.stream(
            session=session,
            event_type=MigrationEventType.AGENT_STEP_ENDED,
            live_frames=_async_frames(["text", {"content": "dict"}, 42, {"text": "more"}]),
        )
        assert "text" in result["content"]
        assert "dict" in result["content"]
        assert "more" in result["content"]
