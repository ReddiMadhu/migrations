"""StreamingDurabilityUnifier — one compact stream embedded in a durable event."""

from __future__ import annotations

from typing import Any, AsyncIterator

from sas2pyspark.agent.cancellation import AbortSignal
from sas2pyspark.core.events import MigrationEventType


class StreamingDurabilityUnifier:
    """Unify live streaming frames with durable event-log persistence."""

    __slots__ = ("_sessions",)

    def __init__(self, *, sessions: Any) -> None:
        self._sessions = sessions

    async def stream(
        self,
        *,
        session: Any,
        event_type: MigrationEventType,
        live_frames: AsyncIterator[Any],
        abort: AbortSignal | None = None,
    ) -> dict[str, Any]:
        collected: list[Any] = []
        interrupted = False

        try:
            async for frame in live_frames:
                if abort is not None and abort.cancelled:
                    interrupted = True
                    break
                collected.append(frame)
        except Exception:
            interrupted = True

        visible_content = self._extract_visible(collected)

        durable_data: dict[str, Any] = {
            "content": visible_content,
            "frameCount": len(collected),
            "interrupted": interrupted,
        }

        await session.append(event_type, durable_data)
        return durable_data

    @staticmethod
    def _extract_visible(frames: list[Any]) -> str:
        parts: list[str] = []
        for frame in frames:
            if isinstance(frame, str):
                parts.append(frame)
            elif isinstance(frame, dict):
                content = frame.get("content") or frame.get("text") or ""
                if content:
                    parts.append(str(content))
        return "".join(parts)
