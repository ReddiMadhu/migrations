"""CacheAwareCompactor — compaction that reuses the warm KV prefix."""

from __future__ import annotations

from typing import Any

from sas2pyspark.llm.cache.pressure import ContextPressureProjector


class CacheAwareCompactor:
    """Compact history by replaying prefix verbatim + appending compaction instruction."""

    __slots__ = ("_pressure",)

    def __init__(self, *, pressure: ContextPressureProjector) -> None:
        self._pressure = pressure

    @staticmethod
    def compact(
        *,
        history: tuple[Any, ...],
        budget: int,
    ) -> tuple[Any, ...]:
        if len(history) <= 2:
            return history
        system_head = history[0] if history else None
        tools = history[1] if len(history) > 1 else None
        shadowed = history[2:] if len(history) > 2 else ()
        compaction_instruction = {"role": "user", "content": "Summarize the above conversation concisely."}
        if system_head is not None and tools is not None:
            return (system_head, tools, *shadowed[: len(shadowed) // 2], compaction_instruction)
        return history
