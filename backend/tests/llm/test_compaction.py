"""Tests for CacheAwareCompactor — prefix-preserving compaction."""

from __future__ import annotations

from typing import Any

import pytest

from sas2pyspark.llm.cache.compactor import CacheAwareCompactor
from sas2pyspark.llm.cache.pressure import ContextPressureProjector


@pytest.fixture
def compactor() -> CacheAwareCompactor:
    return CacheAwareCompactor(pressure=ContextPressureProjector())


def _msg(role: str, content: str) -> dict[str, str]:
    return {"role": role, "content": content}


class TestCacheAwareCompactor:
    def test_short_history_unchanged(self, compactor: CacheAwareCompactor):
        history: tuple[Any, ...] = (_msg("system", "sys"),)
        result = compactor.compact(history=history, budget=1000)
        assert result == history

    def test_two_element_history_unchanged(self, compactor: CacheAwareCompactor):
        history: tuple[Any, ...] = (_msg("system", "sys"), _msg("user", "u1"))
        result = compactor.compact(history=history, budget=1000)
        assert result == history

    def test_system_head_preserved_verbatim(self, compactor: CacheAwareCompactor):
        system = _msg("system", "You are a migration assistant.")
        tools = {"role": "tool", "name": "read_file", "content": "result"}
        history: tuple[Any, ...] = (system, tools, _msg("user", "u1"), _msg("assistant", "a1"))
        result = compactor.compact(history=history, budget=1000)
        assert result[0] == system

    def test_tools_preserved_verbatim(self, compactor: CacheAwareCompactor):
        system = _msg("system", "sys")
        tools = {"role": "tool", "name": "read_file", "content": "result"}
        history: tuple[Any, ...] = (system, tools, _msg("user", "u1"), _msg("assistant", "a1"))
        result = compactor.compact(history=history, budget=1000)
        assert result[1] == tools

    def test_compaction_instruction_appended_as_user(self, compactor: CacheAwareCompactor):
        system = _msg("system", "sys")
        tools = {"role": "tool", "name": "t", "content": "r"}
        history: tuple[Any, ...] = (system, tools, _msg("user", "u1"), _msg("assistant", "a1"))
        result = compactor.compact(history=history, budget=1000)
        last = result[-1]
        assert last["role"] == "user"
        assert "summar" in last["content"].lower() or "compact" in last["content"].lower()

    def test_leading_bytes_match_prior_prefix(self, compactor: CacheAwareCompactor):
        system = _msg("system", "sys")
        tools = {"role": "tool", "name": "t", "content": "r"}
        history: tuple[Any, ...] = (system, tools, _msg("user", "u1"), _msg("assistant", "a1"))
        result = compactor.compact(history=history, budget=1000)
        assert result[0] == history[0]
        assert result[1] == history[1]

    def test_compaction_reduces_history(self, compactor: CacheAwareCompactor):
        system = _msg("system", "sys")
        tools = {"role": "tool", "name": "t", "content": "r"}
        history: tuple[Any, ...] = (
            system, tools,
            _msg("user", "u1"), _msg("assistant", "a1"),
            _msg("user", "u2"), _msg("assistant", "a2"),
            _msg("user", "u3"), _msg("assistant", "a3"),
        )
        result = compactor.compact(history=history, budget=1000)
        assert len(result) < len(history)

    def test_empty_history_unchanged(self, compactor: CacheAwareCompactor):
        result = compactor.compact(history=(), budget=1000)
        assert result == ()

    def test_shadowed_messages_partially_preserved(self, compactor: CacheAwareCompactor):
        system = _msg("system", "sys")
        tools = {"role": "tool", "name": "t", "content": "r"}
        shadowed = (
            _msg("user", "u1"), _msg("assistant", "a1"),
            _msg("user", "u2"), _msg("assistant", "a2"),
        )
        history: tuple[Any, ...] = (system, tools, *shadowed)
        result = compactor.compact(history=history, budget=1000)
        assert result[0] == system
        assert result[1] == tools
        assert len(result) <= len(history)
