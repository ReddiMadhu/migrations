"""Tests for ScopedToolRegistry — scoped registration and dispatch planning."""

from __future__ import annotations

import pytest

from sas2pyspark.agent.registry import (
    DispatchPlan,
    ScopedToolRegistry,
    ToolClassification,
    ToolNotFoundError,
    ToolRegistration,
)


def _dummy_tool(name: str = "tool"):
    def tool(**kwargs):
        return {"name": name, "args": kwargs}
    return tool


class TestScopedToolRegistry:
    def test_register_global_then_resolve(self):
        registry = ScopedToolRegistry()
        tool = _dummy_tool()
        registry.register_global(
            name="read_file", tool=tool, classification=ToolClassification.PARALLEL,
        )
        reg = registry.resolve(scope_id="any", name="read_file")
        assert reg.name == "read_file"
        assert reg.classification == ToolClassification.PARALLEL

    def test_register_scoped_overrides_global(self):
        registry = ScopedToolRegistry()
        global_tool = _dummy_tool("global")
        scoped_tool = _dummy_tool("scoped")
        registry.register_global(
            name="tool_a", tool=global_tool, classification=ToolClassification.PARALLEL,
        )
        registry.register_scoped(
            scope_id="s1", name="tool_a", tool=scoped_tool, classification=ToolClassification.EXCLUSIVE,
        )
        reg = registry.resolve(scope_id="s1", name="tool_a")
        assert reg.tool is scoped_tool
        reg2 = registry.resolve(scope_id="s2", name="tool_a")
        assert reg2.tool is global_tool

    def test_resolve_not_found_raises(self):
        registry = ScopedToolRegistry()
        with pytest.raises(ToolNotFoundError) as exc_info:
            registry.resolve(scope_id="s1", name="nonexistent")
        assert "nonexistent" in str(exc_info.value)

    def test_unregister_global(self):
        registry = ScopedToolRegistry()
        tool = _dummy_tool()
        unregister = registry.register_global(
            name="tool_a", tool=tool, classification=ToolClassification.PARALLEL,
        )
        unregister()
        with pytest.raises(ToolNotFoundError):
            registry.resolve(scope_id="s1", name="tool_a")

    def test_unregister_scoped(self):
        registry = ScopedToolRegistry()
        tool = _dummy_tool()
        unregister = registry.register_scoped(
            scope_id="s1", name="tool_a", tool=tool, classification=ToolClassification.PARALLEL,
        )
        unregister()
        with pytest.raises(ToolNotFoundError):
            registry.resolve(scope_id="s1", name="tool_a")

    def test_list_tools(self):
        registry = ScopedToolRegistry()
        registry.register_global(
            name="global_tool", tool=_dummy_tool(), classification=ToolClassification.PARALLEL,
        )
        registry.register_scoped(
            scope_id="s1", name="scoped_tool", tool=_dummy_tool(), classification=ToolClassification.EXCLUSIVE,
        )
        tools = registry.list_tools(scope_id="s1")
        assert "global_tool" in tools
        assert "scoped_tool" in tools

    def test_list_tools_global_only(self):
        registry = ScopedToolRegistry()
        registry.register_global(
            name="global_tool", tool=_dummy_tool(), classification=ToolClassification.PARALLEL,
        )
        tools = registry.list_tools()
        assert tools == ["global_tool"]

    def test_dispatch_plan_parallel(self):
        registry = ScopedToolRegistry()
        registry.register_global(
            name="read_a", tool=_dummy_tool(), classification=ToolClassification.PARALLEL,
        )
        registry.register_global(
            name="read_b", tool=_dummy_tool(), classification=ToolClassification.PARALLEL,
        )
        plan = registry.dispatch_plan(calls=("read_a", "read_b"), scope_id="s1")
        assert plan.barrier is False
        assert "read_a" in plan.parallel_calls
        assert "read_b" in plan.parallel_calls

    def test_dispatch_plan_exclusive_forms_barrier(self):
        registry = ScopedToolRegistry()
        registry.register_global(
            name="write_a", tool=_dummy_tool(), classification=ToolClassification.EXCLUSIVE,
        )
        registry.register_global(
            name="read_b", tool=_dummy_tool(), classification=ToolClassification.PARALLEL,
        )
        plan = registry.dispatch_plan(calls=("read_b", "write_a"), scope_id="s1")
        assert plan.barrier is True
        assert "write_a" in plan.exclusive_calls
        assert "read_b" in plan.parallel_calls

    def test_dispatch_plan_parallel_capped_at_10(self):
        registry = ScopedToolRegistry()
        for i in range(15):
            registry.register_global(
                name=f"read_{i}", tool=_dummy_tool(), classification=ToolClassification.PARALLEL,
            )
        calls = tuple(f"read_{i}" for i in range(15))
        plan = registry.dispatch_plan(calls=calls, scope_id="s1")
        assert len(plan.parallel_calls) <= 10

    def test_dispatch_plan_unknown_tool_skipped(self):
        registry = ScopedToolRegistry()
        registry.register_global(
            name="read_a", tool=_dummy_tool(), classification=ToolClassification.PARALLEL,
        )
        plan = registry.dispatch_plan(calls=("read_a", "unknown"), scope_id="s1")
        assert "read_a" in plan.parallel_calls

    def test_tool_registration_model(self):
        reg = ToolRegistration(
            name="test", tool=_dummy_tool(), classification=ToolClassification.PARALLEL,
        )
        assert reg.name == "test"
        assert reg.restrictions == ()

    def test_tool_classification_enum(self):
        assert ToolClassification.PARALLEL == "parallel"
        assert ToolClassification.EXCLUSIVE == "exclusive"
