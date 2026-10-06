"""Tests for SystemPromptProjection — append-vs-replace for prefix stability."""

from __future__ import annotations

import pytest

from sas2pyspark.llm.cache.projection import RouteCapabilities, SurfaceOp, SystemPromptProjection


class TestSystemPromptProjection:
    @pytest.mark.parametrize("in_history,continuing,changed,expected", [
        (True, True, True, SurfaceOp.APPEND),
        (True, True, False, SurfaceOp.REPLACE),
        (True, False, True, SurfaceOp.REPLACE),
        (True, False, False, SurfaceOp.REPLACE),
        (False, True, True, SurfaceOp.REPLACE),
        (False, True, False, SurfaceOp.REPLACE),
        (False, False, True, SurfaceOp.REPLACE),
        (False, False, False, SurfaceOp.REPLACE),
    ])
    def test_decision_matrix(self, in_history, continuing, changed, expected):
        route = RouteCapabilities(inHistorySystemUpdates=in_history)
        result = SystemPromptProjection.decide(
            route=route, continuing=continuing, prompt_changed=changed,
        )
        assert result == expected

    def test_append_only_when_all_three_conditions(self):
        route = RouteCapabilities(inHistorySystemUpdates=True)
        result = SystemPromptProjection.decide(
            route=route, continuing=True, prompt_changed=True,
        )
        assert result is SurfaceOp.APPEND
        assert result.value == "append"

    def test_replace_when_route_lacks_capability(self):
        route = RouteCapabilities(inHistorySystemUpdates=False)
        result = SystemPromptProjection.decide(
            route=route, continuing=True, prompt_changed=True,
        )
        assert result is SurfaceOp.REPLACE

    def test_replace_on_first_turn(self):
        route = RouteCapabilities(inHistorySystemUpdates=True)
        result = SystemPromptProjection.decide(
            route=route, continuing=False, prompt_changed=True,
        )
        assert result is SurfaceOp.REPLACE

    def test_replace_when_prompt_unchanged(self):
        route = RouteCapabilities(inHistorySystemUpdates=True)
        result = SystemPromptProjection.decide(
            route=route, continuing=True, prompt_changed=False,
        )
        assert result is SurfaceOp.REPLACE

    def test_pure_function_no_side_effects(self):
        route = RouteCapabilities(inHistorySystemUpdates=True)
        r1 = SystemPromptProjection.decide(route=route, continuing=True, prompt_changed=True)
        r2 = SystemPromptProjection.decide(route=route, continuing=True, prompt_changed=True)
        assert r1 == r2 == SurfaceOp.APPEND

    def test_surface_op_is_str_enum(self):
        assert isinstance(SurfaceOp.APPEND, str)
        assert isinstance(SurfaceOp.REPLACE, str)
        assert SurfaceOp.APPEND == "append"
        assert SurfaceOp.REPLACE == "replace"

    def test_route_capabilities_model(self):
        route = RouteCapabilities(inHistorySystemUpdates=True)
        assert route.in_history_system_updates is True
        route2 = RouteCapabilities(inHistorySystemUpdates=False)
        assert route2.in_history_system_updates is False

    def test_route_capabilities_alias_round_trip(self):
        route = RouteCapabilities.model_validate({"inHistorySystemUpdates": True})
        assert route.in_history_system_updates is True
        dumped = route.model_dump(by_alias=True)
        assert dumped == {"inHistorySystemUpdates": True}
