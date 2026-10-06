"""Tests for PromptOrdering — prefix-stable dispatch wrapper."""

from __future__ import annotations

from typing import Any

import pytest

from sas2pyspark.core.logging import DisjointTokenUsage, TokenMeter
from sas2pyspark.llm.cache.ordering import AdvisoryResponse, PrefixStableRequest, PromptOrdering
from sas2pyspark.llm.cache.projection import RouteCapabilities, SurfaceOp, SystemPromptProjection
from sas2pyspark.services.clock import FixedClock


@pytest.fixture
def ordering(fixed_clock: FixedClock) -> PromptOrdering:
    return PromptOrdering(
        projection=SystemPromptProjection(),
        token_meter=TokenMeter(),
        clock=fixed_clock,
    )


def _messages() -> tuple[Any, ...]:
    return (
        {"role": "system", "content": "You are a migration assistant."},
        {"role": "user", "content": "Translate this SAS code."},
    )


class TestPromptOrdering:
    def test_order_applies_replace_on_first_turn(self, ordering: PromptOrdering):
        route = RouteCapabilities(inHistorySystemUpdates=True)
        result = ordering.order(
            messages=_messages(),
            route=route,
            continuing=False,
            prompt_changed=True,
        )
        assert result.surface_op == SurfaceOp.REPLACE
        assert result.messages == _messages()

    def test_order_applies_append_on_continuing_changed(self, ordering: PromptOrdering):
        route = RouteCapabilities(inHistorySystemUpdates=True)
        result = ordering.order(
            messages=_messages(),
            route=route,
            continuing=True,
            prompt_changed=True,
        )
        assert result.surface_op == SurfaceOp.APPEND

    def test_order_preserves_messages(self, ordering: PromptOrdering):
        route = RouteCapabilities(inHistorySystemUpdates=False)
        msgs = _messages()
        result = ordering.order(
            messages=msgs,
            route=route,
            continuing=True,
            prompt_changed=True,
        )
        assert result.messages == msgs
        assert len(result.messages) == 2

    def test_record_response_reports_to_meter(self, fixed_clock: FixedClock):
        meter = TokenMeter()
        ordering = PromptOrdering(
            projection=SystemPromptProjection(),
            token_meter=meter,
            clock=fixed_clock,
        )
        usage = DisjointTokenUsage(inputTokens=100, cacheReadTokens=50, cacheWriteTokens=25)
        ordering.record_response(usage=usage, cost_units=1.5)
        assert meter.input_tokens == 100
        assert meter.cache_read_tokens == 50
        assert meter.cache_write_tokens == 25
        assert meter.cost_units == 1.5

    def test_record_response_accumulates(self, fixed_clock: FixedClock):
        meter = TokenMeter()
        ordering = PromptOrdering(
            projection=SystemPromptProjection(),
            token_meter=meter,
            clock=fixed_clock,
        )
        ordering.record_response(
            usage=DisjointTokenUsage(inputTokens=100, cacheReadTokens=50, cacheWriteTokens=25),
            cost_units=1.0,
        )
        ordering.record_response(
            usage=DisjointTokenUsage(inputTokens=200, cacheReadTokens=10, cacheWriteTokens=5),
            cost_units=2.0,
        )
        assert meter.input_tokens == 300
        assert meter.cost_units == 3.0

    def test_prefix_stable_request_model(self):
        req = PrefixStableRequest(
            messages=_messages(),
            surfaceOp=SurfaceOp.APPEND,
        )
        assert req.surface_op == SurfaceOp.APPEND
        assert len(req.messages) == 2
        dumped = req.model_dump(by_alias=True)
        assert "surfaceOp" in dumped

    def test_advisory_response_defaults_to_advisory(self):
        resp = AdvisoryResponse(
            content="translated code",
            usage=DisjointTokenUsage(inputTokens=10),
        )
        assert resp.advisory is True

    def test_advisory_response_explicit_non_advisory(self):
        resp = AdvisoryResponse(
            content="result",
            usage=DisjointTokenUsage(inputTokens=10),
            advisory=False,
        )
        assert resp.advisory is False

    def test_advisory_response_usage_preserved(self):
        usage = DisjointTokenUsage(inputTokens=100, cacheReadTokens=50, cacheWriteTokens=25)
        resp = AdvisoryResponse(content="result", usage=usage)
        assert resp.usage.input_tokens == 100
        assert resp.usage.cache_read_tokens == 50
        assert resp.usage.cache_write_tokens == 25

    def test_order_with_empty_messages(self, ordering: PromptOrdering):
        route = RouteCapabilities(inHistorySystemUpdates=True)
        result = ordering.order(
            messages=(),
            route=route,
            continuing=True,
            prompt_changed=True,
        )
        assert result.messages == ()
        assert result.surface_op == SurfaceOp.APPEND
