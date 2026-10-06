"""Tests for ContextPressureProjector and disjoint token-bucket pressure projection."""

from __future__ import annotations

import pytest

from sas2pyspark.core.logging import DisjointTokenUsage, TokenMeter
from sas2pyspark.llm.cache.pressure import ContextPressureProjector, PressureProjection


class TestContextPressureProjector:
    def test_pressure_tokens_sum(self):
        usage = DisjointTokenUsage(inputTokens=100, cacheReadTokens=200, cacheWriteTokens=300)
        result = ContextPressureProjector.project(
            usage=usage, surface_tokens=500, sampled_surface_tokens=400,
        )
        assert result.pressure_tokens == 600

    def test_projected_tokens_includes_unsampled_surface(self):
        usage = DisjointTokenUsage(inputTokens=100, cacheReadTokens=200, cacheWriteTokens=300)
        result = ContextPressureProjector.project(
            usage=usage, surface_tokens=500, sampled_surface_tokens=400,
        )
        assert result.projected_tokens == 600 + 100

    def test_projected_tokens_zero_surface(self):
        usage = DisjointTokenUsage(inputTokens=50, cacheReadTokens=0, cacheWriteTokens=0)
        result = ContextPressureProjector.project(
            usage=usage, surface_tokens=0, sampled_surface_tokens=0,
        )
        assert result.pressure_tokens == 50
        assert result.projected_tokens == 50

    def test_projected_tokens_fully_sampled(self):
        usage = DisjointTokenUsage(inputTokens=100, cacheReadTokens=50, cacheWriteTokens=25)
        result = ContextPressureProjector.project(
            usage=usage, surface_tokens=300, sampled_surface_tokens=300,
        )
        assert result.pressure_tokens == 175
        assert result.projected_tokens == 175

    def test_non_negative_pressure(self):
        usage = DisjointTokenUsage(inputTokens=0, cacheReadTokens=0, cacheWriteTokens=0)
        result = ContextPressureProjector.project(
            usage=usage, surface_tokens=0, sampled_surface_tokens=0,
        )
        assert result.pressure_tokens >= 0
        assert result.projected_tokens >= 0

    def test_should_compact_when_exceeds_budget(self):
        assert ContextPressureProjector.should_compact(projected=1001, budget=1000) is True

    def test_should_not_compact_at_budget(self):
        assert ContextPressureProjector.should_compact(projected=1000, budget=1000) is False

    def test_should_not_compact_below_budget(self):
        assert ContextPressureProjector.should_compact(projected=999, budget=1000) is False

    def test_pressure_projection_model(self):
        proj = PressureProjection(pressureTokens=100, projectedTokens=200)
        assert proj.pressure_tokens == 100
        assert proj.projected_tokens == 200
        dumped = proj.model_dump(by_alias=True)
        assert "pressureTokens" in dumped
        assert "projectedTokens" in dumped


class TestTokenMeterDisjoint:
    def test_record_disjoint_accumulates(self):
        meter = TokenMeter()
        usage1 = DisjointTokenUsage(inputTokens=100, cacheReadTokens=50, cacheWriteTokens=25)
        usage2 = DisjointTokenUsage(inputTokens=200, cacheReadTokens=10, cacheWriteTokens=5)
        meter.record_disjoint(usage1, cost_units=1.0)
        meter.record_disjoint(usage2, cost_units=2.0)
        assert meter.input_tokens == 300
        assert meter.cache_read_tokens == 60
        assert meter.cache_write_tokens == 30
        assert meter.cost_units == 3.0

    def test_pressure_from(self):
        meter = TokenMeter()
        usage = DisjointTokenUsage(inputTokens=100, cacheReadTokens=200, cacheWriteTokens=300)
        meter.record_disjoint(usage)
        assert meter.pressure_from() == 600

    def test_projected_tokens(self):
        meter = TokenMeter()
        usage = DisjointTokenUsage(inputTokens=100, cacheReadTokens=200, cacheWriteTokens=300)
        meter.record_disjoint(usage)
        projected = meter.projected_tokens(surface_tokens=500, sampled_surface_tokens=400)
        assert projected == 700

    def test_snapshot_disjoint(self):
        meter = TokenMeter()
        usage = DisjointTokenUsage(inputTokens=10, cacheReadTokens=20, cacheWriteTokens=30)
        meter.record_disjoint(usage, cost_units=5.0)
        snap = meter.snapshot_disjoint()
        assert snap["inputTokens"] == 10
        assert snap["cacheReadTokens"] == 20
        assert snap["cacheWriteTokens"] == 30
        assert snap["costUnits"] == 5.0

    @pytest.mark.parametrize("wire_key,expected", [
        ("cache_read_input_tokens", "cache_read"),
        ("cacheReadInputTokens", "cache_read"),
        ("cache_creation_input_tokens", "cache_write"),
        ("cacheCreationInputTokens", "cache_write"),
    ])
    def test_map_wire_fields_provider_compat(self, wire_key, expected):
        wire = {"input_tokens": 100, wire_key: 50}
        usage = TokenMeter.map_wire_fields(wire)
        if expected == "cache_read":
            assert usage.cache_read_tokens == 50
            assert usage.cache_write_tokens == 0
        else:
            assert usage.cache_write_tokens == 50
            assert usage.cache_read_tokens == 0
        assert usage.input_tokens == 100

    def test_map_wire_fields_legacy_compat(self):
        wire = {
            "input_tokens": 100,
            "prompt_cache_hit_tokens": 40,
            "prompt_cache_miss_tokens": 60,
        }
        usage = TokenMeter.map_wire_fields(wire)
        assert usage.input_tokens == 100
        assert usage.cache_read_tokens == 40
        assert usage.cache_write_tokens == 60

    def test_record_disjoint_does_not_affect_legacy(self):
        meter = TokenMeter()
        usage = DisjointTokenUsage(inputTokens=100, cacheReadTokens=50, cacheWriteTokens=25)
        meter.record_disjoint(usage)
        legacy = meter.snapshot()
        assert legacy["promptCacheHitTokens"] == 0
        assert legacy["promptCacheMissTokens"] == 0
