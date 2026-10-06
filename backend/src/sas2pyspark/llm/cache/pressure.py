"""ContextPressureProjector — cache-aware context pressure and compaction trigger."""

from __future__ import annotations

from pydantic import Field

from sas2pyspark.core.logging import DisjointTokenUsage
from sas2pyspark.core.models import WorkbenchModel


class PressureProjection(WorkbenchModel):
    pressure_tokens: int = Field(alias="pressureTokens")
    projected_tokens: int = Field(alias="projectedTokens")

    model_config = {**WorkbenchModel.model_config, "populate_by_name": True}


class ContextPressureProjector:
    """Project context pressure from disjoint token buckets."""

    __slots__ = ()

    @staticmethod
    def project(
        *,
        usage: DisjointTokenUsage,
        surface_tokens: int,
        sampled_surface_tokens: int,
    ) -> PressureProjection:
        pressure = usage.input_tokens + usage.cache_read_tokens + usage.cache_write_tokens
        projected = pressure + max(0, surface_tokens - sampled_surface_tokens)
        return PressureProjection(pressureTokens=pressure, projectedTokens=projected)

    @staticmethod
    def should_compact(*, projected: int, budget: int) -> bool:
        return projected > budget
