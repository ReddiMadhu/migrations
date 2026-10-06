"""PromptOrdering — wraps the LLM call site with prefix-stable dispatch."""

from __future__ import annotations

from typing import Any

from pydantic import Field

from sas2pyspark.core.logging import DisjointTokenUsage, TokenMeter
from sas2pyspark.core.models import WorkbenchModel
from sas2pyspark.llm.cache.projection import RouteCapabilities, SurfaceOp, SystemPromptProjection
from sas2pyspark.services.clock import Clock


class PrefixStableRequest(WorkbenchModel):
    messages: tuple[Any, ...]
    surface_op: SurfaceOp = Field(alias="surfaceOp")

    model_config = {**WorkbenchModel.model_config, "populate_by_name": True}


class AdvisoryResponse(WorkbenchModel):
    content: Any
    usage: DisjointTokenUsage
    advisory: bool = True

    model_config = {**WorkbenchModel.model_config, "populate_by_name": True}


class RetryableLlmError(Exception):
    pass


class PromptOrdering:
    """Apply prefix-stable ordering before LLM dispatch and map results to disjoint buckets."""

    __slots__ = ("_projection", "_token_meter", "_clock")

    def __init__(
        self,
        *,
        projection: SystemPromptProjection,
        token_meter: TokenMeter,
        clock: Clock,
    ) -> None:
        self._projection = projection
        self._token_meter = token_meter
        self._clock = clock

    def order(
        self,
        *,
        messages: tuple[Any, ...],
        route: RouteCapabilities,
        continuing: bool,
        prompt_changed: bool,
    ) -> PrefixStableRequest:
        op = self._projection.decide(
            route=route, continuing=continuing, prompt_changed=prompt_changed,
        )
        return PrefixStableRequest(messages=messages, surfaceOp=op)

    def record_response(self, *, usage: DisjointTokenUsage, cost_units: float = 0.0) -> None:
        self._token_meter.record_disjoint(usage, cost_units=cost_units)
