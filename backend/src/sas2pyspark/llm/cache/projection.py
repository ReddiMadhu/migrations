"""SystemPromptProjection — append-vs-replace decision for prefix stability."""

from __future__ import annotations

from enum import Enum

from pydantic import Field

from sas2pyspark.core.models import WorkbenchModel


class SurfaceOp(str, Enum):
    APPEND = "append"
    REPLACE = "replace"


class RouteCapabilities(WorkbenchModel):
    in_history_system_updates: bool = Field(default=False, alias="inHistorySystemUpdates")

    model_config = {**WorkbenchModel.model_config, "populate_by_name": True}


class SystemPromptProjection:
    """Decide how a rendered system prompt reaches the surface without owning the commit."""

    __slots__ = ()

    @staticmethod
    def decide(
        *,
        route: RouteCapabilities,
        continuing: bool,
        prompt_changed: bool,
    ) -> SurfaceOp:
        if route.in_history_system_updates and continuing and prompt_changed:
            return SurfaceOp.APPEND
        return SurfaceOp.REPLACE
