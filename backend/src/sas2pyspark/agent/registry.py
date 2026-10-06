"""ScopedToolRegistry — scoped tool registration with parallel/exclusive classification."""

from __future__ import annotations

from enum import Enum
from typing import Any, Callable

from pydantic import Field

from sas2pyspark.core.models import WorkbenchModel


class ToolClassification(str, Enum):
    PARALLEL = "parallel"
    EXCLUSIVE = "exclusive"


class ToolRegistration(WorkbenchModel):
    name: str
    tool: Any
    classification: ToolClassification
    restrictions: tuple[str, ...] = Field(default_factory=tuple)

    model_config = {**WorkbenchModel.model_config, "populate_by_name": True, "arbitrary_types_allowed": True}


class DispatchPlan(WorkbenchModel):
    parallel_calls: tuple[str, ...] = Field(default_factory=tuple, alias="parallelCalls")
    exclusive_calls: tuple[str, ...] = Field(default_factory=tuple, alias="exclusiveCalls")
    barrier: bool = False

    model_config = {**WorkbenchModel.model_config, "populate_by_name": True}


class ToolNotFoundError(Exception):
    def __init__(self, name: str, scope_id: str | None = None) -> None:
        self.name = name
        self.scope_id = scope_id
        super().__init__(f"tool '{name}' not found in scope '{scope_id}'; use an available tool")


class ScopedToolRegistry:
    """Registry with global + scoped tool registrations. Nearest scope wins."""

    __slots__ = ("_global", "_scoped")

    def __init__(self) -> None:
        self._global: dict[str, ToolRegistration] = {}
        self._scoped: dict[str, dict[str, ToolRegistration]] = {}

    def register_global(
        self,
        *,
        name: str,
        tool: Any,
        classification: ToolClassification,
        restrictions: tuple[str, ...] = (),
    ) -> Callable[[], None]:
        reg = ToolRegistration(
            name=name, tool=tool, classification=classification, restrictions=restrictions,
        )
        self._global[name] = reg

        def unregister() -> None:
            self._global.pop(name, None)

        return unregister

    def register_scoped(
        self,
        *,
        scope_id: str,
        name: str,
        tool: Any,
        classification: ToolClassification,
        restrictions: tuple[str, ...] = (),
    ) -> Callable[[], None]:
        scoped = self._scoped.setdefault(scope_id, {})
        reg = ToolRegistration(
            name=name, tool=tool, classification=classification, restrictions=restrictions,
        )
        scoped[name] = reg

        def unregister() -> None:
            scoped.pop(name, None)
            if not scoped:
                self._scoped.pop(scope_id, None)

        return unregister

    def resolve(self, *, scope_id: str, name: str) -> ToolRegistration:
        scoped = self._scoped.get(scope_id, {})
        if name in scoped:
            return scoped[name]
        if name in self._global:
            return self._global[name]
        raise ToolNotFoundError(name, scope_id)

    def list_tools(self, scope_id: str | None = None) -> list[str]:
        names: set[str] = set(self._global.keys())
        if scope_id is not None:
            names.update(self._scoped.get(scope_id, {}).keys())
        return sorted(names)

    def dispatch_plan(self, *, calls: tuple[str, ...], scope_id: str) -> DispatchPlan:
        parallel: list[str] = []
        exclusive: list[str] = []
        for call_name in calls:
            try:
                reg = self.resolve(scope_id=scope_id, name=call_name)
            except ToolNotFoundError:
                continue
            if reg.classification == ToolClassification.EXCLUSIVE:
                exclusive.append(call_name)
            else:
                parallel.append(call_name)
        barrier = len(exclusive) > 0
        return DispatchPlan(
            parallelCalls=tuple(parallel[:10]),
            exclusiveCalls=tuple(exclusive),
            barrier=barrier,
        )
