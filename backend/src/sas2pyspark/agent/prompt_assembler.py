"""PromptAssembler — ordered section assembly with strict variable interpolation."""

from __future__ import annotations

from typing import Any, Callable

from pydantic import Field

from sas2pyspark.core.json import deep_freeze
from sas2pyspark.core.models import WorkbenchModel


class SectionOrderConflictError(Exception):
    def __init__(self, name: str, existing_order: int) -> None:
        super().__init__(f"section '{name}' conflicts with existing order {existing_order}")


class UnboundVariableError(Exception):
    def __init__(self, variable: str) -> None:
        super().__init__(f"unbound variable '{variable}' — no value provided")


class PromptSection(WorkbenchModel):
    name: str
    order: int
    content: str = ""

    model_config = {**WorkbenchModel.model_config, "populate_by_name": True}


class PromptAssembly(WorkbenchModel):
    sections: tuple[PromptSection, ...] = Field(default_factory=tuple)
    contexts: tuple[Any, ...] = Field(default_factory=tuple)
    tools: tuple[Any, ...] = Field(default_factory=tuple)
    variables: dict[str, str] = Field(default_factory=dict)

    model_config = {**WorkbenchModel.model_config, "populate_by_name": True, "arbitrary_types_allowed": True}


class PromptAssembler:
    """Assemble prompt sections in registered order with strict {{variable}} interpolation."""

    __slots__ = ("_sections", "_providers")

    def __init__(self) -> None:
        self._sections: dict[str, PromptSection] = {}
        self._providers: dict[str, Callable[..., str]] = {}

    def register_section(
        self,
        *,
        name: str,
        order: int,
        provider: Callable[..., str] | None = None,
    ) -> None:
        for existing_name, existing_section in self._sections.items():
            if existing_section.order == order and existing_name != name:
                raise SectionOrderConflictError(name, order)
        self._sections[name] = PromptSection(name=name, order=order)
        if provider is not None:
            self._providers[name] = provider

    def assemble(
        self,
        *,
        scope_id: str | None = None,
        variables: dict[str, str] | None = None,
        tools: tuple[Any, ...] = (),
        contexts: tuple[Any, ...] = (),
    ) -> PromptAssembly:
        sorted_sections = tuple(
            sorted(self._sections.values(), key=lambda s: s.order)
        )
        merged_vars: dict[str, str] = {}
        if variables is not None:
            merged_vars.update(variables)
        assembly = PromptAssembly(
            sections=sorted_sections,
            contexts=contexts,
            tools=tools,
            variables=merged_vars,
        )
        return deep_freeze(assembly)

    def render(self, assembly: PromptAssembly) -> str:
        parts: list[str] = []
        for section in assembly.sections:
            provider = self._providers.get(section.name)
            if provider is not None:
                content = provider(**assembly.variables)
            else:
                content = section.content
            content = self._interpolate(content, assembly.variables)
            parts.append(content)
        return "\n".join(parts)

    @staticmethod
    def _interpolate(text: str, variables: dict[str, str]) -> str:
        result = text
        while "{{" in result and "}}" in result:
            start = result.index("{{")
            end = result.index("}}", start + 2)
            var_name = result[start + 2 : end].strip()
            if var_name not in variables:
                raise UnboundVariableError(var_name)
            result = result[:start] + variables[var_name] + result[end + 2 :]
        return result
