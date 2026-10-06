"""Tests for PromptAssembler — ordered sections and variable interpolation."""

from __future__ import annotations

import pytest

from sas2pyspark.agent.prompt_assembler import (
    PromptAssembler,
    PromptAssembly,
    PromptSection,
    SectionOrderConflictError,
    UnboundVariableError,
)


class TestPromptAssembler:
    def test_register_and_assemble(self):
        assembler = PromptAssembler()
        assembler.register_section(name="system", order=10)
        assembler.register_section(name="task", order=20)
        assembly = assembler.assemble(variables={})
        assert len(assembly.sections) == 2
        assert assembly.sections[0].name == "system"
        assert assembly.sections[1].name == "task"

    def test_section_order_conflict(self):
        assembler = PromptAssembler()
        assembler.register_section(name="system", order=10)
        with pytest.raises(SectionOrderConflictError):
            assembler.register_section(name="task", order=10)

    def test_same_name_reregister_no_conflict(self):
        assembler = PromptAssembler()
        assembler.register_section(name="system", order=10)
        assembler.register_section(name="system", order=10)

    def test_render_with_variables(self):
        assembler = PromptAssembler()
        assembler.register_section(name="greeting", order=10, provider=lambda **kw: "Hello {{name}}!")
        assembly = assembler.assemble(variables={"name": "World"})
        rendered = assembler.render(assembly)
        assert rendered == "Hello World!"

    def test_render_unbound_variable_raises(self):
        assembler = PromptAssembler()
        assembler.register_section(name="greeting", order=10, provider=lambda **kw: "Hello {{name}}!")
        assembly = assembler.assemble(variables={})
        with pytest.raises(UnboundVariableError):
            assembler.render(assembly)

    def test_assemble_with_tools_and_contexts(self):
        assembler = PromptAssembler()
        assembler.register_section(name="system", order=10)
        tools = ({"name": "read_file"},)
        contexts = ({"key": "value"},)
        assembly = assembler.assemble(variables={}, tools=tools, contexts=contexts)
        assert assembly.tools == tools
        assert assembly.contexts == contexts

    def test_assemble_freezes_result(self):
        assembler = PromptAssembler()
        assembler.register_section(name="system", order=10)
        assembly = assembler.assemble(variables={"key": "value"})
        with pytest.raises(Exception):
            assembly.variables = {"new": "unfrozen"}

    def test_render_multiple_sections_ordered(self):
        assembler = PromptAssembler()
        assembler.register_section(name="first", order=10, provider=lambda **kw: "AAA")
        assembler.register_section(name="second", order=20, provider=lambda **kw: "BBB")
        assembler.register_section(name="third", order=30, provider=lambda **kw: "CCC")
        assembly = assembler.assemble(variables={})
        rendered = assembler.render(assembly)
        assert rendered == "AAA\nBBB\nCCC"

    def test_render_empty_assembler(self):
        assembler = PromptAssembler()
        assembly = assembler.assemble(variables={})
        rendered = assembler.render(assembly)
        assert rendered == ""

    def test_prompt_section_model(self):
        section = PromptSection(name="test", order=5, content="hello")
        assert section.name == "test"
        assert section.order == 5
        assert section.content == "hello"

    def test_prompt_assembly_model(self):
        assembly = PromptAssembly(
            sections=(PromptSection(name="s", order=1),),
            variables={"key": "value"},
        )
        assert len(assembly.sections) == 1
        assert assembly.variables == {"key": "value"}

    def test_multiple_variable_interpolation(self):
        assembler = PromptAssembler()
        assembler.register_section(
            name="multi", order=10,
            provider=lambda **kw: "{{greeting}} {{name}}!",
        )
        assembly = assembler.assemble(variables={"greeting": "Hi", "name": "Bob"})
        rendered = assembler.render(assembly)
        assert rendered == "Hi Bob!"
