"""Tests for the supported-subset SAS tokenizer, macro expander, and parser."""

from __future__ import annotations

from sas2pyspark.sas.macro_expander import expand_macros
from sas2pyspark.sas.parser import parse_sas
from sas2pyspark.sas.tokenizer import tokenize


def test_tokenizer_preserves_source_positions_and_literals():
    tokens = tokenize("data x;\n* a comment;\nset y; if name='a;b'; c = a // b; run;")

    assert tokens[0].value.lower() == "data"
    string_token = next(token for token in tokens if token.kind == "STRING")
    assert string_token.value == "'a;b'"
    assert string_token.line == 3
    assert any(token.kind == "COMMENT" and token.value == "* a comment;" for token in tokens)
    assert any(token.kind == "SYMBOL" and token.value == "/" for token in tokens)


def test_macro_expansion_expands_let_and_parameterized_macro():
    source = "%let source=claims; %macro copy(ds); data out; set &ds; run; %mend; %copy(&source);"
    expanded = expand_macros(source)

    assert "set claims" in expanded.source.lower()
    assert expanded.definitions == ("copy",)
    assert not [issue for issue in expanded.issues if issue.code.startswith("UNRESOLVED")]


def test_recursive_parser_builds_data_proc_and_include_nodes_with_spans():
    source = 'data out; set base; by id; run; proc sort; by id; run; %include "leaf.sas";'
    parsed = parse_sas(source, path="root.sas")

    names = [(node.kind, node.name) for node in parsed.nodes]
    assert ("data-step", "out") in names
    assert ("procedure", "sort") in names
    assert ("include", "leaf.sas") in names
    data_step = next(node for node in parsed.nodes if node.kind == "data-step")
    assert data_step.span.start_line == 1
    assert data_step.span.start_offset == 0
    assert data_step.span.end_offset > data_step.span.start_offset
    assert parsed.diagnostics == ()


def test_parser_marks_unresolved_macro_and_unsupported_proc():
    parsed = parse_sas("data out; set &unknown; run; proc report; run;")

    codes = {diagnostic.code for diagnostic in parsed.diagnostics}
    assert "UNRESOLVED_MACRO_VARIABLE" in codes
    assert "UNSUPPORTED_CONSTRUCT" in codes


def test_parser_reports_unexpanded_macro_control_flow():
    parsed = parse_sas("%macro build; %do i=1 %to 3; data out; set x; run; %end; %mend; %build;")

    assert "UNSUPPORTED_MACRO_CONSTRUCT" in {diagnostic.code for diagnostic in parsed.diagnostics}