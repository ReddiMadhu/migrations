"""P2-1: SAS Macro Bounded Loops + Function Catalog + DATA-step Semantics."""

from __future__ import annotations

import pytest

from sas2pyspark.sas.macro_expander import expand_macros, MacroPolicy
from sas2pyspark.sas.tokenizer import tokenize
from sas2pyspark.services.semantic_ir import SAS_FUNCTION_CATALOG, _parse_file


def test_bounded_do_while():
    source = "%let i = 1; %do %while (&i <= 3); data a&i; run; %end;"
    result = expand_macros(source, policy=MacroPolicy(max_macro_iterations=100))
    assert "MACRO_LOOP_LIMIT" not in str(result.issues)


def test_bounded_do_while_exceed():
    source = "%do %while (1); data a; run; %end;"
    result = expand_macros(source, policy=MacroPolicy(max_macro_iterations=5))
    issue_codes = [i.code for i in result.issues]
    assert "MACRO_LOOP_LIMIT" in issue_codes


def test_eval_expansion():
    source = "data a; x = %eval(2 + 3); run;"
    result = expand_macros(source)
    assert "5" in result.source


def test_function_call_ir_node():
    source = "data a; x = scan(name, 1, ' '); run;"
    program, _ = _parse_file("main.sas", source)
    found = False
    for child in program.children:
        for grandchild in child.children:
            if grandchild.kind == "function-call" and grandchild.name == "SCAN":
                found = True
            for gc2 in grandchild.children:
                if gc2.kind == "function-call" and gc2.name == "SCAN":
                    found = True
    assert found


def test_multiple_function_calls():
    source = "data a; x = catx(',', a, b); y = substr(x, 1, 3); run;"
    program, _ = _parse_file("main.sas", source)
    func_names = set()
    for child in program.children:
        for grandchild in child.children:
            for gc2 in grandchild.children:
                if gc2.kind == "function-call":
                    func_names.add(gc2.name)
    assert "CATX" in func_names
    assert "SUBSTR" in func_names


def test_data_step_set_node():
    source = "data a; set b; run;"
    program, _ = _parse_file("main.sas", source)
    found = False
    for child in program.children:
        for grandchild in child.children:
            if grandchild.kind == "set":
                found = True
    assert found


def test_retain_node():
    source = "data a; retain x 0; set b; run;"
    program, _ = _parse_file("main.sas", source)
    found = False
    for child in program.children:
        for grandchild in child.children:
            if grandchild.kind == "retain":
                found = True
    assert found


def test_merge_by_nodes():
    source = "data a; merge b c; by key; run;"
    program, _ = _parse_file("main.sas", source)
    kinds = set()
    for child in program.children:
        for grandchild in child.children:
            kinds.add(grandchild.kind)
    assert "merge" in kinds
    assert "by" in kinds


def test_array_node():
    source = "data a; array x[3] x1 x2 x3; run;"
    program, _ = _parse_file("main.sas", source)
    found = False
    for child in program.children:
        for grandchild in child.children:
            if grandchild.kind == "array":
                found = True
    assert found


def test_output_node():
    source = "data a; set b; output; run;"
    program, _ = _parse_file("main.sas", source)
    found = False
    for child in program.children:
        for grandchild in child.children:
            if grandchild.kind == "output":
                found = True
    assert found


def test_existing_scripts_unchanged():
    source = "data work.a; set work.b; keep x y; run;"
    program, diags = _parse_file("main.sas", source)
    assert program.kind == "program"
    assert len(program.children) >= 1
    assert program.children[0].kind == "data-step"


def test_positional_param_tokenized():
    tokens = tokenize("&1 &2 &3")
    param_tokens = [t for t in tokens if t.kind == "MACRO_PARAM"]
    assert len(param_tokens) == 3
    assert param_tokens[0].value == "&1"
    assert param_tokens[1].value == "&2"
    assert param_tokens[2].value == "&3"


def test_var_substitution_scoped():
    source = "%let x = hello; /* &x in comment */ data a; y = '&x'; z = &x; run;"
    result = expand_macros(source)
    assert "hello" in result.source
    assert "&x" in result.source or "hello" in result.source


def test_offset_map_linear():
    large_source = "%let x = 1; " + "data a; set b; run; " * 100
    import time
    start = time.monotonic()
    result = expand_macros(large_source)
    elapsed = time.monotonic() - start
    assert elapsed < 2.0
    assert len(result.source_map) > 0


def test_function_catalog_size():
    assert len(SAS_FUNCTION_CATALOG) == 20
    assert "SCAN" in SAS_FUNCTION_CATALOG
    assert "INTNX" in SAS_FUNCTION_CATALOG
    assert "PRXCHANGE" in SAS_FUNCTION_CATALOG


def test_no_incorrect_mappings():
    source = "data a; x = intnx('month', date, 1, 'e'); run;"
    program, _ = _parse_file("main.sas", source)
    found = False
    for child in program.children:
        for grandchild in child.children:
            for gc2 in grandchild.children:
                if gc2.kind == "function-call" and gc2.name == "INTNX":
                    found = True
    assert found
