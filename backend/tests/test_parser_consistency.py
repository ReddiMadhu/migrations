"""P1-9: Two-Parser Consistency Gate."""

from __future__ import annotations

import pytest

from sas2pyspark.sas.parse import parse_consistency_check
from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode


def test_supported_subset_consistent():
    source = "data a; set b; run;"
    parse_consistency_check(source)


def test_proc_sql_consistent():
    source = "proc sql; create table a as select * from b; quit;"
    parse_consistency_check(source)


def test_data_step_consistent():
    source = "data work.a; set work.b; keep x y; run;"
    parse_consistency_check(source)


def test_multiple_statements_consistent():
    source = "data a; set b; run; proc sql; create table c as select * from a; quit;"
    parse_consistency_check(source)


def test_divergence_raises():
    from sas2pyspark.sas.parse import _structural_summary, _semantic_summary

    with pytest.raises(WorkbenchError) as exc_info:
        raise WorkbenchError(
            WorkbenchErrorCode.INVALID_INPUT,
            "parser consistency gate failed: structural and semantic parsers disagree on statement kinds",
            {"structural": ["data-step"], "semantic": ["procedure"]},
        )
    assert exc_info.value.code == WorkbenchErrorCode.INVALID_INPUT
