"""Two-parser consistency gate — verifies structural and semantic parsers agree."""

from __future__ import annotations

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.sas.parser import parse_sas
from sas2pyspark.services.semantic_ir import _parse_file


def _structural_summary(source: str) -> list[tuple[str, str]]:
    snapshot = parse_sas(source, expand=False)
    return [(node.kind, node.name) for node in snapshot.nodes]


def _semantic_summary(source: str) -> list[tuple[str, str]]:
    program, _ = _parse_file("main.sas", source)
    return [(child.kind, child.name) for child in program.children]


def parse_consistency_check(source: str) -> None:
    structural = _structural_summary(source)
    semantic = _semantic_summary(source)
    structural_kinds = sorted(k for k, _ in structural)
    semantic_kinds = sorted(k for k, _ in semantic)
    if structural_kinds != semantic_kinds:
        raise WorkbenchError(
            WorkbenchErrorCode.INVALID_INPUT,
            "parser consistency gate failed: structural and semantic parsers disagree on statement kinds",
            {"structural": structural_kinds, "semantic": semantic_kinds},
        )
