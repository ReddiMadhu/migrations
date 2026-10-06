"""Immutable intermediate representation for parsed SAS source."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class SasSourceSpan:
    path: str
    start_offset: int
    end_offset: int
    start_line: int
    start_column: int
    end_line: int
    end_column: int


@dataclass(frozen=True, slots=True)
class SasIrNode:
    node_id: str
    kind: str
    name: str
    span: SasSourceSpan
    attributes: tuple[tuple[str, str], ...] = ()
    children: tuple["SasIrNode", ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "nodeId": self.node_id,
            "kind": self.kind,
            "name": self.name,
            "span": {
                "path": self.span.path,
                "startOffset": self.span.start_offset,
                "endOffset": self.span.end_offset,
                "startLine": self.span.start_line,
                "startColumn": self.span.start_column,
                "endLine": self.span.end_line,
                "endColumn": self.span.end_column,
            },
            "attributes": [{"key": key, "value": value} for key, value in self.attributes],
            "children": [child.node_id for child in self.children],
        }


@dataclass(frozen=True, slots=True)
class SasParseDiagnostic:
    code: str
    message: str
    span: SasSourceSpan

    def to_dict(self) -> dict[str, object]:
        return {
            "code": self.code,
            "message": self.message,
            "span": {
                "path": self.span.path,
                "startOffset": self.span.start_offset,
                "endOffset": self.span.end_offset,
                "startLine": self.span.start_line,
                "startColumn": self.span.start_column,
                "endLine": self.span.end_line,
                "endColumn": self.span.end_column,
            },
        }


@dataclass(frozen=True, slots=True)
class SasIrSnapshot:
    path: str
    nodes: tuple[SasIrNode, ...]
    diagnostics: tuple[SasParseDiagnostic, ...] = field(default_factory=tuple)
