"""SemanticIrService — bounded structural IR with source spans from SAS parsing."""

from __future__ import annotations

import re
from bisect import bisect_right
from typing import Any, Sequence

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.core.json import assert_safe_segment, snapshot_json
from sas2pyspark.core.events import ArtifactReferenceData
from sas2pyspark.services.scopes import ScopeRef

MAX_SEMANTIC_IR_NODES = 2048
MAX_SEMANTIC_IR_DIAGNOSTICS = 256
MAX_IR_ATTRIBUTES = 32
MAX_IR_NAME_LENGTH = 256
MAX_IR_TEXT_LENGTH = 1024
PARSER_VERSION = "sas-structural-ir-v1"

SemanticIrNodeKind = str  # "program" | "data-step" | "procedure" | "macro" | "include" | "operation" | "statement" | "function-call" | "set" | "merge" | "by" | "first-last" | "retain" | "array" | "do-loop" | "output"

SAS_FUNCTION_CATALOG = frozenset({
    "SCAN", "COMPRESS", "SUBSTR", "CATX", "INTNX", "INTCK", "INPUT", "PUT",
    "PRXCHANGE", "TRIM", "UPCASE", "LOWCASE", "TODAY", "DATE", "MDY",
    "YEAR", "MONTH", "DAY", "CAT", "STRIP",
})

DATA_STEP_KEYWORDS = frozenset({
    "SET", "MERGE", "BY", "RETAIN", "ARRAY", "OUTPUT", "DO", "END",
    "FIRST", "LAST", "KEEP", "DROP", "WHERE", "FORMAT", "IF", "THEN", "ELSE",
})


class SourceSpan:
    __slots__ = ("path", "start_line", "end_line", "start_column", "end_column", "start_offset", "end_offset")

    def __init__(self, **kwargs: Any) -> None:
        self.path = kwargs["path"]
        self.start_line = kwargs["start_line"]
        self.end_line = kwargs["end_line"]
        self.start_column = kwargs["start_column"]
        self.end_column = kwargs["end_column"]
        self.start_offset = kwargs["start_offset"]
        self.end_offset = kwargs["end_offset"]

    def to_dict(self) -> dict[str, Any]:
        return {"path": self.path, "startLine": self.start_line, "endLine": self.end_line, "startColumn": self.start_column, "endColumn": self.end_column, "startOffset": self.start_offset, "endOffset": self.end_offset}


class SemanticIrAttribute:
    __slots__ = ("key", "value")

    def __init__(self, *, key: str, value: str) -> None:
        self.key = key
        self.value = value

    def to_dict(self) -> dict[str, Any]:
        return {"key": self.key, "value": self.value}


class SemanticIrNode:
    __slots__ = ("node_id", "kind", "name", "span", "attributes", "children")

    def __init__(self, **kwargs: Any) -> None:
        self.node_id = kwargs["node_id"]
        self.kind = kwargs["kind"]
        self.name = kwargs["name"]
        self.span = kwargs["span"]
        self.attributes = kwargs["attributes"]
        self.children = kwargs["children"]

    def to_dict(self) -> dict[str, Any]:
        return {"nodeId": self.node_id, "kind": self.kind, "name": self.name, "span": self.span.to_dict(), "attributes": [a.to_dict() for a in self.attributes], "children": list(self.children)}


class SemanticIrFile:
    __slots__ = ("path", "artifact", "line_count")

    def __init__(self, **kwargs: Any) -> None:
        self.path = kwargs["path"]
        self.artifact = kwargs["artifact"]
        self.line_count = kwargs["line_count"]

    def to_dict(self) -> dict[str, Any]:
        return {"path": self.path, "artifact": self.artifact.model_dump(by_alias=True), "lineCount": self.line_count}


class SemanticIrDiagnostic:
    __slots__ = ("code", "message", "span")

    def __init__(self, **kwargs: Any) -> None:
        self.code = kwargs["code"]
        self.message = kwargs["message"]
        self.span = kwargs["span"]

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "span": self.span.to_dict()}


class SemanticIrSnapshot:
    __slots__ = ("ir_id", "graph_id", "migration_id", "scope_id", "root_path", "parser_version", "files", "nodes", "diagnostics")

    def __init__(self, **kwargs: Any) -> None:
        self.ir_id = kwargs["ir_id"]
        self.graph_id = kwargs["graph_id"]
        self.migration_id = kwargs["migration_id"]
        self.scope_id = kwargs["scope_id"]
        self.root_path = kwargs["root_path"]
        self.parser_version = kwargs["parser_version"]
        self.files = kwargs["files"]
        self.nodes = kwargs["nodes"]
        self.diagnostics = kwargs["diagnostics"]

    def to_dict(self) -> dict[str, Any]:
        return {"irId": self.ir_id, "graphId": self.graph_id, "migrationId": self.migration_id, "scopeId": self.scope_id, "rootPath": self.root_path, "parserVersion": self.parser_version, "files": [f.to_dict() for f in self.files], "nodes": [n.to_dict() for n in self.nodes], "diagnostics": [d.to_dict() for d in self.diagnostics]}


def _bounded_text(value: Any, field: str, max_len: int, code: WorkbenchErrorCode) -> str:
    if not isinstance(value, str) or len(value) == 0 or len(value) > max_len or re.search(r"[\x00-\x1f\x7f]", value):
        raise WorkbenchError(code, f"{field} must be a bounded non-empty string", {"field": field, "max": max_len})
    return value


def _normalize_logical_path(value: Any, field: str, code: WorkbenchErrorCode) -> str:
    text = _bounded_text(value, field, 1024, code).replace("\\", "/")
    if text.startswith("/") or re.match(r"^[A-Za-z]:/", text):
        raise WorkbenchError(code, f"{field} must be relative", {"field": field, "path": text})
    parts: list[str] = []
    for part in text.split("/"):
        if part == "" or part == ".":
            continue
        if part == "..":
            raise WorkbenchError(code, f"{field} cannot escape the workspace", {"field": field, "path": text})
        parts.append(part)
    if not parts:
        raise WorkbenchError(code, f"{field} cannot be empty", {"field": field, "path": text})
    return "/".join(parts)


def _line_infos(text: str) -> list[dict[str, Any]]:
    delimiters = list(re.finditer(r"\r\n|\n|\r", text))
    result: list[dict[str, Any]] = []
    block_state = {"block": False}
    start = 0
    for i in range(len(delimiters) + 1):
        delim = delimiters[i] if i < len(delimiters) else None
        end = delim.start() if delim else len(text)
        raw = text[start:end]
        visible = _mask_sas_line(raw, block_state)
        result.append({"text": raw, "visible": visible, "line": i + 1, "startOffset": start, "endOffset": end})
        if delim is None:
            break
        start = end + len(delim.group())
    if not result:
        result.append({"text": "", "visible": "", "line": 1, "startOffset": 0, "endOffset": 0})
    return result


def _mask_sas_line(raw: str, state: dict[str, bool]) -> str:
    if not state["block"] and raw.lstrip().startswith("%*"):
        return " " * len(raw)
    chars = list(raw)
    quote: str | None = None
    i = 0
    while i < len(chars):
        ch = chars[i]
        nxt = chars[i + 1] if i + 1 < len(chars) else None
        if state["block"]:
            if ch == "*" and nxt == "/":
                chars[i] = " "
                chars[i + 1] = " "
                state["block"] = False
                i += 2
                continue
            chars[i] = " "
            i += 1
            continue
        if quote is not None:
            if ch == quote:
                quote = None
            i += 1
            continue
        if ch == "/" and nxt == "*":
            chars[i] = " "
            chars[i + 1] = " "
            state["block"] = True
            i += 2
            continue
        if ch == "/" and nxt == "/":
            for rest in range(i, len(chars)):
                chars[rest] = " "
            break
        if ch in ("'", '"'):
            quote = ch
        i += 1
    return "".join(chars)


def _span(lines: list[dict[str, Any]], start_idx: int, end_idx: int, path: str) -> SourceSpan:
    start = lines[start_idx] if start_idx < len(lines) else lines[-1]
    end = lines[end_idx] if end_idx < len(lines) else lines[-1]
    return SourceSpan(path=path, start_line=start["line"], end_line=end["line"], start_column=1, end_column=len(end["text"]) + 1, start_offset=start["startOffset"], end_offset=end["endOffset"])


def _attr(key: str, value: str) -> SemanticIrAttribute:
    return SemanticIrAttribute(key=key, value=value)


def _statement_name(text: str, fallback: str) -> str:
    match = re.match(r"^\s*([A-Za-z_%][A-Za-z0-9_.%]*)", text)
    return (match.group(1) if match else fallback)[:MAX_IR_NAME_LENGTH]


class _DraftNode:
    __slots__ = ("kind", "name", "span", "attributes", "children")

    def __init__(self, *, kind: str, name: str, span: SourceSpan, attributes: list[SemanticIrAttribute], children: list["_DraftNode"]) -> None:
        self.kind = kind
        self.name = name
        self.span = span
        self.attributes = attributes
        self.children = children


def _find_block_end(lines: list[dict[str, Any]], start: int, matcher: re.Pattern[str]) -> int:
    for i in range(start + 1, len(lines)):
        if matcher.match(lines[i]["visible"].strip() or ""):
            return i
    return len(lines) - 1


def _sas_statements(text: str) -> list[tuple[str, int, int]]:
    statements: list[tuple[str, int, int]] = []
    buffer: list[str] = []
    start = 0
    quote: str | None = None
    block_comment = False
    line_comment = False
    statement_comment = False
    index = 0
    while index < len(text):
        char = text[index]
        next_char = text[index + 1] if index + 1 < len(text) else ""
        if line_comment:
            if char in "\r\n":
                line_comment = False
                buffer.append(" ")
            index += 1
            continue
        if statement_comment:
            if char == ";":
                statement_comment = False
                start = index + 1
                buffer.clear()
            index += 1
            continue
        if block_comment:
            if char == "*" and next_char == "/":
                block_comment = False
                buffer.extend((" ", " "))
                index += 2
            else:
                buffer.append("\n" if char in "\r\n" else " ")
                index += 1
            continue
        if quote is not None:
            buffer.append(char)
            if char == quote:
                if next_char == quote:
                    buffer.append(next_char)
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if char in ("'", '"'):
            quote = char
            buffer.append(char)
            index += 1
            continue
        if char == "/" and next_char == "*":
            block_comment = True
            buffer.extend((" ", " "))
            index += 2
            continue
        if char == "%" and next_char == "*" and not "".join(buffer).strip():
            statement_comment = True
            index += 2
            continue
        if char == "/" and next_char == "/":
            line_comment = True
            index += 2
            continue
        if char == "*" and not "".join(buffer).strip():
            statement_comment = True
            index += 1
            continue
        buffer.append(char)
        if char == ";":
            value = "".join(buffer).strip()
            if value:
                leading = len("".join(buffer)) - len("".join(buffer).lstrip())
                statements.append((value, start + leading, index + 1))
            buffer.clear()
            start = index + 1
        index += 1
    trailing = "".join(buffer).strip()
    if trailing:
        leading = len("".join(buffer)) - len("".join(buffer).lstrip())
        statements.append((trailing, start + leading, len(text)))
    return statements


def _span_for_offsets(text: str, start: int, end: int, path: str) -> SourceSpan:
    line_starts = [0]
    line_starts.extend(match.end() for match in re.finditer(r"\r\n|\n|\r", text))
    start_line_index = bisect_right(line_starts, start) - 1
    end_line_index = bisect_right(line_starts, max(start, end - 1)) - 1
    return SourceSpan(
        path=path,
        start_line=start_line_index + 1,
        end_line=end_line_index + 1,
        start_column=start - line_starts[start_line_index] + 1,
        end_column=end - line_starts[end_line_index] + 1,
        start_offset=start,
        end_offset=end,
    )


def _dynamic_statement(text: str) -> bool:
    without_strings = re.sub(r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"", " ", text)
    return bool(re.search(r"\b(?:call\s+execute|%sysexec|options\s+insert)\b", without_strings, re.I))


def _body_nodes(lines: list[dict[str, Any]], start: int, end: int, path: str, parent: _DraftNode, diagnostics: list[SemanticIrDiagnostic]) -> None:
    for i in range(start, end + 1):
        if i >= len(lines):
            continue
        line = lines[i]
        text = line["visible"].strip()
        if text == "" or re.match(r"^%mend\b", text, re.I) or re.match(r"^run\s*;", text, re.I):
            continue
        if re.search(r"\b(?:call\s+execute|%sysexec|options\s+insert)\b", text, re.I):
            diagnostics.append(SemanticIrDiagnostic(code="DYNAMIC_CODE", message="dynamic SAS code requires a future resolver", span=_span(lines, i, i, path)))
        kind = "operation" if re.match(r"^(set|merge|join|by|where|keep|drop|format|sort|group|summary|retain|array|output)\b", text, re.I) else "statement"
        parent.children.append(_DraftNode(kind=kind, name=_statement_name(text, "statement"), span=_span(lines, i, i, path), attributes=[], children=[]))


def _parse_file(path: str, text: str) -> tuple[_DraftNode, list[SemanticIrDiagnostic]]:
    diagnostics: list[SemanticIrDiagnostic] = []
    program = _DraftNode(
        kind="program", name=path,
        span=_span_for_offsets(text, 0, len(text), path),
        attributes=[], children=[],
    )
    statements = _sas_statements(text)
    active: _DraftNode | None = None
    active_start = 0
    active_kind = ""
    for source, start, end in statements:
        statement = source.strip()
        if not statement:
            continue
        normalized = statement.rstrip(";").strip()
        span = _span_for_offsets(text, start, end, path)
        if active is not None:
            if active_kind == "macro" and re.match(r"^%mend\b", normalized, re.I):
                active.span = _span_for_offsets(text, active_start, end, path)
                program.children.append(active)
                active = None
                active_kind = ""
                continue
            if active_kind in ("data-step", "procedure") and re.match(r"^(?:run|quit)\b", normalized, re.I):
                active.span = _span_for_offsets(text, active_start, end, path)
                program.children.append(active)
                active = None
                active_kind = ""
                continue
            if _dynamic_statement(statement):
                diagnostics.append(SemanticIrDiagnostic(
                    code="DYNAMIC_CODE",
                    message="dynamic SAS code requires a future resolver",
                    span=span,
                ))
            if re.search(r"&[A-Za-z_][A-Za-z0-9_]*", statement) or re.match(r"^%(?:if|let|do|global|local)\b", normalized, re.I):
                diagnostics.append(SemanticIrDiagnostic(
                    code="MACRO_EXPANSION_REQUIRED",
                    message="SAS macro references require expansion before translation",
                    span=span,
                ))
            kind = "operation" if re.match(
                r"^(set|merge|join|by|where|keep|drop|format|sort|group|summary|retain|array|output)\b",
                normalized, re.I,
            ) else "statement"
            first_word = normalized.split()[0].upper() if normalized.split() else ""
            if first_word == "SET":
                kind = "set"
            elif first_word == "MERGE":
                kind = "merge"
            elif first_word == "BY":
                kind = "by"
            elif first_word == "RETAIN":
                kind = "retain"
            elif first_word == "ARRAY":
                kind = "array"
            elif first_word == "OUTPUT":
                kind = "output"
            elif first_word == "DO":
                kind = "do-loop"
            elif re.match(r"^first\.\w+|^last\.\w+", normalized, re.I):
                kind = "first-last"
            func_matches = re.findall(r"\b(" + "|".join(SAS_FUNCTION_CATALOG) + r")\s*\(", statement, re.I)
            func_children = []
            for func_name in func_matches:
                func_children.append(_DraftNode(
                    kind="function-call",
                    name=func_name.upper(),
                    span=span,
                    attributes=[_attr("function", func_name.upper())],
                    children=[],
                ))
            active.children.append(_DraftNode(
                kind=kind,
                name=_statement_name(normalized, "statement"),
                span=span,
                attributes=[],
                children=func_children,
            ))
            continue

        include = re.match(r'^%include\s+(?:"([^"]+)"|\'([^\']+)\'|([^\s;]+))', normalized, re.I)
        if include:
            target = include.group(1) or include.group(2) or include.group(3) or "unknown"
            program.children.append(_DraftNode(
                kind="include",
                name=_statement_name(target, "include"),
                span=span,
                attributes=[_attr("target", target)],
                children=[],
            ))
            continue

        macro = re.match(r"^%macro\s+([A-Za-z_][\w]*)(?:\s*\(([^)]*)\))?", normalized, re.I)
        if macro:
            name = macro.group(1)
            diagnostics.append(SemanticIrDiagnostic(
                code="MACRO_EXPANSION_REQUIRED",
                message="SAS macro definitions require expansion before translation",
                span=span,
            ))
            active = _DraftNode(
                kind="macro", name=name, span=span,
                attributes=[_attr("parameters", macro.group(2))] if macro.group(2) else [],
                children=[],
            )
            active_start = start
            active_kind = "macro"
            continue

        data = re.match(r"^data\b(?:\s+(.+))?", normalized, re.I)
        if data:
            outputs = (data.group(1) or "_null_").strip()
            active = _DraftNode(
                kind="data-step",
                name=_statement_name(outputs, "_null_"),
                span=span,
                attributes=[_attr("outputs", outputs)] if data.group(1) else [],
                children=[],
            )
            active_start = start
            active_kind = "data-step"
            continue

        proc = re.match(r"^proc\s+([A-Za-z_][\w]*)", normalized, re.I)
        if proc:
            active = _DraftNode(kind="procedure", name=proc.group(1), span=span, attributes=[], children=[])
            active_start = start
            active_kind = "procedure"
            continue

        if _dynamic_statement(statement):
            diagnostics.append(SemanticIrDiagnostic(
                code="DYNAMIC_CODE",
                message="dynamic SAS code requires a future resolver",
                span=span,
            ))
        elif re.search(r"&[A-Za-z_][A-Za-z0-9_]*", statement) or re.match(r"^%[A-Za-z_][A-Za-z0-9_]*", normalized):
            diagnostics.append(SemanticIrDiagnostic(
                code="MACRO_EXPANSION_REQUIRED",
                message="SAS macro references require expansion before translation",
                span=span,
            ))
        program.children.append(_DraftNode(
            kind="statement",
            name=_statement_name(normalized, "statement"),
            span=span,
            attributes=[],
            children=[],
        ))

    if active is not None:
        active.span = _span_for_offsets(text, active_start, len(text), path)
        diagnostics.append(SemanticIrDiagnostic(
            code="UNTERMINATED_BLOCK",
            message=f"{active_kind} block has no closing terminator",
            span=active.span,
        ))
        program.children.append(active)
    return program, diagnostics


def _flatten(programs: list[_DraftNode], ir_id: str) -> list[SemanticIrNode]:
    nodes: list[SemanticIrNode] = []
    seq = [0]

    def visit(draft: _DraftNode) -> str:
        if len(nodes) >= MAX_SEMANTIC_IR_NODES:
            raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, "semantic IR node inventory exceeds its limit", {"max": MAX_SEMANTIC_IR_NODES})
        seq[0] += 1
        node_id = f"{ir_id}-n{seq[0]:05d}"
        children = [visit(child) for child in draft.children]
        nodes.append(SemanticIrNode(node_id=node_id, kind=draft.kind, name=draft.name, span=draft.span, attributes=draft.attributes[:MAX_IR_ATTRIBUTES], children=children))
        return node_id

    for program in programs:
        visit(program)
    return nodes


class SemanticIrService:
    def __init__(self) -> None:
        pass

    def create(
        self, *,
        session: Any, scope: ScopeRef, graph: Any, root_path: str, ir_id: str | None = None,
    ) -> SemanticIrSnapshot:
        scope = session._scopes.require_by_id(scope.id) if hasattr(session, "_scopes") else scope
        iid = assert_safe_segment(ir_id or f"ir-{session._clock.uuid()}", "irId") if hasattr(session, "_clock") else (ir_id or "ir")
        files: list[SemanticIrFile] = []
        programs: list[_DraftNode] = []
        all_diagnostics: list[SemanticIrDiagnostic] = []
        for f in graph.files:
            if not f.is_sas:
                continue
            if hasattr(session, "_artifacts"):
                text = session._artifacts.read_reference(scope, f.artifact).decode("utf-8")
            else:
                text = ""
            line_count = len(re.split(r"\r\n|\n|\r", text)) if text else 1
            files.append(SemanticIrFile(path=f.path, artifact=f.artifact, line_count=line_count))
            program, diags = _parse_file(f.path, text)
            programs.append(program)
            all_diagnostics.extend(diags)
        nodes = _flatten(programs, iid)
        return SemanticIrSnapshot(
            ir_id=iid, graph_id=graph.graph_id, migration_id=session.migration_id,
            scope_id=scope.id, root_path=root_path, parser_version=PARSER_VERSION,
            files=files, nodes=nodes, diagnostics=all_diagnostics[:MAX_SEMANTIC_IR_DIAGNOSTICS],
        )


def _span_from_dict(d: dict[str, Any]) -> SourceSpan:
    return SourceSpan(
        path=d["path"],
        start_line=d["startLine"],
        end_line=d["endLine"],
        start_column=d["startColumn"],
        end_column=d["endColumn"],
        start_offset=d["startOffset"],
        end_offset=d["endOffset"],
    )


def semantic_ir_from_event(value: Any) -> SemanticIrSnapshot:
    if not isinstance(value, dict):
        raise WorkbenchError(WorkbenchErrorCode.SESSION_CORRUPTION, "semantic IR event data is invalid")
    raw = value.get("ir", value)
    files = [SemanticIrFile(path=f["path"], artifact=ArtifactReferenceData(**f["artifact"]), line_count=f["lineCount"]) for f in raw.get("files", [])]
    nodes = []
    for n in raw.get("nodes", []):
        nodes.append(SemanticIrNode(
            node_id=n["nodeId"], kind=n["kind"], name=n["name"],
            span=_span_from_dict(n["span"]),
            attributes=[SemanticIrAttribute(key=a["key"], value=a["value"]) for a in n.get("attributes", [])],
            children=n.get("children", []),
        ))
    diagnostics = []
    for d in raw.get("diagnostics", []):
        diagnostics.append(SemanticIrDiagnostic(code=d["code"], message=d["message"], span=_span_from_dict(d["span"])))
    return SemanticIrSnapshot(
        ir_id=raw["irId"], graph_id=raw["graphId"], migration_id=raw["migrationId"],
        scope_id=raw["scopeId"], root_path=raw["rootPath"], parser_version=raw["parserVersion"],
        files=files, nodes=nodes, diagnostics=diagnostics,
    )
