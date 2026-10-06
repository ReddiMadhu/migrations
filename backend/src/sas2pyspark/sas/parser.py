"""Recursive-descent parser for common SAS statement and block structures."""

from __future__ import annotations

import re
from bisect import bisect_right
from dataclasses import dataclass, field

from sas2pyspark.sas.ir import SasIrNode, SasIrSnapshot, SasParseDiagnostic, SasSourceSpan
from sas2pyspark.sas.macro_expander import expand_macros
from sas2pyspark.sas.tokenizer import SasToken, significant_tokens, tokenize


@dataclass(slots=True)
class _Draft:
    kind: str
    name: str
    start: SasToken
    end: SasToken
    attributes: list[tuple[str, str]] = field(default_factory=list)
    children: list["_Draft"] = field(default_factory=list)


_OPERATION_WORDS = {
    "SET", "MERGE", "BY", "WHERE", "KEEP", "DROP", "RENAME", "FORMAT", "INFORMAT",
    "RETAIN", "ARRAY", "OUTPUT", "LENGTH", "LABEL", "IF", "ELSE", "DO", "END",
}
_SUPPORTED_PROCS = {"SQL", "SORT", "FREQ", "MEANS", "SUMMARY"}


def _span(path: str, source: str, start: SasToken, end: SasToken) -> SasSourceSpan:
    return SasSourceSpan(
        path=path,
        start_offset=start.start_offset,
        end_offset=end.end_offset,
        start_line=start.line,
        start_column=start.column,
        end_line=end.end_line,
        end_column=end.end_column,
    )


def _restore_source_token(token: SasToken, original: str, source_map: tuple[int, ...]) -> SasToken:
    start_offset = source_map[min(token.start_offset, len(source_map) - 1)]
    end_offset = source_map[min(token.end_offset, len(source_map) - 1)]
    line_starts = [0]
    line_starts.extend(match.end() for match in re.finditer(r"\r\n|\n|\r", original))
    start_line_index = max(0, bisect_right(line_starts, start_offset) - 1)
    end_line_index = max(0, bisect_right(line_starts, max(start_offset, end_offset - 1)) - 1)
    return SasToken(
        kind=token.kind,
        value=token.value,
        start_offset=start_offset,
        end_offset=end_offset,
        line=start_line_index + 1,
        column=start_offset - line_starts[start_line_index] + 1,
        end_line=end_line_index + 1,
        end_column=end_offset - line_starts[end_line_index] + 1,
    )


def _statement_groups(tokens: list[SasToken]) -> list[list[SasToken]]:
    groups: list[list[SasToken]] = []
    current: list[SasToken] = []
    for token in tokens:
        if token.kind == "EOF":
            break
        current.append(token)
        if token.value == ";":
            groups.append(current)
            current = []
    if current:
        groups.append(current)
    return groups


def _text(tokens: list[SasToken]) -> str:
    return " ".join(token.value for token in tokens if token.value != ";").strip()


def parse_sas(source: str, *, path: str = "main.sas", expand: bool = True) -> SasIrSnapshot:
    expansion = expand_macros(source) if expand else None
    parse_source = expansion.source if expansion is not None else source
    source_map = expansion.source_map if expansion is not None else tuple(range(len(source) + 1))
    tokens = significant_tokens(tokenize(parse_source))
    groups = _statement_groups(tokens)
    roots: list[_Draft] = []
    active: _Draft | None = None
    macro_stack: list[_Draft] = []
    diagnostics: list[SasParseDiagnostic] = []

    if expansion is not None:
        for macro_match in re.finditer(r"%macro\s+([A-Za-z_][\w]*)", source, re.I):
            mend_match = re.search(r"%mend(?:\s+[A-Za-z_]\w*)?\s*;", source[macro_match.end():], re.I)
            macro_end = macro_match.end() + mend_match.end() if mend_match else len(source)
            macro_start_token = next((token for token in significant_tokens(tokenize(source)) if token.start_offset >= macro_match.start()), None)
            macro_end_token = next((token for token in reversed(significant_tokens(tokenize(source))) if token.end_offset <= macro_end), None)
            if macro_start_token and macro_end_token:
                roots.append(_Draft(
                    "macro",
                    macro_match.group(1),
                    macro_start_token,
                    macro_end_token,
                    [("expanded", "true")],
                ))
        for issue in expansion.issues:
            start = max(0, min(issue.start_offset, len(source)))
            end = max(start, min(issue.end_offset, len(source)))
            start_token = next((token for token in tokenize(source) if token.start_offset >= start and token.kind != "EOF"), SasToken("EOF", "", start, start, 1, 1, 1, 1))
            end_token = next((token for token in reversed(tokenize(source)) if token.end_offset <= end and token.kind != "EOF"), start_token)
            diagnostics.append(SasParseDiagnostic(issue.code, issue.message, _span(path, source, start_token, end_token)))

    for token in tokens:
        if token.kind == "MACRO" and token.value.lower() in {"%do", "%if", "%sysfunc"}:
            original_token = _restore_source_token(token, source, source_map)
            diagnostics.append(SasParseDiagnostic(
                "UNSUPPORTED_MACRO_CONSTRUCT",
                f"{token.value} requires a SAS macro processor and is not statically expanded",
                _span(path, source, original_token, original_token),
            ))

    for group in groups:
        visible = [
            _restore_source_token(token, source, source_map)
            for token in group
            if token.kind != "COMMENT"
        ]
        if not visible:
            continue
        words = [token.value.upper() for token in visible]
        first = words[0]
        statement_text = _text(visible)
        end_token = visible[-1]
        if first == "%MACRO":
            name = visible[1].value if len(visible) > 1 else "anonymous"
            draft = _Draft("macro", name, visible[0], end_token)
            macro_stack.append(draft)
            active = draft
            continue
        if first == "%MEND":
            if macro_stack:
                macro = macro_stack.pop()
                macro.end = end_token
                roots.append(macro)
                active = None
            else:
                diagnostics.append(SasParseDiagnostic("UNMATCHED_MEND", "%MEND has no matching %MACRO", _span(path, source, visible[0], end_token)))
            continue
        if first == "%INCLUDE":
            match = re.search(r"%include\s+(['\"]?)([^;'\"]+)\1", statement_text, re.I)
            target = match.group(2).strip() if match else "unknown"
            node = _Draft("include", target, visible[0], end_token, [("target", target)])
            (active.children if active else roots).append(node)
            continue
        if first == "DATA":
            output = visible[1].value if len(visible) > 1 else "_null_"
            active = _Draft("data-step", output, visible[0], end_token, [("outputs", statement_text[len("data"):].strip())])
            continue
        if first == "PROC":
            proc_name = visible[1].value.upper() if len(visible) > 1 else "UNKNOWN"
            active = _Draft("procedure", proc_name.lower(), visible[0], end_token)
            if proc_name not in _SUPPORTED_PROCS:
                diagnostics.append(SasParseDiagnostic("UNSUPPORTED_CONSTRUCT", f"PROC {proc_name} is not in the supported subset", _span(path, source, visible[0], end_token)))
            continue
        if first in ("RUN", "QUIT"):
            if active is not None:
                active.end = end_token
                (macro_stack[-1].children if macro_stack else roots).append(active)
                active = None
            continue
        kind = "operation" if first in _OPERATION_WORDS else "statement"
        parent = active
        if parent is None and macro_stack:
            parent = macro_stack[-1]
        node = _Draft(kind, first.lower(), visible[0], end_token, [("text", statement_text[:1024])])
        if parent is not None:
            parent.children.append(node)
        else:
            roots.append(node)

    if active is not None:
        diagnostics.append(SasParseDiagnostic("UNTERMINATED_BLOCK", f"{active.kind} block has no RUN or QUIT terminator", _span(path, source, active.start, active.end)))
        (macro_stack[-1].children if macro_stack else roots).append(active)
    for macro in macro_stack:
        diagnostics.append(SasParseDiagnostic("UNTERMINATED_MACRO", f"macro {macro.name} has no %MEND", _span(path, source, macro.start, macro.end)))
        roots.append(macro)

    counter = 0

    def freeze(draft: _Draft) -> SasIrNode:
        nonlocal counter
        counter += 1
        node_id = f"{path}-n{counter:05d}"
        return SasIrNode(
            node_id=node_id,
            kind=draft.kind,
            name=draft.name,
            span=_span(path, source, draft.start, draft.end),
            attributes=tuple(draft.attributes[:32]),
            children=tuple(freeze(child) for child in draft.children),
        )

    return SasIrSnapshot(path=path, nodes=tuple(freeze(root) for root in roots), diagnostics=tuple(diagnostics))
