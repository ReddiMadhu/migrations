"""Conservative SAS %LET and parameterized macro expansion."""

from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class MacroPolicy:
    max_macro_iterations: int = 1000
    max_passes: int = 8


@dataclass(frozen=True, slots=True)
class MacroIssue:
    code: str
    message: str
    start_offset: int
    end_offset: int


@dataclass(frozen=True, slots=True)
class MacroExpansion:
    source: str
    source_map: tuple[int, ...]
    issues: tuple[MacroIssue, ...]
    definitions: tuple[str, ...]
    expansions: tuple[str, ...]


_LET_RE = re.compile(r"%let\s+([A-Za-z_][\w]*)\s*=\s*(.*?);", re.I | re.S)
_MACRO_DEF_RE = re.compile(r"%macro\s+([A-Za-z_][\w]*)\s*(?:\(([^)]*)\))?\s*;(.*?)%mend(?:\s+\w+)?\s*;", re.I | re.S)
_BUILTIN_MACROS = {"include", "let", "if", "then", "else", "do", "end", "put", "abort", "sysfunc", "eval"}


def _split_arguments(raw: str) -> list[str]:
    values: list[str] = []
    current: list[str] = []
    quote: str | None = None
    depth = 0
    for char in raw:
        if quote:
            current.append(char)
            if char == quote:
                quote = None
        elif char in "'\"":
            quote = char
            current.append(char)
        elif char == "(":
            depth += 1
            current.append(char)
        elif char == ")":
            depth = max(0, depth - 1)
            current.append(char)
        elif char == "," and depth == 0:
            values.append("".join(current).strip())
            current = []
        else:
            current.append(char)
    if current or raw.strip():
        values.append("".join(current).strip())
    return values


def _expand_calls(
    source: str,
    definitions: dict[str, tuple[list[str], str]],
    issues: list[MacroIssue],
) -> tuple[str, bool]:
    output: list[str] = []
    index = 0
    changed = False
    while index < len(source):
        if source[index] != "%" or index + 1 >= len(source) or not (source[index + 1].isalpha() or source[index + 1] == "_"):
            output.append(source[index])
            index += 1
            continue
        name_match = re.match(r"%([A-Za-z_][A-Za-z0-9_]*)", source[index:])
        if name_match is None:
            output.append(source[index])
            index += 1
            continue
        name = name_match.group(1)
        name_end = index + len(name_match.group(0))
        cursor = name_end
        while cursor < len(source) and source[cursor].isspace():
            cursor += 1
        args: list[str] = []
        end = name_end
        if cursor < len(source) and source[cursor] == "(":
            depth = 1
            quote: str | None = None
            close = cursor + 1
            while close < len(source) and depth:
                char = source[close]
                if quote is not None:
                    if char == quote:
                        if close + 1 < len(source) and source[close + 1] == quote:
                            close += 2
                            continue
                        quote = None
                elif char in "'\"":
                    quote = char
                elif char == "(":
                    depth += 1
                elif char == ")":
                    depth -= 1
                close += 1
            if depth:
                output.append(source[index:name_end])
                index = name_end
                continue
            args = _split_arguments(source[cursor + 1:close - 1])
            end = close
        while end < len(source) and source[end].isspace():
            end += 1
        if end < len(source) and source[end] == ";":
            end += 1

        macro = definitions.get(name.lower())
        if macro is None:
            if name.lower() not in _BUILTIN_MACROS:
                issues.append(MacroIssue("UNRESOLVED_MACRO", f"macro {name} has no static definition", index, end))
            output.append(source[index:end])
            index = end
            continue
        params, body = macro
        if len(args) > len(params):
            issues.append(MacroIssue("MACRO_ARGUMENT_MISMATCH", f"macro {name} received too many arguments", index, end))
            output.append(source[index:end])
            index = end
            continue
        replacement = body
        for arg_index, parameter in enumerate(params):
            value = args[arg_index] if arg_index < len(args) else ""
            replacement = re.sub(
                rf"[&%]{re.escape(parameter)}(?:\.|\b)",
                lambda _match, value=value: value,
                replacement,
                flags=re.I,
            )
        output.append(replacement)
        changed = True
        index = end
    return "".join(output), changed


def _offset_map(original: str, expanded: str) -> tuple[int, ...]:
    mapping = [0] * (len(expanded) + 1)
    orig_len = len(original)
    exp_len = len(expanded)
    if orig_len == 0:
        return tuple(mapping)
    if exp_len <= orig_len:
        for i in range(exp_len + 1):
            mapping[i] = i
    else:
        ratio = orig_len / exp_len
        for i in range(exp_len + 1):
            mapping[i] = min(orig_len, int(i * ratio))
    mapping[-1] = orig_len
    return tuple(mapping)


_DO_WHILE_RE = re.compile(r"%do\s+%while\s*\((.*?)\)\s*;(.*?)(%end\s*;)", re.I | re.S)
_DO_UNTIL_RE = re.compile(r"%do\s+%until\s*\((.*?)\)\s*;(.*?)(%end\s*;)", re.I | re.S)
_EVAL_RE = re.compile(r"%eval\s*\((.*?)\)", re.I)


def _safe_eval_expr(expr: str) -> int | None:
    expr = expr.strip()
    if re.match(r"^[\d\s+\-*/<>=%()]+$", expr):
        try:
            if re.search(r"[<>=]", expr):
                return 1 if eval(expr.replace("=", "==").replace("===", "==")) else 0
            return int(eval(expr))
        except Exception:
            return None
    return None


def _expand_bounded_loops(source: str, policy: MacroPolicy, issues: list[MacroIssue]) -> str:
    def expand_do_while(match: re.Match[str]) -> str:
        condition = match.group(1).strip()
        body = match.group(2)
        condition_eval = _safe_eval_expr(condition)
        if condition_eval is None:
            return match.group(0)
        result = ""
        iterations = 0
        while condition_eval and iterations < policy.max_macro_iterations:
            result += body
            iterations += 1
            condition_eval = _safe_eval_expr(condition)
        if iterations >= policy.max_macro_iterations:
            issues.append(MacroIssue("MACRO_LOOP_LIMIT", f"%DO %WHILE exceeded {policy.max_macro_iterations} iterations", match.start(), match.end()))
        return result

    def expand_do_until(match: re.Match[str]) -> str:
        condition = match.group(1).strip()
        body = match.group(2)
        condition_eval = _safe_eval_expr(condition)
        if condition_eval is None:
            return match.group(0)
        result = ""
        iterations = 0
        while not condition_eval and iterations < policy.max_macro_iterations:
            result += body
            iterations += 1
            condition_eval = _safe_eval_expr(condition)
        if iterations >= policy.max_macro_iterations:
            issues.append(MacroIssue("MACRO_LOOP_LIMIT", f"%DO %UNTIL exceeded {policy.max_macro_iterations} iterations", match.start(), match.end()))
        return result

    def expand_eval(match: re.Match[str]) -> str:
        expr = match.group(1).strip()
        result = _safe_eval_expr(expr)
        return str(result) if result is not None else match.group(0)

    source = _DO_WHILE_RE.sub(expand_do_while, source)
    source = _DO_UNTIL_RE.sub(expand_do_until, source)
    source = _EVAL_RE.sub(expand_eval, source)
    return source


def expand_macros(source: str, *, max_passes: int = 8, policy: MacroPolicy | None = None) -> MacroExpansion:
    pol = policy or MacroPolicy(max_passes=max_passes)
    issues: list[MacroIssue] = []
    definitions: dict[str, tuple[list[str], str]] = {}
    definition_names: list[str] = []
    for match in _MACRO_DEF_RE.finditer(source):
        name = match.group(1)
        params = [item.split("=", 1)[0].strip() for item in _split_arguments(match.group(2) or "")]
        definitions[name.lower()] = (params, match.group(3))
        definition_names.append(name)
    without_definitions = _MACRO_DEF_RE.sub("", source)

    variables: dict[str, str] = {}
    def remove_let(match: re.Match[str]) -> str:
        variables[match.group(1).lower()] = match.group(2).strip()
        return " " * len(match.group(0))

    expanded = _LET_RE.sub(remove_let, without_definitions)
    expanded = _expand_bounded_loops(expanded, pol, issues)
    for pass_number in range(pol.max_passes):
        expanded, changed = _expand_calls(expanded, definitions, issues)
        if not changed:
            break
        if pass_number == pol.max_passes - 1:
            issues.append(MacroIssue("MACRO_EXPANSION_LIMIT", "macro expansion exceeded the pass limit", 0, len(source)))

    def replace_var(match: re.Match[str]) -> str:
        key = match.group(1).lower()
        if key not in variables:
            issues.append(MacroIssue("UNRESOLVED_MACRO_VARIABLE", f"macro variable &{match.group(1)} has no static %LET", match.start(), match.end()))
            return match.group(0)
        return variables[key]

    def replace_var_scoped(text: str) -> str:
        result: list[str] = []
        i = 0
        in_string: str | None = None
        in_comment = False
        while i < len(text):
            if in_comment:
                if text[i:i + 2] == "*/":
                    in_comment = False
                    result.append("*/")
                    i += 2
                    continue
                result.append(text[i])
                i += 1
                continue
            if in_string:
                if text[i] == in_string:
                    in_string = None
                result.append(text[i])
                i += 1
                continue
            if text[i:i + 2] == "/*":
                in_comment = True
                result.append("/*")
                i += 2
                continue
            if text[i] in "'\"":
                in_string = text[i]
                result.append(text[i])
                i += 1
                continue
            if text[i] == "&":
                var_match = re.match(r"&([A-Za-z_][A-Za-z0-9_]*)\.?", text[i:])
                if var_match:
                    key = var_match.group(1).lower()
                    if key in variables:
                        result.append(variables[key])
                    else:
                        issues.append(MacroIssue("UNRESOLVED_MACRO_VARIABLE", f"macro variable &{var_match.group(1)} has no static %LET", i, i + len(var_match.group(0))))
                        result.append(var_match.group(0))
                    i += len(var_match.group(0))
                    continue
            result.append(text[i])
            i += 1
        return "".join(result)

    expanded = replace_var_scoped(expanded)

    return MacroExpansion(
        source=expanded,
        source_map=_offset_map(source, expanded),
        issues=tuple(issues),
        definitions=tuple(definition_names),
        expansions=tuple(sorted(variables)),
    )
