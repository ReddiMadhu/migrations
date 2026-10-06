"""Position-preserving tokenizer for the supported SAS source subset."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True, slots=True)
class SasToken:
    kind: str
    value: str
    start_offset: int
    end_offset: int
    line: int
    column: int
    end_line: int
    end_column: int


_KEYWORDS = {
    "DATA", "PROC", "RUN", "QUIT", "SET", "MERGE", "BY", "WHERE", "IF", "THEN",
    "ELSE", "DO", "END", "RETAIN", "KEEP", "DROP", "RENAME", "FORMAT", "INFORMAT",
    "LIBNAME", "OUTPUT", "ARRAY", "LENGTH", "LABEL", "FREQ", "MEANS", "SUMMARY",
    "SORT", "SQL", "TABLE", "CREATE", "SELECT", "FROM", "JOIN", "ON", "GROUP", "ORDER",
    "HAVING", "AS", "AND", "OR", "NOT", "IN", "IS", "NULL", "FIRST", "LAST",
}


def tokenize(source: str) -> list[SasToken]:
    tokens: list[SasToken] = []
    index = 0
    line = 1
    column = 1
    length = len(source)

    def advance(text: str) -> None:
        nonlocal line, column
        line_breaks = text.count("\n")
        if line_breaks:
            line += line_breaks
            column = len(text.rsplit("\n", 1)[-1]) + 1
        else:
            column += len(text)

    while index < length:
        char = source[index]
        start = index
        start_line = line
        start_column = column
        if char.isspace():
            end = index + 1
            while end < length and source[end].isspace():
                end += 1
            advance(source[index:end])
            index = end
            continue
        if source.startswith("/*", index):
            close = source.find("*/", index + 2)
            end = length if close < 0 else close + 2
            value = source[index:end]
            tokens.append(SasToken("COMMENT", value, start, end, start_line, start_column, line + value.count("\n"), len(value.rsplit("\n", 1)[-1]) + 1))
            advance(value)
            index = end
            continue
        if char == "%" and source.startswith("%*", index):
            close = source.find(";", index + 2)
            end = length if close < 0 else close + 1
            value = source[index:end]
            tokens.append(SasToken("COMMENT", value, start, end, start_line, start_column, line + value.count("\n"), len(value.rsplit("\n", 1)[-1]) + 1))
            advance(value)
            index = end
            continue
        if char == "*":
            statement_start = source.rfind(";", 0, index) + 1
            if not source[statement_start:index].strip():
                close = source.find(";", index + 1)
                if close >= 0:
                    end = close + 1
                    value = source[index:end]
                    tokens.append(SasToken("COMMENT", value, start, end, start_line, start_column, line + value.count("\n"), len(value.rsplit("\n", 1)[-1]) + 1))
                    advance(value)
                    index = end
                    continue
        if char in "'\"":
            quote = char
            end = index + 1
            while end < length:
                if source[end] == quote:
                    if end + 1 < length and source[end + 1] == quote:
                        end += 2
                        continue
                    end += 1
                    break
                end += 1
            value = source[index:end]
            tokens.append(SasToken("STRING", value, start, end, start_line, start_column, line + value.count("\n"), len(value.rsplit("\n", 1)[-1]) + 1))
            advance(value)
            index = end
            continue
        if char == "%" or char == "&":
            match = re.match(r"[%&][A-Za-z_][A-Za-z0-9_]*", source[index:])
            if match:
                value = match.group(0)
                end = index + len(value)
                tokens.append(SasToken("MACRO", value, start, end, start_line, start_column, start_line, start_column + len(value)))
                advance(value)
                index = end
                continue
            positional = re.match(r"&(?:[0-9]+)", source[index:])
            if positional:
                value = positional.group(0)
                end = index + len(value)
                tokens.append(SasToken("MACRO_PARAM", value, start, end, start_line, start_column, start_line, start_column + len(value)))
                advance(value)
                index = end
                continue
        word = re.match(r"[A-Za-z_][A-Za-z0-9_.$]*", source[index:])
        if word:
            value = word.group(0)
            end = index + len(value)
            upper = value.upper()
            kind: Literal["KEYWORD", "IDENTIFIER"] = "KEYWORD" if upper in _KEYWORDS else "IDENTIFIER"
            tokens.append(SasToken(kind, value, start, end, start_line, start_column, start_line, start_column + len(value)))
            advance(value)
            index = end
            continue
        number = re.match(r"(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][+-]?\d+)?", source[index:])
        if number:
            value = number.group(0)
            end = index + len(value)
            tokens.append(SasToken("NUMBER", value, start, end, start_line, start_column, start_line, start_column + len(value)))
            advance(value)
            index = end
            continue
        two = source[index:index + 2]
        value = two if two in ("<=", ">=", "^=", "~=") else char
        end = index + len(value)
        tokens.append(SasToken("SYMBOL", value, start, end, start_line, start_column, start_line, start_column + len(value)))
        advance(value)
        index = end

    tokens.append(SasToken("EOF", "", length, length, line, column, line, column))
    return tokens


def significant_tokens(tokens: list[SasToken]) -> list[SasToken]:
    return [token for token in tokens if token.kind != "COMMENT"]
