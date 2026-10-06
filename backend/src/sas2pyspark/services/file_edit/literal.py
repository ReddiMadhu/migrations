"""LiteralEditEngine — exact substring search/replace with CRLF normalization."""

from __future__ import annotations

from sas2pyspark.core.errors import FsError, FsErrorCode


def _normalize_line_endings(text: str) -> str:
    return text.replace("\r\n", "\n")


def _detect_line_endings(text: str) -> str:
    if "\r\n" in text[:4096]:
        return "\r\n"
    return "\n"


def _restore_line_endings(text: str, line_ending: str) -> str:
    if line_ending == "\r\n":
        return text.replace("\n", "\r\n")
    return text


def _count_occurrences(content: str, needle: str) -> int:
    count = 0
    index = 0
    while True:
        found = content.find(needle, index)
        if found == -1:
            return count
        count += 1
        index = found + len(needle)


class LiteralEditEngine:
    """Exact literal search/replace — no fuzzy, no regex, no indentation normalization."""

    __slots__ = ()

    @staticmethod
    def apply_literal_edit(
        *,
        content: str,
        old_string: str,
        new_string: str,
        replace_all: bool,
    ) -> str:
        if len(old_string) == 0:
            raise FsError(
                FsErrorCode.FS_EDIT_NOT_FOUND,
                "old_string must be a non-empty string",
                remedy="provide a non-empty old_string",
            )
        if old_string == new_string:
            raise FsError(
                FsErrorCode.FS_EDIT_NOT_FOUND,
                "old_string and new_string are identical; no edit needed",
                remedy="provide a different new_string or skip the edit",
            )

        line_ending = _detect_line_endings(content)
        normalized_content = _normalize_line_endings(content)
        old_norm = _normalize_line_endings(old_string)
        new_norm = _normalize_line_endings(new_string)

        occurrences = _count_occurrences(normalized_content, old_norm)
        if occurrences == 0:
            raise FsError(
                FsErrorCode.FS_EDIT_NOT_FOUND,
                f"old_string was not found in the content",
            )
        if not replace_all and occurrences > 1:
            raise FsError(
                FsErrorCode.FS_AMBIGUOUS_EDIT,
                f"old_string matched {occurrences} times; provide a more specific old_string or set replace_all to true",
            )

        edited = normalized_content.replace(old_norm, new_norm) if replace_all else normalized_content.replace(old_norm, new_norm, 1)
        return _restore_line_endings(edited, line_ending)
