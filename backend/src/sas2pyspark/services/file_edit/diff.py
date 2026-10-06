"""DiffPresenter — structured diff hunks for UI presentation only (never a patch pipeline)."""

from __future__ import annotations

import difflib

from sas2pyspark.core.models import WorkbenchModel


class DiffHunk(WorkbenchModel):
    old_start: int
    new_start: int
    old_lines: tuple[str, ...]
    new_lines: tuple[str, ...]


class DiffHunks(WorkbenchModel):
    hunks: tuple[DiffHunk, ...]


class DiffPresenter:
    """Compute structured diff hunks with context lines for display."""

    __slots__ = ()

    @staticmethod
    def diff(*, old_content: str, new_content: str, context_lines: int = 3) -> DiffHunks:
        old_lines = old_content.splitlines(keepends=False)
        new_lines = new_content.splitlines(keepends=False)
        matcher = difflib.SequenceMatcher(a=old_lines, b=new_lines)
        hunks: list[DiffHunk] = []
        for tag, i1, i2, j1, j2 in matcher.get_opcodes():
            if tag == "equal" and (i2 - i1) <= context_lines * 2:
                continue
            hunks.append(DiffHunk(
                old_start=i1 + 1,
                new_start=j1 + 1,
                old_lines=tuple(old_lines[i1:i2]),
                new_lines=tuple(new_lines[j1:j2]),
            ))
        return DiffHunks(hunks=tuple(hunks))
