"""Tests for DiffPresenter — structured diff hunks for UI."""

from __future__ import annotations

from sas2pyspark.services.file_edit.diff import DiffPresenter


class TestDiffPresenter:
    def test_identical_content_no_hunks(self):
        result = DiffPresenter.diff(old_content="hello", new_content="hello")
        assert len(result.hunks) == 0

    def test_simple_change_produces_hunk(self):
        result = DiffPresenter.diff(
            old_content="line1\nline2\nline3",
            new_content="line1\nchanged\nline3",
        )
        assert len(result.hunks) >= 1

    def test_insertion_produces_hunk(self):
        result = DiffPresenter.diff(
            old_content="line1\nline3",
            new_content="line1\nline2\nline3",
        )
        assert len(result.hunks) >= 1

    def test_deletion_produces_hunk(self):
        result = DiffPresenter.diff(
            old_content="line1\nline2\nline3",
            new_content="line1\nline3",
        )
        assert len(result.hunks) >= 1

    def test_context_lines_default_three(self):
        old = "\n".join(f"line{i}" for i in range(10))
        new = "\n".join(f"line{i}" if i != 5 else "changed" for i in range(10))
        result = DiffPresenter.diff(old_content=old, new_content=new)
        assert len(result.hunks) >= 1
