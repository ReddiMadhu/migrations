"""Tests for LiteralEditEngine — exact search/replace with CRLF normalization."""

from __future__ import annotations

import pytest

from sas2pyspark.core.errors import FsError, FsErrorCode
from sas2pyspark.services.file_edit.literal import LiteralEditEngine


class TestLiteralEdit:
    def test_simple_replacement(self):
        result = LiteralEditEngine.apply_literal_edit(
            content="hello world", old_string="world", new_string="Python", replace_all=False,
        )
        assert result == "hello Python"

    def test_replace_all(self):
        result = LiteralEditEngine.apply_literal_edit(
            content="a b a b a", old_string="a", new_string="X", replace_all=True,
        )
        assert result == "X b X b X"

    def test_single_replace_unique_match(self):
        result = LiteralEditEngine.apply_literal_edit(
            content="foo UNIQUE_MARKER bar", old_string="UNIQUE_MARKER", new_string="X", replace_all=False,
        )
        assert result == "foo X bar"

    def test_multiple_match_without_replace_all_raises(self):
        with pytest.raises(FsError) as exc_info:
            LiteralEditEngine.apply_literal_edit(
                content="a b a b a", old_string="a", new_string="X", replace_all=False,
            )
        assert exc_info.value.code == FsErrorCode.FS_AMBIGUOUS_EDIT

    def test_no_match_raises(self):
        with pytest.raises(FsError) as exc_info:
            LiteralEditEngine.apply_literal_edit(
                content="hello", old_string="xyz", new_string="abc", replace_all=False,
            )
        assert exc_info.value.code == FsErrorCode.FS_EDIT_NOT_FOUND

    def test_ambiguous_match_raises(self):
        with pytest.raises(FsError) as exc_info:
            LiteralEditEngine.apply_literal_edit(
                content="a a a", old_string="a", new_string="b", replace_all=False,
            )
        assert exc_info.value.code == FsErrorCode.FS_AMBIGUOUS_EDIT

    def test_empty_old_string_raises(self):
        with pytest.raises(FsError) as exc_info:
            LiteralEditEngine.apply_literal_edit(
                content="hello", old_string="", new_string="x", replace_all=False,
            )
        assert exc_info.value.code == FsErrorCode.FS_EDIT_NOT_FOUND

    def test_identical_strings_raises(self):
        with pytest.raises(FsError) as exc_info:
            LiteralEditEngine.apply_literal_edit(
                content="hello", old_string="hello", new_string="hello", replace_all=False,
            )
        assert exc_info.value.code == FsErrorCode.FS_EDIT_NOT_FOUND

    def test_crlf_normalization(self):
        content = "line1\r\nline2\r\nline3"
        result = LiteralEditEngine.apply_literal_edit(
            content=content, old_string="line2", new_string="replaced", replace_all=False,
        )
        assert "replaced" in result
        assert "\r\n" in result

    def test_delete_via_empty_new_string(self):
        result = LiteralEditEngine.apply_literal_edit(
            content="hello world", old_string="world", new_string="", replace_all=False,
        )
        assert result == "hello "

    def test_multiline_replacement(self):
        content = "def foo():\n    pass\n\ndef bar():\n    pass"
        result = LiteralEditEngine.apply_literal_edit(
            content=content,
            old_string="def foo():\n    pass",
            new_string="def foo():\n    return 42",
            replace_all=False,
        )
        assert "return 42" in result
        assert "def bar()" in result
