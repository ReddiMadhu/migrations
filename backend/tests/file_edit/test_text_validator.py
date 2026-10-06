"""Tests for TextValidator — binary/encoding/size rejection."""

from __future__ import annotations

import pytest

from sas2pyspark.core.errors import FsError, FsErrorCode
from sas2pyspark.services.file_edit.text_validator import MAX_EDIT_BYTES, TextValidator


class TestTextValidator:
    def test_valid_text_returns_decoded(self):
        result = TextValidator.validate(content=b"hello world")
        assert result == "hello world"

    def test_nul_byte_raises(self):
        with pytest.raises(FsError) as exc_info:
            TextValidator.validate(content=b"hello\x00world")
        assert exc_info.value.code == FsErrorCode.FS_NOT_TEXT

    def test_invalid_utf8_raises(self):
        with pytest.raises(FsError) as exc_info:
            TextValidator.validate(content=b"\xff\xfe")
        assert exc_info.value.code == FsErrorCode.FS_NOT_TEXT

    def test_oversized_raises(self):
        content = b"x" * (MAX_EDIT_BYTES + 1)
        with pytest.raises(FsError) as exc_info:
            TextValidator.validate(content=content)
        assert exc_info.value.code == FsErrorCode.FS_NOT_TEXT

    def test_empty_content_returns_empty(self):
        result = TextValidator.validate(content=b"")
        assert result == ""

    def test_unicode_text_returns_decoded(self):
        result = TextValidator.validate(content="héllo wörld".encode("utf-8"))
        assert result == "héllo wörld"
