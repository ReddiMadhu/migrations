"""Tests for FsError — stable code, remedy, and cause chaining."""

from __future__ import annotations

from sas2pyspark.core.errors import FsError, FsErrorCode, WorkbenchErrorCode


class TestFsError:
    def test_preserves_stable_code(self):
        err = FsError(FsErrorCode.FS_EDIT_NOT_FOUND, "not found")
        assert err.code == WorkbenchErrorCode.FS_EDIT_NOT_FOUND

    def test_default_remedy(self):
        err = FsError(FsErrorCode.FS_STALE_VERSION, "stale")
        assert err.remedy == "re-read, then retry"

    def test_custom_remedy(self):
        err = FsError(FsErrorCode.FS_AMBIGUOUS_EDIT, "ambiguous", remedy="provide more context")
        assert err.remedy == "provide more context"

    def test_cause_chaining(self):
        original = ValueError("bad value")
        err = FsError(FsErrorCode.FS_NOT_TEXT, "bad utf-8", cause=original)
        assert err.cause is original

    def test_message_preserved(self):
        err = FsError(FsErrorCode.FS_SANDBOX_DENIED, "denied")
        assert str(err) == "denied"

    def test_all_fs_codes_in_workbench_error_code(self):
        for code in FsErrorCode:
            assert WorkbenchErrorCode(code.value) is not None
