"""Tests for SandboxFence — per-call filesystem containment."""

from __future__ import annotations

import os

import pytest

from sas2pyspark.core.errors import FsError, FsErrorCode
from sas2pyspark.services.file_edit.sandbox import SandboxFence, SandboxPolicy


class TestSandboxFence:
    def test_read_only_denies_writes(self, tmp_path):
        fence = SandboxFence(workspace_root=str(tmp_path))
        with pytest.raises(FsError) as exc_info:
            fence.check(target=str(tmp_path / "file.py"), policy=SandboxPolicy.READ_ONLY, is_write=True)
        assert exc_info.value.code == FsErrorCode.FS_SANDBOX_DENIED

    def test_read_only_allows_reads(self, tmp_path):
        fence = SandboxFence(workspace_root=str(tmp_path))
        result = fence.check(target=str(tmp_path / "file.py"), policy=SandboxPolicy.READ_ONLY, is_write=False)
        assert result is not None

    def test_workspace_write_allows_inside(self, tmp_path):
        fence = SandboxFence(workspace_root=str(tmp_path))
        target = str(tmp_path / "file.py")
        result = fence.check(target=target, policy=SandboxPolicy.WORKSPACE_WRITE, is_write=True)
        assert result == os.path.realpath(target)

    def test_workspace_write_denies_outside(self, tmp_path):
        fence = SandboxFence(workspace_root=str(tmp_path))
        outside = os.path.join(os.path.dirname(str(tmp_path)), "outside.py")
        with pytest.raises(FsError) as exc_info:
            fence.check(target=outside, policy=SandboxPolicy.WORKSPACE_WRITE, is_write=True)
        assert exc_info.value.code == FsErrorCode.FS_SANDBOX_DENIED

    def test_danger_full_access_passes(self, tmp_path):
        fence = SandboxFence(workspace_root=str(tmp_path))
        result = fence.check(target="/anywhere/file.py", policy=SandboxPolicy.DANGER_FULL_ACCESS, is_write=True)
        assert result is not None

    def test_recheck_at_commit_passes_inside(self, tmp_path):
        fence = SandboxFence(workspace_root=str(tmp_path))
        target = str(tmp_path / "file.py")
        fence.recheck_at_commit(target=target, policy=SandboxPolicy.WORKSPACE_WRITE)

    def test_recheck_at_commit_detects_escape(self, tmp_path):
        fence = SandboxFence(workspace_root=str(tmp_path))
        outside = os.path.join(os.path.dirname(str(tmp_path)), "outside.py")
        with pytest.raises(FsError) as exc_info:
            fence.recheck_at_commit(target=outside, policy=SandboxPolicy.WORKSPACE_WRITE)
        assert exc_info.value.code == FsErrorCode.FS_SANDBOX_DENIED
