"""SandboxFence — per-call filesystem containment (path checks, not OS isolation)."""

from __future__ import annotations

import os
from enum import Enum

from sas2pyspark.core.errors import FsError, FsErrorCode


class SandboxPolicy(str, Enum):
    READ_ONLY = "read-only"
    WORKSPACE_WRITE = "workspace-write"
    DANGER_FULL_ACCESS = "danger-full-access"


class SandboxFence:
    """Per-call containment fence — re-canonicalizes paths and checks containment."""

    __slots__ = ("_workspace_root",)

    def __init__(self, *, workspace_root: str) -> None:
        self._workspace_root = os.path.realpath(workspace_root)

    def check(self, *, target: str, policy: SandboxPolicy, is_write: bool) -> str:
        if policy == SandboxPolicy.DANGER_FULL_ACCESS:
            return os.path.realpath(target)

        if policy == SandboxPolicy.READ_ONLY and is_write:
            raise FsError(
                FsErrorCode.FS_SANDBOX_DENIED,
                f'write denied under read-only policy for "{target}"',
                remedy="switch to workspace-write or danger-full-access policy",
            )

        canonical = os.path.realpath(target)
        if not canonical.startswith(self._workspace_root + os.sep) and canonical != self._workspace_root:
            raise FsError(
                FsErrorCode.FS_SANDBOX_DENIED,
                f'"{target}" is outside the workspace root',
                remedy="use a path inside the workspace",
            )
        return canonical

    def recheck_at_commit(self, *, target: str, policy: SandboxPolicy) -> None:
        if policy == SandboxPolicy.DANGER_FULL_ACCESS:
            return
        if policy == SandboxPolicy.READ_ONLY:
            return
        canonical = os.path.realpath(target)
        if not canonical.startswith(self._workspace_root + os.sep) and canonical != self._workspace_root:
            raise FsError(
                FsErrorCode.FS_SANDBOX_DENIED,
                f'"{target}" escaped the workspace root (TOCTOU)',
                remedy="re-read the target and retry",
            )
