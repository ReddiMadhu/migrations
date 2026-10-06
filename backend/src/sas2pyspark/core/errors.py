"""Typed workbench errors with reason codes — fail-closed diagnostics."""

from __future__ import annotations

from enum import Enum
from typing import Any


class WorkbenchErrorCode(str, Enum):
    INVALID_INPUT = "INVALID_INPUT"
    DUPLICATE_ARTIFACT = "DUPLICATE_ARTIFACT"
    ARTIFACT_NOT_FOUND = "ARTIFACT_NOT_FOUND"
    ARTIFACT_SCOPE_VIOLATION = "ARTIFACT_SCOPE_VIOLATION"
    SESSION_NOT_FOUND = "SESSION_NOT_FOUND"
    SESSION_ALREADY_EXISTS = "SESSION_ALREADY_EXISTS"
    SESSION_OWNERSHIP_LOST = "SESSION_OWNERSHIP_LOST"
    SESSION_CORRUPTION = "SESSION_CORRUPTION"
    EVIDENCE_TAMPERED = "EVIDENCE_TAMPERED"
    UNSAFE_FORK_BOUNDARY = "UNSAFE_FORK_BOUNDARY"
    PROVIDER_UNAVAILABLE = "PROVIDER_UNAVAILABLE"
    PERSISTENCE_UNAVAILABLE = "PERSISTENCE_UNAVAILABLE"
    EXECUTION_UNKNOWN = "EXECUTION_UNKNOWN"
    POLICY_DENIED = "POLICY_DENIED"
    APPROVAL_REQUIRED = "APPROVAL_REQUIRED"
    STALE_APPROVAL = "STALE_APPROVAL"
    REPAIR_OSCILLATION = "REPAIR_OSCILLATION"
    UNSUPPORTED_CONSTRUCT = "UNSUPPORTED_CONSTRUCT"
    RECONCILIATION_BLOCKED = "RECONCILIATION_BLOCKED"
    UNKNOWN_FORMAT = "UNKNOWN_FORMAT"
    SCHEMA_MISMATCH = "SCHEMA_MISMATCH"
    AGGREGATE_MISMATCH = "AGGREGATE_MISMATCH"
    ROW_MISMATCH = "ROW_MISMATCH"
    REPAIR_BUDGET_EXCEEDED = "REPAIR_BUDGET_EXCEEDED"
    REPAIR_REJECTED = "REPAIR_REJECTED"
    TRANSLATION_FAILED = "TRANSLATION_FAILED"
    STATIC_VALIDATION_FAILED = "STATIC_VALIDATION_FAILED"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    CONFIG_ERROR = "CONFIG_ERROR"
    NO_GUARDS_REGISTERED = "NO_GUARDS_REGISTERED"
    BUDGET_EXCEEDED = "BUDGET_EXCEEDED"
    AUTHENTICATION_ERROR = "AUTHENTICATION_ERROR"
    FS_NOT_FOUND = "FS_NOT_FOUND"
    FS_NOT_TEXT = "FS_NOT_TEXT"
    FS_STALE_VERSION = "FS_STALE_VERSION"
    FS_NOT_OBSERVED = "FS_NOT_OBSERVED"
    FS_AMBIGUOUS_EDIT = "FS_AMBIGUOUS_EDIT"
    FS_EDIT_NOT_FOUND = "FS_EDIT_NOT_FOUND"
    FS_SANDBOX_DENIED = "FS_SANDBOX_DENIED"


class FsErrorCode(str, Enum):
    FS_NOT_FOUND = "FS_NOT_FOUND"
    FS_NOT_TEXT = "FS_NOT_TEXT"
    FS_STALE_VERSION = "FS_STALE_VERSION"
    FS_NOT_OBSERVED = "FS_NOT_OBSERVED"
    FS_AMBIGUOUS_EDIT = "FS_AMBIGUOUS_EDIT"
    FS_EDIT_NOT_FOUND = "FS_EDIT_NOT_FOUND"
    FS_SANDBOX_DENIED = "FS_SANDBOX_DENIED"


_DEFAULT_REMEDY = "re-read, then retry"


class WorkbenchError(Exception):
    __slots__ = ("code", "details", "message")

    def __init__(
        self,
        code: WorkbenchErrorCode,
        message: str,
        details: dict[str, Any] | None = None,
        *,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = dict(details) if details else {}

    def __repr__(self) -> str:
        return f"WorkbenchError(code={self.code.value!r}, message={self.message!r})"


class FsError(WorkbenchError):
    __slots__ = ("remedy", "cause")

    def __init__(
        self,
        code: FsErrorCode,
        message: str,
        *,
        remedy: str = _DEFAULT_REMEDY,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(WorkbenchErrorCode(code.value), message)
        self.remedy = remedy
        self.cause = cause

    def __repr__(self) -> str:
        return f"FsError(code={self.code.value!r}, message={self.message!r}, remedy={self.remedy!r})"


def error_message(error: BaseException | str) -> str:
    if isinstance(error, BaseException):
        return str(error)
    return str(error)
