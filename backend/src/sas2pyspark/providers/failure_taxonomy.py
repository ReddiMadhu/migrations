"""Failure taxonomy — bounded evidence-based classification of execution failures."""

from __future__ import annotations

from enum import Enum
from typing import Any


class FailureTaxonomy(str, Enum):
    AUTHENTICATION_ERROR = "AUTHENTICATION_ERROR"
    TRANSIENT_INFRASTRUCTURE_ERROR = "TRANSIENT_INFRASTRUCTURE_ERROR"
    DRIVER_OOM = "DRIVER_OOM"
    EXECUTOR_OOM = "EXECUTOR_OOM"
    PREEMPTION = "PREEMPTION"
    TIMEOUT = "TIMEOUT"
    USER_CODE = "USER_CODE"
    DEPENDENCY = "DEPENDENCY"
    UNKNOWN = "UNKNOWN"


_OOM_PATTERNS = (
    "out of memory",
    "oom",
    "java.lang.outofmemoryerror",
    "container killed by yarn",
    "exceeding memory limits",
)

_PREEMPTION_PATTERNS = (
    "preempted",
    "preemption",
    "lost executors",
    "executor lost",
)

_TIMEOUT_PATTERNS = (
    "timed out",
    "timeout",
    "deadline exceeded",
    "ran longer than",
)

_AUTH_PATTERNS = (
    "unauthorized",
    "authentication",
    "invalid token",
    "permission denied",
    "access denied",
)

_TRANSIENT_PATTERNS = (
    "temporary unavailable",
    "service unavailable",
    "internal server error",
    "connection refused",
    "network error",
    "retryable",
)

_DEPENDENCY_PATTERNS = (
    "module not found",
    "importerror",
    "modulenotfounderror",
    "no such file or directory",
    "dependency",
)


def _matches_any(text: str, patterns: tuple[str, ...]) -> bool:
    lower = text.lower()
    return any(p in lower for p in patterns)


def classify_failure(status: Any | None = None, error: BaseException | None = None) -> FailureTaxonomy:
    """Classify a failure from the external run status and/or exception.

    Evidence-based: only returns a specific category when the status/error
    text contains a recognized signal. Unrecognized failures return UNKNOWN.
    """
    parts: list[str] = []
    if status is not None:
        msg = getattr(status, "provider_message", None) or ""
        state = getattr(status, "result_state", None) or getattr(status, "state", None) or ""
        parts.extend([str(msg), str(state)])
    if error is not None:
        parts.append(str(error))
        parts.append(type(error).__name__)
    combined = " ".join(parts)

    if not combined.strip():
        return FailureTaxonomy.UNKNOWN

    if _matches_any(combined, _AUTH_PATTERNS):
        return FailureTaxonomy.AUTHENTICATION_ERROR
    if _matches_any(combined, _OOM_PATTERNS):
        if "executor" in combined.lower():
            return FailureTaxonomy.EXECUTOR_OOM
        return FailureTaxonomy.DRIVER_OOM
    if _matches_any(combined, _PREEMPTION_PATTERNS):
        return FailureTaxonomy.PREEMPTION
    if _matches_any(combined, _TIMEOUT_PATTERNS):
        return FailureTaxonomy.TIMEOUT
    if _matches_any(combined, _TRANSIENT_PATTERNS):
        return FailureTaxonomy.TRANSIENT_INFRASTRUCTURE_ERROR
    if _matches_any(combined, _DEPENDENCY_PATTERNS):
        return FailureTaxonomy.DEPENDENCY

    state_upper = combined.upper()
    if "FAILED" in state_upper or "ERROR" in state_upper:
        return FailureTaxonomy.USER_CODE

    return FailureTaxonomy.UNKNOWN
