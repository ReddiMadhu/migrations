"""API utilities — error mapping and shared helpers."""

from __future__ import annotations

from sas2pyspark.core.errors import WorkbenchErrorCode


def error_to_http_code(code: WorkbenchErrorCode) -> int:
    mapping = {
        WorkbenchErrorCode.SESSION_NOT_FOUND: 404,
        WorkbenchErrorCode.SESSION_ALREADY_EXISTS: 409,
        WorkbenchErrorCode.ARTIFACT_NOT_FOUND: 404,
        WorkbenchErrorCode.POLICY_DENIED: 403,
        WorkbenchErrorCode.INVALID_INPUT: 422,
        WorkbenchErrorCode.SESSION_CORRUPTION: 500,
        WorkbenchErrorCode.EVIDENCE_TAMPERED: 500,
    }
    return mapping.get(code, 400)
