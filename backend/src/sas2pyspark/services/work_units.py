"""WorkUnitService — bounded repair context with status transitions."""

from __future__ import annotations

import re
from typing import Any, Sequence

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.core.events import MigrationEventType
from sas2pyspark.core.json import assert_safe_segment, snapshot_json
from sas2pyspark.services.clock import Clock
from sas2pyspark.services.scopes import ScopeRef, ScopeService, is_descendant

MAX_WORK_UNIT_DEPENDENCIES = 256
MAX_WORK_UNIT_IR_NODES = 512
MAX_WORK_UNIT_SKILLS = 64

WorkUnitStatus = str  # "OPEN" | "ACTIVE" | "REPAIRING" | "BLOCKED" | "CERTIFIED" | "CLOSED"

_VALID_STATUSES = {"OPEN", "ACTIVE", "REPAIRING", "BLOCKED", "CERTIFIED", "CLOSED"}

_TRANSITIONS: dict[str, set[str]] = {
    "OPEN": {"ACTIVE", "REPAIRING", "BLOCKED"},
    "ACTIVE": {"REPAIRING", "BLOCKED", "CERTIFIED"},
    "REPAIRING": {"ACTIVE", "BLOCKED"},
    "BLOCKED": {"ACTIVE", "REPAIRING", "CERTIFIED"},
    "CERTIFIED": set(),
    "CLOSED": set(),
}

_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")


def _bounded_text(value: Any, field: str, max_len: int, code: WorkbenchErrorCode) -> str:
    if not isinstance(value, str) or len(value) == 0 or len(value) > max_len or _CONTROL_RE.search(value):
        raise WorkbenchError(code, f"{field} must be a bounded non-empty string", {"field": field, "max": max_len})
    return value


def _safe_segment(value: Any, field: str, code: WorkbenchErrorCode) -> str:
    text = _bounded_text(value, field, 128, code)
    try:
        return assert_safe_segment(text, field)
    except TypeError as e:
        raise WorkbenchError(code, f"{field} is not a safe identifier", {"field": field, "cause": str(e)}) from e


def _string_list(value: Any, field: str, max_len: int, code: WorkbenchErrorCode, kind: str) -> list[str]:
    if not isinstance(value, (list, tuple)):
        raise WorkbenchError(code, f"{field} must be an array", {"field": field})
    if len(value) > max_len:
        raise WorkbenchError(code, f"{field} exceeds its bounded limit", {"field": field, "max": max_len, "count": len(value)})
    seen: set[str] = set()
    result: list[str] = []
    for item in value:
        text = _safe_segment(item, field, code) if kind == "identifier" else _bounded_text(item, field, 512, code)
        if text in seen:
            raise WorkbenchError(code, f"{field} contains a duplicate value", {"field": field, "value": text})
        seen.add(text)
        result.append(text)
    return result


class WorkUnitDataReference:
    __slots__ = ("backend", "uri", "format", "version", "content_digest")

    def __init__(self, *, backend: str, uri: str, format: str | None = None, version: str | None = None, content_digest: str | None = None) -> None:
        self.backend = backend
        self.uri = uri
        self.format = format
        self.version = version
        self.content_digest = content_digest

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"backend": self.backend, "uri": self.uri}
        if self.format is not None:
            d["format"] = self.format
        if self.version is not None:
            d["version"] = self.version
        if self.content_digest is not None:
            d["contentDigest"] = self.content_digest
        return d


class MigrationWorkUnit:
    __slots__ = (
        "work_unit_id", "migration_id", "scope_id", "sas_module",
        "dependency_closure", "ir_node_ids", "ir_snapshot_id", "macro_resolution_id",
        "input_snapshot_id", "reference_output", "candidate_id", "reconciliation_id",
        "failure_signature", "required_skills", "status", "created_at", "updated_at", "closed_reason",
    )

    def __init__(self, **kwargs: Any) -> None:
        for slot in self.__slots__:
            setattr(self, slot, kwargs.get(slot))

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "workUnitId": self.work_unit_id,
            "migrationId": self.migration_id,
            "scopeId": self.scope_id,
            "sasModule": self.sas_module,
            "dependencyClosure": list(self.dependency_closure),
            "irNodeIds": list(self.ir_node_ids),
            "requiredSkills": list(self.required_skills),
            "status": self.status,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
        }
        for key, attr, alias in [
            ("irSnapshotId", "ir_snapshot_id", "irSnapshotId"),
            ("macroResolutionId", "macro_resolution_id", "macroResolutionId"),
            ("inputSnapshotId", "input_snapshot_id", "inputSnapshotId"),
            ("candidateId", "candidate_id", "candidateId"),
            ("reconciliationId", "reconciliation_id", "reconciliationId"),
            ("failureSignature", "failure_signature", "failureSignature"),
            ("closedReason", "closed_reason", "closedReason"),
        ]:
            val = getattr(self, attr)
            if val is not None:
                d[alias] = val
        if self.reference_output is not None:
            d["referenceOutput"] = self.reference_output.to_dict()
        return d


def work_unit_snapshot(value: Any, code: WorkbenchErrorCode = WorkbenchErrorCode.INVALID_INPUT) -> MigrationWorkUnit:
    try:
        raw = snapshot_json(value) if not isinstance(value, dict) else value
    except TypeError as e:
        raise WorkbenchError(code, "work unit is not safe JSON", {"cause": str(e)}) from e
    if not isinstance(raw, dict):
        raise WorkbenchError(code, "work unit must be an object")
    status = raw.get("status")
    if status not in _VALID_STATUSES:
        raise WorkbenchError(code, "work unit status is invalid", {"status": status})
    wu = MigrationWorkUnit(
        work_unit_id=_safe_segment(raw.get("workUnitId"), "workUnitId", code),
        migration_id=_bounded_text(raw.get("migrationId"), "migrationId", 512, code),
        scope_id=_bounded_text(raw.get("scopeId"), "scopeId", 2048, code),
        sas_module=_bounded_text(raw.get("sasModule"), "sasModule", 512, code),
        dependency_closure=_string_list(raw.get("dependencyClosure"), "dependencyClosure", MAX_WORK_UNIT_DEPENDENCIES, code, "text"),
        ir_node_ids=_string_list(raw.get("irNodeIds"), "irNodeIds", MAX_WORK_UNIT_IR_NODES, code, "identifier"),
        required_skills=_string_list(raw.get("requiredSkills"), "requiredSkills", MAX_WORK_UNIT_SKILLS, code, "identifier"),
        status=status,
        created_at=_bounded_text(raw.get("createdAt"), "createdAt", 128, code),
        updated_at=_bounded_text(raw.get("updatedAt"), "updatedAt", 128, code),
    )
    if raw.get("irSnapshotId") is not None:
        wu.ir_snapshot_id = _safe_segment(raw["irSnapshotId"], "irSnapshotId", code)
    if raw.get("macroResolutionId") is not None:
        wu.macro_resolution_id = _safe_segment(raw["macroResolutionId"], "macroResolutionId", code)
    if raw.get("inputSnapshotId") is not None:
        wu.input_snapshot_id = _safe_segment(raw["inputSnapshotId"], "inputSnapshotId", code)
    if raw.get("candidateId") is not None:
        wu.candidate_id = _safe_segment(raw["candidateId"], "candidateId", code)
    if raw.get("reconciliationId") is not None:
        wu.reconciliation_id = _safe_segment(raw["reconciliationId"], "reconciliationId", code)
    if raw.get("failureSignature") is not None:
        wu.failure_signature = _bounded_text(raw["failureSignature"], "failureSignature", 1024, code)
    if raw.get("closedReason") is not None:
        wu.closed_reason = _bounded_text(raw["closedReason"], "closedReason", 1024, code)
    if raw.get("referenceOutput") is not None:
        ro = raw["referenceOutput"]
        backend = ro.get("backend")
        if backend not in ("databricks", "artifact"):
            raise WorkbenchError(code, "referenceOutput.backend must be databricks or artifact")
        wu.reference_output = WorkUnitDataReference(
            backend=backend,
            uri=_bounded_text(ro.get("uri"), "referenceOutput.uri", 2048, code),
            format=ro.get("format"),
            version=ro.get("version"),
            content_digest=ro.get("contentDigest"),
        )
    return wu


def work_unit_from_event(value: Any) -> MigrationWorkUnit:
    if not isinstance(value, dict):
        raise WorkbenchError(WorkbenchErrorCode.SESSION_CORRUPTION, "work unit event data is invalid")
    return work_unit_snapshot(value.get("workUnit"), WorkbenchErrorCode.SESSION_CORRUPTION)


def _assert_transition(current: str, next_status: str) -> None:
    if current == next_status:
        return
    if next_status not in _TRANSITIONS.get(current, set()):
        raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, "work unit status transition is not allowed", {"current": current, "next": next_status})


class WorkUnitService:
    def __init__(self, clock: Clock, scopes: ScopeService) -> None:
        self._clock = clock
        self._scopes = scopes

    def create(
        self, *,
        session: Any, scope: ScopeRef, sas_module: str, dependency_closure: Sequence[str],
        work_unit_id: str | None = None, ir_node_ids: Sequence[str] | None = None,
        ir_snapshot_id: str | None = None, macro_resolution_id: str | None = None,
        input_snapshot_id: str | None = None, reference_output: WorkUnitDataReference | None = None,
        candidate_id: str | None = None, required_skills: Sequence[str] | None = None,
    ) -> MigrationWorkUnit:
        target = self._scopes.require_by_id(scope.id)
        self._scopes.assert_scope_owned(self._scopes.require_by_id(session.scope.id), target)
        wu_id = _safe_segment(work_unit_id or f"work-unit-{self._clock.uuid()}", "workUnitId", WorkbenchErrorCode.INVALID_INPUT)
        projection = session.projection
        if wu_id in projection["workUnits"]:
            raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, f"work unit {wu_id} already exists in this session", {"workUnitId": wu_id})
        if ir_snapshot_id is not None and ir_snapshot_id not in projection["semanticIrs"]:
            raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, "work unit IR snapshot is not present in the session projection", {"irSnapshotId": ir_snapshot_id})
        if macro_resolution_id is not None and macro_resolution_id not in projection["macroResolutions"]:
            raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, "work unit macro resolution is not present in the session projection", {"macroResolutionId": macro_resolution_id})
        now = self._clock.now()
        wu = work_unit_snapshot({
            "workUnitId": wu_id,
            "migrationId": session.migration_id,
            "scopeId": target.id,
            "sasModule": sas_module,
            "dependencyClosure": list(dependency_closure),
            "irNodeIds": list(ir_node_ids or []),
            "requiredSkills": list(required_skills or []),
            "status": "OPEN",
            "createdAt": now,
            "updatedAt": now,
            **({"irSnapshotId": ir_snapshot_id} if ir_snapshot_id else {}),
            **({"macroResolutionId": macro_resolution_id} if macro_resolution_id else {}),
            **({"inputSnapshotId": input_snapshot_id} if input_snapshot_id else {}),
            **({"candidateId": candidate_id} if candidate_id else {}),
            **({"referenceOutput": reference_output.to_dict()} if reference_output else {}),
        })
        session._service.append_event(session, MigrationEventType.WORK_UNIT_CREATED, {"workUnit": wu.to_dict()})
        return session.projection["workUnits"][wu_id]

    def get(self, session: Any, work_unit_id: str) -> MigrationWorkUnit | None:
        self._scopes.require_by_id(session.scope.id)
        wu = session.projection["workUnits"].get(work_unit_id)
        if wu and not self._scopes.get(wu.scope_id):
            raise WorkbenchError(WorkbenchErrorCode.POLICY_DENIED, "work unit scope is no longer active", {"workUnitId": work_unit_id, "scopeId": wu.scope_id})
        return wu

    def list(self, session: Any) -> list[MigrationWorkUnit]:
        self._scopes.require_by_id(session.scope.id)
        return sorted(session.projection["workUnits"].values(), key=lambda w: w.work_unit_id)

    def require(self, session: Any, work_unit_id: str) -> MigrationWorkUnit:
        wu = self.get(session, work_unit_id)
        if wu is None:
            raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, f"work unit {work_unit_id} does not exist in session {session.id}", {"workUnitId": work_unit_id, "sessionId": session.id})
        return wu

    def require_for_scope(self, session: Any, work_unit_id: str, scope: ScopeRef) -> MigrationWorkUnit:
        wu = self.require(session, work_unit_id)
        session_scope = self._scopes.require_by_id(session.scope.id)
        target = self._scopes.require_by_id(scope.id)
        self._scopes.assert_scope_owned(session_scope, target)
        wu_scope = self._scopes.get(wu.scope_id)
        if wu_scope is None or not is_descendant(wu_scope, target):
            raise WorkbenchError(WorkbenchErrorCode.POLICY_DENIED, "work unit does not own the requested execution scope", {"workUnitId": work_unit_id, "workUnitScope": wu.scope_id, "targetScope": target.id})
        return wu

    def update(
        self, *,
        session: Any, work_unit_id: str, candidate_id: str | None = None,
        reconciliation_id: str | None = None, failure_signature: str | None = None,
        status: str | None = None, reason: str | None = None,
    ) -> MigrationWorkUnit:
        current = self.require(session, work_unit_id)
        if current.status == "CLOSED":
            raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, "closed work units cannot be updated", {"workUnitId": work_unit_id})
        next_status = status or current.status
        _assert_transition(current.status, next_status)
        data = current.to_dict()
        if candidate_id is not None:
            data["candidateId"] = candidate_id
        if reconciliation_id is not None:
            data["reconciliationId"] = reconciliation_id
        if failure_signature is not None:
            data["failureSignature"] = failure_signature
        data["status"] = next_status
        data["updatedAt"] = self._clock.now()
        next_wu = work_unit_snapshot(data)
        session._service.append_event(session, MigrationEventType.WORK_UNIT_UPDATED, {"workUnit": next_wu.to_dict(), "reason": reason or "coordinator-update"})
        return session.projection["workUnits"][work_unit_id]

    def close(self, *, session: Any, work_unit_id: str, reason: str) -> MigrationWorkUnit:
        current = self.require(session, work_unit_id)
        if current.status == "CLOSED":
            raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, "work unit is already closed", {"workUnitId": work_unit_id})
        reason_text = _bounded_text(reason, "reason", 1024, WorkbenchErrorCode.INVALID_INPUT)
        data = current.to_dict()
        data["status"] = "CLOSED"
        data["closedReason"] = reason_text
        data["updatedAt"] = self._clock.now()
        next_wu = work_unit_snapshot(data)
        session._service.append_event(session, MigrationEventType.WORK_UNIT_CLOSED, {"workUnit": next_wu.to_dict(), "reason": reason_text})
        return session.projection["workUnits"][work_unit_id]
