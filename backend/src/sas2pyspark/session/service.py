"""MigrationSessionService — sessions, event append, projection fold, fork."""

from __future__ import annotations

from typing import Any, Sequence

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.core.hash import GENESIS_HASH, sha256_text
from sas2pyspark.core.json import assert_safe_segment, snapshot_json
from sas2pyspark.core.events import MigrationEvent, MigrationEventType
from sas2pyspark.services.clock import Clock
from sas2pyspark.services.evidence import EvidenceService, EventHashInput
from sas2pyspark.services.policy import policy_fingerprint, ExecutionPolicy
from sas2pyspark.services.scopes import ScopeRef, ScopeService, is_descendant, scope_chain
from sas2pyspark.session.persistence import SessionHeader, PersistedSession, JsonlSessionPersistence

MigrationState = str  # "CREATED" | "TRANSLATING" | ... | "FAILED"


def _initial_projection() -> dict[str, Any]:
    return {
        "state": "CREATED",
        "lastSequence": 0,
        "lastHash": GENESIS_HASH,
        "openExecutionIds": set(),
        "executionOutcomes": {},
        "dependencyGraphs": {},
        "semanticIrs": {},
        "macroResolutions": {},
        "workUnits": {},
        "forkCount": 0,
    }


def _apply_event(projection: dict[str, Any], event: MigrationEvent) -> None:
    projection["lastSequence"] = event.sequence
    projection["lastHash"] = event.hash
    data = event.data
    et = event.type.value
    if et == "migration/created":
        projection["state"] = "CREATED"
    elif et == "translation/started":
        projection["state"] = "TRANSLATING"
    elif et == "translation/completed":
        projection["state"] = "CANDIDATE_READY"
    elif et == "execution/started":
        projection["state"] = "EXECUTING"
        if isinstance(data.get("executionId"), str):
            projection["openExecutionIds"].add(data["executionId"])
    elif et == "execution/completed":
        eid = data.get("executionId")
        if isinstance(eid, str):
            projection["openExecutionIds"].discard(eid)
            projection["executionOutcomes"][eid] = str(data.get("outcome"))
        projection["state"] = "RECONCILING" if data.get("outcome") == "SUCCEEDED" else "DIAGNOSING"
    elif et == "execution/unknown":
        eid = data.get("executionId")
        if isinstance(eid, str):
            projection["openExecutionIds"].discard(eid)
            projection["executionOutcomes"][eid] = "UNKNOWN"
        projection["state"] = "HUMAN_REQUIRED"
    elif et == "reconciliation/started":
        projection["state"] = "RECONCILING"
    elif et == "reconciliation/completed":
        projection["state"] = "AWAITING_OPERATOR" if data.get("certifiable") is True else "DIAGNOSING"
    elif et == "reconciliation/failed":
        projection["state"] = "HUMAN_REQUIRED"
    elif et == "repair/started":
        projection["state"] = "REPAIRING"
    elif et in ("repair/proposed", "patch/applied"):
        projection["state"] = "CANDIDATE_READY"
    elif et == "certificate/issued":
        projection["state"] = "CERTIFIED"
    elif et == "migration/escalated":
        projection["state"] = "HUMAN_REQUIRED"
    elif et == "session/forked":
        projection["forkCount"] += 1
    elif et == "dependency/graph-created":
        from sas2pyspark.services.workspace_discovery import dependency_graph_from_event
        graph = dependency_graph_from_event(data)
        projection["dependencyGraphs"][graph.graph_id] = graph
    elif et == "semantic-ir/created":
        from sas2pyspark.services.semantic_ir import semantic_ir_from_event
        ir = semantic_ir_from_event(data)
        projection["semanticIrs"][ir.ir_id] = ir
    elif et == "macro-resolution/created":
        from sas2pyspark.services.macro_resolution import macro_resolution_from_event
        resolution = macro_resolution_from_event(data)
        projection["macroResolutions"][resolution.resolution_id] = resolution
    elif et in ("work-unit/created", "work-unit/updated", "work-unit/closed"):
        from sas2pyspark.services.work_units import work_unit_from_event
        wu = work_unit_from_event(data)
        projection["workUnits"][wu.work_unit_id] = wu
    elif et in ("agent/started", "agent/idle"):
        projection.setdefault("driverPhase", et)
        projection["driverPhase"] = et
    elif et == "agent/turn-started":
        projection.setdefault("turnCount", 0)
        projection["turnCount"] = projection.get("turnCount", 0) + 1
    elif et == "agent/turn-ended":
        pass
    elif et == "agent/step-started":
        projection.setdefault("stepCount", 0)
        projection["stepCount"] = projection.get("stepCount", 0) + 1
    elif et == "agent/step-ended":
        pass
    elif et in ("tool/started", "tool/completed", "tool/denied", "tool/blocked", "tool/outcome-unknown", "tool/not-started"):
        tool_results = projection.setdefault("toolResults", [])
        tool_results.append({"type": et, "callId": data.get("callId"), "toolName": data.get("toolName")})
    elif et in ("cache/hit", "cache/miss", "cache/evicted"):
        cache_stats = projection.setdefault("cacheStats", {})
        cache_stats[et] = cache_stats.get(et, 0) + 1
    elif et in ("compaction/started", "compaction/completed"):
        history = projection.setdefault("compactionHistory", [])
        history.append({"type": et, "data": data})
    elif et in ("delegation/started", "delegation/completed"):
        projection.setdefault("agentDepth", data.get("childDepth", 0))
    elif et == "file/edit-applied":
        edits = projection.setdefault("fileEdits", [])
        edits.append({"artifactDigest": data.get("artifactDigest"), "target": data.get("target")})


def _fold(events: Sequence[MigrationEvent]) -> dict[str, Any]:
    projection = _initial_projection()
    for event in events:
        _apply_event(projection, event)
    return projection


def _tenant_of(scope: ScopeRef) -> str:
    cursor: ScopeRef | None = scope
    while cursor is not None:
        if cursor.kind == "tenant":
            return cursor.id[len("tenant/"):]
        cursor = cursor.parent
    raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, "scope is not attached to a tenant", {"scope": scope.id})


class MigrationSession:
    """A migration session handle — events, projection, append/flush."""

    __slots__ = ("id", "migration_id", "scope", "header", "_events", "_projection", "_service", "_scopes", "_artifacts", "_clock", "truncated_tail", "corrupt_lines")

    def __init__(self, service: "MigrationSessionService", scope: ScopeRef, header: SessionHeader, events: list[MigrationEvent], projection: dict[str, Any], *, truncated_tail: bool = False, corrupt_lines: list[int] | None = None) -> None:
        self._service = service
        self.id = header.session_id
        self.migration_id = header.migration_id
        self.scope = scope
        self.header = header
        self._events = list(events)
        self._projection = projection
        self._scopes = service._scopes
        self._artifacts = service._artifacts
        self._clock = service._clock
        self.truncated_tail = truncated_tail
        self.corrupt_lines = corrupt_lines or []

    @property
    def events(self) -> list[MigrationEvent]:
        return list(self._events)

    @property
    def projection(self) -> dict[str, Any]:
        return {
            "state": self._projection["state"],
            "lastSequence": self._projection["lastSequence"],
            "lastHash": self._projection["lastHash"],
            "openExecutionIds": set(self._projection["openExecutionIds"]),
            "executionOutcomes": dict(self._projection["executionOutcomes"]),
            "dependencyGraphs": dict(self._projection["dependencyGraphs"]),
            "semanticIrs": dict(self._projection["semanticIrs"]),
            "macroResolutions": dict(self._projection["macroResolutions"]),
            "workUnits": dict(self._projection["workUnits"]),
            "forkCount": self._projection["forkCount"],
        }

    async def append(self, type: MigrationEventType, data: dict[str, Any]) -> MigrationEvent:
        return self._service.append_event(self, type, data)

    async def flush(self) -> None:
        self._service.flush(self)

    def _current_projection(self) -> dict[str, Any]:
        return self._projection

    def _add_event(self, event: MigrationEvent) -> None:
        self._events.append(event)


class MigrationSessionService:
    def __init__(self, persistence: JsonlSessionPersistence, evidence: EvidenceService, scopes: ScopeService, clock: Clock, artifacts: Any = None) -> None:
        self._persistence = persistence
        self._evidence = evidence
        self._scopes = scopes
        self._clock = clock
        self._artifacts = artifacts
        self._sessions: dict[str, MigrationSession] = {}

    async def create(
        self, *,
        migration_id: str, scope: ScopeRef, title: str, session_id: str | None = None,
    ) -> MigrationSession:
        scope = self._scopes.require_by_id(scope.id)
        if session_id is not None and session_id in self._sessions:
            raise WorkbenchError(WorkbenchErrorCode.SESSION_ALREADY_EXISTS, "session is already open")
        sid = assert_safe_segment(session_id or f"session-{self._clock.uuid()}", "sessionId")
        if sid in self._sessions:
            raise WorkbenchError(WorkbenchErrorCode.SESSION_ALREADY_EXISTS, f"session {sid} is already open")
        header = SessionHeader(session_id=sid, migration_id=migration_id, scope_id=scope.id, created_at=self._clock.now())
        self._persistence.create(header=header)
        handle = MigrationSession(self, scope, header, [], _initial_projection())
        self._sessions[sid] = handle
        try:
            await handle.append(MigrationEventType.MIGRATION_CREATED, {
                "migrationId": migration_id,
                "tenantId": _tenant_of(scope),
                "title": title,
            })
        except Exception:
            del self._sessions[sid]
            raise
        return handle

    async def open(self, session_id: str) -> MigrationSession:
        existing = self._sessions.get(session_id)
        if existing:
            return existing
        persisted = self._persistence.read(session_id)
        scope = self._scopes.require_by_id(persisted.header.scope_id)
        self._evidence.verify(persisted.events)
        handle = MigrationSession(self, scope, persisted.header, persisted.events, _fold(persisted.events), truncated_tail=persisted.truncated_tail, corrupt_lines=persisted.corrupt_lines)
        self._sessions[session_id] = handle
        return handle

    async def find_by_migration_id(self, migration_id: str) -> MigrationSession | None:
        for session in self._sessions.values():
            if session.migration_id == migration_id:
                return session
        for sid in self._persistence.list_sessions():
            persisted = self._persistence.read(sid)
            if persisted.header.migration_id == migration_id:
                scope = self._scopes.require_by_id(persisted.header.scope_id)
                self._evidence.verify(persisted.events)
                handle = MigrationSession(self, scope, persisted.header, persisted.events, _fold(persisted.events), truncated_tail=persisted.truncated_tail, corrupt_lines=persisted.corrupt_lines)
                self._sessions[sid] = handle
                return handle
        return None

    async def replay(self, session_id: str) -> MigrationSession:
        persisted = self._persistence.read(session_id)
        scope = self._scopes.require_by_id(persisted.header.scope_id)
        self._evidence.verify(persisted.events)
        return MigrationSession(self, scope, persisted.header, persisted.events, _fold(persisted.events), truncated_tail=persisted.truncated_tail, corrupt_lines=persisted.corrupt_lines)

    def safe_boundary(self, session: MigrationSession, requested: int | None = None) -> int:
        events = session.events
        open_exec: set[str] = set()
        last_safe = 0
        for event in events:
            data = event.data
            if event.type.value == "execution/started" and isinstance(data.get("executionId"), str):
                open_exec.add(data["executionId"])
            if event.type.value in ("execution/completed", "execution/unknown") and isinstance(data.get("executionId"), str):
                open_exec.discard(data["executionId"])
            if not open_exec:
                last_safe = event.sequence
        if requested is None:
            return last_safe
        if not isinstance(requested, int) or requested < 0 or requested > len(events):
            raise WorkbenchError(WorkbenchErrorCode.UNSAFE_FORK_BOUNDARY, "fork boundary is outside the durable session", {"requested": requested, "length": len(events)})
        if requested > last_safe:
            raise WorkbenchError(WorkbenchErrorCode.UNSAFE_FORK_BOUNDARY, "fork boundary contains an open external execution", {"requested": requested, "safeBoundary": last_safe})
        return requested

    def append_event(self, session: MigrationSession, type: MigrationEventType, data: dict[str, Any]) -> MigrationEvent:
        scope = self._scopes.require_by_id(session.scope.id)
        previous = session._current_projection()
        expected_sequence = previous["lastSequence"] + 1
        previous_hash = previous["lastHash"]
        created_at = self._clock.now()
        snapshot = snapshot_json(data)
        event_id = f"{session.id}:{expected_sequence}"
        hash = self._evidence.hash(EventHashInput(
            migration_id=session.migration_id, scope_id=scope.id,
            sequence=expected_sequence, type=type, created_at=created_at,
            data=snapshot, previous_hash=previous_hash,
        ))
        event = MigrationEvent(
            eventId=event_id, migrationId=session.migration_id, scopeId=scope.id,
            sequence=expected_sequence, type=type, createdAt=created_at,
            data=snapshot, previousHash=previous_hash, hash=hash,
        )
        self._persistence.append(session.id, event)
        _apply_event(previous, event)
        session._add_event(event)
        from sas2pyspark.core.logging import log_event_appended

        log_event_appended(
            session_id=session.id,
            migration_id=session.migration_id,
            sequence=event.sequence,
            event_type=event.type.value,
            event_hash=event.hash,
        )
        return event

    def flush(self, session: MigrationSession) -> None:
        self._persistence.flush(session.id)
