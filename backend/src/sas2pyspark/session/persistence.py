"""JSONL session persistence — append-only event log with durable writes."""

from __future__ import annotations

import json
import os
import sys
from typing import Any, Protocol, Sequence, runtime_checkable

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.core.json import assert_safe_segment, snapshot_json
from sas2pyspark.core.events import MigrationEvent, MigrationEventType

_IS_WINDOWS = sys.platform == "win32"


@runtime_checkable
class SessionPersistence(Protocol):
    """Session persistence protocol — JSONL and Postgres both implement this."""

    def create(self, *, header: SessionHeader, initial_events: Sequence[MigrationEvent] | None = None) -> None: ...
    def read(self, session_id: str) -> PersistedSession: ...
    def append(self, session_id: str, event: MigrationEvent) -> None: ...
    def flush(self, session_id: str) -> None: ...
    def list_sessions(self) -> list[str]: ...
    def dispose(self) -> None: ...


def _fsync_file(path: str) -> None:
    try:
        with open(path, "rb") as f:
            os.fsync(f.fileno())
    except (OSError, AttributeError):
        pass


def _file_lock(f):
    if _IS_WINDOWS:
        import msvcrt
        try:
            msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
            yield
            msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            yield
    else:
        import fcntl
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)


import contextlib


@contextlib.contextmanager
def _acquire_lock(f):
    if _IS_WINDOWS:
        import msvcrt
        locked = False
        try:
            msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
            locked = True
        except OSError:
            pass
        try:
            yield
        finally:
            if locked:
                try:
                    msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
                except OSError:
                    pass
    else:
        import fcntl
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)


class SessionHeader:
    __slots__ = ("session_id", "migration_id", "scope_id", "created_at", "parent_session_id", "parent_sequence", "fork_id", "candidate_id", "policy_snapshot", "guard_snapshot", "artifact_base")

    def __init__(self, **kwargs: Any) -> None:
        self.session_id = kwargs["session_id"]
        self.migration_id = kwargs["migration_id"]
        self.scope_id = kwargs["scope_id"]
        self.created_at = kwargs["created_at"]
        self.parent_session_id = kwargs.get("parent_session_id")
        self.parent_sequence = kwargs.get("parent_sequence")
        self.fork_id = kwargs.get("fork_id")
        self.candidate_id = kwargs.get("candidate_id")
        self.policy_snapshot = kwargs.get("policy_snapshot")
        self.guard_snapshot = kwargs.get("guard_snapshot")
        self.artifact_base = kwargs.get("artifact_base")

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"sessionId": self.session_id, "migrationId": self.migration_id, "scopeId": self.scope_id, "createdAt": self.created_at}
        for attr, alias in [
            ("parent_session_id", "parentSessionId"), ("parent_sequence", "parentSequence"),
            ("fork_id", "forkId"), ("candidate_id", "candidateId"),
            ("policy_snapshot", "policySnapshot"), ("guard_snapshot", "guardSnapshot"),
            ("artifact_base", "artifactBase"),
        ]:
            val = getattr(self, attr)
            if val is not None:
                d[alias] = val
        return d


class PersistedSession:
    __slots__ = ("header", "events", "truncated_tail", "corrupt_lines")

    def __init__(self, *, header: SessionHeader, events: list[MigrationEvent], truncated_tail: bool, corrupt_lines: list[int] | None = None) -> None:
        self.header = header
        self.events = events
        self.truncated_tail = truncated_tail
        self.corrupt_lines = corrupt_lines or []


def _parse_header(value: Any) -> SessionHeader:
    if not isinstance(value, dict):
        raise WorkbenchError(WorkbenchErrorCode.SESSION_CORRUPTION, "session header is not an object")
    for key in ("sessionId", "migrationId", "scopeId", "createdAt"):
        if not isinstance(value.get(key), str):
            raise WorkbenchError(WorkbenchErrorCode.SESSION_CORRUPTION, f"session header is missing {key}")
    return SessionHeader(
        session_id=value["sessionId"], migration_id=value["migrationId"],
        scope_id=value["scopeId"], created_at=value["createdAt"],
        parent_session_id=value.get("parentSessionId"),
        parent_sequence=value.get("parentSequence"),
        fork_id=value.get("forkId"), candidate_id=value.get("candidateId"),
        policy_snapshot=value.get("policySnapshot"), guard_snapshot=value.get("guardSnapshot"),
        artifact_base=value.get("artifactBase"),
    )


def _parse_event(value: Any) -> MigrationEvent:
    if not isinstance(value, dict):
        raise WorkbenchError(WorkbenchErrorCode.SESSION_CORRUPTION, "session event is not an object")
    for key in ("eventId", "migrationId", "scopeId", "type", "createdAt", "previousHash", "hash"):
        if not isinstance(value.get(key), str):
            raise WorkbenchError(WorkbenchErrorCode.SESSION_CORRUPTION, f"session event is missing {key}")
    if not isinstance(value.get("sequence"), int) or value["sequence"] < 1:
        raise WorkbenchError(WorkbenchErrorCode.SESSION_CORRUPTION, "session event sequence is invalid")
    if not isinstance(value.get("data"), dict):
        raise WorkbenchError(WorkbenchErrorCode.SESSION_CORRUPTION, "session event data is invalid")
    return MigrationEvent(**value)


class JsonlSessionPersistence:
    """JSONL-backed session persistence — one directory per session."""

    def __init__(self, root_dir: str, *, strict: bool = False) -> None:
        self._root_dir = os.path.abspath(root_dir)
        self._strict = strict
        os.makedirs(os.path.join(self._root_dir, "sessions"), exist_ok=True)

    def _session_dir(self, session_id: str) -> str:
        safe = assert_safe_segment(session_id, "sessionId")
        path = os.path.join(self._root_dir, "sessions", safe)
        return path

    def _header_path(self, session_id: str) -> str:
        return os.path.join(self._session_dir(session_id), "header.json")

    def _events_path(self, session_id: str) -> str:
        return os.path.join(self._session_dir(session_id), "events.jsonl")

    def create(self, *, header: SessionHeader, initial_events: Sequence[MigrationEvent] | None = None) -> None:
        d = self._session_dir(header.session_id)
        if os.path.exists(d):
            raise WorkbenchError(WorkbenchErrorCode.SESSION_ALREADY_EXISTS, f"session {header.session_id} already exists")
        os.makedirs(d)
        with open(self._header_path(header.session_id), "x") as f:
            f.write(json.dumps(header.to_dict(), indent=2) + "\n")
        with open(self._events_path(header.session_id), "x") as f:
            for event in (initial_events or []):
                f.write(json.dumps(event.model_dump(by_alias=True)) + "\n")

    def read(self, session_id: str) -> PersistedSession:
        safe = assert_safe_segment(session_id, "sessionId")
        header_path = self._header_path(safe)
        events_path = self._events_path(safe)
        if not os.path.exists(header_path):
            raise WorkbenchError(WorkbenchErrorCode.SESSION_NOT_FOUND, f"session {session_id} does not exist", {"sessionId": session_id})
        with open(header_path) as f:
            header = _parse_header(json.load(f))
        events: list[MigrationEvent] = []
        truncated = False
        corrupt_lines: list[int] = []
        if os.path.exists(events_path):
            with open(events_path) as f:
                lines = f.read().splitlines()
            for idx, line in enumerate(lines, 1):
                if not line.strip():
                    continue
                try:
                    events.append(_parse_event(json.loads(line)))
                except (json.JSONDecodeError, WorkbenchError):
                    corrupt_lines.append(idx)
                    truncated = True
        if corrupt_lines and self._strict:
            raise WorkbenchError(
                WorkbenchErrorCode.SESSION_CORRUPTION,
                f"session {session_id} has {len(corrupt_lines)} corrupt event line(s)",
                {"sessionId": session_id, "corruptLines": corrupt_lines, "lineRange": [corrupt_lines[0], corrupt_lines[-1]]},
            )
        return PersistedSession(header=header, events=events, truncated_tail=truncated, corrupt_lines=corrupt_lines)

    def append(self, session_id: str, event: MigrationEvent) -> None:
        safe = assert_safe_segment(session_id, "sessionId")
        events_path = self._events_path(safe)
        if not os.path.exists(events_path):
            raise WorkbenchError(WorkbenchErrorCode.SESSION_NOT_FOUND, f"session {session_id} does not exist", {"sessionId": session_id})
        line = json.dumps(event.model_dump(by_alias=True)) + "\n"
        with open(events_path, "a") as f:
            with _acquire_lock(f):
                f.write(line)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except (OSError, AttributeError):
                    pass

    def flush(self, session_id: str) -> None:
        safe = assert_safe_segment(session_id, "sessionId")
        events_path = self._events_path(safe)
        if os.path.exists(events_path):
            _fsync_file(events_path)

    def list_sessions(self) -> list[str]:
        sessions_dir = os.path.join(self._root_dir, "sessions")
        if not os.path.exists(sessions_dir):
            return []
        result = []
        for entry in os.listdir(sessions_dir):
            header_path = os.path.join(sessions_dir, entry, "header.json")
            if os.path.exists(header_path):
                result.append(entry)
        return result

    def dispose(self) -> None:
        pass
