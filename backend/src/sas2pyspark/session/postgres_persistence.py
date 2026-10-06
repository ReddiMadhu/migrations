"""Postgres-backed session persistence — transactional append with row-level locking."""

from __future__ import annotations

import json
from typing import Any, Sequence

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.core.events import MigrationEvent
from sas2pyspark.session.persistence import (
    PersistedSession,
    SessionHeader,
    SessionPersistence,
    _parse_event,
    _parse_header,
)


class PostgresSessionPersistence:
    """Postgres-backed session persistence with transactional append and row-level locks."""

    def __init__(self, dsn: str, *, connection_factory: Any = None) -> None:
        self._dsn = dsn
        self._connection_factory = connection_factory
        self._conn: Any = None

    def _get_connection(self) -> Any:
        if self._conn is not None:
            return self._conn
        if self._connection_factory is not None:
            self._conn = self._connection_factory(self._dsn)
        else:
            import psycopg2
            self._conn = psycopg2.connect(self._dsn)
        self._conn.autocommit = False
        return self._conn

    def _ensure_schema(self) -> None:
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS migration_sessions (
                    session_id TEXT PRIMARY KEY,
                    migration_id TEXT NOT NULL,
                    scope_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    header JSONB NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS migration_events (
                    session_id TEXT NOT NULL REFERENCES migration_sessions(session_id),
                    sequence INTEGER NOT NULL,
                    event JSONB NOT NULL,
                    PRIMARY KEY (session_id, sequence)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_session ON migration_events(session_id)
            """)
        conn.commit()

    def create(self, *, header: SessionHeader, initial_events: Sequence[MigrationEvent] | None = None) -> None:
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM migration_sessions WHERE session_id = %s", (header.session_id,))
            if cur.fetchone() is not None:
                raise WorkbenchError(WorkbenchErrorCode.SESSION_ALREADY_EXISTS, f"session {header.session_id} already exists")
            cur.execute(
                "INSERT INTO migration_sessions (session_id, migration_id, scope_id, created_at, header) VALUES (%s, %s, %s, %s, %s)",
                (header.session_id, header.migration_id, header.scope_id, header.created_at, json.dumps(header.to_dict())),
            )
            for event in (initial_events or []):
                cur.execute(
                    "INSERT INTO migration_events (session_id, sequence, event) VALUES (%s, %s, %s)",
                    (header.session_id, event.sequence, json.dumps(event.model_dump(by_alias=True))),
                )
        conn.commit()

    def read(self, session_id: str) -> PersistedSession:
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT header FROM migration_sessions WHERE session_id = %s", (session_id,))
            row = cur.fetchone()
            if row is None:
                raise WorkbenchError(WorkbenchErrorCode.SESSION_NOT_FOUND, f"session {session_id} does not exist", {"sessionId": session_id})
            header = _parse_header(row[0] if isinstance(row[0], dict) else json.loads(row[0]))
            cur.execute(
                "SELECT event FROM migration_events WHERE session_id = %s ORDER BY sequence",
                (session_id,),
            )
            events: list[MigrationEvent] = []
            corrupt_lines: list[int] = []
            for idx, (event_data,) in enumerate(cur.fetchall(), 1):
                try:
                    events.append(_parse_event(event_data if isinstance(event_data, dict) else json.loads(event_data)))
                except (json.JSONDecodeError, WorkbenchError):
                    corrupt_lines.append(idx)
            truncated = len(corrupt_lines) > 0
        return PersistedSession(header=header, events=events, truncated_tail=truncated, corrupt_lines=corrupt_lines)

    def append(self, session_id: str, event: MigrationEvent) -> None:
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM migration_sessions WHERE session_id = %s FOR UPDATE", (session_id,))
            if cur.fetchone() is None:
                raise WorkbenchError(WorkbenchErrorCode.SESSION_NOT_FOUND, f"session {session_id} does not exist", {"sessionId": session_id})
            cur.execute(
                "INSERT INTO migration_events (session_id, sequence, event) VALUES (%s, %s, %s)",
                (session_id, event.sequence, json.dumps(event.model_dump(by_alias=True))),
            )
        conn.commit()

    def flush(self, session_id: str) -> None:
        pass

    def dispose(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
