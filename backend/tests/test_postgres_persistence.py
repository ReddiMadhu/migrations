"""P0-9: PostgresSessionPersistence adapter — transactional append with row-level locking.

Tests are marked @pytest.mark.postgres and skipped by default (no Postgres in sandbox).
Run with: pytest -m postgres
"""

from __future__ import annotations

import pytest

from sas2pyspark.session.persistence import SessionPersistence, SessionHeader
from sas2pyspark.session.postgres_persistence import PostgresSessionPersistence

pytestmark = pytest.mark.postgres


def _make_header(session_id: str = "sess-test") -> SessionHeader:
    return SessionHeader(
        session_id=session_id,
        migration_id="mig-test",
        scope_id="tenant/tenant-a/migration/migration-a",
        created_at="2026-01-01T00:00:01.000Z",
    )


def test_postgres_persistence_satisfies_protocol():
    p = PostgresSessionPersistence("postgresql://localhost/test")
    assert isinstance(p, SessionPersistence)


def test_postgres_persistence_has_required_methods():
    p = PostgresSessionPersistence("postgresql://localhost/test")
    assert hasattr(p, "create")
    assert hasattr(p, "read")
    assert hasattr(p, "append")
    assert hasattr(p, "flush")
    assert hasattr(p, "dispose")


def test_postgres_persistence_accepts_connection_factory():
    class MockConn:
        def __init__(self):
            self.autocommit = False

        def cursor(self):
            class MockCursor:
                def __enter__(self): return self
                def __exit__(self, *args): pass
                def execute(self, *args): pass
                def fetchone(self): return None
                def fetchall(self): return []
            return MockCursor()

        def commit(self): pass
        def close(self): pass

    def factory(dsn):
        return MockConn()

    p = PostgresSessionPersistence("postgresql://localhost/test", connection_factory=factory)
    assert p._connection_factory is factory


def test_postgres_persistence_dsn_stored():
    p = PostgresSessionPersistence("postgresql://localhost/testdb")
    assert p._dsn == "postgresql://localhost/testdb"


def test_postgres_persistence_dispose_closes_connection():
    class MockConn:
        closed = False

        def __init__(self):
            self.autocommit = False

        def cursor(self):
            class MockCursor:
                def __enter__(self): return self
                def __exit__(self, *args): pass
                def execute(self, *args): pass
                def fetchone(self): return None
                def fetchall(self): return []
            return MockCursor()

        def commit(self): pass
        def close(self): self.closed = True

    conn = MockConn()
    p = PostgresSessionPersistence("postgresql://localhost/test", connection_factory=lambda dsn: conn)
    p._get_connection()
    p.dispose()
    assert conn.closed is True
    assert p._conn is None


def test_jsonl_persistence_satisfies_protocol():
    from sas2pyspark.session.persistence import JsonlSessionPersistence
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        p = JsonlSessionPersistence(tmp)
        assert isinstance(p, SessionPersistence)
