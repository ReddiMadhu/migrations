"""Pytest configuration and shared fixtures — port of test/helpers.ts."""

from __future__ import annotations

import asyncio
import shutil
import sys
import warnings
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

if sys.platform == "win32":
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="asyncio")

    import socket as _socket
    from asyncio.base_events import BaseEventLoop

    _orig_socketpair = _socket.socketpair

    def _safe_socketpair(family=_socket.AF_INET, type=_socket.SOCK_STREAM, proto=0):
        try:
            return _orig_socketpair(family, type, proto)
        except OSError:
            class _NullSock:
                def fileno(self): raise OSError("null socket")
                def setblocking(self, flag): pass
                def send(self, data): return len(data)
                def recv(self, n): return b""
                def close(self): pass
            return _NullSock(), _NullSock()

    _socket.socketpair = _safe_socketpair

    def _noop_make_self_pipe(self):
        class _FakeSock:
            def fileno(self): return -1
            def setblocking(self, flag): pass
            def close(self): pass
        self._ssock = _FakeSock()
        self._csock = _FakeSock()

    def _noop_close_self_pipe(self):
        pass

    def _noop_write_to_self(self):
        pass

    def _noop_read_from_self(self):
        pass

    def _noop_loop_self_reading(self, f=None):
        pass

    def _safe_call_soon_threadsafe(self, callback, *args, context=None):
        return self.call_soon(callback, *args, context=context)

    _patch_methods = {
        "_make_self_pipe": _noop_make_self_pipe,
        "_close_self_pipe": _noop_close_self_pipe,
        "_write_to_self": _noop_write_to_self,
        "_read_from_self": _noop_read_from_self,
        "_loop_self_reading": _noop_loop_self_reading,
        "call_soon_threadsafe": _safe_call_soon_threadsafe,
    }

    for _cls in [BaseEventLoop]:
        for _name, _fn in _patch_methods.items():
            setattr(_cls, _name, _fn)

    try:
        from asyncio.proactor_events import BaseProactorEventLoop
        for _name, _fn in _patch_methods.items():
            setattr(BaseProactorEventLoop, _name, _fn)
    except ImportError:
        pass

    try:
        from asyncio.selector_events import BaseSelectorEventLoop
        for _name, _fn in _patch_methods.items():
            setattr(BaseSelectorEventLoop, _name, _fn)
    except ImportError:
        pass

    async def _sync_to_thread(func, /, *args, **kwargs):
        return func(*args, **kwargs)
    asyncio.to_thread = _sync_to_thread

    def _sync_run_in_executor(self, executor, func, /, *args, **kwargs):
        fut = self.create_future()
        try:
            fut.set_result(func(*args, **kwargs))
        except BaseException as exc:
            fut.set_exception(exc)
        return fut

    BaseEventLoop.run_in_executor = _sync_run_in_executor

import pytest
import pytest_asyncio

from sas2pyspark.services.clock import FixedClock
from sas2pyspark.workbench import WorkbenchConfig, WorkbenchRuntime, create_workbench


def pytest_collection_modifyitems(config, items):
    skip_postgres = pytest.mark.skip(reason="requires a live Postgres instance (run with -m postgres)")
    for item in items:
        if "postgres" in item.keywords:
            if not config.getoption("-m") or "postgres" not in config.getoption("-m"):
                item.add_marker(skip_postgres)

_TEST_TMP_ROOT = Path(__file__).resolve().parent.parent / ".test_tmp"
_counter = 0


@pytest.fixture
def fixed_clock() -> FixedClock:
    return FixedClock()


@pytest.fixture
def test_dir() -> Iterator[Path]:
    global _counter
    _counter += 1
    path = _TEST_TMP_ROOT / f"run-{_counter}"
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest_asyncio.fixture
async def workbench(test_dir: Path, fixed_clock: FixedClock) -> AsyncIterator[WorkbenchRuntime]:
    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        yield runtime
    finally:
        await runtime.dispose()


def make_scopes(runtime: WorkbenchRuntime, suffix: str = ""):
    tenant = runtime.scopes.create_tenant(f"tenant-a{suffix}")
    migration = runtime.scopes.create_child(tenant, "migration", f"migration-a{suffix}")
    attempt = runtime.scopes.create_child(migration, "attempt", f"attempt-a{suffix}")
    candidate = runtime.scopes.create_child(attempt, "candidate", f"candidate-a{suffix}")
    return tenant, migration, attempt, candidate


async def make_session(runtime: WorkbenchRuntime, scope=None, session_id: str = "session-test"):
    if scope is None:
        scope = runtime.scopes.require_by_id("tenant/tenant-a/migration/migration-a")
    return await runtime.sessions.create(
        migration_id="migration-a", scope=scope, title="test migration", session_id=session_id,
    )
