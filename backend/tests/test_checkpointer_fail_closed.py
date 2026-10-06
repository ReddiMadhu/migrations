"""P0-5: Checkpointer fail-closed — production requires durable storage, no silent MemorySaver fallback."""

from __future__ import annotations

import pytest

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.graph.checkpointer import resolve_checkpointer


def test_development_uses_memory_saver():
    cp = resolve_checkpointer(mode="development")
    assert type(cp).__name__ in ("MemorySaver", "InMemorySaver")


def test_explicit_checkpointer_is_returned():
    sentinel = object()
    assert resolve_checkpointer(checkpointer=sentinel) is sentinel


def test_production_fails_closed_without_sqlite(tmp_path, monkeypatch):
    import builtins
    real_import = builtins.__import__

    def _block_sqlite(name, *args, **kwargs):
        if "langgraph.checkpoint.sqlite" in name:
            raise ModuleNotFoundError("blocked for test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _block_sqlite)
    with pytest.raises(WorkbenchError) as exc_info:
        resolve_checkpointer(
            mode="production",
            checkpoint_path=str(tmp_path / "cp.sqlite"),
        )
    assert exc_info.value.code == WorkbenchErrorCode.CONFIG_ERROR


def test_environment_production_fails_closed(tmp_path, monkeypatch):
    import builtins
    real_import = builtins.__import__

    def _block_sqlite(name, *args, **kwargs):
        if "langgraph.checkpoint.sqlite" in name:
            raise ModuleNotFoundError("blocked for test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _block_sqlite)
    with pytest.raises(WorkbenchError):
        resolve_checkpointer(
            mode="development",
            environment="production",
            checkpoint_path=str(tmp_path / "cp.sqlite"),
        )


def test_production_with_explicit_checkpointer_bypasses_resolution():
    sentinel = object()
    cp = resolve_checkpointer(checkpointer=sentinel, mode="production", environment="production")
    assert cp is sentinel


@pytest.mark.asyncio
async def test_workbench_production_checkpointer_fail_closed(test_dir, monkeypatch):
    import builtins
    real_import = builtins.__import__

    def _block_sqlite(name, *args, **kwargs):
        if "langgraph.checkpoint.sqlite" in name:
            raise ModuleNotFoundError("blocked for test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _block_sqlite)
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench
    with pytest.raises(WorkbenchError) as exc_info:
        await create_workbench(WorkbenchConfig(
            root_dir=str(test_dir),
            checkpointer_mode="production",
            environment="production",
        ))
    assert exc_info.value.code == WorkbenchErrorCode.CONFIG_ERROR


@pytest.mark.asyncio
async def test_workbench_development_uses_memory_saver(test_dir):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench
    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir)))
    try:
        assert type(runtime.checkpointer).__name__ in ("MemorySaver", "InMemorySaver")
    finally:
        await runtime.dispose()


@pytest.mark.asyncio
async def test_repair_subgraph_uses_parent_checkpointer(test_dir, fixed_clock):
    from sas2pyspark.graph.checkpointer import resolve_checkpointer
    from sas2pyspark.graph.subgraphs.repair import build_repair_subgraph
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench

    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        parent_cp = resolve_checkpointer(mode="development")
        subgraph = build_repair_subgraph(runtime, checkpointer=parent_cp)
        assert subgraph is not None
    finally:
        await runtime.dispose()


def test_cosmos_fails_closed_without_endpoint_or_key(monkeypatch):
    monkeypatch.delenv("AZURE_COSMOS_ENDPOINT", raising=False)
    monkeypatch.delenv("AZURE_COSMOS_KEY", raising=False)
    with pytest.raises(WorkbenchError) as exc_info:
        resolve_checkpointer(mode="cosmos")
    assert exc_info.value.code == WorkbenchErrorCode.CONFIG_ERROR


def test_cosmos_fails_closed_missing_package(monkeypatch):
    import builtins
    real_import = builtins.__import__

    def _block_cosmos(name, *args, **kwargs):
        if "azure" in name:
            raise ImportError("blocked for test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _block_cosmos)
    with pytest.raises(WorkbenchError) as exc_info:
        resolve_checkpointer(mode="cosmos")
    assert exc_info.value.code == WorkbenchErrorCode.CONFIG_ERROR


def test_cosmos_mode_alias_azure_cosmos(monkeypatch):
    monkeypatch.delenv("AZURE_COSMOS_ENDPOINT", raising=False)
    monkeypatch.delenv("AZURE_COSMOS_KEY", raising=False)
    with pytest.raises(WorkbenchError) as exc_info:
        resolve_checkpointer(mode="azure_cosmos")
    assert exc_info.value.code == WorkbenchErrorCode.CONFIG_ERROR


@pytest.mark.asyncio
async def test_workbench_cosmos_fails_closed_without_credentials(test_dir, monkeypatch):
    monkeypatch.delenv("AZURE_COSMOS_ENDPOINT", raising=False)
    monkeypatch.delenv("AZURE_COSMOS_KEY", raising=False)
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench

    with pytest.raises(WorkbenchError) as exc_info:
        await create_workbench(WorkbenchConfig(
            root_dir=str(test_dir),
            checkpointer_mode="cosmos",
        ))
    assert exc_info.value.code == WorkbenchErrorCode.CONFIG_ERROR
