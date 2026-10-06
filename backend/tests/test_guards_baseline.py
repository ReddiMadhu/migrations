"""P0-1: Baseline guards registered by default (fail-closed)."""

from __future__ import annotations

import warnings

import pytest
import pytest_asyncio

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.services.guards import GuardService, GuardRequest
from sas2pyspark.services.baseline_guards import register_baseline_guards
from sas2pyspark.workbench import WorkbenchConfig, WorkbenchRuntime, create_workbench

from .conftest import make_scopes


def _make_guard_request(runtime: WorkbenchRuntime, scope, artifact) -> GuardRequest:
    return GuardRequest(
        scope=scope, action="spark.submit", artifact=artifact,
        input_snapshot_id="snap-test", environment_fingerprint="env-test",
        policy_fingerprint="policy-test",
    )


async def _store_pyspark(runtime: WorkbenchRuntime, scope, source: str):
    return runtime.artifacts.create(scope=scope, data=source, media_type="text/x-python", created_by="test")


def test_empty_guard_list_denies_by_default():
    svc = GuardService()
    assert svc._fail_closed is True
    from unittest.mock import MagicMock
    mock_request = MagicMock()
    mock_request.scope.id = "test-scope"
    mock_request.action = "spark.submit"
    with pytest.raises(WorkbenchError) as exc_info:
        svc.evaluate(mock_request)
    assert exc_info.value.code == WorkbenchErrorCode.POLICY_DENIED
    assert exc_info.value.details.get("reasonCode") == WorkbenchErrorCode.NO_GUARDS_REGISTERED.value


def test_fail_closed_false_allows_with_warning():
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        svc = GuardService(fail_closed=False)
    assert svc._fail_closed is False


@pytest_asyncio.fixture
async def guard_workbench(test_dir, fixed_clock):
    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        yield runtime
    finally:
        await runtime.dispose()


@pytest.mark.asyncio
async def test_baseline_guards_registered_in_workbench(guard_workbench):
    assert len(guard_workbench.guards._guards) >= 7


@pytest.mark.asyncio
async def test_forbidden_imports_extended(guard_workbench):
    tenant, migration, attempt, candidate = make_scopes(guard_workbench)
    for mod in ["pickle", "ctypes", "importlib", "sys", "marshal", "shutil", "pathlib", "multiprocessing"]:
        source = f"import {mod}\n"
        artifact = await _store_pyspark(guard_workbench, candidate, source)
        request = _make_guard_request(guard_workbench, candidate, artifact)
        with pytest.raises(WorkbenchError, match="denied"):
            guard_workbench.guards.evaluate(request)


@pytest.mark.asyncio
async def test_builtins_eval_bypass_detected(guard_workbench):
    tenant, migration, attempt, candidate = make_scopes(guard_workbench)
    source = "import builtins\nbuiltins.eval('1+1')\n"
    artifact = await _store_pyspark(guard_workbench, candidate, source)
    request = _make_guard_request(guard_workbench, candidate, artifact)
    with pytest.raises(WorkbenchError, match="denied"):
        guard_workbench.guards.evaluate(request)


@pytest.mark.asyncio
async def test_udf_decorator_detected(guard_workbench):
    tenant, migration, attempt, candidate = make_scopes(guard_workbench)
    source = "from pyspark.sql.functions import udf\n@udf\ndef my_udf(x):\n    return x + 1\n"
    artifact = await _store_pyspark(guard_workbench, candidate, source)
    request = _make_guard_request(guard_workbench, candidate, artifact)
    with pytest.raises(WorkbenchError, match="denied"):
        guard_workbench.guards.evaluate(request)


@pytest.mark.asyncio
async def test_dynamic_sql_detected(guard_workbench):
    tenant, migration, attempt, candidate = make_scopes(guard_workbench)
    source = 'spark.sql(f"SELECT * FROM {table}")\n'
    artifact = await _store_pyspark(guard_workbench, candidate, source)
    request = _make_guard_request(guard_workbench, candidate, artifact)
    with pytest.raises(WorkbenchError, match="denied"):
        guard_workbench.guards.evaluate(request)


@pytest.mark.asyncio
async def test_dynamic_import_indirection_detected(guard_workbench):
    tenant, migration, attempt, candidate = make_scopes(guard_workbench)
    source = "import importlib\nimportlib.import_module('os')\n"
    artifact = await _store_pyspark(guard_workbench, candidate, source)
    request = _make_guard_request(guard_workbench, candidate, artifact)
    with pytest.raises(WorkbenchError, match="denied"):
        guard_workbench.guards.evaluate(request)


@pytest.mark.asyncio
async def test_globals_indirection_detected(guard_workbench):
    tenant, migration, attempt, candidate = make_scopes(guard_workbench)
    source = 'globals()["eval"]("1+1")\n'
    artifact = await _store_pyspark(guard_workbench, candidate, source)
    request = _make_guard_request(guard_workbench, candidate, artifact)
    with pytest.raises(WorkbenchError, match="denied"):
        guard_workbench.guards.evaluate(request)


@pytest.mark.asyncio
async def test_getattr_builtins_indirection_detected(guard_workbench):
    tenant, migration, attempt, candidate = make_scopes(guard_workbench)
    source = 'getattr(__builtins__, "eval")("1+1")\n'
    artifact = await _store_pyspark(guard_workbench, candidate, source)
    request = _make_guard_request(guard_workbench, candidate, artifact)
    with pytest.raises(WorkbenchError, match="denied"):
        guard_workbench.guards.evaluate(request)


@pytest.mark.asyncio
async def test_existing_denials_preserved(guard_workbench):
    tenant, migration, attempt, candidate = make_scopes(guard_workbench)
    source = "import os\nos.system('rm -rf /')\n"
    artifact = await _store_pyspark(guard_workbench, candidate, source)
    request = _make_guard_request(guard_workbench, candidate, artifact)
    with pytest.raises(WorkbenchError, match="denied"):
        guard_workbench.guards.evaluate(request)


@pytest.mark.asyncio
async def test_clean_pyspark_allowed(guard_workbench):
    tenant, migration, attempt, candidate = make_scopes(guard_workbench)
    source = "from pyspark.sql import SparkSession\nspark = SparkSession.builder.getOrCreate()\ndf = spark.read.parquet('/Volumes/data/input')\ndf.write.mode('overwrite').parquet('/Volumes/data/output')\n"
    artifact = await _store_pyspark(guard_workbench, candidate, source)
    request = _make_guard_request(guard_workbench, candidate, artifact)
    guard_workbench.guards.evaluate(request)
