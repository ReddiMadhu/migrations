"""P0-6: Reconciler Protocol + InMemoryReconciler adapter + graph wiring."""

from __future__ import annotations

import json
import os

import pytest

from sas2pyspark.services.reconciler import Reconciler, InMemoryReconciler

from .conftest import make_scopes


@pytest.mark.asyncio
async def test_inmemory_reconciler_satisfies_protocol(test_dir, fixed_clock):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench
    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        assert isinstance(runtime.reconciler, Reconciler)
    finally:
        await runtime.dispose()


@pytest.mark.asyncio
async def test_reconcile_returns_dict_with_required_fields(test_dir, fixed_clock):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench
    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        _, _, _, candidate = make_scopes(runtime)
        ref_path = os.path.join(str(test_dir), "reference.json")
        with open(ref_path, "w") as f:
            json.dump([{"a": 1, "b": 2}, {"a": 3, "b": 4}], f)
        candidate_data = json.dumps([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
        record = runtime.artifacts.create(scope=candidate, data=candidate_data, media_type="application/json", created_by="test")
        from sas2pyspark.services.artifacts import artifact_reference
        candidate_ref = artifact_reference(record).model_dump(by_alias=True)
        from sas2pyspark.services.reconciliation import DEFAULT_RECONCILIATION_POLICY
        policy = DEFAULT_RECONCILIATION_POLICY.model_dump(by_alias=True)
        result = runtime.reconciler.reconcile(
            reference_output_path=ref_path,
            candidate_artifacts=[candidate_ref],
            input_snapshot_id="snap-1",
            environment_fingerprint="env-1",
            policy=policy,
            scope_id=candidate.id,
        )
        assert isinstance(result, dict)
        assert "certifiable" in result
        assert "differenceCount" in result
        assert "schemaVerdict" in result
        assert "aggregateVerdict" in result
        assert "rowVerdict" in result
        assert "statisticalVerdict" in result
        assert result["certifiable"] is True
        assert result["differenceCount"] == 0
    finally:
        await runtime.dispose()


@pytest.mark.asyncio
async def test_reconcile_detects_differences(test_dir, fixed_clock):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench
    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        _, _, _, candidate = make_scopes(runtime)
        ref_path = os.path.join(str(test_dir), "reference.json")
        with open(ref_path, "w") as f:
            json.dump([{"a": 1, "b": 2}, {"a": 3, "b": 4}], f)
        candidate_data = json.dumps([{"a": 1, "b": 2}, {"a": 3, "b": 99}])
        record = runtime.artifacts.create(scope=candidate, data=candidate_data, media_type="application/json", created_by="test")
        from sas2pyspark.services.artifacts import artifact_reference
        candidate_ref = artifact_reference(record).model_dump(by_alias=True)
        from sas2pyspark.services.reconciliation import DEFAULT_RECONCILIATION_POLICY
        policy = DEFAULT_RECONCILIATION_POLICY.model_dump(by_alias=True)
        result = runtime.reconciler.reconcile(
            reference_output_path=ref_path,
            candidate_artifacts=[candidate_ref],
            input_snapshot_id="snap-1",
            environment_fingerprint="env-1",
            policy=policy,
            scope_id=candidate.id,
        )
        assert result["certifiable"] is False
        assert result["differenceCount"] > 0
    finally:
        await runtime.dispose()


@pytest.mark.asyncio
async def test_reconcile_csv_reference(test_dir, fixed_clock):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench
    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        _, _, _, candidate = make_scopes(runtime)
        ref_path = os.path.join(str(test_dir), "reference.csv")
        with open(ref_path, "w") as f:
            f.write("a,b\n1,2\n3,4\n")
        candidate_data = "a,b\n1,2\n3,4\n"
        record = runtime.artifacts.create(scope=candidate, data=candidate_data, media_type="text/csv", created_by="test")
        from sas2pyspark.services.artifacts import artifact_reference
        candidate_ref = artifact_reference(record).model_dump(by_alias=True)
        from sas2pyspark.services.reconciliation import DEFAULT_RECONCILIATION_POLICY
        policy = DEFAULT_RECONCILIATION_POLICY.model_dump(by_alias=True)
        result = runtime.reconciler.reconcile(
            reference_output_path=ref_path,
            candidate_artifacts=[candidate_ref],
            input_snapshot_id="snap-1",
            environment_fingerprint="env-1",
            policy=policy,
            scope_id=candidate.id,
        )
        assert result["certifiable"] is True
    finally:
        await runtime.dispose()


@pytest.mark.asyncio
async def test_reconcile_missing_reference_path_raises(test_dir, fixed_clock):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench
    from sas2pyspark.core.errors import WorkbenchError
    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        _, _, _, candidate = make_scopes(runtime)
        candidate_data = json.dumps([{"a": 1}])
        record = runtime.artifacts.create(scope=candidate, data=candidate_data, media_type="application/json", created_by="test")
        from sas2pyspark.services.artifacts import artifact_reference
        candidate_ref = artifact_reference(record).model_dump(by_alias=True)
        from sas2pyspark.services.reconciliation import DEFAULT_RECONCILIATION_POLICY
        policy = DEFAULT_RECONCILIATION_POLICY.model_dump(by_alias=True)
        with pytest.raises(WorkbenchError):
            runtime.reconciler.reconcile(
                reference_output_path=os.path.join(str(test_dir), "nonexistent.json"),
                candidate_artifacts=[candidate_ref],
                input_snapshot_id="snap-1",
                environment_fingerprint="env-1",
                policy=policy,
                scope_id=candidate.id,
            )
    finally:
        await runtime.dispose()


@pytest.mark.asyncio
async def test_reconcile_empty_candidate_artifacts_raises(test_dir, fixed_clock):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench
    from sas2pyspark.core.errors import WorkbenchError
    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        _, _, _, candidate = make_scopes(runtime)
        ref_path = os.path.join(str(test_dir), "reference.json")
        with open(ref_path, "w") as f:
            json.dump([{"a": 1}], f)
        from sas2pyspark.services.reconciliation import DEFAULT_RECONCILIATION_POLICY
        policy = DEFAULT_RECONCILIATION_POLICY.model_dump(by_alias=True)
        with pytest.raises(WorkbenchError):
            runtime.reconciler.reconcile(
                reference_output_path=ref_path,
                candidate_artifacts=[],
                input_snapshot_id="snap-1",
                environment_fingerprint="env-1",
                policy=policy,
                scope_id=candidate.id,
            )
    finally:
        await runtime.dispose()


@pytest.mark.asyncio
async def test_graph_uses_reconciler_by_default(test_dir, fixed_clock):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench
    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        assert hasattr(runtime.reconciler, "reconcile")
        assert callable(runtime.reconciler.reconcile)
    finally:
        await runtime.dispose()
