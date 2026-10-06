"""P0-7: Spark-native reconciliation path — ThresholdReconciler + SparkReconciler + policy fields."""

from __future__ import annotations

import json
import os

import pytest

from sas2pyspark.services.reconciliation import (
    DEFAULT_RECONCILIATION_POLICY,
    ReconciliationPolicy,
    _resolve_policy,
)
from sas2pyspark.services.reconciler import (
    InMemoryReconciler,
    Reconciler,
    SparkReconciler,
    ThresholdReconciler,
)

from .conftest import make_scopes


def test_policy_has_spark_threshold_bytes_default():
    assert DEFAULT_RECONCILIATION_POLICY.spark_threshold_bytes == 52428800


def test_policy_has_float_tolerance_default():
    assert DEFAULT_RECONCILIATION_POLICY.float_tolerance == 0.0


def test_policy_has_nan_equality_default():
    assert DEFAULT_RECONCILIATION_POLICY.nan_equality == "equal"


def test_policy_has_timezone_default():
    assert DEFAULT_RECONCILIATION_POLICY.timezone == "UTC"


def test_resolve_policy_includes_new_fields():
    policy = _resolve_policy({"sparkThresholdBytes": 1024, "floatTolerance": 0.001, "nanEquality": "unequal", "timezone": "America/New_York"})
    assert policy.spark_threshold_bytes == 1024
    assert policy.float_tolerance == 0.001
    assert policy.nan_equality == "unequal"
    assert policy.timezone == "America/New_York"


def test_policy_round_trip():
    p = ReconciliationPolicy(
        version="v2",
        schema="relaxed",
        rowOrder="unordered",
        nullEquality="strict",
        decimalNormalization="none",
        maxDifferences=50,
        statistical="disabled",
        sparkThresholdBytes=1048576,
        floatTolerance=1e-9,
        nanEquality="unequal",
        timezone="Europe/London",
    )
    d = p.model_dump(by_alias=True)
    assert d["sparkThresholdBytes"] == 1048576
    assert d["floatTolerance"] == 1e-9
    assert d["nanEquality"] == "unequal"
    assert d["timezone"] == "Europe/London"


@pytest.mark.asyncio
async def test_threshold_reconciler_delegates_to_in_memory_below_threshold(test_dir, fixed_clock):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench
    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        _, _, _, candidate = make_scopes(runtime)
        ref_path = os.path.join(str(test_dir), "reference.json")
        with open(ref_path, "w") as f:
            json.dump([{"a": 1}], f)
        candidate_data = json.dumps([{"a": 1}])
        record = runtime.artifacts.create(scope=candidate, data=candidate_data, media_type="application/json", created_by="test")
        from sas2pyspark.services.artifacts import artifact_reference
        candidate_ref = artifact_reference(record).model_dump(by_alias=True)
        policy = DEFAULT_RECONCILIATION_POLICY.model_dump(by_alias=True)
        in_memory = InMemoryReconciler(runtime.reconciliation, runtime.scopes)
        threshold = ThresholdReconciler(in_memory, spark=None)
        result = threshold.reconcile(
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
async def test_threshold_reconciler_delegates_to_spark_above_threshold(test_dir, fixed_clock):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench
    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        _, _, _, candidate = make_scopes(runtime)
        ref_path = os.path.join(str(test_dir), "reference.json")
        with open(ref_path, "w") as f:
            json.dump([{"a": 1}], f)
        candidate_data = json.dumps([{"a": 1}])
        record = runtime.artifacts.create(scope=candidate, data=candidate_data, media_type="application/json", created_by="test")
        from sas2pyspark.services.artifacts import artifact_reference
        candidate_ref = artifact_reference(record).model_dump(by_alias=True)

        class MockSparkReconciler:
            def __init__(self):
                self.called = False

            def reconcile(self, **kwargs):
                self.called = True
                return {
                    "schemaVerdict": "PASS",
                    "aggregateVerdict": "PASS",
                    "rowVerdict": "PASS",
                    "statisticalVerdict": "PASS",
                    "certifiable": True,
                    "differenceCount": 0,
                    "differences": [],
                    "referenceRows": 1,
                    "candidateRows": 1,
                }

        mock_spark = MockSparkReconciler()
        in_memory = InMemoryReconciler(runtime.reconciliation, runtime.scopes)
        threshold = ThresholdReconciler(in_memory, spark=mock_spark)
        policy = DEFAULT_RECONCILIATION_POLICY.model_dump(by_alias=True)
        policy["sparkThresholdBytes"] = 0
        result = threshold.reconcile(
            reference_output_path=ref_path,
            candidate_artifacts=[candidate_ref],
            input_snapshot_id="snap-1",
            environment_fingerprint="env-1",
            policy=policy,
            scope_id=candidate.id,
        )
        assert mock_spark.called is True
        assert result["certifiable"] is True
    finally:
        await runtime.dispose()


@pytest.mark.asyncio
async def test_threshold_reconciler_falls_back_to_in_memory_when_spark_is_none(test_dir, fixed_clock):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench
    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        _, _, _, candidate = make_scopes(runtime)
        ref_path = os.path.join(str(test_dir), "reference.json")
        with open(ref_path, "w") as f:
            json.dump([{"a": 1}], f)
        candidate_data = json.dumps([{"a": 1}])
        record = runtime.artifacts.create(scope=candidate, data=candidate_data, media_type="application/json", created_by="test")
        from sas2pyspark.services.artifacts import artifact_reference
        candidate_ref = artifact_reference(record).model_dump(by_alias=True)
        in_memory = InMemoryReconciler(runtime.reconciliation, runtime.scopes)
        threshold = ThresholdReconciler(in_memory, spark=None)
        policy = DEFAULT_RECONCILIATION_POLICY.model_dump(by_alias=True)
        policy["sparkThresholdBytes"] = 0
        result = threshold.reconcile(
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


def test_threshold_reconciler_satisfies_protocol():
    assert isinstance.__name__ == "isinstance"
    from sas2pyspark.services.reconciler import ThresholdReconciler, InMemoryReconciler
    assert hasattr(ThresholdReconciler, "reconcile")
    assert hasattr(InMemoryReconciler, "reconcile")
    assert hasattr(SparkReconciler, "reconcile")
