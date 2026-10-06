"""P0-8: Reconciliation correctness fixes — decimal errors, row_index, CRLF, single _row_key."""

from __future__ import annotations

import json
import os

import pytest

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.services.reconciliation import (
    DEFAULT_RECONCILIATION_POLICY,
    ReconciliationPolicy,
    ReconciliationService,
    _add_decimal,
    _resolve_policy,
    _summarize_dataset,
)

from .conftest import make_scopes


def test_add_decimal_raises_on_non_numeric():
    with pytest.raises(WorkbenchError) as exc_info:
        _add_decimal("abc", "123")
    assert exc_info.value.code == WorkbenchErrorCode.INVALID_INPUT


def test_add_decimal_raises_on_nan_string():
    with pytest.raises(WorkbenchError):
        _add_decimal("not_a_number", "123")


def test_add_decimal_succeeds_on_valid_numbers():
    result = _add_decimal("1.5", "2.5")
    assert result == "4"


def test_add_decimal_handles_negative():
    result = _add_decimal("-1.5", "1.5")
    assert result == "0"


@pytest.mark.asyncio
async def test_unordered_row_index_reflects_actual_position(test_dir, fixed_clock):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench
    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        _, _, _, candidate = make_scopes(runtime)
        ref_path = os.path.join(str(test_dir), "reference.json")
        with open(ref_path, "w") as f:
            json.dump([{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}], f)
        candidate_data = json.dumps([{"a": 1}, {"a": 2}, {"a": 3}, {"a": 99}])
        record = runtime.artifacts.create(scope=candidate, data=candidate_data, media_type="application/json", created_by="test")
        from sas2pyspark.services.artifacts import artifact_reference
        candidate_ref = artifact_reference(record).model_dump(by_alias=True)
        policy = DEFAULT_RECONCILIATION_POLICY.model_dump(by_alias=True)
        policy["rowOrder"] = "unordered"
        result = runtime.reconciler.reconcile(
            reference_output_path=ref_path,
            candidate_artifacts=[candidate_ref],
            input_snapshot_id="snap-1",
            environment_fingerprint="env-1",
            policy=policy,
            scope_id=candidate.id,
        )
        row_diffs = [d for d in result["differences"] if d["kind"] == "row"]
        assert len(row_diffs) > 0
        for diff in row_diffs:
            assert diff["row"] is not None
            assert isinstance(diff["row"], int)
            assert diff["row"] >= 0
    finally:
        await runtime.dispose()


@pytest.mark.asyncio
async def test_ordered_row_index_reflects_actual_position(test_dir, fixed_clock):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench
    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        _, _, _, candidate = make_scopes(runtime)
        ref_path = os.path.join(str(test_dir), "reference.json")
        with open(ref_path, "w") as f:
            json.dump([{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}], f)
        candidate_data = json.dumps([{"a": 1}, {"a": 2}, {"a": 3}, {"a": 99}])
        record = runtime.artifacts.create(scope=candidate, data=candidate_data, media_type="application/json", created_by="test")
        from sas2pyspark.services.artifacts import artifact_reference
        candidate_ref = artifact_reference(record).model_dump(by_alias=True)
        policy = DEFAULT_RECONCILIATION_POLICY.model_dump(by_alias=True)
        policy["rowOrder"] = "ordered"
        result = runtime.reconciler.reconcile(
            reference_output_path=ref_path,
            candidate_artifacts=[candidate_ref],
            input_snapshot_id="snap-1",
            environment_fingerprint="env-1",
            policy=policy,
            scope_id=candidate.id,
        )
        row_diffs = [d for d in result["differences"] if d["kind"] == "row"]
        assert len(row_diffs) > 0
        assert row_diffs[0]["row"] == 3
    finally:
        await runtime.dispose()


@pytest.mark.asyncio
async def test_crlf_csv_reconciliation(test_dir, fixed_clock):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench
    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        _, _, _, candidate = make_scopes(runtime)
        ref_path = os.path.join(str(test_dir), "reference.csv")
        with open(ref_path, "wb") as f:
            f.write(b"a,b\r\n1,2\r\n3,4\r\n")
        candidate_data = "a,b\n1,2\n3,4\n"
        record = runtime.artifacts.create(scope=candidate, data=candidate_data, media_type="text/csv", created_by="test")
        from sas2pyspark.services.artifacts import artifact_reference
        candidate_ref = artifact_reference(record).model_dump(by_alias=True)
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


def test_summarize_dataset_computes_row_hashes_once():
    rows = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    policy = DEFAULT_RECONCILIATION_POLICY
    ds = _summarize_dataset(rows, policy)
    assert len(ds.row_hashes) == 2
    assert ds.profile_hash != ""
    assert all(isinstance(h, str) for h in ds.row_hashes)


def test_dataset_hash_uses_provided_row_hashes():
    from sas2pyspark.services.reconciliation import _dataset_hash
    row_hashes = ["hash1", "hash2", "hash3"]
    headers = ["a", "b"]
    policy = DEFAULT_RECONCILIATION_POLICY
    h = _dataset_hash(row_hashes, headers, policy)
    assert isinstance(h, str)
    assert h != ""


def test_dataset_hash_unordered_sorts():
    from sas2pyspark.services.reconciliation import _dataset_hash
    headers = ["a"]
    policy = DEFAULT_RECONCILIATION_POLICY
    h1 = _dataset_hash(["hash_a", "hash_b"], headers, policy)
    h2 = _dataset_hash(["hash_b", "hash_a"], headers, policy)
    assert h1 != h2
    policy_unordered = _resolve_policy({"rowOrder": "unordered"})
    h1u = _dataset_hash(["hash_a", "hash_b"], headers, policy_unordered)
    h2u = _dataset_hash(["hash_b", "hash_a"], headers, policy_unordered)
    assert h1u == h2u
