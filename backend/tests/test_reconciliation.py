"""Reconciliation tests — four verdicts, decimal drift, policy fingerprints."""

from __future__ import annotations

import json

import pytest

from sas2pyspark.services.reconciliation import (
    ReconciliationService,
    ReconciliationPolicy,
    DEFAULT_RECONCILIATION_POLICY,
    _resolve_policy,
    _summarize_dataset,
    _add_decimal,
    _trim_zeros,
)
from sas2pyspark.services.artifacts import artifact_reference

from .conftest import make_scopes


def test_all_pass_verdicts_are_certifiable():
    service = ReconciliationService.__new__(ReconciliationService)
    policy = DEFAULT_RECONCILIATION_POLICY
    rows = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    ref = _summarize_dataset(rows, policy)
    cand = _summarize_dataset(rows, policy)
    summary = service.compare(ref, cand, policy)
    assert summary.schema_verdict == "PASS"
    assert summary.aggregate_verdict == "PASS"
    assert summary.row_verdict == "PASS"
    assert summary.statistical_verdict == "PASS"
    assert summary.certifiable is True


def test_row_mismatch_fails():
    service = ReconciliationService.__new__(ReconciliationService)
    policy = DEFAULT_RECONCILIATION_POLICY
    ref = _summarize_dataset([{"a": 1}, {"a": 2}], policy)
    cand = _summarize_dataset([{"a": 1}, {"a": 3}], policy)
    summary = service.compare(ref, cand, policy)
    assert summary.row_verdict == "FAIL"
    assert summary.certifiable is False


def test_schema_mismatch_fails():
    service = ReconciliationService.__new__(ReconciliationService)
    policy = DEFAULT_RECONCILIATION_POLICY
    ref = _summarize_dataset([{"a": 1, "b": 2}], policy)
    cand = _summarize_dataset([{"a": 1, "c": 2}], policy)
    summary = service.compare(ref, cand, policy)
    assert summary.schema_verdict == "FAIL"


def test_unordered_row_order_passes_with_permutation():
    service = ReconciliationService.__new__(ReconciliationService)
    policy = _resolve_policy({"rowOrder": "unordered"})
    ref = _summarize_dataset([{"a": 1}, {"a": 2}, {"a": 3}], policy)
    cand = _summarize_dataset([{"a": 3}, {"a": 1}, {"a": 2}], policy)
    summary = service.compare(ref, cand, policy)
    assert summary.row_verdict == "PASS"


def test_decimal_sum_drift_detected():
    service = ReconciliationService.__new__(ReconciliationService)
    policy = DEFAULT_RECONCILIATION_POLICY
    ref = _summarize_dataset([{"a": 0.1}, {"a": 0.2}], policy)
    cand = _summarize_dataset([{"a": 0.3}], policy)
    summary = service.compare(ref, cand, policy)
    assert summary.aggregate_verdict == "FAIL"


def test_add_decimal_handles_fractions():
    assert _add_decimal("0.1", "0.2") == "0.3"
    assert _add_decimal("1.5", "2.5") == "4"
    assert _add_decimal("-1.0", "1.0") == "0"


def test_trim_zeros_normalizes():
    assert _trim_zeros("1.50") == "1.5"
    assert _trim_zeros("0.100") == "0.1"
    assert _trim_zeros("100.0") == "100"
    assert _trim_zeros("-0.0") == "0"


def test_relaxed_schema_is_not_applicable():
    service = ReconciliationService.__new__(ReconciliationService)
    policy = _resolve_policy({"schema": "relaxed"})
    ref = _summarize_dataset([{"a": 1, "b": 2}], policy)
    cand = _summarize_dataset([{"a": 1, "c": 2}], policy)
    summary = service.compare(ref, cand, policy)
    assert summary.schema_verdict == "NOT_APPLICABLE"


def test_disabled_statistical_is_not_applicable():
    service = ReconciliationService.__new__(ReconciliationService)
    policy = _resolve_policy({"statistical": "disabled"})
    ref = _summarize_dataset([{"a": 1}], policy)
    cand = _summarize_dataset([{"a": 1}], policy)
    summary = service.compare(ref, cand, policy)
    assert summary.statistical_verdict == "NOT_APPLICABLE"


def test_policy_fingerprint_is_deterministic():
    from sas2pyspark.services.policy import canonical_fingerprint
    p1 = DEFAULT_RECONCILIATION_POLICY
    p2 = DEFAULT_RECONCILIATION_POLICY
    assert canonical_fingerprint(p1.model_dump(by_alias=True)) == canonical_fingerprint(p2.model_dump(by_alias=True))


async def test_reconciliation_with_json_artifacts(workbench):
    from sas2pyspark.core.events import MigrationEventType
    from .conftest import make_session
    _, _, _, candidate = make_scopes(workbench)
    session = await make_session(workbench)
    ref_data = json.dumps([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}])
    cand_data = json.dumps([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}])
    ref_artifact = workbench.artifacts.create(scope=candidate, data=ref_data, media_type="application/json", created_by="test")
    cand_artifact = workbench.artifacts.create(scope=candidate, data=cand_data, media_type="application/json", created_by="test")
    ref_ds = workbench.reconciliation._load_dataset(candidate, ref_artifact, DEFAULT_RECONCILIATION_POLICY)
    cand_ds = workbench.reconciliation._load_dataset(candidate, cand_artifact, DEFAULT_RECONCILIATION_POLICY)
    summary = workbench.reconciliation.compare(ref_ds, cand_ds, DEFAULT_RECONCILIATION_POLICY)
    assert summary.certifiable is True
