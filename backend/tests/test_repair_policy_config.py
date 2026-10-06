"""P1-8: Repair Policy Configurable + Near-Oscillation Detection."""

from __future__ import annotations

import pytest

from sas2pyspark.graph.repair_policy import (
    RepairPolicy,
    budget_exceeded,
    near_oscillation,
    reconciliation_signature,
    signature_seen,
    progress_error,
)


def test_configurable_max_iterations():
    policy = RepairPolicy(maximumRepairIterations=10)
    assert policy.maximum_repair_iterations == 10
    assert not budget_exceeded(9, policy.maximum_repair_iterations)
    assert budget_exceeded(10, policy.maximum_repair_iterations)


def test_exceeded_routes_human_required():
    policy = RepairPolicy(maximumRepairIterations=3)
    assert budget_exceeded(3, policy.maximum_repair_iterations)
    assert not budget_exceeded(2, policy.maximum_repair_iterations)


def test_exact_oscillation_detected():
    result = {
        "schemaVerdict": "FAIL", "aggregateVerdict": "PASS",
        "rowVerdict": "FAIL", "statisticalVerdict": "PASS",
        "differenceCount": 5, "differences": [{"column": "x", "rowIndex": 1}],
    }
    sig = reconciliation_signature(result)
    history = [{"resultSignature": sig, "iteration": 1}]
    assert signature_seen(history, sig)


def test_near_oscillation_detected():
    sig_a = "sha256:" + "a" * 56 + "12345678"
    sig_b = "sha256:" + "a" * 56 + "12345679"
    history = [{"resultSignature": sig_a, "iteration": 1}]
    assert near_oscillation(history, sig_b, threshold=0.75)


def test_near_oscillation_not_detected():
    sig_a = "sha256:" + "a" * 64
    sig_b = "sha256:" + "b" * 64
    history = [{"resultSignature": sig_a, "iteration": 1}]
    assert not near_oscillation(history, sig_b, threshold=0.85)


def test_signature_bounded():
    large_diffs = [{"column": f"col_{i}", "rowIndex": i, "kind": "value"} for i in range(200)]
    result_large = {
        "schemaVerdict": "FAIL", "differenceCount": 200, "differences": large_diffs,
    }
    result_small = {
        "schemaVerdict": "FAIL", "differenceCount": 200,
        "differences": large_diffs[:50] + [{"column": "different", "rowIndex": 999}],
    }
    sig_large = reconciliation_signature(result_large)
    sig_small = reconciliation_signature(result_small)
    assert isinstance(sig_large, str)
    assert isinstance(sig_small, str)
    assert len(sig_large) < 500
    result_full = {
        "schemaVerdict": "FAIL", "differenceCount": 200, "differences": large_diffs,
    }
    result_no_diffs = {
        "schemaVerdict": "FAIL", "differenceCount": 200, "differences": None,
    }
    assert reconciliation_signature(result_full) != reconciliation_signature(result_no_diffs)


def test_no_oscillation_continues():
    sig_1 = "abc:def:ghi"
    sig_2 = "jkl:mno:pqr"
    sig_3 = "stu:vwx:yz0"
    history = [{"resultSignature": sig_1}, {"resultSignature": sig_2}]
    assert not signature_seen(history, sig_3)
    assert not near_oscillation(history, sig_3, threshold=0.85)


def test_repair_policy_defaults():
    policy = RepairPolicy()
    assert policy.maximum_repair_iterations == 5
    assert policy.near_oscillation_threshold == 0.85
    assert policy.oscillation_history_cap == 10


def test_progress_error_still_works():
    previous = {"differenceCount": 10, "schemaVerdict": "FAIL"}
    current = {"differenceCount": 7, "schemaVerdict": "FAIL"}
    assert progress_error(previous, current, 1) is None
    assert progress_error(previous, current, 5) is not None
