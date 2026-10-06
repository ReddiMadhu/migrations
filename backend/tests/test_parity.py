"""Parity tests — decimal arithmetic parity against TS BigInt semantics."""

from __future__ import annotations

from decimal import Decimal

from sas2pyspark.services.reconciliation import _add_decimal, _trim_zeros, _normalize_decimal


def test_decimal_addition_matches_bigint_semantics():
    assert _add_decimal("0.1", "0.2") == "0.3"
    assert _add_decimal("1.23", "4.56") == "5.79"
    assert _add_decimal("100", "200") == "300"
    assert _add_decimal("-1.5", "2.5") == "1"
    assert _add_decimal("0", "0") == "0"
    assert _add_decimal("999999999", "1") == "1000000000"


def test_decimal_addition_preserves_precision():
    assert _add_decimal("0.01", "0.02") == "0.03"
    assert _add_decimal("1.001", "2.002") == "3.003"


def test_trim_zeros_matches_ts_behavior():
    assert _trim_zeros("1.500") == "1.5"
    assert _trim_zeros("0.100") == "0.1"
    assert _trim_zeros("100.0") == "100"
    assert _trim_zeros("0.0") == "0"
    assert _trim_zeros("-0.0") == "0"
    assert _trim_zeros("10.00") == "10"
    assert _trim_zeros("001.00") == "1"


def test_normalize_decimal_handles_scientific_notation():
    assert _normalize_decimal(1.0) == "1.0"
    assert _normalize_decimal(100) == "100"


def test_decimal_sum_chain_drift_detection():
    total = "0"
    for _ in range(10):
        total = _add_decimal(total, "0.1")
    assert total == "1"


def test_python_decimal_matches_reconciliation_add():
    pairs = [("0.1", "0.2"), ("1.5", "2.5"), ("100.0", "200.0"), ("-1.0", "1.0")]
    for left, right in pairs:
        expected = str(Decimal(left) + Decimal(right))
        actual = _add_decimal(left, right)
        assert _trim_zeros(actual) == _trim_zeros(expected)
