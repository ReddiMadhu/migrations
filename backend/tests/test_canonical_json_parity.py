"""P0-2: Canonical JSON parity (ensure_ascii + NaN/Inf rejection)."""

from __future__ import annotations

import json

import pytest

from sas2pyspark.core.json import canonical_json, snapshot_json, _NonFiniteError


def test_ensure_ascii_true_non_ascii():
    result = canonical_json({"k": "café"})
    assert "caf\\u00e9" in result
    assert result == json.dumps({"k": "café"}, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def test_nan_rejected_before_hashing():
    with pytest.raises((_NonFiniteError, TypeError, ValueError)):
        canonical_json({"v": float("nan")})


def test_inf_rejected_before_hashing():
    with pytest.raises((_NonFiniteError, TypeError, ValueError)):
        canonical_json({"v": float("inf")})


def test_neg_inf_rejected():
    with pytest.raises((_NonFiniteError, TypeError, ValueError)):
        canonical_json({"v": float("-inf")})


def test_ascii_digest_unchanged():
    obj = {"b": 1, "a": [2, 3], "c": "hello"}
    result = canonical_json(obj)
    expected = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    assert result == expected
    assert result == '{"a":[2,3],"b":1,"c":"hello"}'


def test_sorted_keys_compact_separators():
    obj = {"z": 1, "a": 2, "m": 3}
    result = canonical_json(obj)
    assert result == '{"a":2,"m":3,"z":1}'


def test_neg_zero_normalized():
    result = canonical_json({"v": -0.0})
    assert result == '{"v":0.0}'


def test_nested_non_ascii_escaped():
    result = canonical_json({"outer": {"inner": "日本語"}})
    assert "\\u" in result
    assert "日本語" not in result


def test_agents_md_canonical_form_parity():
    obj = {"name": "test", "value": 42, "items": [1, 2, 3]}
    result = canonical_json(obj)
    expected = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    assert result == expected
