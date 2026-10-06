"""Tests for secret-safe structured fields and model usage accounting."""

from __future__ import annotations

from sas2pyspark.core.logging import TokenMeter, redact_value


def test_redaction_covers_nested_secrets_and_connection_strings():
    redacted = redact_value({
        "credential_ref": "DATABRICKS_TOKEN",
        "nested": {"password": "not-for-logs"},
        "detail": "connect https://user:password@example.invalid/service",
        "sourceString": "ordinary SAS source summary",
        "dbConnectionPool": "pool-3",
        "clientSecret": "private-value",
    })

    assert redacted["credential_ref"] == "[REDACTED]"
    assert redacted["nested"]["password"] == "[REDACTED]"
    assert "https://" not in redacted["detail"]
    assert redacted["sourceString"] == "ordinary SAS source summary"
    assert redacted["dbConnectionPool"] == "pool-3"
    assert redacted["clientSecret"] == "[REDACTED]"


def test_token_meter_accumulates_cache_usage_and_cost():
    meter = TokenMeter()
    meter.record({"prompt_cache_hit_tokens": 8, "prompt_cache_miss_tokens": 12, "completion_tokens": 5}, cost_units=0.2)
    meter.record({"input_tokens": 3, "output_tokens": 2}, cost_units=0.1)

    assert meter.snapshot() == {
        "promptCacheHitTokens": 8,
        "promptCacheMissTokens": 15,
        "completionTokens": 7,
        "costUnits": 0.30000000000000004,
    }


def test_token_meter_treats_none_and_malformed_counters_as_zero():
    meter = TokenMeter()
    meter.record({
        "prompt_cache_hit_tokens": None,
        "prompt_cache_miss_tokens": None,
        "input_tokens": 6,
        "completion_tokens": "bad",
        "output_tokens": 4,
    })

    assert meter.snapshot() == {
        "promptCacheHitTokens": 0,
        "promptCacheMissTokens": 0,
        "completionTokens": 4,
        "costUnits": 0.0,
    }