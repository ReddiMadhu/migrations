"""OpenTelemetry metrics and observability for Azure Cosmos DB persistence.

Provides standard metric instrumentation for Cosmos operations:
- cosmos_checkpoint_put_latency_ms
- cosmos_checkpoint_ru_consumed
- cosmos_checkpoint_overflow_to_blob_total
- cosmos_session_append_etag_conflict_total
- cosmos_circuit_breaker_state
- cosmos_checkpoint_payload_size_bytes
- cosmos_persistence_retry_total
- cosmos_persistence_error_total

Falls back gracefully to in-memory counters and structured logging if OpenTelemetry
is not installed or configured.
"""

from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

# Try importing OpenTelemetry SDK/API gracefully
try:
    from opentelemetry import metrics

    _meter = metrics.get_meter("sas2pyspark.cosmos")
    _has_otel = True
except ImportError:
    _meter = None  # type: ignore[assignment]
    _has_otel = False

# In-memory accumulator for testing & local verification
_SNAPSHOT: dict[str, Any] = {
    "ru_consumed": 0.0,
    "overflow_count": 0,
    "etag_conflict_count": 0,
    "circuit_breaker_state": "CLOSED",
    "retries": 0,
    "errors": 0,
    "latencies": [],
    "payload_sizes": [],
}


def reset_telemetry_snapshot() -> None:
    """Reset the in-memory test telemetry snapshot."""
    global _SNAPSHOT
    _SNAPSHOT = {
        "ru_consumed": 0.0,
        "overflow_count": 0,
        "etag_conflict_count": 0,
        "circuit_breaker_state": "CLOSED",
        "retries": 0,
        "errors": 0,
        "latencies": [],
        "payload_sizes": [],
    }


def get_telemetry_snapshot() -> dict[str, Any]:
    """Return a copy of the recorded in-memory telemetry snapshot."""
    return dict(_SNAPSHOT)


# OTel instruments (initialized if available)
_put_latency_hist = (
    _meter.create_histogram(
        name="cosmos_checkpoint_put_latency_ms",
        unit="ms",
        description="Latency of Cosmos checkpoint put operations in milliseconds",
    )
    if _has_otel and _meter
    else None
)

_ru_counter = (
    _meter.create_counter(
        name="cosmos_checkpoint_ru_consumed",
        unit="RU",
        description="Request units consumed by Cosmos persistence operations",
    )
    if _has_otel and _meter
    else None
)

_overflow_counter = (
    _meter.create_counter(
        name="cosmos_checkpoint_overflow_to_blob_total",
        unit="1",
        description="Total checkpoint payloads overflowed to immutable blob store",
    )
    if _has_otel and _meter
    else None
)

_etag_conflict_counter = (
    _meter.create_counter(
        name="cosmos_session_append_etag_conflict_total",
        unit="1",
        description="Total session append ETag concurrency conflicts encountered",
    )
    if _has_otel and _meter
    else None
)

_payload_size_hist = (
    _meter.create_histogram(
        name="cosmos_checkpoint_payload_size_bytes",
        unit="bytes",
        description="Payload size in bytes of saved checkpoints",
    )
    if _has_otel and _meter
    else None
)

_retry_counter = (
    _meter.create_counter(
        name="cosmos_persistence_retry_total",
        unit="1",
        description="Total Cosmos persistence retry attempts",
    )
    if _has_otel and _meter
    else None
)

_error_counter = (
    _meter.create_counter(
        name="cosmos_persistence_error_total",
        unit="1",
        description="Total unhandled Cosmos persistence errors",
    )
    if _has_otel and _meter
    else None
)


def record_checkpoint_put_latency(duration_ms: float, *, operation: str = "put") -> None:
    _SNAPSHOT["latencies"].append(duration_ms)
    if _put_latency_hist:
        _put_latency_hist.record(duration_ms, {"operation": operation})


def record_ru_consumed(ru: float, *, operation: str = "checkpoint") -> None:
    _SNAPSHOT["ru_consumed"] += ru
    if _ru_counter:
        _ru_counter.add(ru, {"operation": operation})


def record_blob_overflow() -> None:
    _SNAPSHOT["overflow_count"] += 1
    if _overflow_counter:
        _overflow_counter.add(1)


def record_etag_conflict() -> None:
    _SNAPSHOT["etag_conflict_count"] += 1
    if _etag_conflict_counter:
        _etag_conflict_counter.add(1)


def record_circuit_breaker_state(state: str) -> None:
    _SNAPSHOT["circuit_breaker_state"] = state


def record_checkpoint_payload_size(size_bytes: int) -> None:
    _SNAPSHOT["payload_sizes"].append(size_bytes)
    if _payload_size_hist:
        _payload_size_hist.record(size_bytes)


def record_persistence_retry(operation: str, reason: str) -> None:
    _SNAPSHOT["retries"] += 1
    if _retry_counter:
        _retry_counter.add(1, {"operation": operation, "reason": reason})


def record_persistence_error(operation: str, reason: str) -> None:
    _SNAPSHOT["errors"] += 1
    if _error_counter:
        _error_counter.add(1, {"operation": operation, "reason": reason})
