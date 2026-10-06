"""Reliability layer for Azure Cosmos DB — retry policies, backoff, and circuit breaker.

Protects against transient network failures, 429 throttles, and 503 outages
while failing fast when persistent outages trip the circuit breaker.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from enum import Enum
from typing import Any, Callable, Coroutine, TypeVar

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.graph.cosmos_telemetry import (
    record_circuit_breaker_state,
    record_persistence_error,
    record_persistence_retry,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CircuitState(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    """In-memory circuit breaker protecting against downstream persistence outages."""

    def __init__(
        self,
        *,
        failure_threshold: int = 5,
        cooldown_seconds: float = 30.0,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self.state = CircuitState.CLOSED
        self.consecutive_failures = 0
        self.last_failure_time: float = 0.0
        self._half_open_probe_in_flight = False

    def can_execute(self) -> bool:
        """Check if execution is permitted under current circuit state."""
        now = time.monotonic()
        if self.state == CircuitState.CLOSED:
            return True

        if self.state == CircuitState.OPEN:
            if now - self.last_failure_time >= self.cooldown_seconds:
                self.state = CircuitState.HALF_OPEN
                self._half_open_probe_in_flight = False
                record_circuit_breaker_state(CircuitState.HALF_OPEN.value)
                logger.info("cosmos_circuit_breaker_transitioned_to_half_open")
            else:
                return False

        if self.state == CircuitState.HALF_OPEN:
            if not self._half_open_probe_in_flight:
                self._half_open_probe_in_flight = True
                return True
            return False

        return False

    def record_success(self) -> None:
        """Record successful operation, resetting consecutive failures."""
        if self.state != CircuitState.CLOSED:
            self.state = CircuitState.CLOSED
            record_circuit_breaker_state(CircuitState.CLOSED.value)
            logger.info("cosmos_circuit_breaker_closed")
        self.consecutive_failures = 0
        self._half_open_probe_in_flight = False

    def record_failure(self) -> None:
        """Record operation failure, potentially tripping the circuit to OPEN."""
        self.consecutive_failures += 1
        self.last_failure_time = time.monotonic()
        self._half_open_probe_in_flight = False

        if self.state == CircuitState.HALF_OPEN or self.consecutive_failures >= self.failure_threshold:
            if self.state != CircuitState.OPEN:
                self.state = CircuitState.OPEN
                record_circuit_breaker_state(CircuitState.OPEN.value)
                logger.warning(
                    "cosmos_circuit_breaker_tripped_to_open",
                    extra={"consecutive_failures": self.consecutive_failures},
                )


_DEFAULT_CIRCUIT_BREAKER = CircuitBreaker()


def is_transient_error(exc: Exception) -> bool:
    """Classify if an exception is transient (safe to retry)."""
    # Check for Cosmos HTTP status codes
    status_code = getattr(exc, "status_code", None)
    if status_code is not None:
        if status_code in (429, 503, 408):
            return True
        if status_code in (400, 401, 403, 404, 409, 412, 413):
            return False

    # Check for network / connection errors
    if isinstance(exc, (ConnectionError, TimeoutError, OSError)):
        return True

    # Check for string representation or class names if SDK exceptions without direct attribute
    exc_name = type(exc).__name__
    if "Timeout" in exc_name or "Connection" in exc_name or "ServiceUnavailable" in exc_name:
        return True

    return False


def get_retry_after(exc: Exception, fallback: float) -> float:
    """Extract retry-after duration from exception headers if available."""
    headers = getattr(exc, "headers", None) or {}
    # Cosmos x-ms-retry-after-ms header
    retry_ms = headers.get("x-ms-retry-after-ms")
    if retry_ms:
        try:
            return float(retry_ms) / 1000.0
        except (ValueError, TypeError):
            pass
    return fallback


def run_with_retry(
    fn: Callable[..., T],
    *args: Any,
    operation: str = "cosmos_op",
    circuit_breaker: CircuitBreaker | None = None,
    max_attempts: int = 4,
    base_backoff: float = 0.1,
    max_backoff: float = 2.0,
    **kwargs: Any,
) -> T:
    """Execute a synchronous function with retry logic and circuit breaker protection."""
    cb = circuit_breaker or _DEFAULT_CIRCUIT_BREAKER
    if not cb.can_execute():
        raise WorkbenchError(
            WorkbenchErrorCode.PERSISTENCE_UNAVAILABLE,
            f"Cosmos persistence circuit breaker is {cb.state.value}; rejecting {operation}",
            {"state": cb.state.value, "operation": operation},
        )

    attempt = 0
    backoff = base_backoff

    while True:
        attempt += 1
        try:
            result = fn(*args, **kwargs)
            cb.record_success()
            return result
        except Exception as exc:
            if not is_transient_error(exc) or attempt >= max_attempts:
                cb.record_failure()
                record_persistence_error(operation, type(exc).__name__)
                if isinstance(exc, WorkbenchError):
                    raise
                raise WorkbenchError(
                    WorkbenchErrorCode.PERSISTENCE_UNAVAILABLE,
                    f"Cosmos persistence operation '{operation}' failed after {attempt} attempt(s): {exc}",
                    {"operation": operation, "attempt": attempt, "error": str(exc)},
                ) from exc

            # Compute backoff with jitter
            retry_after = get_retry_after(exc, backoff)
            jitter = random.uniform(0.8, 1.2)
            sleep_duration = min(retry_after * jitter, max_backoff)

            record_persistence_retry(operation, type(exc).__name__)
            logger.info(
                "retrying_cosmos_operation",
                extra={
                    "operation": operation,
                    "attempt": attempt,
                    "sleep": sleep_duration,
                    "error": str(exc),
                },
            )
            time.sleep(sleep_duration)
            backoff = min(backoff * 2, max_backoff)


async def arun_with_retry(
    coro_fn: Callable[..., Coroutine[Any, Any, T]],
    *args: Any,
    operation: str = "cosmos_async_op",
    circuit_breaker: CircuitBreaker | None = None,
    max_attempts: int = 4,
    base_backoff: float = 0.1,
    max_backoff: float = 2.0,
    **kwargs: Any,
) -> T:
    """Execute an asynchronous coroutine with retry logic and circuit breaker protection."""
    cb = circuit_breaker or _DEFAULT_CIRCUIT_BREAKER
    if not cb.can_execute():
        raise WorkbenchError(
            WorkbenchErrorCode.PERSISTENCE_UNAVAILABLE,
            f"Cosmos persistence circuit breaker is {cb.state.value}; rejecting {operation}",
            {"state": cb.state.value, "operation": operation},
        )

    attempt = 0
    backoff = base_backoff

    while True:
        attempt += 1
        try:
            result = await coro_fn(*args, **kwargs)
            cb.record_success()
            return result
        except Exception as exc:
            if not is_transient_error(exc) or attempt >= max_attempts:
                cb.record_failure()
                record_persistence_error(operation, type(exc).__name__)
                if isinstance(exc, WorkbenchError):
                    raise
                raise WorkbenchError(
                    WorkbenchErrorCode.PERSISTENCE_UNAVAILABLE,
                    f"Cosmos persistence operation '{operation}' failed after {attempt} attempt(s): {exc}",
                    {"operation": operation, "attempt": attempt, "error": str(exc)},
                ) from exc

            retry_after = get_retry_after(exc, backoff)
            jitter = random.uniform(0.8, 1.2)
            sleep_duration = min(retry_after * jitter, max_backoff)

            record_persistence_retry(operation, type(exc).__name__)
            logger.info(
                "retrying_cosmos_async_operation",
                extra={
                    "operation": operation,
                    "attempt": attempt,
                    "sleep": sleep_duration,
                    "error": str(exc),
                },
            )
            await asyncio.sleep(sleep_duration)
            backoff = min(backoff * 2, max_backoff)
