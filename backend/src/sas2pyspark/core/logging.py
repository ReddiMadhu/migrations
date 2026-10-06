"""Structured logging configuration with recursive secret redaction."""

from __future__ import annotations

import re
from typing import Any

from pydantic import Field

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.core.models import WorkbenchModel

_REDACTED = "[REDACTED]"
_SECRET_KEY = re.compile(r"(?i)(?:^|[_-])(?:token|password|passwd|secret|credential|api[_-]?key|authorization|dsn)(?:[_-]|$)")
_CONNECTION_KEY = re.compile(r"(?i)(?:^|[_-])connection(?:[_-]?(?:string|uri|url|dsn))?$|(?:^|[_-])(?:jdbc|odbc)[_-]?(?:string|uri|url|dsn)$")
_SECRET_VALUE = re.compile(
    r"(?i)(dapi_[A-Za-z0-9]{20,}|(?:token|password|client_secret)\s*[:=]\s*[^\s,;]+|(?:https?|jdbc|odbc)://[^\s,;]+)"
)


class TokenBudgetPolicy(WorkbenchModel):
    """Optional hard ceilings for cumulative model usage."""

    maximum_cost_units: float | None = Field(default=None, alias="maximumCostUnits")
    maximum_prompt_tokens: int | None = Field(default=None, alias="maximumPromptTokens")
    maximum_completion_tokens: int | None = Field(default=None, alias="maximumCompletionTokens")
    maximum_total_tokens: int | None = Field(default=None, alias="maximumTotalTokens")

    model_config = {**WorkbenchModel.model_config, "populate_by_name": True}


class BudgetExceeded(WorkbenchError):
    def __init__(self, message: str, *, usage: dict[str, Any] | None = None, policy: TokenBudgetPolicy | None = None) -> None:
        details = {"usage": usage or {}, "policy": policy.model_dump(by_alias=True) if policy else {}}
        super().__init__(WorkbenchErrorCode.BUDGET_EXCEEDED, message, details)


class DisjointTokenUsage(WorkbenchModel):
    """Three disjoint token buckets — billed input = input + cacheRead + cacheWrite."""

    input_tokens: int = Field(default=0, ge=0, alias="inputTokens")
    cache_read_tokens: int = Field(default=0, ge=0, alias="cacheReadTokens")
    cache_write_tokens: int = Field(default=0, ge=0, alias="cacheWriteTokens")

    model_config = {**WorkbenchModel.model_config, "populate_by_name": True}


def redact_value(value: Any, *, key: str = "") -> Any:
    normalized_key = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", key)
    if _SECRET_KEY.search(normalized_key) or _CONNECTION_KEY.search(normalized_key):
        return _REDACTED
    if isinstance(value, dict):
        return {name: redact_value(item, key=name) for name, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact_value(item) for item in value]
    if isinstance(value, str):
        return _SECRET_VALUE.sub(_REDACTED, value)
    return value


def redact_event_dict(logger: Any, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    del logger, method_name
    return redact_value(event_dict)


def configure_logging(*, json_logs: bool = False) -> None:
    try:
        import structlog
    except ModuleNotFoundError:
        import logging

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )
        return

    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        redact_event_dict,
    ]
    if json_logs:
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(20),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def log_event_appended(
    *, session_id: str, migration_id: str, sequence: int, event_type: str, event_hash: str,
) -> None:
    fields = redact_value({
        "session_id": session_id,
        "migration_id": migration_id,
        "sequence": sequence,
        "event_type": event_type,
        "event_hash": event_hash,
    })
    try:
        import structlog

        structlog.get_logger(__name__).info("migration_event_appended", **fields)
    except ModuleNotFoundError:
        import logging

        logging.getLogger(__name__).info("migration_event_appended %s", fields)


def log_model_usage(*, session_id: str, usage: dict[str, int | float]) -> None:
    fields = redact_value({"session_id": session_id, **usage})
    try:
        import structlog

        structlog.get_logger(__name__).info("translation_usage_recorded", **fields)
    except ModuleNotFoundError:
        import logging

        logging.getLogger(__name__).info("translation_usage_recorded %s", fields)


class TokenMeter:
    """Accumulate model usage metadata without depending on a provider-specific SDK."""

    __slots__ = (
        "prompt_cache_hit_tokens",
        "prompt_cache_miss_tokens",
        "completion_tokens",
        "cost_units",
        "input_tokens",
        "cache_read_tokens",
        "cache_write_tokens",
    )

    def __init__(self) -> None:
        self.prompt_cache_hit_tokens = 0
        self.prompt_cache_miss_tokens = 0
        self.completion_tokens = 0
        self.cost_units = 0.0
        self.input_tokens = 0
        self.cache_read_tokens = 0
        self.cache_write_tokens = 0

    @staticmethod
    def _read_usage_value(usage: dict[str, Any], *keys: str) -> Any:
        for key in keys:
            if key in usage:
                return usage[key]
        return None

    def record(self, usage: dict[str, Any], *, cost_units: float = 0.0) -> None:
        if not isinstance(usage, dict):
            return

        hit = self._read_usage_value(
            usage,
            "prompt_cache_hit_tokens",
            "promptCacheHitTokens",
            "prompt_cache_hit",
            "promptCacheHit",
        )
        miss = self._read_usage_value(
            usage,
            "prompt_cache_miss_tokens",
            "promptCacheMissTokens",
            "prompt_cache_miss",
            "promptCacheMiss",
            "input_tokens",
            "inputTokens",
        )

        completion = None
        for key in ("completion_tokens", "completionTokens", "output_tokens", "outputTokens"):
            value = usage.get(key)
            if value is not None:
                try:
                    completion = int(value)
                    break
                except (TypeError, ValueError):
                    continue
        if completion is None:
            completion = 0

        explicit_cost = self._read_usage_value(usage, "cost_units", "costUnits", "cost")
        if explicit_cost is not None:
            try:
                cost_units = max(0.0, float(explicit_cost))
            except (TypeError, ValueError):
                cost_units = max(0.0, float(cost_units))

        self.prompt_cache_hit_tokens += self._count(hit)
        self.prompt_cache_miss_tokens += self._count(miss)
        self.completion_tokens += self._count(completion)
        self.cost_units += max(0.0, float(cost_units))

    @staticmethod
    def _count(value: Any) -> int:
        if value is None:
            return 0
        try:
            return max(0, int(value))
        except (TypeError, ValueError, OverflowError):
            return 0

    def check_budget(self, policy: TokenBudgetPolicy | None = None) -> None:
        if policy is None or not any(
            value is not None for value in (
                policy.maximum_cost_units,
                policy.maximum_prompt_tokens,
                policy.maximum_completion_tokens,
                policy.maximum_total_tokens,
            )
        ):
            return

        usage = self.snapshot()
        prompt_tokens = int(usage["promptCacheHitTokens"]) + int(usage["promptCacheMissTokens"])
        completion_tokens = int(usage["completionTokens"])
        total_tokens = prompt_tokens + completion_tokens
        cost_units = float(usage["costUnits"])

        if policy.maximum_cost_units is not None and cost_units > float(policy.maximum_cost_units):
            raise BudgetExceeded(
                f"token budget exceeded: cost {cost_units} exceeds maximum {policy.maximum_cost_units}",
                usage=usage,
                policy=policy,
            )
        if policy.maximum_prompt_tokens is not None and prompt_tokens > int(policy.maximum_prompt_tokens):
            raise BudgetExceeded(
                f"token budget exceeded: prompt tokens {prompt_tokens} exceeds maximum {policy.maximum_prompt_tokens}",
                usage=usage,
                policy=policy,
            )
        if policy.maximum_completion_tokens is not None and completion_tokens > int(policy.maximum_completion_tokens):
            raise BudgetExceeded(
                f"token budget exceeded: completion tokens {completion_tokens} exceeds maximum {policy.maximum_completion_tokens}",
                usage=usage,
                policy=policy,
            )
        if policy.maximum_total_tokens is not None and total_tokens > int(policy.maximum_total_tokens):
            raise BudgetExceeded(
                f"token budget exceeded: total tokens {total_tokens} exceeds maximum {policy.maximum_total_tokens}",
                usage=usage,
                policy=policy,
            )

    def snapshot(self) -> dict[str, int | float]:
        return {
            "promptCacheHitTokens": self.prompt_cache_hit_tokens,
            "promptCacheMissTokens": self.prompt_cache_miss_tokens,
            "completionTokens": self.completion_tokens,
            "costUnits": self.cost_units,
        }

    def snapshot_disjoint(self) -> dict[str, int | float]:
        return {
            "inputTokens": self.input_tokens,
            "cacheReadTokens": self.cache_read_tokens,
            "cacheWriteTokens": self.cache_write_tokens,
            "costUnits": self.cost_units,
        }

    def record_disjoint(self, usage: DisjointTokenUsage, *, cost_units: float = 0.0) -> None:
        self.input_tokens += int(usage.input_tokens)
        self.cache_read_tokens += int(usage.cache_read_tokens)
        self.cache_write_tokens += int(usage.cache_write_tokens)
        self.cost_units += max(0.0, float(cost_units))

    def pressure_from(self) -> int:
        return self.input_tokens + self.cache_read_tokens + self.cache_write_tokens

    def projected_tokens(self, *, surface_tokens: int, sampled_surface_tokens: int) -> int:
        return self.pressure_from() + max(0, surface_tokens - sampled_surface_tokens)

    @staticmethod
    def map_wire_fields(usage: dict[str, Any]) -> DisjointTokenUsage:
        input_tokens = TokenMeter._count(
            TokenMeter._read_usage_value(usage, "input_tokens", "inputTokens", "prompt_tokens")
        )
        cache_read = TokenMeter._count(
            TokenMeter._read_usage_value(
                usage,
                "cache_read_input_tokens",
                "cacheReadInputTokens",
                "cache_read_tokens",
                "cacheReadTokens",
                "prompt_cache_hit_tokens",
                "promptCacheHitTokens",
            )
        )
        cache_write = TokenMeter._count(
            TokenMeter._read_usage_value(
                usage,
                "cache_creation_input_tokens",
                "cacheCreationInputTokens",
                "cache_write_tokens",
                "cacheWriteTokens",
                "prompt_cache_miss_tokens",
                "promptCacheMissTokens",
            )
        )
        return DisjointTokenUsage(
            input_tokens=input_tokens,
            cache_read_tokens=cache_read,
            cache_write_tokens=cache_write,
        )
