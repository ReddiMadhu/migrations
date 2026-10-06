"""Frozen Pydantic v2 model foundation — immutability, extra-forbid, NaN/Inf rejection."""

from __future__ import annotations

import math
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator


class WorkbenchModel(BaseModel):
    """Base for all domain models: frozen, extra-forbid, validate on assignment."""

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        validate_assignment=True,
        arbitrary_types_allowed=True,
        use_enum_values=False,
    )

    @field_validator("*", mode="before")
    @classmethod
    def _reject_nan_inf(cls, value: Any) -> Any:
        if isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError("non-finite float is not allowed")
            if value == 0.0 and math.copysign(1.0, value) == -1.0:
                return 0.0
        return value
