"""Canonical JSON serialization — deterministic digests and replay comparisons."""

from __future__ import annotations

import json
import math
import re
from typing import Any

_SAFE_SEGMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class _NonFiniteError(TypeError):
    pass


def _is_plain_object(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    return True


def snapshot_json(value: Any, path: str = "$") -> Any:
    """Convert untrusted domain data to a lossless JSON value without retaining caller references."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise _NonFiniteError(f"{path} contains a non-finite number")
        return 0.0 if _is_neg_zero(value) else value
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, entry in value.items():
            if entry is None:
                result[key] = None
                continue
            if entry is _UNDEFINED_SENTINEL:
                raise TypeError(f"{path}.{key} is undefined; omit optional fields instead")
            result[key] = snapshot_json(entry, f"{path}.{key}")
        return result
    if isinstance(value, (list, tuple)):
        return [snapshot_json(entry, f"{path}[{i}]") for i, entry in enumerate(value)]
    raise TypeError(f"{path} contains unsupported value of type {type(value).__name__}")


_UNDEFINED_SENTINEL = object()


def _is_neg_zero(value: float) -> bool:
    return value == 0.0 and math.copysign(1.0, value) == -1.0


def _canonicalize(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonicalize(value[key]) for key in sorted(value.keys())}
    if isinstance(value, list):
        return [_canonicalize(entry) for entry in value]
    return value


def canonical_json(value: Any) -> str:
    """Canonical JSON used for digests and deterministic replay comparisons.

    Aligned with AGENTS.md canonical form: json.dumps(..., sort_keys=True, separators=(",", ":"))
    whose default ensure_ascii=True. Routes through snapshot_json first so NaN/Infinity
    are rejected before hashing/freezing.
    """
    return json.dumps(_canonicalize(snapshot_json(value)), separators=(",", ":"), ensure_ascii=True, sort_keys=True)


def deep_freeze(value: Any) -> Any:
    """Best-effort freeze — Pydantic frozen models handle most immutability; this is for raw dicts."""
    if isinstance(value, dict):
        return {k: deep_freeze(v) for k, v in value.items()}
    if isinstance(value, list):
        return tuple(deep_freeze(entry) for entry in value)
    return value


def clone_json(value: Any) -> Any:
    return json.loads(json.dumps(value))


def assert_safe_segment(value: str, label: str) -> str:
    if not isinstance(value, str) or not _SAFE_SEGMENT_RE.match(value):
        raise TypeError(f"{label} must be a safe path segment")
    if value == "." or value == "..":
        raise TypeError(f"{label} must not be a dot path")
    return value
