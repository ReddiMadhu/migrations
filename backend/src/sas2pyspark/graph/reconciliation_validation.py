"""Pure validation shared by the main and repair reconciliation paths."""

from __future__ import annotations

from typing import Any

_VERDICTS = ("schemaVerdict", "aggregateVerdict", "rowVerdict", "statisticalVerdict")
_ALLOWED = {"PASS", "FAIL", "NOT_APPLICABLE"}


def reconciliation_validation_error(result: Any) -> str | None:
    if not isinstance(result, dict):
        return "reconciliation provider returned an invalid verdict"
    difference_count = result.get("differenceCount")
    if not isinstance(result.get("certifiable"), bool):
        return "reconciliation provider returned an invalid certifiable flag"
    if not isinstance(difference_count, int) or isinstance(difference_count, bool) or difference_count < 0:
        return "reconciliation provider returned an invalid difference count"
    if any(result.get(key) not in _ALLOWED for key in _VERDICTS):
        return "reconciliation provider returned an invalid verdict"
    expected = (
        all(result[key] == "PASS" for key in _VERDICTS[:3])
        and result["statisticalVerdict"] != "FAIL"
        and difference_count == 0
    )
    if result["certifiable"] != expected:
        return "reconciliation certifiable flag conflicts with its verdicts"
    return None
