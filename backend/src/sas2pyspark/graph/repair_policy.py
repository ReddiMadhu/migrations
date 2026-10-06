"""Pure deterministic repair-loop budget, progress, and oscillation checks."""

from __future__ import annotations

from typing import Any

from pydantic import Field

from sas2pyspark.core.models import WorkbenchModel
from sas2pyspark.services.policy import canonical_fingerprint


class RepairPolicy(WorkbenchModel):
    maximum_repair_iterations: int = Field(default=5, alias="maximumRepairIterations")
    near_oscillation_threshold: float = Field(default=0.85, alias="nearOscillationThreshold")
    oscillation_history_cap: int = Field(default=10, alias="oscillationHistoryCap")

    model_config = {**WorkbenchModel.model_config, "populate_by_name": True}


def budget_exceeded(iteration: int, maximum: int) -> bool:
    return iteration >= maximum


def _bounded_diff_signature(differences: Any, *, cap: int = 50) -> str:
    if not isinstance(differences, list):
        return canonical_fingerprint({"diff": None})
    bounded = []
    for item in differences[:cap]:
        if isinstance(item, dict):
            bounded.append({
                k: item.get(k)
                for k in sorted(item.keys())
                if k in ("column", "rowIndex", "kind", "expected", "actual")
            })
        else:
            bounded.append(str(item))
    return canonical_fingerprint({"diffs": bounded})


def reconciliation_signature(result: dict[str, Any]) -> str:
    return canonical_fingerprint({
        "schemaVerdict": result.get("schemaVerdict"),
        "aggregateVerdict": result.get("aggregateVerdict"),
        "rowVerdict": result.get("rowVerdict"),
        "statisticalVerdict": result.get("statisticalVerdict"),
        "diffSignature": _bounded_diff_signature(result.get("differences")),
    })


def signature_seen(history: list[dict[str, Any]], signature: str) -> bool:
    return any(item.get("resultSignature") == signature for item in history)


def near_oscillation(
    history: list[dict[str, Any]],
    current_signature: str,
    threshold: float = 0.85,
    *,
    cap: int = 10,
) -> bool:
    recent = history[-cap:] if len(history) > cap else history
    if not recent:
        return False
    current_len = len(current_signature)
    for item in recent:
        prior_sig = item.get("resultSignature", "")
        if not prior_sig:
            continue
        prior_len = len(prior_sig)
        min_len = min(current_len, prior_len)
        max_len = max(current_len, prior_len)
        if max_len == 0:
            continue
        matching = sum(1 for i in range(min_len) if current_signature[i] == prior_sig[i])
        similarity = matching / max_len
        if similarity >= threshold:
            return True
    return False


def progress_error(
    previous: dict[str, Any], current: dict[str, Any], minimum_reduction: int,
) -> str | None:
    previous_count = previous.get("differenceCount")
    current_count = current.get("differenceCount")
    if (
        not isinstance(previous_count, int)
        or isinstance(previous_count, bool)
        or not isinstance(current_count, int)
        or isinstance(current_count, bool)
        or current_count < 0
    ):
        return "repair verifier omitted valid difference counts needed to validate expected effect"
    for key in ("schemaVerdict", "aggregateVerdict", "rowVerdict", "statisticalVerdict"):
        if previous.get(key) != "FAIL" and current.get(key) == "FAIL":
            return f"repair introduced a regression in {key}"
    if previous_count - current_count < minimum_reduction:
        return "repair did not achieve its predicted minimum difference reduction"
    return None
