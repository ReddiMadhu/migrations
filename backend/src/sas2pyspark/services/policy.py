"""PolicyService — execution policy monotonicity and fingerprinting."""

from __future__ import annotations

from typing import Any, Sequence

from pydantic import Field

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.core.hash import sha256_json
from sas2pyspark.core.json import snapshot_json
from sas2pyspark.core.models import WorkbenchModel

NetworkAccess = str  # "none" | "allowlist" | "any"
DataClassification = str  # "public" | "internal" | "confidential" | "restricted"

_NETWORK_RANK: dict[str, int] = {"none": 0, "allowlist": 1, "any": 2}
_CLASSIFICATION_RANK: dict[str, int] = {
    "public": 0, "internal": 1, "confidential": 2, "restricted": 3,
}


class ExecutionPolicy(WorkbenchModel):
    version: str
    network: NetworkAccess
    allowed_hosts: tuple[str, ...] = Field(alias="allowedHosts")
    allowed_volume_prefixes: tuple[str, ...] = Field(alias="allowedVolumePrefixes")
    maximum_runtime_ms: int = Field(alias="maximumRuntimeMs")
    maximum_cost_units: int = Field(alias="maximumCostUnits")
    maximum_data_classification: DataClassification = Field(alias="maximumDataClassification")
    approval_required: bool = Field(alias="approvalRequired")
    allowed_actions: tuple[str, ...] = Field(alias="allowedActions")
    max_wait_seconds: int = Field(default=900, alias="maxWaitSeconds")
    poll_interval_seconds: int = Field(default=3, alias="pollIntervalSeconds")
    cancellation_deadline: bool = Field(default=False, alias="cancellationDeadline")
    max_log_bytes: int = Field(default=65536, alias="maxLogBytes")
    max_log_lines: int = Field(default=500, alias="maxLogLines")

    model_config = {**WorkbenchModel.model_config, "populate_by_name": True}


DEFAULT_EXECUTION_POLICY = ExecutionPolicy(
    version="policy-v1",
    network="none",
    allowedHosts=(),
    allowedVolumePrefixes=("/Volumes/migration",),
    maximumRuntimeMs=15 * 60 * 1000,
    maximumCostUnits=10,
    maximumDataClassification="internal",
    approvalRequired=True,
    allowedActions=("spark.submit", "sas.execute", "diagnostic.run"),
)


def _is_subset(child: Sequence[str], parent: Sequence[str]) -> bool:
    parent_set = set(parent)
    return all(item in parent_set for item in child)


def _prefix_allowed(prefix: str, parent_prefixes: Sequence[str]) -> bool:
    normalized = prefix.replace("\\", "/").rstrip("/")
    for parent_prefix in parent_prefixes:
        np = parent_prefix.replace("\\", "/").rstrip("/")
        if normalized == np or normalized.startswith(f"{np}/"):
            return True
    return False


def assert_policy_no_wider(parent: ExecutionPolicy, child: ExecutionPolicy) -> None:
    violations: list[str] = []
    if _NETWORK_RANK.get(child.network, 0) > _NETWORK_RANK.get(parent.network, 0):
        violations.append("network")
    if not _is_subset(child.allowed_hosts, parent.allowed_hosts):
        violations.append("allowedHosts")
    if not all(_prefix_allowed(p, parent.allowed_volume_prefixes) for p in child.allowed_volume_prefixes):
        violations.append("allowedVolumePrefixes")
    if child.maximum_runtime_ms > parent.maximum_runtime_ms:
        violations.append("maximumRuntimeMs")
    if child.maximum_cost_units > parent.maximum_cost_units:
        violations.append("maximumCostUnits")
    if _CLASSIFICATION_RANK.get(child.maximum_data_classification, 0) > _CLASSIFICATION_RANK.get(parent.maximum_data_classification, 0):
        violations.append("maximumDataClassification")
    if not _is_subset(child.allowed_actions, parent.allowed_actions):
        violations.append("allowedActions")
    if violations:
        raise WorkbenchError(
            WorkbenchErrorCode.POLICY_DENIED,
            "child execution policy widens its parent",
            {"violations": violations},
        )


def effective_policy(parent: ExecutionPolicy, requested: dict[str, Any] | None = None) -> ExecutionPolicy:
    r = requested or {}
    child = ExecutionPolicy(
        version=r.get("version", parent.version),
        network=r.get("network", parent.network),
        allowedHosts=tuple(r.get("allowedHosts", parent.allowed_hosts)),
        allowedVolumePrefixes=tuple(r.get("allowedVolumePrefixes", parent.allowed_volume_prefixes)),
        maximumRuntimeMs=r.get("maximumRuntimeMs", parent.maximum_runtime_ms),
        maximumCostUnits=r.get("maximumCostUnits", parent.maximum_cost_units),
        maximumDataClassification=r.get("maximumDataClassification", parent.maximum_data_classification),
        approvalRequired=(r.get("approvalRequired", parent.approval_required) or parent.approval_required),
        allowedActions=tuple(r.get("allowedActions", parent.allowed_actions)),
        maxWaitSeconds=r.get("maxWaitSeconds", parent.max_wait_seconds),
        pollIntervalSeconds=r.get("pollIntervalSeconds", parent.poll_interval_seconds),
        cancellationDeadline=r.get("cancellationDeadline", parent.cancellation_deadline),
    )
    assert_policy_no_wider(parent, child)
    return child


def policy_fingerprint(policy: ExecutionPolicy) -> str:
    return sha256_json(snapshot_json(policy.model_dump(by_alias=True)))


def canonical_fingerprint(value: Any) -> str:
    return sha256_json(snapshot_json(value))


def validate_policy(policy: ExecutionPolicy) -> ExecutionPolicy:
    if policy.maximum_runtime_ms <= 0 or policy.maximum_cost_units < 0:
        raise WorkbenchError(WorkbenchErrorCode.POLICY_DENIED, "policy runtime/cost limits must be positive")
    if policy.network == "none" and len(policy.allowed_hosts) > 0:
        raise WorkbenchError(WorkbenchErrorCode.POLICY_DENIED, "network=none cannot allow hosts")
    return policy


class PolicyService:
    def resolve(self, scope_policy: ExecutionPolicy) -> ExecutionPolicy:
        return validate_policy(scope_policy)
