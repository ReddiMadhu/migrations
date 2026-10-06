"""ApprovalService — identity-bound approval with 8-field fingerprint freshness."""

from __future__ import annotations

from typing import Any

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.core.hash import sha256_json
from sas2pyspark.core.json import snapshot_json
from sas2pyspark.services.clock import Clock
from sas2pyspark.services.scopes import ScopeRef
from sas2pyspark.services.artifacts import ArtifactRecord
from sas2pyspark.services.policy import ExecutionPolicy


class ApprovalBinding:
    __slots__ = (
        "scope_id", "action", "artifact_semantic_hash", "artifact_source_hash",
        "policy_version", "policy_fingerprint", "environment_fingerprint", "input_snapshot_id",
    )

    def __init__(
        self,
        *,
        scope_id: str,
        action: str,
        artifact_semantic_hash: str,
        artifact_source_hash: str,
        policy_version: str,
        policy_fingerprint: str,
        environment_fingerprint: str,
        input_snapshot_id: str,
    ) -> None:
        self.scope_id = scope_id
        self.action = action
        self.artifact_semantic_hash = artifact_semantic_hash
        self.artifact_source_hash = artifact_source_hash
        self.policy_version = policy_version
        self.policy_fingerprint = policy_fingerprint
        self.environment_fingerprint = environment_fingerprint
        self.input_snapshot_id = input_snapshot_id

    def to_dict(self) -> dict[str, Any]:
        return {
            "scopeId": self.scope_id,
            "action": self.action,
            "artifactSemanticHash": self.artifact_semantic_hash,
            "artifactSourceHash": self.artifact_source_hash,
            "policyVersion": self.policy_version,
            "policyFingerprint": self.policy_fingerprint,
            "environmentFingerprint": self.environment_fingerprint,
            "inputSnapshotId": self.input_snapshot_id,
        }


class ApprovalRecord:
    __slots__ = ("approval_id", "decision", "binding", "approved_at", "approver")

    def __init__(self, *, approval_id: str, decision: str, binding: ApprovalBinding, approved_at: str, approver: str) -> None:
        self.approval_id = approval_id
        self.decision = decision
        self.binding = binding
        self.approved_at = approved_at
        self.approver = approver


def _fingerprint(binding: ApprovalBinding) -> str:
    return sha256_json(snapshot_json(binding.to_dict()))


def binding_for(
    scope: ScopeRef,
    action: str,
    artifact: ArtifactRecord,
    policy: ExecutionPolicy,
    policy_hash: str,
    environment_fingerprint: str,
    input_snapshot_id: str,
) -> ApprovalBinding:
    return ApprovalBinding(
        scope_id=scope.id,
        action=action,
        artifact_semantic_hash=artifact.semantic_hash,
        artifact_source_hash=artifact.source_hash,
        policy_version=policy.version,
        policy_fingerprint=policy_hash,
        environment_fingerprint=environment_fingerprint,
        input_snapshot_id=input_snapshot_id,
    )


class ApprovalService:
    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._records: dict[str, ApprovalRecord] = {}

    def grant(self, binding: ApprovalBinding, approver: str = "operator") -> ApprovalRecord:
        record = ApprovalRecord(
            approval_id=self._clock.uuid(),
            decision="ALLOW",
            binding=binding,
            approved_at=self._clock.now(),
            approver=approver,
        )
        self._records[_fingerprint(binding)] = record
        return record

    def assert_approved(self, binding: ApprovalBinding) -> ApprovalRecord:
        record = self._records.get(_fingerprint(binding))
        if record:
            return record
        stale = any(
            c.binding.scope_id == binding.scope_id
            and c.binding.action == binding.action
            and c.binding.artifact_semantic_hash == binding.artifact_semantic_hash
            and c.binding.artifact_source_hash == binding.artifact_source_hash
            for c in self._records.values()
        )
        if stale:
            raise WorkbenchError(
                WorkbenchErrorCode.STALE_APPROVAL,
                "the matching approval is bound to a different policy, environment, or input snapshot",
            )
        raise WorkbenchError(WorkbenchErrorCode.APPROVAL_REQUIRED, "no matching approval is available for this execution")

    def revoke(self, approval_id: str) -> bool:
        for key, record in list(self._records.items()):
            if record.approval_id == approval_id:
                del self._records[key]
                return True
        return False

    def is_approved(
        self, scope: ScopeRef, action: str, artifact: ArtifactRecord,
        policy: ExecutionPolicy, policy_hash: str, environment_fingerprint: str, input_snapshot_id: str,
    ) -> bool:
        return _fingerprint(binding_for(scope, action, artifact, policy, policy_hash, environment_fingerprint, input_snapshot_id)) in self._records
