"""Candidate approval preparation and bare LangGraph interrupt node."""

from __future__ import annotations

from typing import Any

from langgraph.types import interrupt

from sas2pyspark.core.events import MigrationEventType
from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.graph.state import MigrationGraphState
from sas2pyspark.services.approvals import binding_for
from sas2pyspark.services.artifacts import ArtifactRecord
from sas2pyspark.services.policy import policy_fingerprint


def artifact_record_from_state(raw: dict[str, Any]) -> ArtifactRecord:
    return ArtifactRecord(
        artifact_id=raw["artifactId"],
        source_hash=raw["sourceHash"],
        semantic_hash=raw["semanticHash"],
        semantic_hash_version=raw["semanticHashVersion"],
        size=raw["size"],
        media_type=raw["mediaType"],
        created_by=raw["createdBy"],
        created_at=raw["createdAt"],
        storage_location=raw["storageLocation"],
        scope_id=raw["scopeId"],
    )


def make_approval_request_node(runtime: Any):
    async def prepare_approval(state: MigrationGraphState) -> dict[str, Any]:
        scope = runtime.scopes.require_by_id(state.get("scope_id", ""))
        session = await runtime.sessions.open(state.get("session_id", ""))
        policy = runtime.policy.resolve(scope.policy)
        candidate = state.get("candidate_artifact")
        snapshot_id = state.get("input_snapshot_id")
        environment = state.get("environment_fingerprint")
        if candidate is None:
            return {
                "state": "HUMAN_REQUIRED",
                "pending_approval": False,
                "error": "candidate artifact is missing before approval",
            }
        request = None
        if policy.approval_required:
            if not snapshot_id or not environment:
                return {
                    "state": "HUMAN_REQUIRED",
                    "pending_approval": False,
                    "error": "approval requires input snapshot and environment identities",
                }
            request = {
                "question": "Approve this PySpark candidate for execution?",
                "candidateArtifact": candidate,
                "scopeId": scope.id,
                "inputSnapshotId": snapshot_id,
                "environmentFingerprint": environment,
                "policyFingerprint": policy_fingerprint(policy),
            }
        await session.append(MigrationEventType.TRANSLATION_COMPLETED, {
            "workUnitId": (state.get("work_unit") or {}).get("workUnitId"),
            "candidateArtifact": candidate,
            "approvalRequest": request,
        })
        await session.flush()
        return {
            "state": "CANDIDATE_READY",
            "pending_approval": request is not None,
            "approval_request": request,
        }
    return prepare_approval


def make_approve_node(runtime: Any):
    async def approve_candidate(state: MigrationGraphState) -> dict[str, Any]:
        scope = runtime.scopes.require_by_id(state.get("scope_id", ""))
        policy = runtime.policy.resolve(scope.policy)
        if not policy.approval_required:
            return {"state": "CANDIDATE_READY", "pending_approval": False}

        raw_record = state.get("candidate_artifact_record")
        snapshot_id = state.get("input_snapshot_id")
        environment = state.get("environment_fingerprint")
        request = state.get("approval_request")
        if raw_record is None or not snapshot_id or not environment or request is None:
            return {
                "state": "HUMAN_REQUIRED",
                "pending_approval": False,
                "error": "approval requires a candidate, input snapshot, and environment fingerprint",
            }
        artifact = artifact_record_from_state(raw_record)
        fingerprint = policy_fingerprint(policy)
        binding = binding_for(scope, "spark.submit", artifact, policy, fingerprint, environment, snapshot_id)
        if (
            request.get("scopeId") != scope.id
            or request.get("candidateArtifact", {}).get("digest") != (state.get("candidate_artifact") or {}).get("digest")
            or request.get("inputSnapshotId") != snapshot_id
            or request.get("environmentFingerprint") != environment
            or request.get("policyFingerprint") != fingerprint
        ):
            raise WorkbenchError(
                WorkbenchErrorCode.STALE_APPROVAL,
                "approval binding changed while operator review was pending",
            )

        decision = interrupt(request)
        session = await runtime.sessions.open(state.get("session_id", ""))
        if decision is not True:
            await session.append(MigrationEventType.MIGRATION_ESCALATED, {
                "reasonCode": "CANDIDATE_REJECTED",
                "candidateArtifact": state.get("candidate_artifact"),
                "scopeId": scope.id,
            })
            await session.flush()
            return {
                "state": "HUMAN_REQUIRED",
                "pending_approval": False,
                "approval_request": None,
                "error": "operator rejected candidate",
            }
        record = runtime.approvals.grant(binding)
        return {
            "state": "CANDIDATE_READY",
            "pending_approval": False,
            "approval_request": None,
            "approval_receipt": {
                "approvalId": record.approval_id,
                "approver": record.approver,
                "approvedAt": record.approved_at,
                "binding": binding.to_dict(),
            },
        }
    return approve_candidate
