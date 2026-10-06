"""Checkpointable repair iteration with guarded proposal, execution, and verification."""

from __future__ import annotations

import inspect
from typing import Any

from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from sas2pyspark.core.events import MigrationEventType
from sas2pyspark.graph.checkpointer import resolve_checkpointer
from sas2pyspark.graph.nodes.approve import artifact_record_from_state
from sas2pyspark.graph.nodes.guard import make_guard_node
from sas2pyspark.graph.state import MigrationGraphState
from sas2pyspark.graph.reconciliation_validation import reconciliation_validation_error
from sas2pyspark.services.approvals import binding_for
from sas2pyspark.services.artifacts import ArtifactReferenceData, artifact_reference
from sas2pyspark.services.policy import policy_fingerprint
from sas2pyspark.graph.repair_policy import (
    RepairPolicy,
    budget_exceeded,
    near_oscillation,
    progress_error,
    reconciliation_signature,
    signature_seen,
)

_DEFAULT_REPAIR_POLICY = RepairPolicy()


def build_repair_subgraph(runtime: Any, *, checkpointer: Any = None) -> Any:
    async def diagnose(state: MigrationGraphState) -> dict[str, Any]:
        iteration = state.get("repair_iteration", 0)
        repair_policy = getattr(runtime, "repair_policy", None) or _DEFAULT_REPAIR_POLICY
        max_iterations = repair_policy.maximum_repair_iterations
        repairer = getattr(runtime, "repairer", None)
        diagnose_fn = getattr(repairer, "diagnose", None)
        if budget_exceeded(iteration, max_iterations):
            return {"state": "HUMAN_REQUIRED", "error": "repair iteration budget exceeded"}
        if not callable(diagnose_fn) or not state.get("last_reconciliation_result"):
            return {"state": "HUMAN_REQUIRED", "error": "repair diagnosis provider or reconciliation evidence is unavailable"}
        try:
            diagnosis = diagnose_fn(
                reconciliation=state["last_reconciliation_result"],
                repair_history=state.get("repair_history", []),
                model_context=state.get("model_context") or {},
                iteration=iteration + 1,
            )
            if inspect.isawaitable(diagnosis):
                diagnosis = await diagnosis
        except Exception as exc:
            return {"state": "HUMAN_REQUIRED", "error": f"repair diagnosis failed ({type(exc).__name__})"}
        if not isinstance(diagnosis, dict) or not diagnosis:
            return {"state": "HUMAN_REQUIRED", "error": "repair diagnosis provider returned no structured finding"}
        session = await runtime.sessions.open(state.get("session_id", ""))
        await session.append(MigrationEventType.REPAIR_STARTED, {
            "workUnitId": (state.get("work_unit") or {}).get("workUnitId"),
            "iteration": iteration + 1,
            "priorDifferenceCount": (state.get("last_reconciliation_result") or {}).get("differenceCount"),
            "diagnosis": diagnosis,
        })
        await session.flush()
        return {"state": "REPAIRING", "repair_iteration": iteration + 1, "diagnosis": diagnosis}

    async def propose_patch(state: MigrationGraphState) -> dict[str, Any]:
        repairer = getattr(runtime, "repairer", None)
        propose_fn = getattr(repairer, "propose_patch", None)
        current_ref = state.get("candidate_artifact")
        if not callable(propose_fn) or current_ref is None:
            return {"state": "HUMAN_REQUIRED", "error": "repair patch provider or current candidate is unavailable"}
        scope = runtime.scopes.require_by_id(state.get("scope_id", ""))
        try:
            current_source = runtime.artifacts.read_reference(scope, ArtifactReferenceData(**current_ref)).decode("utf-8")
            proposal = propose_fn(
                current_candidate=current_source,
                diagnosis=state.get("diagnosis") or {},
                model_context=state.get("model_context") or {},
                reconciliation=state.get("last_reconciliation_result") or {},
            )
            if inspect.isawaitable(proposal):
                proposal = await proposal
        except Exception as exc:
            return {"state": "HUMAN_REQUIRED", "error": f"repair proposal failed ({type(exc).__name__})"}
        if (
            not isinstance(proposal, dict)
            or not isinstance(proposal.get("candidateSource"), str)
            or not proposal["candidateSource"].strip()
            or not isinstance(proposal.get("hypothesis"), str)
            or not proposal["hypothesis"].strip()
            or not isinstance(proposal.get("expectedEffect"), dict)
            or proposal["expectedEffect"].get("metric") != "differenceCount"
            or not isinstance(proposal["expectedEffect"].get("minimumReduction"), int)
            or proposal["expectedEffect"]["minimumReduction"] < 1
        ):
            return {"state": "HUMAN_REQUIRED", "error": "repair proposal lacks source, hypothesis, or a measurable expected effect"}

        record = runtime.artifacts.create(
            scope=scope,
            data=proposal["candidateSource"],
            media_type="text/x-python",
            created_by="repair-patch-proposal",
            parent_artifact=ArtifactReferenceData(**current_ref),
        )
        session = await runtime.sessions.open(state.get("session_id", ""))
        candidate_ref = artifact_reference(record).model_dump(by_alias=True)
        await session.append(MigrationEventType.REPAIR_PROPOSED, {
            "workUnitId": (state.get("work_unit") or {}).get("workUnitId"),
            "candidateArtifact": candidate_ref,
            "parentArtifact": current_ref,
            "hypothesis": proposal["hypothesis"],
            "expectedEffect": proposal["expectedEffect"],
        })
        await session.flush()
        return {
            "state": "TRANSLATING",
            "candidate_artifact": candidate_ref,
            "candidate_artifact_record": record.to_dict(),
            "repair_proposal": {
                "hypothesis": proposal["hypothesis"],
                "expectedEffect": proposal["expectedEffect"],
            },
        }

    async def guard_patch(state: MigrationGraphState) -> dict[str, Any]:
        proposal = state.get("repair_proposal") or {}
        if not proposal.get("hypothesis") or not proposal.get("expectedEffect"):
            return {"state": "HUMAN_REQUIRED", "error": "repair patch is missing its hypothesis or expected effect"}
        result = await make_guard_node(runtime)(state)
        if result.get("state") != "CANDIDATE_READY":
            return result
        session = await runtime.sessions.open(state.get("session_id", ""))
        await session.append(MigrationEventType.PATCH_APPLIED, {
            "workUnitId": (state.get("work_unit") or {}).get("workUnitId"),
            "candidateArtifact": state.get("candidate_artifact"),
            "parentArtifact": (state.get("candidate_artifact_record") or {}).get("parentArtifact"),
            "hypothesis": proposal["hypothesis"],
        })
        await session.flush()
        return {
            "state": "CANDIDATE_READY",
            "repair_proposal": proposal,
        }

    async def prepare_patch_approval(state: MigrationGraphState) -> dict[str, Any]:
        scope = runtime.scopes.require_by_id(state.get("scope_id", ""))
        policy = runtime.policy.resolve(scope.policy)
        if not policy.approval_required:
            return {"state": "CANDIDATE_READY", "pending_approval": False, "approval_request": None}
        record = state.get("candidate_artifact_record")
        snapshot_id = state.get("input_snapshot_id")
        environment = state.get("environment_fingerprint")
        if record is None or not snapshot_id or not environment:
            return {"state": "HUMAN_REQUIRED", "error": "repair approval identities are incomplete"}
        fingerprint = policy_fingerprint(policy)
        request = {
            "question": "Approve this repair candidate for execution?",
            "candidateArtifact": state.get("candidate_artifact"),
            "scopeId": scope.id,
            "inputSnapshotId": snapshot_id,
            "environmentFingerprint": environment,
            "policyFingerprint": fingerprint,
            "hypothesis": (state.get("repair_proposal") or {}).get("hypothesis"),
            "expectedEffect": (state.get("repair_proposal") or {}).get("expectedEffect"),
        }
        return {"state": "CANDIDATE_READY", "pending_approval": True, "approval_request": request}

    async def approve_patch(state: MigrationGraphState) -> dict[str, Any]:
        scope = runtime.scopes.require_by_id(state.get("scope_id", ""))
        policy = runtime.policy.resolve(scope.policy)
        if not policy.approval_required:
            return {"state": "CANDIDATE_READY", "pending_approval": False}
        record = state.get("candidate_artifact_record")
        snapshot_id = state.get("input_snapshot_id")
        environment = state.get("environment_fingerprint")
        request = state.get("approval_request")
        if record is None or not snapshot_id or not environment or request is None:
            return {"state": "HUMAN_REQUIRED", "pending_approval": False, "error": "repair approval request is missing"}
        artifact = artifact_record_from_state(record)
        fingerprint = policy_fingerprint(policy)
        binding = binding_for(scope, "spark.submit", artifact, policy, fingerprint, environment, snapshot_id)
        if (
            request.get("scopeId") != scope.id
            or request.get("candidateArtifact", {}).get("digest") != (state.get("candidate_artifact") or {}).get("digest")
            or request.get("inputSnapshotId") != snapshot_id
            or request.get("environmentFingerprint") != environment
            or request.get("policyFingerprint") != fingerprint
        ):
            from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
            raise WorkbenchError(WorkbenchErrorCode.STALE_APPROVAL, "repair approval binding changed while review was pending")
        decision = interrupt(request)
        session = await runtime.sessions.open(state.get("session_id", ""))
        if decision is not True:
            await session.append(MigrationEventType.MIGRATION_ESCALATED, {
                "reasonCode": "REPAIR_CANDIDATE_REJECTED",
                "candidateArtifact": state.get("candidate_artifact"),
            })
            await session.flush()
            return {"state": "HUMAN_REQUIRED", "pending_approval": False, "approval_request": None, "error": "operator rejected repair candidate"}
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

    async def execute_candidate(state: MigrationGraphState) -> dict[str, Any]:
        scope = runtime.scopes.require_by_id(state.get("scope_id", ""))
        session = await runtime.sessions.open(state.get("session_id", ""))
        record = state.get("candidate_artifact_record")
        snapshot_id = state.get("input_snapshot_id")
        environment = state.get("environment_fingerprint")
        if record is None or not snapshot_id or not environment:
            return {"state": "HUMAN_REQUIRED", "error": "repair execution identities are incomplete"}
        try:
            result = await runtime.execution.run_spark(
                session=session,
                scope=scope,
                artifact=artifact_record_from_state(record),
                input_snapshot_id=snapshot_id,
                environment_fingerprint=environment,
                execution_id=f"repair-{session.id}-{state.get('repair_iteration', 0)}",
                work_unit_id=(state.get("work_unit") or {}).get("workUnitId"),
            )
        except Exception as exc:
            return {"state": "HUMAN_REQUIRED", "error": f"repair execution failed ({type(exc).__name__})"}
        if result.get("outcome") != "SUCCEEDED":
            return {"state": "HUMAN_REQUIRED", "error": f"repair execution outcome is {result.get('outcome', 'UNKNOWN')}"}
        outputs = [artifact_reference(item).model_dump(by_alias=True) for item in result.get("outputArtifacts", [])]
        return {"state": "RECONCILING", "execution_outcome": {"outcome": "SUCCEEDED", "outputArtifacts": outputs}}

    async def reconcile(state: MigrationGraphState) -> dict[str, Any]:
        reconciler = getattr(runtime, "reconciler", None) or getattr(runtime, "reconciliation", None)
        reconcile_fn = getattr(reconciler, "reconcile", None)
        execution = state.get("execution_outcome") or {}
        if not callable(reconcile_fn) or not state.get("reference_output_path") or not execution.get("outputArtifacts"):
            return {"state": "HUMAN_REQUIRED", "error": "repair verifier inputs are unavailable"}
        scope = runtime.scopes.require_by_id(state.get("scope_id", ""))
        policy = runtime.policy.resolve(scope.policy)
        try:
            result = reconcile_fn(
                reference_output_path=state["reference_output_path"],
                candidate_artifacts=execution["outputArtifacts"],
                input_snapshot_id=state.get("input_snapshot_id"),
                environment_fingerprint=state.get("environment_fingerprint"),
                policy=policy.model_dump(by_alias=True),
                scope_id=scope.id,
            )
            if inspect.isawaitable(result):
                result = await result
        except Exception as exc:
            return {"state": "HUMAN_REQUIRED", "error": f"repair reconciliation failed ({type(exc).__name__})"}
        validation_error = reconciliation_validation_error(result)
        if validation_error:
            return {"state": "HUMAN_REQUIRED", "error": validation_error}
        previous = state.get("last_reconciliation_result") or {}
        current_count = result.get("differenceCount")
        expected_effect = (state.get("repair_proposal") or {}).get("expectedEffect") or {}
        progress_failure = progress_error(
            previous,
            result,
            expected_effect.get("minimumReduction", 1),
        )
        if progress_failure:
            return {"state": "HUMAN_REQUIRED", "error": progress_failure}
        session = await runtime.sessions.open(state.get("session_id", ""))
        await session.append(MigrationEventType.RECONCILIATION_COMPLETED, result)
        await session.flush()
        history = state.get("repair_history", [])
        signature = reconciliation_signature(result)
        if signature_seen(history, signature):
            return {"state": "HUMAN_REQUIRED", "error": "repair returned to a prior reconciliation signature"}
        repair_policy = getattr(runtime, "repair_policy", None) or _DEFAULT_REPAIR_POLICY
        if near_oscillation(history, signature, repair_policy.near_oscillation_threshold, cap=repair_policy.oscillation_history_cap):
            return {"state": "HUMAN_REQUIRED", "error": "repair near-oscillation detected: reconciliation signatures are too similar to recent attempts"}
        candidate_digest = (state.get("candidate_artifact") or {}).get("digest")
        attempt = {
            "iteration": state.get("repair_iteration", 0),
            "artifactDigest": candidate_digest,
            "hypothesis": (state.get("repair_proposal") or {}).get("hypothesis"),
            "resultSignature": signature,
            "differenceCount": current_count,
        }
        return {
            "state": "RECONCILING",
            "last_reconciliation_result": result,
            "repair_history": [*history, attempt],
        }

    subgraph = StateGraph(MigrationGraphState)
    subgraph.add_node("diagnose", diagnose)
    subgraph.add_node("propose_patch", propose_patch)
    subgraph.add_node("guard_patch", guard_patch)
    subgraph.add_node("prepare_patch_approval", prepare_patch_approval)
    subgraph.add_node("approve_patch", approve_patch)
    subgraph.add_node("execute_candidate", execute_candidate)
    subgraph.add_node("reconcile", reconcile)
    subgraph.add_edge(START, "diagnose")

    def route(state: MigrationGraphState, next_node: str) -> str:
        error = state.get("error")
        return "human_required" if state.get("state") == "HUMAN_REQUIRED" or (isinstance(error, str) and bool(error.strip())) else next_node

    subgraph.add_conditional_edges("diagnose", lambda state: route(state, "propose_patch"), {
        "propose_patch": "propose_patch", "human_required": END,
    })
    subgraph.add_conditional_edges("propose_patch", lambda state: route(state, "guard_patch"), {
        "guard_patch": "guard_patch", "human_required": END,
    })
    subgraph.add_conditional_edges("guard_patch", lambda state: route(state, "prepare_patch_approval"), {
        "prepare_patch_approval": "prepare_patch_approval", "human_required": END,
    })
    subgraph.add_conditional_edges("prepare_patch_approval", lambda state: route(state, "approve_patch"), {
        "approve_patch": "approve_patch", "human_required": END,
    })
    subgraph.add_conditional_edges("approve_patch", lambda state: route(state, "execute_candidate"), {
        "execute_candidate": "execute_candidate", "human_required": END,
    })
    subgraph.add_conditional_edges("execute_candidate", lambda state: route(state, "reconcile"), {
        "reconcile": "reconcile", "human_required": END,
    })
    subgraph.add_edge("reconcile", END)
    saver = checkpointer or getattr(runtime, "checkpointer", None)
    if saver is None:
        from sas2pyspark.graph.checkpointer import resolve_checkpointer
        saver = resolve_checkpointer(
            mode=getattr(runtime, "checkpointer_mode", "development"),
            checkpoint_path=getattr(runtime, "checkpoint_path", None),
            environment=getattr(runtime, "environment", "development"),
        )
    return subgraph.compile(checkpointer=saver)