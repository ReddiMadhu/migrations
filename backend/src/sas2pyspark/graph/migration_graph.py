"""Top-level LangGraph migration graph — assembles the guarded workflow.

Topology: discover → parse → resolve_macros → create_work_unit → assemble_model_context
  → propose → guard → approve → execute → reconcile → route
  route: certify → END | repair → propose loop | human_required → interrupt → END

The default in-memory checkpointer is for local runs; production should inject a durable saver.
"""

from __future__ import annotations

import inspect
from typing import Any

from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from sas2pyspark.core.events import MigrationEventType
from sas2pyspark.graph.checkpointer import resolve_checkpointer
from sas2pyspark.graph.state import MigrationGraphState
from sas2pyspark.graph.nodes.approve import (
    artifact_record_from_state,
    make_approve_node,
    make_approval_request_node,
)
from sas2pyspark.graph.nodes.discover import make_discover_node
from sas2pyspark.graph.nodes.parse import make_parse_node
from sas2pyspark.graph.nodes.resolve_macros import make_resolve_macros_node
from sas2pyspark.graph.nodes.work_unit import make_work_unit_node
from sas2pyspark.graph.nodes.model_context import make_model_context_node
from sas2pyspark.graph.nodes.propose import make_propose_node
from sas2pyspark.graph.nodes.guard import make_guard_node
from sas2pyspark.graph.nodes.route import route_after_reconcile
from sas2pyspark.graph.reconciliation_validation import reconciliation_validation_error
from sas2pyspark.graph.subgraphs.repair import build_repair_subgraph
from sas2pyspark.services.artifacts import ArtifactRecord, artifact_reference
from sas2pyspark.services.policy import canonical_fingerprint, policy_fingerprint


def _make_operator_decision_request_node(runtime: Any):
    async def prepare_operator_decision(state: MigrationGraphState) -> dict[str, Any]:
        reconciliation = state.get("last_reconciliation_result") or {}
        if reconciliation.get("certifiable") is not True:
            return {"state": "HUMAN_REQUIRED", "error": "operator certification decision requires passing reconciliation"}
        request = {
            "question": "Reconciliation passed. Issue the migration certificate?",
            "candidateArtifact": state.get("candidate_artifact"),
            "reconciliation": reconciliation,
        }
        return {"state": "RECONCILING", "pending_operator_decision": True, "operator_decision_request": request}
    return prepare_operator_decision


def _make_operator_decision_node(runtime: Any):
    async def operator_decision(state: MigrationGraphState) -> dict[str, Any]:
        request = state.get("operator_decision_request")
        if request is None:
            return {"state": "HUMAN_REQUIRED", "error": "operator decision request is missing"}
        decision = interrupt(request)
        session = await runtime.sessions.open(state.get("session_id", ""))
        if decision is not True:
            await session.append(MigrationEventType.MIGRATION_ESCALATED, {
                "reasonCode": "CERTIFICATE_REJECTED",
                "candidateArtifact": state.get("candidate_artifact"),
            })
            await session.flush()
            return {
                "state": "HUMAN_REQUIRED",
                "pending_operator_decision": False,
                "operator_decision_request": None,
                "error": "operator rejected certificate issuance",
            }
        reconciliation = state.get("last_reconciliation_result") or {}
        decision_id = canonical_fingerprint({
            "sessionId": session.id,
            "candidateArtifact": state.get("candidate_artifact"),
            "reconciliation": reconciliation,
        })
        return {
            "state": "CERTIFICATION_APPROVED",
            "pending_operator_decision": False,
            "operator_decision_request": None,
            "operator_decision": {
                "decisionId": decision_id,
                "decision": "ISSUE_CERTIFICATE",
                "candidateArtifact": state.get("candidate_artifact"),
                "reconciliation": reconciliation,
            },
        }
    return operator_decision


def _make_certify_node(runtime: Any):
    async def certify(state: MigrationGraphState) -> dict[str, Any]:
        result = state.get("last_reconciliation_result") or {}
        if result.get("certifiable") is not True:
            return {"state": "HUMAN_REQUIRED", "error": "certificate requires a certifiable reconciliation"}
        session = await runtime.sessions.open(state.get("session_id", ""))
        scope = runtime.scopes.require_by_id(state.get("scope_id", ""))
        policy = runtime.policy.resolve(scope.policy)
        certificate: dict[str, Any] = {
            "sessionId": session.id,
            "migrationId": session.migration_id,
            "scopeId": scope.id,
            "sourceArtifact": state.get("sas_source_artifact"),
            "candidateArtifact": state.get("candidate_artifact"),
            "inputSnapshotId": state.get("input_snapshot_id"),
            "environmentFingerprint": state.get("environment_fingerprint"),
            "policyFingerprint": policy_fingerprint(policy),
            "reconciliation": result,
            "executionApproval": state.get("approval_receipt"),
            "operatorDecision": state.get("operator_decision"),
        }
        certificate["certificateId"] = canonical_fingerprint(certificate)
        await session.append(MigrationEventType.CERTIFICATE_ISSUED, certificate)
        await session.flush()
        return {"state": "CERTIFIED"}
    return certify


def _make_translation_started_node(runtime: Any):
    async def translation_started(state: MigrationGraphState) -> dict[str, Any]:
        session = await runtime.sessions.open(state.get("session_id", ""))
        model_context = state.get("model_context") or {}
        await session.append(MigrationEventType.TRANSLATION_STARTED, {
            "workUnitId": (state.get("work_unit") or {}).get("workUnitId"),
            "contextDigest": model_context.get("contextDigest"),
            "sourceArtifact": state.get("sas_source_artifact"),
        })
        await session.flush()
        return {"state": "TRANSLATING"}
    return translation_started


def _make_human_required_node(runtime: Any):
    async def human_required(state: MigrationGraphState) -> dict[str, Any]:
        session = await runtime.sessions.open(state.get("session_id", ""))
        if session.projection["state"] != "HUMAN_REQUIRED":
            await session.append(MigrationEventType.MIGRATION_ESCALATED, {
                "reasonCode": "OPERATOR_REVIEW_REQUIRED",
                "detail": state.get("error", "migration requires operator review"),
                "workUnitId": (state.get("work_unit") or {}).get("workUnitId"),
            })
            await session.flush()
        return {"state": "HUMAN_REQUIRED", "pending_approval": False}
    return human_required


def build_migration_graph(runtime: Any, *, checkpointer: Any = None) -> Any:
    """Build and compile the top-level LangGraph migration graph.

    Args:
        runtime: WorkbenchRuntime instance with all services wired.
        checkpointer: LangGraph checkpointer (default: MemorySaver).

    Returns:
        Compiled LangGraph runnable.
    """
    saver = checkpointer or getattr(runtime, "checkpointer", None) or resolve_checkpointer(
        mode=getattr(runtime, "checkpointer_mode", "development"),
        checkpoint_path=getattr(runtime, "checkpoint_path", None),
        environment=getattr(runtime, "environment", "development"),
    )

    graph = StateGraph(MigrationGraphState)

    async def _execute_candidate(state: MigrationGraphState) -> dict[str, Any]:
        raw_record = state.get("candidate_artifact_record")
        input_snapshot_id = state.get("input_snapshot_id")
        environment = state.get("environment_fingerprint")
        if raw_record is None or not input_snapshot_id or not environment:
            return {
                "state": "HUMAN_REQUIRED",
                "error": "execution requires candidate, input snapshot, and environment identities",
            }
        scope = runtime.scopes.require_by_id(state.get("scope_id", ""))
        provider = scope.get_provider("executor.spark")
        if provider is None:
            return {"state": "HUMAN_REQUIRED", "error": "no Spark executor is mounted in candidate scope"}
        session = await runtime.sessions.open(state.get("session_id", ""))
        artifact = artifact_record_from_state(raw_record)
        try:
            result = await runtime.execution.run_spark(
                session=session,
                scope=scope,
                artifact=artifact,
                input_snapshot_id=input_snapshot_id,
                environment_fingerprint=environment,
                execution_id=f"exec-{session.id}-{state.get('repair_iteration', 0)}",
                work_unit_id=(state.get("work_unit") or {}).get("workUnitId"),
            )
        except Exception as exc:
            return {
                "state": "HUMAN_REQUIRED",
                "error": f"execution failed or outcome is unknown ({type(exc).__name__}); operator review required",
            }
        if result.get("outcome") != "SUCCEEDED":
            return {"state": "HUMAN_REQUIRED", "error": f"execution outcome is {result.get('outcome', 'UNKNOWN')}"}
        output_references = [artifact_reference(item).model_dump(by_alias=True) for item in result.get("outputArtifacts", [])]
        return {
            "state": "RECONCILING",
            "execution_outcome": {"outcome": result.get("outcome"), "outputArtifacts": output_references},
        }

    async def _reconcile_candidate(state: MigrationGraphState) -> dict[str, Any]:
        reconciler = getattr(runtime, "reconciler", None) or getattr(runtime, "reconciliation", None)
        execution = state.get("execution_outcome") or {}
        if reconciler is None or not state.get("reference_output_path") or not execution.get("outputArtifacts"):
            return {
                "state": "HUMAN_REQUIRED",
                "error": "independent reconciliation provider, reference output, or candidate output is unavailable",
            }
        scope = runtime.scopes.require_by_id(state.get("scope_id", ""))
        session = await runtime.sessions.open(state.get("session_id", ""))
        policy = runtime.policy.resolve(scope.policy)
        reconcile = getattr(reconciler, "reconcile", None)
        if not callable(reconcile):
            return {"state": "HUMAN_REQUIRED", "error": "reconciliation provider has no reconcile method"}
        await session.append(MigrationEventType.RECONCILIATION_STARTED, {
            "workUnitId": (state.get("work_unit") or {}).get("workUnitId"),
            "candidateArtifact": state.get("candidate_artifact"),
            "inputSnapshotId": state.get("input_snapshot_id"),
            "environmentFingerprint": state.get("environment_fingerprint"),
            "policyFingerprint": policy_fingerprint(policy),
        })
        await session.flush()

        async def fail(reason: str) -> dict[str, Any]:
            await session.append(MigrationEventType.RECONCILIATION_FAILED, {
                "workUnitId": (state.get("work_unit") or {}).get("workUnitId"),
                "reasonCode": "RECONCILIATION_PROVIDER_FAILED",
                "detail": reason,
            })
            await session.flush()
            return {"state": "HUMAN_REQUIRED", "error": reason}

        try:
            response = reconcile(
                reference_output_path=state["reference_output_path"],
                candidate_artifacts=execution["outputArtifacts"],
                input_snapshot_id=state.get("input_snapshot_id"),
                environment_fingerprint=state.get("environment_fingerprint"),
                policy=policy.model_dump(by_alias=True),
                scope_id=scope.id,
            )
            if inspect.isawaitable(response):
                response = await response
        except Exception as exc:
            return await fail(f"reconciliation provider failed ({type(exc).__name__})")
        validation_error = reconciliation_validation_error(response)
        if validation_error:
            return await fail(validation_error)
        await session.append(MigrationEventType.RECONCILIATION_COMPLETED, response)
        await session.flush()
        return {"state": "RECONCILING", "last_reconciliation_result": response}

    graph.add_node("discover", make_discover_node(runtime))
    graph.add_node("parse", make_parse_node(runtime))
    graph.add_node("resolve_macros", make_resolve_macros_node(runtime))
    graph.add_node("create_work_unit", make_work_unit_node(runtime))
    graph.add_node("assemble_model_context", make_model_context_node(runtime))
    graph.add_node("propose", make_propose_node(runtime))
    graph.add_node("guard", make_guard_node(runtime))
    graph.add_node("approve", make_approve_node(runtime))
    graph.add_node("approval_request", make_approval_request_node(runtime))
    graph.add_node("operator_decision_request", _make_operator_decision_request_node(runtime))
    graph.add_node("operator_decision", _make_operator_decision_node(runtime))
    graph.add_node("execute", _execute_candidate)
    graph.add_node("reconcile", _reconcile_candidate)
    graph.add_node("certify", _make_certify_node(runtime))
    graph.add_node("human_required", _make_human_required_node(runtime))
    graph.add_node("repair", build_repair_subgraph(runtime, checkpointer=saver))
    graph.add_node("translation_started", _make_translation_started_node(runtime))

    graph.add_edge(START, "discover")
    def _route_or_continue(next_node: str):
        def route(state: MigrationGraphState) -> str:
            error = state.get("error")
            return "human_required" if state.get("state") == "HUMAN_REQUIRED" or (isinstance(error, str) and bool(error.strip())) else next_node
        return route

    graph.add_conditional_edges("discover", _route_or_continue("parse"), {"parse": "parse", "human_required": "human_required"})
    graph.add_conditional_edges("parse", _route_or_continue("resolve_macros"), {"resolve_macros": "resolve_macros", "human_required": "human_required"})
    graph.add_conditional_edges("resolve_macros", _route_or_continue("create_work_unit"), {"create_work_unit": "create_work_unit", "human_required": "human_required"})
    graph.add_conditional_edges("create_work_unit", _route_or_continue("assemble_model_context"), {"assemble_model_context": "assemble_model_context", "human_required": "human_required"})
    graph.add_conditional_edges("assemble_model_context", _route_or_continue("translation_started"), {"translation_started": "translation_started", "human_required": "human_required"})
    graph.add_edge("translation_started", "propose")
    graph.add_conditional_edges("propose", _route_or_continue("guard"), {"guard": "guard", "human_required": "human_required"})
    graph.add_conditional_edges("guard", _route_or_continue("approval_request"), {"approval_request": "approval_request", "human_required": "human_required"})
    graph.add_conditional_edges("approval_request", _route_or_continue("approve"), {"approve": "approve", "human_required": "human_required"})
    graph.add_conditional_edges("approve", _route_or_continue("execute"), {"execute": "execute", "human_required": "human_required"})
    graph.add_conditional_edges("execute", _route_or_continue("reconcile"), {"reconcile": "reconcile", "human_required": "human_required"})
    def _route_reconciliation(state: MigrationGraphState) -> str:
        error = state.get("error")
        if state.get("state") == "HUMAN_REQUIRED" or (isinstance(error, str) and bool(error.strip())):
            return "human_required"
        return route_after_reconcile(state)

    graph.add_conditional_edges("reconcile", _route_reconciliation, {
        "certify": "operator_decision_request",
        "repair": "repair",
        "human_required": "human_required",
    })
    graph.add_conditional_edges("operator_decision_request", _route_or_continue("operator_decision"), {
        "operator_decision": "operator_decision", "human_required": "human_required",
    })
    graph.add_conditional_edges("operator_decision", lambda state: "certify" if state.get("state") == "CERTIFICATION_APPROVED" else "human_required", {
        "certify": "certify", "human_required": "human_required",
    })
    def _route_after_repair(state: MigrationGraphState) -> str:
        error = state.get("error")
        if state.get("state") == "HUMAN_REQUIRED" or (isinstance(error, str) and bool(error.strip())):
            return "human_required"
        return route_after_reconcile(state)

    graph.add_conditional_edges("repair", _route_after_repair, {
        "certify": "operator_decision_request",
        "repair": "repair",
        "human_required": "human_required",
    })
    graph.add_edge("certify", END)
    graph.add_edge("human_required", END)

    return graph.compile(checkpointer=saver)
