"""Repair loop integration tests using deterministic local providers."""

from __future__ import annotations

from langgraph.types import Command

from sas2pyspark.core.events import MigrationEventType
from sas2pyspark.graph.migration_graph import build_migration_graph
from sas2pyspark.graph.repair_policy import (
    budget_exceeded,
    progress_error,
    reconciliation_signature,
    signature_seen,
)
from sas2pyspark.services.policy import ExecutionPolicy
from sas2pyspark.services.artifacts import artifact_reference
from sas2pyspark.services.executors import DummySparkExecutor
from sas2pyspark.workbench import WorkbenchConfig, create_workbench

from .conftest import make_scopes, make_session


class Translator:
    def translate(self, **_kwargs):
        return "from pyspark.sql import functions as F\n"


class Repairer:
    def diagnose(self, **_kwargs):
        return {"category": "row", "finding": "one known filter mismatch"}

    def propose_patch(self, **_kwargs):
        return {
            "candidateSource": "from pyspark.sql import functions as F\n# repaired candidate\n",
            "hypothesis": "preserve-null-filter-semantics",
            "expectedEffect": {"metric": "differenceCount", "minimumReduction": 1},
        }


class Reconciler:
    def __init__(self):
        self.calls = 0

    def reconcile(self, **_kwargs):
        self.calls += 1
        if self.calls == 1:
            return {
                "schemaVerdict": "PASS",
                "aggregateVerdict": "PASS",
                "rowVerdict": "FAIL",
                "statisticalVerdict": "NOT_APPLICABLE",
                "differenceCount": 2,
                "certifiable": False,
                "differences": [{"kind": "row", "path": "rows[0]"}],
            }
        return {
            "schemaVerdict": "PASS",
            "aggregateVerdict": "PASS",
            "rowVerdict": "PASS",
            "statisticalVerdict": "NOT_APPLICABLE",
            "differenceCount": 0,
            "certifiable": True,
            "differences": [],
        }


async def test_repair_converges_and_still_requires_operator_certificate(test_dir, fixed_clock):
    reconciler = Reconciler()
    runtime = await create_workbench(WorkbenchConfig(
        root_dir=str(test_dir),
        clock=fixed_clock,
        translator=Translator(),
        reconciler=reconciler,
        repairer=Repairer(),
    ))
    try:
        _, _, _, candidate_scope = make_scopes(runtime)

        def output_for_request(request):
            return "col1,col2\n1,2\n3,4\n"

        executor = DummySparkExecutor(runtime.artifacts, output_fn=output_for_request)
        runtime.scopes.mount(candidate_scope, "executor.spark", executor)
        session = await make_session(runtime, scope=candidate_scope, session_id="repair-converges")
        sas_artifact = runtime.artifacts.create(
            scope=candidate_scope,
            data="data out; set input; run;",
            media_type="text/x-sas",
            created_by="repair-test",
        )
        graph = build_migration_graph(runtime)
        config = {"configurable": {"thread_id": session.id}}
        state = {
            "session_id": session.id,
            "migration_id": session.migration_id,
            "scope_id": candidate_scope.id,
            "state": "CREATED",
            "sas_source_artifact": artifact_reference(sas_artifact).model_dump(by_alias=True),
            "input_snapshot_id": "input-snapshot-v1",
            "environment_fingerprint": "test-environment-v1",
            "reference_output_path": "/Volumes/test/reference.csv",
            "repair_history": [],
            "repair_iteration": 0,
            "open_execution_ids": [],
            "execution_outcomes": {},
            "pending_approval": False,
        }

        paused = await graph.ainvoke(state, config=config)
        checkpoint = graph.get_state(config)
        assert paused["state"] == "RECONCILING"
        assert checkpoint.values["pending_operator_decision"] is True
        assert reconciler.calls == 2
        assert checkpoint.values["repair_iteration"] == 1
        assert checkpoint.values["last_reconciliation_result"]["certifiable"] is True

        final = await graph.ainvoke(Command(resume=True), config=config)
        assert final["state"] == "CERTIFIED"
        assert session.projection["state"] == "CERTIFIED"
        assert any(event.type is MigrationEventType.REPAIR_STARTED for event in session.events)
        assert any(event.type is MigrationEventType.REPAIR_PROPOSED for event in session.events)
        assert any(event.type is MigrationEventType.PATCH_APPLIED for event in session.events)
    finally:
        await runtime.dispose()


def test_repair_policy_detects_budget_regression_and_non_improvement():
    assert budget_exceeded(3, 3)
    assert not budget_exceeded(2, 3)
    previous = {
        "schemaVerdict": "PASS",
        "aggregateVerdict": "PASS",
        "rowVerdict": "FAIL",
        "statisticalVerdict": "NOT_APPLICABLE",
        "differenceCount": 4,
    }
    regressed = {**previous, "schemaVerdict": "FAIL", "differenceCount": 2}
    unchanged = {**previous, "differenceCount": 4}
    assert progress_error(previous, regressed, 1) == "repair introduced a regression in schemaVerdict"
    assert progress_error(previous, unchanged, 1) == "repair did not achieve its predicted minimum difference reduction"


def test_repair_policy_detects_repeated_mismatch_signature_across_counts():
    first = {
        "schemaVerdict": "PASS",
        "aggregateVerdict": "PASS",
        "rowVerdict": "FAIL",
        "statisticalVerdict": "NOT_APPLICABLE",
        "differenceCount": 3,
        "differences": [{"kind": "row", "path": "rows[4]"}],
    }
    improved_count_same_mismatch = {**first, "differenceCount": 2}
    previous_signature = reconciliation_signature(first)

    assert reconciliation_signature(improved_count_same_mismatch) == previous_signature
    assert signature_seen([{"resultSignature": previous_signature}], reconciliation_signature(improved_count_same_mismatch))


async def test_repair_candidate_can_be_rejected_at_approval_interrupt(test_dir, fixed_clock):
    policy = ExecutionPolicy(
        version="repair-approval-v1",
        network="none",
        allowedHosts=(),
        allowedVolumePrefixes=(str(test_dir),),
        maximumRuntimeMs=60000,
        maximumCostUnits=10,
        maximumDataClassification="internal",
        approvalRequired=True,
        allowedActions=("spark.submit", "sas.execute"),
    )
    runtime = await create_workbench(WorkbenchConfig(
        root_dir=str(test_dir),
        clock=fixed_clock,
        base_policy=policy,
        translator=Translator(),
        reconciler=Reconciler(),
        repairer=Repairer(),
    ))
    try:
        _, _, _, candidate_scope = make_scopes(runtime)
        runtime.scopes.mount(candidate_scope, "executor.spark", runtime.dummy_spark)
        session = await make_session(runtime, scope=candidate_scope, session_id="repair-rejects")
        sas_artifact = runtime.artifacts.create(
            scope=candidate_scope,
            data="data out; set input; run;",
            media_type="text/x-sas",
            created_by="repair-approval-test",
        )
        graph = build_migration_graph(runtime)
        config = {"configurable": {"thread_id": session.id}}
        state = {
            "session_id": session.id,
            "migration_id": session.migration_id,
            "scope_id": candidate_scope.id,
            "state": "CREATED",
            "sas_source_artifact": artifact_reference(sas_artifact).model_dump(by_alias=True),
            "input_snapshot_id": "input-snapshot-v1",
            "environment_fingerprint": "test-environment-v1",
            "reference_output_path": "/Volumes/test/reference.csv",
            "repair_history": [],
            "repair_iteration": 0,
            "open_execution_ids": [],
            "execution_outcomes": {},
            "pending_approval": False,
        }

        initial_approval = await graph.ainvoke(state, config=config)
        assert initial_approval["state"] == "CANDIDATE_READY"
        assert graph.get_state(config).values["pending_approval"] is True

        repair_approval = await graph.ainvoke(Command(resume=True), config=config)
        checkpoint = graph.get_state(config, subgraphs=True)
        sub_state = checkpoint.tasks[0].state if checkpoint.tasks else checkpoint
        assert sub_state.values["state"] == "CANDIDATE_READY"
        assert sub_state.values["pending_approval"] is True
        assert sub_state.values["approval_request"]["question"] == "Approve this repair candidate for execution?"

        rejected = await graph.ainvoke(Command(resume=False), config=config)
        assert rejected["state"] == "HUMAN_REQUIRED"
        assert any(
            event.type is MigrationEventType.MIGRATION_ESCALATED
            and event.data.get("reasonCode") == "REPAIR_CANDIDATE_REJECTED"
            for event in session.events
        )
    finally:
        await runtime.dispose()