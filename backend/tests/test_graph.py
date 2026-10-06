"""End-to-end graph tests — migration graph runs with dummy provider."""

from __future__ import annotations

import pytest
from langgraph.types import Command
from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode

from sas2pyspark.graph.migration_graph import build_migration_graph
from sas2pyspark.graph.state import MigrationGraphState
from sas2pyspark.services.policy import ExecutionPolicy
from sas2pyspark.workbench import WorkbenchConfig, create_workbench

from .conftest import make_scopes, make_session


async def test_graph_compiles(workbench):
    graph = build_migration_graph(workbench)
    assert graph is not None


async def test_workbench_sets_default_reconciler_to_reconciliation_service(test_dir, fixed_clock):
    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        from sas2pyspark.services.reconciler import InMemoryReconciler
        assert isinstance(runtime.reconciler, InMemoryReconciler)
        assert runtime.reconciler._service is runtime.reconciliation
    finally:
        await runtime.dispose()


async def test_workbench_exposes_default_checkpointer_policy(test_dir, fixed_clock):
    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        assert runtime.checkpointer is not None
        assert type(runtime.checkpointer).__name__ in {"MemorySaver", "InMemorySaver"}
        graph = build_migration_graph(runtime)
        assert graph.checkpointer is runtime.checkpointer
    finally:
        await runtime.dispose()


async def test_graph_runs_end_to_end_with_dummy(workbench):
    _, _, _, candidate = make_scopes(workbench)
    session = await make_session(workbench)
    artifact = workbench.artifacts.create(
        scope=candidate, data="data temp; set in; run;",
        media_type="text/x-sas", created_by="test",
    )
    from sas2pyspark.services.artifacts import artifact_reference
    ref = artifact_reference(artifact)
    graph = build_migration_graph(workbench)
    initial_state: MigrationGraphState = {
        "session_id": session.id,
        "migration_id": session.migration_id,
        "scope_id": candidate.id,
        "state": "CREATED",
        "sas_source_artifact": ref.model_dump(by_alias=True),
        "repair_history": [],
        "repair_iteration": 0,
        "open_execution_ids": [],
        "execution_outcomes": {},
        "pending_approval": False,
    }
    result = await graph.ainvoke(
        initial_state,
        config={"configurable": {"thread_id": session.id}},
    )
    assert result is not None
    assert result["state"] == "HUMAN_REQUIRED"
    assert result["error"] == "translation provider or SAS source artifact is unavailable"
    event_types = [event.type.value for event in session.events]
    assert "dependency/graph-created" in event_types
    assert "semantic-ir/created" in event_types
    assert "macro-resolution/created" in event_types
    assert "work-unit/created" in event_types


async def test_graph_state_has_all_channels():
    fields = set(MigrationGraphState.__annotations__.keys())
    expected = {
        "session_id", "migration_id", "scope_id", "state",
        "sas_source_artifact", "candidate_artifact",
        "reference_input_path", "reference_output_path",
        "ir_snapshot", "macro_resolution", "work_unit", "model_context",
        "last_reconciliation_result", "repair_history", "repair_iteration",
        "open_execution_ids", "execution_outcomes", "messages",
        "error", "pending_approval",
    }
    assert expected.issubset(fields)


async def test_graph_requires_pre_execution_and_post_reconciliation_decisions(test_dir, fixed_clock):
    class Translator:
        def translate(self, **_kwargs):
            return "from pyspark.sql import functions as F\n"

    class Reconciler:
        def reconcile(self, **_kwargs):
            return {
                "schemaVerdict": "PASS",
                "aggregateVerdict": "PASS",
                "rowVerdict": "PASS",
                "statisticalVerdict": "NOT_APPLICABLE",
                "differenceCount": 0,
                "certifiable": True,
            }

    policy = ExecutionPolicy(
        version="test-approval-v1",
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
    ))
    try:
        _, _, _, candidate = make_scopes(runtime)
        runtime.scopes.mount(candidate, "executor.spark", runtime.dummy_spark)
        session = await make_session(runtime, scope=candidate, session_id="approval-flow")
        artifact = runtime.artifacts.create(
            scope=candidate,
            data="data temp; set in; run;",
            media_type="text/x-sas",
            created_by="test",
        )
        from sas2pyspark.services.artifacts import artifact_reference
        graph = build_migration_graph(runtime)
        config = {"configurable": {"thread_id": session.id}}
        initial: MigrationGraphState = {
            "session_id": session.id,
            "migration_id": session.migration_id,
            "scope_id": candidate.id,
            "state": "CREATED",
            "sas_source_artifact": artifact_reference(artifact).model_dump(by_alias=True),
            "input_snapshot_id": "snapshot-test",
            "environment_fingerprint": "env-test",
            "reference_output_path": "/Volumes/test/reference.csv",
            "repair_history": [],
            "repair_iteration": 0,
            "open_execution_ids": [],
            "execution_outcomes": {},
            "pending_approval": False,
        }

        paused_for_execution = await graph.ainvoke(initial, config=config)
        checkpoint = graph.get_state(config)
        assert paused_for_execution["state"] == "CANDIDATE_READY"
        assert checkpoint.values["pending_approval"] is True

        paused_for_certificate = await graph.ainvoke(Command(resume=True), config=config)
        checkpoint = graph.get_state(config)
        assert paused_for_certificate["state"] == "RECONCILING"
        assert checkpoint.values["pending_operator_decision"] is True
        assert session.projection["state"] == "AWAITING_OPERATOR"

        completed = await graph.ainvoke(Command(resume=True), config=config)
        assert completed["state"] == "CERTIFIED"
        assert session.projection["state"] == "CERTIFIED"
        event_types = [event.type.value for event in session.events]
        assert event_types[-1] == "certificate/issued"
        certificate = session.events[-1].data
        assert certificate["executionApproval"]["approvalId"]
        assert certificate["operatorDecision"]["decision"] == "ISSUE_CERTIFICATE"

        stale_session = await make_session(runtime, scope=candidate, session_id="stale-approval-flow")
        stale_initial = dict(initial)
        stale_initial["session_id"] = stale_session.id
        stale_initial["sas_source_artifact"] = artifact_reference(artifact).model_dump(by_alias=True)
        await graph.ainvoke(stale_initial, config={"configurable": {"thread_id": stale_session.id}})
        candidate.policy = ExecutionPolicy(
            version="changed-after-review",
            network="none",
            allowedHosts=(),
            allowedVolumePrefixes=(str(test_dir),),
            maximumRuntimeMs=60000,
            maximumCostUnits=10,
            maximumDataClassification="internal",
            approvalRequired=True,
            allowedActions=("spark.submit", "sas.execute"),
        )
        with pytest.raises(WorkbenchError) as stale_error:
            await graph.ainvoke(
                Command(resume=True),
                config={"configurable": {"thread_id": stale_session.id}},
            )
        assert stale_error.value.code == WorkbenchErrorCode.STALE_APPROVAL
    finally:
        await runtime.dispose()


async def test_route_after_reconcile_certifiable():
    from sas2pyspark.graph.nodes.route import route_after_reconcile
    state = {"last_reconciliation_result": {"certifiable": True}}
    assert route_after_reconcile(state) == "certify"


async def test_route_after_reconcile_mismatch():
    from sas2pyspark.graph.nodes.route import route_after_reconcile
    state = {"last_reconciliation_result": {"certifiable": False, "aggregateVerdict": "PASS"}}
    assert route_after_reconcile(state) == "repair"


async def test_route_after_reconcile_mismatch_routes_to_repair():
    from sas2pyspark.graph.nodes.route import route_after_reconcile
    state = {"last_reconciliation_result": {"certifiable": False, "aggregateVerdict": "FAIL"}}
    assert route_after_reconcile(state) == "repair"


async def test_route_after_reconcile_missing_result_escalates():
    from sas2pyspark.graph.nodes.route import route_after_reconcile
    assert route_after_reconcile({}) == "human_required"
