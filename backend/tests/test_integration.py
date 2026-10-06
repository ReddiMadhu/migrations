"""End-to-end integration tests — full migration graph with all services wired."""

from __future__ import annotations

from langgraph.types import Command

from sas2pyspark.core.events import MigrationEventType
from sas2pyspark.graph.migration_graph import build_migration_graph
from sas2pyspark.services.artifacts import artifact_reference
from sas2pyspark.services.executors import DummySparkExecutor
from sas2pyspark.services.policy import ExecutionPolicy
from sas2pyspark.workbench import WorkbenchConfig, create_workbench

from .conftest import make_scopes, make_session


class _Translator:
    def translate(self, **_kwargs):
        return "from pyspark.sql import functions as F\n"


class _Reconciler:
    def __init__(self):
        self.calls = 0

    def reconcile(self, **_kwargs):
        self.calls += 1
        return {
            "schemaVerdict": "PASS",
            "aggregateVerdict": "PASS",
            "rowVerdict": "PASS",
            "statisticalVerdict": "NOT_APPLICABLE",
            "differenceCount": 0,
            "certifiable": True,
            "differences": [],
        }


async def test_full_migration_with_approval_and_certification(test_dir, fixed_clock):
    policy = ExecutionPolicy(
        version="integration-v1",
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
        translator=_Translator(),
        reconciler=_Reconciler(),
    ))
    try:
        _, _, _, candidate = make_scopes(runtime)
        runtime.scopes.mount(candidate, "executor.spark", runtime.dummy_spark)
        session = await make_session(runtime, scope=candidate, session_id="integration-full")
        sas_art = runtime.artifacts.create(
            scope=candidate, data="data out; set in; run;",
            media_type="text/x-sas", created_by="integration-test",
        )
        graph = build_migration_graph(runtime)
        config = {"configurable": {"thread_id": session.id}}
        initial = {
            "session_id": session.id,
            "migration_id": session.migration_id,
            "scope_id": candidate.id,
            "state": "CREATED",
            "sas_source_artifact": artifact_reference(sas_art).model_dump(by_alias=True),
            "input_snapshot_id": "snapshot-int",
            "environment_fingerprint": "env-int",
            "reference_output_path": "/Volumes/test/reference.csv",
            "repair_history": [],
            "repair_iteration": 0,
            "open_execution_ids": [],
            "execution_outcomes": {},
            "pending_approval": False,
        }

        paused = await graph.ainvoke(initial, config=config)
        assert paused["state"] == "CANDIDATE_READY"
        assert graph.get_state(config).values["pending_approval"] is True

        paused_op = await graph.ainvoke(Command(resume=True), config=config)
        assert paused_op["state"] == "RECONCILING"
        assert graph.get_state(config).values["pending_operator_decision"] is True

        completed = await graph.ainvoke(Command(resume=True), config=config)
        assert completed["state"] == "CERTIFIED"
        assert session.projection["state"] == "CERTIFIED"

        event_types = [e.type.value for e in session.events]
        assert "workspace/discovered" in event_types
        assert "semantic-ir/created" in event_types
        assert "macro-resolution/created" in event_types
        assert "work-unit/created" in event_types
        assert "translation/started" in event_types
        assert "translation/completed" in event_types
        assert "execution/started" in event_types
        assert "execution/completed" in event_types
        assert "reconciliation/started" in event_types
        assert "reconciliation/completed" in event_types
        assert "certificate/issued" in event_types
        assert event_types[-1] == "certificate/issued"
    finally:
        await runtime.dispose()


async def test_migration_without_approval_certifies_directly(test_dir, fixed_clock):
    runtime = await create_workbench(WorkbenchConfig(
        root_dir=str(test_dir),
        clock=fixed_clock,
        translator=_Translator(),
        reconciler=_Reconciler(),
    ))
    try:
        _, _, _, candidate = make_scopes(runtime)
        runtime.scopes.mount(candidate, "executor.spark", runtime.dummy_spark)
        session = await make_session(runtime, scope=candidate, session_id="integration-no-approval")
        sas_art = runtime.artifacts.create(
            scope=candidate, data="data out; set in; run;",
            media_type="text/x-sas", created_by="integration-test",
        )
        graph = build_migration_graph(runtime)
        config = {"configurable": {"thread_id": session.id}}
        initial = {
            "session_id": session.id,
            "migration_id": session.migration_id,
            "scope_id": candidate.id,
            "state": "CREATED",
            "sas_source_artifact": artifact_reference(sas_art).model_dump(by_alias=True),
            "input_snapshot_id": "snapshot-int",
            "environment_fingerprint": "env-int",
            "reference_output_path": "/Volumes/test/reference.csv",
            "repair_history": [],
            "repair_iteration": 0,
            "open_execution_ids": [],
            "execution_outcomes": {},
            "pending_approval": False,
        }

        paused_op = await graph.ainvoke(initial, config=config)
        assert paused_op["state"] == "RECONCILING"
        assert graph.get_state(config).values["pending_operator_decision"] is True

        completed = await graph.ainvoke(Command(resume=True), config=config)
        assert completed["state"] == "CERTIFIED"
    finally:
        await runtime.dispose()


async def test_migration_without_translator_escalates(workbench):
    _, _, _, candidate = make_scopes(workbench)
    session = await make_session(workbench)
    sas_art = workbench.artifacts.create(
        scope=candidate, data="data out; set in; run;",
        media_type="text/x-sas", created_by="test",
    )
    graph = build_migration_graph(workbench)
    config = {"configurable": {"thread_id": session.id}}
    initial = {
        "session_id": session.id,
        "migration_id": session.migration_id,
        "scope_id": candidate.id,
        "state": "CREATED",
        "sas_source_artifact": artifact_reference(sas_art).model_dump(by_alias=True),
        "repair_history": [],
        "repair_iteration": 0,
        "open_execution_ids": [],
        "execution_outcomes": {},
        "pending_approval": False,
    }
    result = await graph.ainvoke(initial, config=config)
    assert result["state"] == "HUMAN_REQUIRED"
    assert "translation provider" in result["error"]


async def test_certificate_contains_all_required_fields(test_dir, fixed_clock):
    runtime = await create_workbench(WorkbenchConfig(
        root_dir=str(test_dir),
        clock=fixed_clock,
        translator=_Translator(),
        reconciler=_Reconciler(),
    ))
    try:
        _, _, _, candidate = make_scopes(runtime)
        runtime.scopes.mount(candidate, "executor.spark", runtime.dummy_spark)
        session = await make_session(runtime, scope=candidate, session_id="integration-cert")
        sas_art = runtime.artifacts.create(
            scope=candidate, data="data out; set in; run;",
            media_type="text/x-sas", created_by="integration-test",
        )
        graph = build_migration_graph(runtime)
        config = {"configurable": {"thread_id": session.id}}
        initial = {
            "session_id": session.id,
            "migration_id": session.migration_id,
            "scope_id": candidate.id,
            "state": "CREATED",
            "sas_source_artifact": artifact_reference(sas_art).model_dump(by_alias=True),
            "input_snapshot_id": "snapshot-cert",
            "environment_fingerprint": "env-cert",
            "reference_output_path": "/Volumes/test/reference.csv",
            "repair_history": [],
            "repair_iteration": 0,
            "open_execution_ids": [],
            "execution_outcomes": {},
            "pending_approval": False,
        }
        await graph.ainvoke(initial, config=config)
        await graph.ainvoke(Command(resume=True), config=config)
        await graph.ainvoke(Command(resume=True), config=config)

        cert_event = session.events[-1]
        assert cert_event.type is MigrationEventType.CERTIFICATE_ISSUED
        cert = cert_event.data
        assert cert["sessionId"] == session.id
        assert cert["migrationId"] == session.migration_id
        assert cert["scopeId"] == candidate.id
        assert cert["sourceArtifact"]
        assert cert["candidateArtifact"]
        assert cert["inputSnapshotId"] == "snapshot-cert"
        assert cert["environmentFingerprint"] == "env-cert"
        assert cert["policyFingerprint"]
        assert cert["reconciliation"]["certifiable"] is True
        assert cert["operatorDecision"]["decision"] == "ISSUE_CERTIFICATE"
        assert cert["certificateId"]
    finally:
        await runtime.dispose()
