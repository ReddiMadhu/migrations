"""Execution tests — execution seam, idempotency, unknown status, guards."""

from __future__ import annotations

import pytest

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.core.events import MigrationEventType
from sas2pyspark.core.hash import sha256_text
from sas2pyspark.services.artifacts import artifact_reference
from sas2pyspark.services.approvals import binding_for
from sas2pyspark.services.guards import GuardService, GuardRequest, GuardDecision
from sas2pyspark.services.executors import DummySparkExecutor

from .conftest import make_scopes, make_session


async def test_idempotency_key_is_deterministic():
    key1 = sha256_text("session-1:exec-1")[:64]
    key2 = sha256_text("session-1:exec-1")[:64]
    assert key1 == key2
    key3 = sha256_text("session-1:exec-2")[:64]
    assert key1 != key3


async def test_guard_denies_approved_request(workbench):
    _, _, _, candidate = make_scopes(workbench)
    session = await make_session(workbench)
    artifact = workbench.artifacts.create(scope=candidate, data="x", media_type="text/plain", created_by="test")

    def deny_guard(request: GuardRequest) -> GuardDecision:
        return GuardDecision(allow=False, reason_code="TEST_DENY", detail="test denial")

    workbench.guards.register("test-deny-guard", deny_guard)
    with pytest.raises(WorkbenchError) as exc_info:
        await workbench.execution.run_spark(
            session=session, scope=candidate, artifact=artifact,
            input_snapshot_id="input-1", environment_fingerprint="env-1",
            execution_id="exec-1",
        )
    assert exc_info.value.code == WorkbenchErrorCode.POLICY_DENIED


async def test_dummy_spark_success_mode(workbench):
    _, _, _, candidate = make_scopes(workbench)
    workbench.scopes.mount(candidate, "executor.spark", workbench.dummy_spark)
    session = await make_session(workbench, scope=candidate)
    artifact = workbench.artifacts.create(scope=candidate, data="print('hello')", media_type="text/x-python", created_by="test")
    result = await workbench.execution.run_spark(
        session=session, scope=candidate, artifact=artifact,
        input_snapshot_id="input-1", environment_fingerprint="env-1",
        execution_id="exec-1",
    )
    assert result["outcome"] == "SUCCEEDED"
    assert result["runId"] == "dummy-exec-1"


async def test_unknown_status_sets_human_required(workbench, test_dir):
    from sas2pyspark.services.executors import DummySparkExecutor
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench
    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir / "wb-unknown"), dummy_mode="unknown"))
    try:
        _, _, _, candidate = make_scopes(runtime)
        runtime.scopes.mount(candidate, "executor.spark", runtime.dummy_spark)
        session = await make_session(runtime, scope=candidate)
        artifact = runtime.artifacts.create(scope=candidate, data="x", media_type="text/plain", created_by="test")
        result = await runtime.execution.run_spark(
            session=session, scope=candidate, artifact=artifact,
            input_snapshot_id="input-1", environment_fingerprint="env-1",
            execution_id="exec-1",
        )
        assert result["outcome"] == "UNKNOWN"
        assert session.projection["state"] == "HUMAN_REQUIRED"
    finally:
        await runtime.dispose()


async def test_guard_can_deny_but_not_authorize(workbench):
    _, _, _, candidate = make_scopes(workbench)
    session = await make_session(workbench)
    artifact = workbench.artifacts.create(scope=candidate, data="x", media_type="text/plain", created_by="test")

    allow_guard_called = [False]

    def allow_guard(request: GuardRequest) -> GuardDecision:
        allow_guard_called[0] = True
        return GuardDecision(allow=True)

    workbench.guards.register("allow-guard", allow_guard)
    with pytest.raises(WorkbenchError) as exc_info:
        await workbench.execution.run_spark(
            session=session, scope=candidate, artifact=artifact,
            input_snapshot_id="input-1", environment_fingerprint="env-1",
            execution_id="exec-1",
        )
    assert exc_info.value.code == WorkbenchErrorCode.PROVIDER_UNAVAILABLE
    assert allow_guard_called[0] is True
