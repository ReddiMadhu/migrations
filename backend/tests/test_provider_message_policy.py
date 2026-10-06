"""P1-4: Provider Message Retained + Policy Enforcement."""

from __future__ import annotations

import pytest

from sas2pyspark.core.events import MigrationEventType
from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.core.logging import redact_value
from sas2pyspark.services.policy import ExecutionPolicy
from sas2pyspark.services.executors import ExternalRunStatus
from sas2pyspark.services.artifacts import ArtifactRecord
from sas2pyspark.services.execution import ExecutionService
from unittest.mock import MagicMock


def _make_policy(test_dir, **overrides) -> ExecutionPolicy:
    defaults = dict(
        version="v1",
        network="none",
        allowedHosts=(),
        allowedVolumePrefixes=(str(test_dir),),
        maximumRuntimeMs=60000,
        maximumCostUnits=10,
        maximumDataClassification="internal",
        approvalRequired=False,
        allowedActions=("spark.submit",),
    )
    defaults.update(overrides)
    return ExecutionPolicy(**defaults)


def _make_artifact(test_dir, scope_id, art_id="art-1") -> ArtifactRecord:
    return ArtifactRecord(
        artifact_id=art_id, source_hash="sha256:abc", semantic_hash="",
        semantic_hash_version="", size=3, media_type="text/x-python",
        created_by="test", created_at="2026-01-01T00:00:01.000Z",
        storage_location=str(test_dir), scope_id=scope_id,
    )


@pytest.mark.asyncio
async def test_completed_event_has_provider_message(test_dir, fixed_clock):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench

    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        from .conftest import make_scopes, make_session
        _, _, _, candidate = make_scopes(runtime)
        session = await make_session(runtime, scope=candidate)

        mock_provider = MagicMock()
        mock_provider.submit.return_value = {"runId": "run-msg-1"}
        mock_provider.wait_for_terminal.return_value = ExternalRunStatus(
            state="SUCCEEDED", run_id="run-msg-1",
            provider_message="Job completed successfully", result_state="SUCCESS",
        )
        candidate.mount_provider("executor.spark", mock_provider)
        candidate.policy = _make_policy(test_dir)

        artifact = _make_artifact(test_dir, candidate.id)
        exec_service = ExecutionService(runtime.scopes, runtime.policy, runtime.approvals, runtime.guards)
        await exec_service.run_spark(
            session=session, scope=candidate, artifact=artifact,
            input_snapshot_id="snap-1", environment_fingerprint="env-1",
            execution_id="exec-msg-1",
        )

        completed = next(e for e in session.events if e.type.value == "execution/completed")
        assert completed.data.get("providerMessage") == "Job completed successfully"
    finally:
        await runtime.dispose()


@pytest.mark.asyncio
async def test_completed_event_has_state_message(test_dir, fixed_clock):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench

    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        from .conftest import make_scopes, make_session
        _, _, _, candidate = make_scopes(runtime)
        session = await make_session(runtime, scope=candidate)

        mock_provider = MagicMock()
        mock_provider.submit.return_value = {"runId": "run-msg-2"}
        mock_provider.wait_for_terminal.return_value = ExternalRunStatus(
            state="FAILED", run_id="run-msg-2",
            provider_message="OOM error", result_state="OUT_OF_MEMORY",
        )
        candidate.mount_provider("executor.spark", mock_provider)
        candidate.policy = _make_policy(test_dir)

        artifact = _make_artifact(test_dir, candidate.id, art_id="art-2")
        exec_service = ExecutionService(runtime.scopes, runtime.policy, runtime.approvals, runtime.guards)
        await exec_service.run_spark(
            session=session, scope=candidate, artifact=artifact,
            input_snapshot_id="snap-1", environment_fingerprint="env-1",
            execution_id="exec-msg-2",
        )

        completed = next(e for e in session.events if e.type.value == "execution/completed")
        assert completed.data.get("stateMessage") == "OUT_OF_MEMORY"
    finally:
        await runtime.dispose()


@pytest.mark.asyncio
async def test_unknown_event_has_provider_message(test_dir, fixed_clock):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench

    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        from .conftest import make_scopes, make_session
        _, _, _, candidate = make_scopes(runtime)
        session = await make_session(runtime, scope=candidate)

        mock_provider = MagicMock()
        mock_provider.submit.return_value = {"runId": "run-msg-3"}
        mock_provider.wait_for_terminal.return_value = ExternalRunStatus(
            state="RUNNING", run_id="run-msg-3",
            provider_message="Still executing", result_state="RUNNING",
        )
        candidate.mount_provider("executor.spark", mock_provider)
        candidate.policy = _make_policy(test_dir, maximumRuntimeMs=999999999)

        artifact = _make_artifact(test_dir, candidate.id, art_id="art-3")
        exec_service = ExecutionService(runtime.scopes, runtime.policy, runtime.approvals, runtime.guards)
        await exec_service.run_spark(
            session=session, scope=candidate, artifact=artifact,
            input_snapshot_id="snap-1", environment_fingerprint="env-1",
            execution_id="exec-msg-3",
        )

        unknown = next(e for e in session.events if e.type.value == "execution/unknown")
        assert unknown.data.get("providerMessage") == "Still executing"
        assert unknown.data.get("stateMessage") == "RUNNING"
    finally:
        await runtime.dispose()


@pytest.mark.asyncio
async def test_provider_message_redacted(test_dir, fixed_clock):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench

    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        from .conftest import make_scopes, make_session
        _, _, _, candidate = make_scopes(runtime)
        session = await make_session(runtime, scope=candidate)

        secret_msg = "token=abc123 dapi_tokenXYZ123456789012345678"
        mock_provider = MagicMock()
        mock_provider.submit.return_value = {"runId": "run-redact-1"}
        mock_provider.wait_for_terminal.return_value = ExternalRunStatus(
            state="SUCCEEDED", run_id="run-redact-1",
            provider_message=secret_msg, result_state="SUCCESS",
        )
        candidate.mount_provider("executor.spark", mock_provider)
        candidate.policy = _make_policy(test_dir)

        artifact = _make_artifact(test_dir, candidate.id, art_id="art-redact")
        exec_service = ExecutionService(runtime.scopes, runtime.policy, runtime.approvals, runtime.guards)
        await exec_service.run_spark(
            session=session, scope=candidate, artifact=artifact,
            input_snapshot_id="snap-1", environment_fingerprint="env-1",
            execution_id="exec-redact-1",
        )

        completed = next(e for e in session.events if e.type.value == "execution/completed")
        stored_msg = completed.data.get("providerMessage", "")
        assert "abc123" not in stored_msg
        assert "dapi_tokenXYZ" not in stored_msg
    finally:
        await runtime.dispose()


@pytest.mark.asyncio
async def test_runtime_policy_enforced_pre_submit(test_dir, fixed_clock):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench

    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        from .conftest import make_scopes, make_session
        _, _, _, candidate = make_scopes(runtime)
        session = await make_session(runtime, scope=candidate)

        mock_provider = MagicMock()
        candidate.mount_provider("executor.spark", mock_provider)
        candidate.policy = _make_policy(test_dir, maximumRuntimeMs=5000)

        artifact = _make_artifact(test_dir, candidate.id, art_id="art-pre")
        exec_service = ExecutionService(runtime.scopes, runtime.policy, runtime.approvals, runtime.guards)
        with pytest.raises(WorkbenchError) as exc_info:
            await exec_service.run_spark(
                session=session, scope=candidate, artifact=artifact,
                input_snapshot_id="snap-1", environment_fingerprint="env-1",
                execution_id="exec-pre-submit",
                estimated_runtime_ms=10000,
            )
        assert exc_info.value.code == WorkbenchErrorCode.POLICY_DENIED
        mock_provider.submit.assert_not_called()
    finally:
        await runtime.dispose()


@pytest.mark.asyncio
async def test_runtime_policy_enforced_post_poll(test_dir, fixed_clock):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench

    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        from .conftest import make_scopes, make_session
        _, _, _, candidate = make_scopes(runtime)
        session = await make_session(runtime, scope=candidate)

        import time as _time

        def slow_status(run_id):
            _time.sleep(0.06)
            return ExternalRunStatus(
                state="RUNNING", run_id=run_id,
                provider_message="still running", result_state="RUNNING",
            )

        mock_provider = MagicMock()
        mock_provider.submit.return_value = {"runId": "run-timeout-1"}
        mock_provider.wait_for_terminal = slow_status
        mock_provider.cancel.return_value = {"accepted": True}
        candidate.mount_provider("executor.spark", mock_provider)
        candidate.policy = _make_policy(test_dir, maximumRuntimeMs=10)

        artifact = _make_artifact(test_dir, candidate.id, art_id="art-timeout")
        exec_service = ExecutionService(runtime.scopes, runtime.policy, runtime.approvals, runtime.guards)
        result = await exec_service.run_spark(
            session=session, scope=candidate, artifact=artifact,
            input_snapshot_id="snap-1", environment_fingerprint="env-1",
            execution_id="exec-timeout-1",
        )
        assert result["outcome"] == "TIMED_OUT"
        mock_provider.cancel.assert_called_once_with("run-timeout-1")

        completed = next(e for e in session.events if e.type.value == "execution/completed")
        assert completed.data.get("outcome") == "TIMED_OUT"
    finally:
        await runtime.dispose()


def test_event_types_preserved():
    expected_types = {
        "migration/created", "workspace/discovered", "dependency/graph-created",
        "semantic-ir/created", "macro-resolution/created",
        "work-unit/created", "work-unit/updated", "work-unit/closed",
        "source/registered", "snapshot/created",
        "translation/started", "translation/completed",
        "artifact/created", "execution/started", "execution/completed",
        "reconciliation/started", "reconciliation/completed", "reconciliation/failed",
        "difference/report-created",
        "repair/started", "repair/proposed", "patch/applied",
        "certificate/issued", "migration/escalated",
        "session/forked", "session/recovered", "execution/unknown",
    }
    actual_types = {e.value for e in MigrationEventType}
    assert expected_types.issubset(actual_types)
    assert len(expected_types) == 27
