"""P1-2: Configurable Timeout + Detached Execution + Re-poll + Cancel."""

from __future__ import annotations

import pytest

from sas2pyspark.services.policy import (
    DEFAULT_EXECUTION_POLICY,
    ExecutionPolicy,
    effective_policy,
    validate_policy,
)


def test_policy_has_max_wait_seconds_default():
    assert DEFAULT_EXECUTION_POLICY.max_wait_seconds == 900


def test_policy_has_poll_interval_seconds_default():
    assert DEFAULT_EXECUTION_POLICY.poll_interval_seconds == 3


def test_policy_has_cancellation_deadline_default():
    assert DEFAULT_EXECUTION_POLICY.cancellation_deadline is False


def test_policy_fields_backward_compatible():
    p = ExecutionPolicy(
        version="v1",
        network="none",
        allowedHosts=(),
        allowedVolumePrefixes=("/Volumes/test",),
        maximumRuntimeMs=60000,
        maximumCostUnits=10,
        maximumDataClassification="internal",
        approvalRequired=False,
        allowedActions=("spark.submit",),
    )
    assert p.max_wait_seconds == 900
    assert p.poll_interval_seconds == 3
    assert p.cancellation_deadline is False


def test_effective_policy_inherits_new_fields():
    parent = ExecutionPolicy(
        version="v1",
        network="none",
        allowedHosts=(),
        allowedVolumePrefixes=("/Volumes/test",),
        maximumRuntimeMs=60000,
        maximumCostUnits=10,
        maximumDataClassification="internal",
        approvalRequired=False,
        allowedActions=("spark.submit",),
        maxWaitSeconds=120,
        pollIntervalSeconds=5,
        cancellationDeadline=True,
    )
    child = effective_policy(parent)
    assert child.max_wait_seconds == 120
    assert child.poll_interval_seconds == 5
    assert child.cancellation_deadline is True


def test_effective_policy_override_new_fields():
    parent = ExecutionPolicy(
        version="v1",
        network="none",
        allowedHosts=(),
        allowedVolumePrefixes=("/Volumes/test",),
        maximumRuntimeMs=60000,
        maximumCostUnits=10,
        maximumDataClassification="internal",
        approvalRequired=False,
        allowedActions=("spark.submit",),
    )
    child = effective_policy(parent, {"maxWaitSeconds": 30, "pollIntervalSeconds": 1, "cancellationDeadline": True})
    assert child.max_wait_seconds == 30
    assert child.poll_interval_seconds == 1
    assert child.cancellation_deadline is True


def test_policy_round_trip():
    p = ExecutionPolicy(
        version="v1",
        network="none",
        allowedHosts=(),
        allowedVolumePrefixes=("/Volumes/test",),
        maximumRuntimeMs=60000,
        maximumCostUnits=10,
        maximumDataClassification="internal",
        approvalRequired=False,
        allowedActions=("spark.submit",),
        maxWaitSeconds=60,
        pollIntervalSeconds=2,
        cancellationDeadline=True,
    )
    d = p.model_dump(by_alias=True)
    assert d["maxWaitSeconds"] == 60
    assert d["pollIntervalSeconds"] == 2
    assert d["cancellationDeadline"] is True


@pytest.mark.asyncio
async def test_unknown_event_persists_run_id(test_dir, fixed_clock):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench
    from sas2pyspark.services.executors import ExternalRunStatus
    from unittest.mock import MagicMock

    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        from .conftest import make_scopes, make_session
        _, _, _, candidate = make_scopes(runtime)
        session = await make_session(runtime, scope=candidate)

        mock_provider = MagicMock()
        mock_provider.submit.return_value = {"runId": "run-123"}
        mock_provider.wait_for_terminal.return_value = ExternalRunStatus(
            state="RUNNING", run_id="run-123", provider_message="still running", result_state="RUNNING",
        )
        candidate.mount_provider("executor.spark", mock_provider)

        from sas2pyspark.core.events import MigrationEventType
        await session.append(MigrationEventType.EXECUTION_STARTED, {
            "executionId": "exec-1",
            "provider": "executor.spark",
            "runId": "run-123",
        })
        await session.append(MigrationEventType.EXECUTION_UNKNOWN, {
            "executionId": "exec-1",
            "reason": "UNRESOLVED_EXTERNAL_OUTCOME",
            "runId": "run-123",
            "provider": "executor.spark",
        })
        await session.flush()

        unknown = next(e for e in session.events if e.type.value == "execution/unknown")
        assert unknown.data.get("runId") == "run-123"
    finally:
        await runtime.dispose()


@pytest.mark.asyncio
async def test_cancellation_deadline_cancels_on_timeout(test_dir, fixed_clock):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench
    from sas2pyspark.services.executors import ExternalRunStatus
    from unittest.mock import MagicMock, AsyncMock

    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        from .conftest import make_scopes, make_session
        _, _, _, candidate = make_scopes(runtime)
        session = await make_session(runtime, scope=candidate)

        mock_provider = MagicMock()
        mock_provider.submit.return_value = {"runId": "run-456"}
        mock_provider.wait_for_terminal.return_value = ExternalRunStatus(
            state="UNKNOWN", run_id="run-456", provider_message="timed out", result_state="UNKNOWN",
        )
        mock_provider.cancel.return_value = {"accepted": True}
        candidate.mount_provider("executor.spark", mock_provider)

        from sas2pyspark.services.execution import ExecutionService
        from sas2pyspark.services.artifacts import ArtifactRecord

        scope = candidate
        scope.policy = ExecutionPolicy(
            version="v1",
            network="none",
            allowedHosts=(),
            allowedVolumePrefixes=(str(test_dir),),
            maximumRuntimeMs=60000,
            maximumCostUnits=10,
            maximumDataClassification="internal",
            approvalRequired=False,
            allowedActions=("spark.submit",),
            cancellationDeadline=True,
        )

        artifact = ArtifactRecord(
            artifact_id="art-1", source_hash="sha256:abc", semantic_hash="",
            semantic_hash_version="", size=3, media_type="text/x-python",
            created_by="test", created_at="2026-01-01T00:00:01.000Z",
            storage_location=str(test_dir), scope_id=scope.id,
        )

        exec_service = ExecutionService(runtime.scopes, runtime.policy, runtime.approvals, runtime.guards)
        result = await exec_service.run_spark(
            session=session, scope=scope, artifact=artifact,
            input_snapshot_id="snap-1", environment_fingerprint="env-1",
            execution_id="exec-cancel-test",
        )
        assert result["outcome"] == "UNKNOWN"
        mock_provider.cancel.assert_called_once_with("run-456")
    finally:
        await runtime.dispose()


@pytest.mark.asyncio
async def test_no_cancel_without_cancellation_deadline(test_dir, fixed_clock):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench
    from sas2pyspark.services.executors import ExternalRunStatus
    from unittest.mock import MagicMock

    runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    try:
        from .conftest import make_scopes, make_session
        _, _, _, candidate = make_scopes(runtime)
        session = await make_session(runtime, scope=candidate)

        mock_provider = MagicMock()
        mock_provider.submit.return_value = {"runId": "run-789"}
        mock_provider.wait_for_terminal.return_value = ExternalRunStatus(
            state="UNKNOWN", run_id="run-789", provider_message="timed out", result_state="UNKNOWN",
        )
        mock_provider.cancel.return_value = {"accepted": True}
        candidate.mount_provider("executor.spark", mock_provider)

        from sas2pyspark.services.execution import ExecutionService
        from sas2pyspark.services.artifacts import ArtifactRecord

        scope = candidate
        scope.policy = ExecutionPolicy(
            version="v1",
            network="none",
            allowedHosts=(),
            allowedVolumePrefixes=(str(test_dir),),
            maximumRuntimeMs=60000,
            maximumCostUnits=10,
            maximumDataClassification="internal",
            approvalRequired=False,
            allowedActions=("spark.submit",),
            cancellationDeadline=False,
        )

        artifact = ArtifactRecord(
            artifact_id="art-2", source_hash="sha256:def", semantic_hash="",
            semantic_hash_version="", size=3, media_type="text/x-python",
            created_by="test", created_at="2026-01-01T00:00:01.000Z",
            storage_location=str(test_dir), scope_id=scope.id,
        )

        exec_service = ExecutionService(runtime.scopes, runtime.policy, runtime.approvals, runtime.guards)
        result = await exec_service.run_spark(
            session=session, scope=scope, artifact=artifact,
            input_snapshot_id="snap-1", environment_fingerprint="env-1",
            execution_id="exec-no-cancel-test",
        )
        assert result["outcome"] == "UNKNOWN"
        mock_provider.cancel.assert_not_called()
    finally:
        await runtime.dispose()
