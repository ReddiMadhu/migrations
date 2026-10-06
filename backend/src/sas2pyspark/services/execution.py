"""ExecutionService — guarded execution seam: policy → approval → guard → submit → poll → output."""

from __future__ import annotations

import asyncio
import re
import time
from typing import Any

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode, error_message
from sas2pyspark.core.hash import sha256_text
from sas2pyspark.core.json import assert_safe_segment
from sas2pyspark.core.logging import redact_value
from sas2pyspark.core.events import ExecutionOutcome, ExternalRunState
from sas2pyspark.services.artifacts import ArtifactRecord, artifact_reference
from sas2pyspark.services.approvals import ApprovalService, binding_for
from sas2pyspark.services.guards import GuardService, GuardRequest
from sas2pyspark.services.policy import PolicyService, policy_fingerprint
from sas2pyspark.services.scopes import ScopeRef, ScopeService
from sas2pyspark.services.executors import SparkExecutor, ExternalSubmitRequest, ExternalRunStatus
from sas2pyspark.session.service import MigrationSession


def _within_prefix(path: str, prefix: str) -> bool:
    np = path.replace("\\", "/").rstrip("/")
    npp = prefix.replace("\\", "/").rstrip("/")
    return np == npp or np.startswith(f"{npp}/")


def _status_outcome(status: ExternalRunStatus) -> ExecutionOutcome:
    mapping = {
        "SUCCEEDED": "SUCCEEDED", "FAILED": "FAILED", "CANCELED": "CANCELED",
        "TIMED_OUT": "TIMED_OUT", "PARTIAL_SUCCESS": "PARTIAL_SUCCESS",
    }
    return mapping.get(status.state, "UNKNOWN")


class ExecutionService:
    def __init__(self, scopes: ScopeService, policy: PolicyService, approvals: ApprovalService, guards: GuardService) -> None:
        self._scopes = scopes
        self._policy = policy
        self._approvals = approvals
        self._guards = guards

    async def run_spark(
        self, *,
        session: MigrationSession, scope: ScopeRef, artifact: ArtifactRecord,
        input_snapshot_id: str, environment_fingerprint: str, execution_id: str,
        action: str | None = None, work_unit_id: str | None = None,
        estimated_runtime_ms: int | None = None,
        signal: Any = None,
    ) -> dict[str, Any]:
        scope = self._scopes.require_by_id(scope.id)
        self._scopes.assert_scope_owned(self._scopes.require_by_id(session.scope.id), scope)
        act = action or "spark.submit"
        assert_safe_segment(execution_id, "executionId")
        assert_safe_segment(input_snapshot_id, "inputSnapshotId")
        policy = self._policy.resolve(scope.policy)
        if estimated_runtime_ms is not None and estimated_runtime_ms > policy.maximum_runtime_ms:
            raise WorkbenchError(
                WorkbenchErrorCode.POLICY_DENIED,
                "estimated runtime exceeds policy maximum",
                {"estimated": estimated_runtime_ms, "maximum": policy.maximum_runtime_ms},
            )
        if act not in policy.allowed_actions:
            raise WorkbenchError(WorkbenchErrorCode.POLICY_DENIED, "action is not allowed by scope policy", {"action": act, "policy": policy.version})
        if not any(_within_prefix(scope.workspace, p) for p in policy.allowed_volume_prefixes):
            raise WorkbenchError(WorkbenchErrorCode.POLICY_DENIED, "scope workspace is outside policy volume prefixes", {"scope": scope.id, "workspace": scope.workspace, "prefixes": list(policy.allowed_volume_prefixes)})
        policy_hash = policy_fingerprint(policy)
        guard_request = GuardRequest(
            scope=scope, action=act, artifact=artifact,
            input_snapshot_id=input_snapshot_id, environment_fingerprint=environment_fingerprint,
            policy_fingerprint=policy_hash,
        )
        if policy.approval_required:
            self._approvals.assert_approved(binding_for(scope, act, artifact, policy, policy_hash, environment_fingerprint, input_snapshot_id))
        self._guards.evaluate(guard_request)
        provider = scope.get_provider("executor.spark")
        if provider is None:
            raise WorkbenchError(WorkbenchErrorCode.PROVIDER_UNAVAILABLE, "no Spark executor is mounted in the target scope", {"scope": scope.id})
        idempotency_key = sha256_text(f"{session.id}:{execution_id}")[:64]
        append_data: dict[str, Any] = {
            "executionId": execution_id, "provider": "executor.spark", "action": act,
            "artifact": artifact_reference(artifact).model_dump(by_alias=True),
            "inputSnapshotId": input_snapshot_id, "policyFingerprint": policy_hash,
            "environmentFingerprint": environment_fingerprint, "idempotencyKey": idempotency_key,
        }
        if work_unit_id is not None:
            append_data["workUnitId"] = work_unit_id
        await session.append("execution/started", append_data)
        await session.flush()
        submit_time = time.monotonic() if policy.maximum_runtime_ms > 0 else None
        try:
            request = ExternalSubmitRequest(
                scope=scope, execution_id=execution_id, artifact=artifact,
                input_snapshot_id=input_snapshot_id, environment_fingerprint=environment_fingerprint,
                policy_fingerprint=policy_hash, idempotency_key=idempotency_key,
                signal=signal,
            )
            try:
                submitted = await asyncio.to_thread(provider.submit, request, signal=signal)
            except TypeError:
                submitted = await asyncio.to_thread(provider.submit, request)
            run_id = submitted["runId"]
        except Exception as e:
            await self._mark_unknown(session, execution_id, work_unit_id, None, "SUBMIT_FAILED")
            raise WorkbenchError(WorkbenchErrorCode.EXECUTION_UNKNOWN, "external submission outcome is unknown", {"cause": error_message(e)}) from e
        try:
            wait_for_terminal = getattr(provider, "wait_for_terminal", None)
            if callable(wait_for_terminal):
                status = await asyncio.to_thread(wait_for_terminal, run_id)
            else:
                status = await asyncio.to_thread(provider.status, run_id)
        except Exception as e:
            await self._mark_unknown(session, execution_id, work_unit_id, run_id, "STATUS_FAILED")
            raise WorkbenchError(WorkbenchErrorCode.EXECUTION_UNKNOWN, "external status outcome is unknown", {"cause": error_message(e)}) from e
        if status.state in ("SUBMITTING", "QUEUED", "RUNNING", "TERMINATING", "UNKNOWN"):
            if submit_time is not None:
                elapsed_ms = int((time.monotonic() - submit_time) * 1000)
                if elapsed_ms > policy.maximum_runtime_ms:
                    try:
                        await asyncio.to_thread(provider.cancel, run_id)
                    except Exception:
                        pass
                    timed_out_data: dict[str, Any] = {
                        "executionId": execution_id, "provider": "executor.spark",
                        "outcome": "TIMED_OUT", "runId": run_id,
                    }
                    if work_unit_id is not None:
                        timed_out_data["workUnitId"] = work_unit_id
                    if status.provider_message:
                        timed_out_data["providerMessage"] = redact_value(status.provider_message, key="providerMessage")
                    if status.result_state:
                        timed_out_data["stateMessage"] = redact_value(status.result_state, key="stateMessage")
                    await session.append("execution/completed", timed_out_data)
                    await session.flush()
                    return {"executionId": execution_id, "runId": run_id, "outcome": "TIMED_OUT", "outputArtifacts": []}
            unknown_data: dict[str, Any] = {
                "executionId": execution_id,
                "reason": "UNRESOLVED_EXTERNAL_OUTCOME",
                "runId": run_id,
                "provider": "executor.spark",
            }
            if work_unit_id is not None:
                unknown_data["workUnitId"] = work_unit_id
            if status.provider_message:
                unknown_data["providerMessage"] = redact_value(status.provider_message, key="providerMessage")
            if status.result_state:
                unknown_data["stateMessage"] = redact_value(status.result_state, key="stateMessage")
            if policy.cancellation_deadline:
                try:
                    await asyncio.to_thread(provider.cancel, run_id)
                    unknown_data["cancelled"] = True
                except Exception:
                    unknown_data["cancelled"] = False
            await session.append("execution/unknown", unknown_data)
            await session.flush()
            return {"executionId": execution_id, "runId": run_id, "outcome": "UNKNOWN", "outputArtifacts": []}
        outputs: list[ArtifactRecord] = []
        if status.state == "SUCCEEDED":
            try:
                outputs = await asyncio.to_thread(provider.output, run_id, scope)
            except Exception as e:
                await self._mark_unknown(session, execution_id, work_unit_id, run_id, "OUTPUT_VERIFICATION_FAILED")
                raise WorkbenchError(WorkbenchErrorCode.EXECUTION_UNKNOWN, "external output could not be verified", {"cause": error_message(e)}) from e
        outcome = _status_outcome(status)
        completed_data: dict[str, Any] = {
            "executionId": execution_id, "provider": "executor.spark", "outcome": outcome, "runId": run_id,
        }
        if work_unit_id is not None:
            completed_data["workUnitId"] = work_unit_id
        if outputs:
            completed_data["outputArtifacts"] = [artifact_reference(o).model_dump(by_alias=True) for o in outputs]
        if outcome != "SUCCEEDED":
            completed_data["errorCode"] = status.result_state or status.state
        if status.provider_message:
            completed_data["providerMessage"] = redact_value(status.provider_message, key="providerMessage")
        if status.result_state:
            completed_data["stateMessage"] = redact_value(status.result_state, key="stateMessage")
        await session.append("execution/completed", completed_data)
        return {"executionId": execution_id, "runId": run_id, "outcome": outcome, "outputArtifacts": outputs}

    async def cancel(self, *, session: MigrationSession, scope: ScopeRef, execution_id: str, run_id: str, work_unit_id: str | None = None) -> dict[str, bool]:
        scope = self._scopes.require_by_id(scope.id)
        provider = scope.get_provider("executor.spark")
        if provider is None:
            raise WorkbenchError(WorkbenchErrorCode.PROVIDER_UNAVAILABLE, "no Spark executor is mounted", {"scope": scope.id})
        return await asyncio.to_thread(provider.cancel, run_id)

    async def _mark_unknown(self, session: MigrationSession, execution_id: str, work_unit_id: str | None, run_id: str | None, error_code: str) -> None:
        data: dict[str, Any] = {"executionId": execution_id, "provider": "executor.spark", "outcome": "UNKNOWN", "errorCode": error_code}
        if work_unit_id is not None:
            data["workUnitId"] = work_unit_id
        if run_id is not None:
            data["runId"] = run_id
        await session.append("execution/completed", data)
