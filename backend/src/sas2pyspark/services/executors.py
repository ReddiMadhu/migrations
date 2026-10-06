"""Dummy executors — deterministic in-process providers for tests and local proofs."""

from __future__ import annotations

import asyncio
from typing import Any, Callable, Protocol

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.core.events import ExternalRunState
from sas2pyspark.services.artifacts import ArtifactRecord, FileSystemArtifactService
from sas2pyspark.services.scopes import ScopeRef


class ExternalSubmitRequest:
    __slots__ = ("scope", "execution_id", "artifact", "input_snapshot_id", "environment_fingerprint", "policy_fingerprint", "idempotency_key", "signal")

    def __init__(
        self, *,
        scope: ScopeRef, execution_id: str, artifact: ArtifactRecord,
        input_snapshot_id: str, environment_fingerprint: str, policy_fingerprint: str, idempotency_key: str,
        signal: asyncio.Event | None = None,
    ) -> None:
        self.scope = scope
        self.execution_id = execution_id
        self.artifact = artifact
        self.input_snapshot_id = input_snapshot_id
        self.environment_fingerprint = environment_fingerprint
        self.policy_fingerprint = policy_fingerprint
        self.idempotency_key = idempotency_key
        self.signal = signal


class ExternalRunStatus:
    __slots__ = ("state", "run_id", "provider_message", "result_state")

    def __init__(self, *, state: ExternalRunState, run_id: str, provider_message: str | None = None, result_state: str | None = None) -> None:
        self.state = state
        self.run_id = run_id
        self.provider_message = provider_message
        self.result_state = result_state


class SparkExecutor(Protocol):
    def submit(self, request: ExternalSubmitRequest, *, signal: asyncio.Event | None = None) -> dict[str, str]: ...
    def status(self, run_id: str) -> ExternalRunStatus: ...
    def output(self, run_id: str, scope: ScopeRef) -> list[ArtifactRecord]: ...
    def cancel(self, run_id: str) -> dict[str, bool]: ...
    def get_driver_logs(self, run_id: str, *, max_bytes: int = 65536, max_lines: int = 500) -> str: ...


class DummySparkExecutor:
    def __init__(self, artifacts: FileSystemArtifactService, *, mode: str = "success", output_fn: Callable[[ExternalSubmitRequest], str] | None = None) -> None:
        self._artifacts = artifacts
        self._mode = mode
        self._output_fn = output_fn
        self._requests: dict[str, ExternalSubmitRequest] = {}
        self._signals: dict[str, asyncio.Event | None] = {}

    @property
    def mode(self) -> str:
        return self._mode

    def submit(self, request: ExternalSubmitRequest, *, signal: asyncio.Event | None = None) -> dict[str, str]:
        run_id = f"dummy-{request.execution_id}"
        self._requests[run_id] = request
        self._signals[run_id] = signal
        return {"runId": run_id}

    def status(self, run_id: str) -> ExternalRunStatus:
        signal = self._signals.get(run_id)
        if signal is not None and signal.is_set():
            return ExternalRunStatus(state="CANCELED", run_id=run_id, result_state="CANCELED_BY_SIGNAL")
        if self._mode == "unknown":
            return ExternalRunStatus(state="UNKNOWN", run_id=run_id, result_state="PROVIDER_UNKNOWN")
        if self._mode == "failure":
            return ExternalRunStatus(state="FAILED", run_id=run_id, result_state="DUMMY_FAILURE")
        return ExternalRunStatus(state="SUCCEEDED", run_id=run_id, result_state="SUCCESS")

    def output(self, run_id: str, scope: ScopeRef) -> list[ArtifactRecord]:
        if self._mode == "unknown":
            raise WorkbenchError(WorkbenchErrorCode.EXECUTION_UNKNOWN, "dummy provider has an unknown outcome", {"runId": run_id})
        if self._mode == "failure":
            raise WorkbenchError(WorkbenchErrorCode.EXECUTION_UNKNOWN, "dummy provider failed", {"runId": run_id})
        request = self._requests.get(run_id)
        data = self._output_fn(request) if request and self._output_fn else f"run={run_id}\nstatus=SUCCESS\n"
        artifact = self._artifacts.create(
            scope=scope, data=data, media_type="text/plain",
            created_by="dummy-spark-executor", label="dummy-output",
        )
        return [artifact]

    def cancel(self, run_id: str) -> dict[str, bool]:
        return {"accepted": True}

    def get_driver_logs(self, run_id: str, *, max_bytes: int = 65536, max_lines: int = 500) -> str:
        return ""


class ReferenceCsvSasExecutor:
    def __init__(self, artifacts: FileSystemArtifactService) -> None:
        self._artifacts = artifacts

    def execute(self, *, scope: ScopeRef, execution_id: str, source: ArtifactRecord, input_snapshot_id: str, reference: bytes | str) -> dict[str, Any]:
        output = self._artifacts.create(
            scope=scope, data=reference, media_type="text/csv",
            created_by="reference-csv-sas-executor", label="reference-output",
        )
        return {"runId": f"reference-{execution_id}", "output": output}
