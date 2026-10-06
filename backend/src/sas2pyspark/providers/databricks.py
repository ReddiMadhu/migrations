"""Databricks Jobs adapter; artifact transport stays an explicit injected boundary."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.services.artifacts import ArtifactRecord
from sas2pyspark.services.executors import (
    ExternalRunStatus,
    ExternalSubmitRequest,
)
from sas2pyspark.services.scopes import ScopeRef
from sas2pyspark.providers.secret_resolver import SecretResolver


@runtime_checkable
class DatabricksCredentialProvider(Protocol):
    """Credential provider protocol — production uses SDK default chain, dev uses static token."""

    def workspace_client(self, *, host: str) -> Any: ...


class DefaultCredentialChainProvider:
    """Production credential provider — uses the Databricks SDK default credential chain (OAuth M2M, etc.)."""

    def workspace_client(self, *, host: str) -> Any:
        from databricks.sdk import WorkspaceClient
        return WorkspaceClient(host=host)


class StaticTokenProvider:
    """Dev credential provider — uses an explicit static token resolved from the environment."""

    def __init__(self, secret_resolver: SecretResolver, credential_ref: str) -> None:
        self._secret_resolver = secret_resolver
        self._credential_ref = credential_ref

    def workspace_client(self, *, host: str) -> Any:
        from databricks.sdk import WorkspaceClient
        token = self._secret_resolver.resolve(self._credential_ref)
        return WorkspaceClient(host=host, token=token)


_LIFECYCLE_STATES = {
    "PENDING": "QUEUED",
    "QUEUED": "QUEUED",
    "RUNNING": "RUNNING",
    "TERMINATING": "TERMINATING",
    "TERMINATED": "FAILED",
    "SKIPPED": "FAILED",
    "INTERNAL_ERROR": "FAILED",
    "BLOCKED": "FAILED",
}
_RESULT_STATES = {
    "SUCCESS": "SUCCEEDED",
    "SUCCESS_WITH_FAILURES": "FAILED",
    "FAILED": "FAILED",
    "TIMEDOUT": "TIMED_OUT",
    "CANCELED": "CANCELED",
    "CANCELLED": "CANCELED",
}


def _enum_text(value: Any) -> str:
    raw = getattr(value, "value", value)
    return str(raw or "").upper()


class DatabricksSparkExecutor:
    """One-time Lakeflow Jobs adapter using a configured, reviewed job ID.

    `artifact_uri` and `input_uri` must return governed remote locations; this adapter
    never uploads local artifact-store paths or streams datasets through the API.
    `output_materializer` verifies a remote manifest and materializes its outputs.
    """

    def __init__(
        self,
        *,
        workspace_client: Any,
        job_id: int,
        artifact_uri: Callable[[ArtifactRecord], str],
        input_uri: Callable[[str], str],
        output_materializer: Callable[[Any, ScopeRef], list[ArtifactRecord]],
        poll_interval_seconds: float = 3.0,
        max_wait_seconds: float = 900.0,
        max_attempts: int = 3,
        retry_base_seconds: float = 0.5,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if job_id <= 0:
            raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, "Databricks job ID must be positive")
        if poll_interval_seconds <= 0 or max_wait_seconds <= 0:
            raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, "Databricks polling bounds must be positive")
        self._client = workspace_client
        self._job_id = job_id
        self._artifact_uri = artifact_uri
        self._input_uri = input_uri
        self._output_materializer = output_materializer
        self._poll_interval_seconds = poll_interval_seconds
        self._max_wait_seconds = max_wait_seconds
        self._max_attempts = max(1, max_attempts)
        self._retry_base_seconds = max(0.0, retry_base_seconds)
        self._sleep = sleep
        self._monotonic = monotonic

    @classmethod
    def from_secret_resolver(
        cls,
        *,
        secret_resolver: SecretResolver,
        credential_ref: str,
        host: str,
        job_id: int,
        artifact_uri: Callable[[ArtifactRecord], str],
        input_uri: Callable[[str], str],
        output_materializer: Callable[[Any, ScopeRef], list[ArtifactRecord]],
        poll_interval_seconds: float = 3.0,
        max_wait_seconds: float = 900.0,
        max_attempts: int = 3,
        retry_base_seconds: float = 0.5,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        credential_provider: DatabricksCredentialProvider | None = None,
    ) -> "DatabricksSparkExecutor":
        provider = credential_provider or StaticTokenProvider(secret_resolver, credential_ref)
        try:
            client = provider.workspace_client(host=host)
        except Exception as exc:
            raise WorkbenchError(
                WorkbenchErrorCode.AUTHENTICATION_ERROR,
                "Databricks authentication failed",
                {"host": host},
                cause=exc,
            ) from exc
        return cls(
            workspace_client=client,
            job_id=job_id,
            artifact_uri=artifact_uri,
            input_uri=input_uri,
            output_materializer=output_materializer,
            poll_interval_seconds=poll_interval_seconds,
            max_wait_seconds=max_wait_seconds,
            max_attempts=max_attempts,
            retry_base_seconds=retry_base_seconds,
            sleep=sleep,
            monotonic=monotonic,
        )

    def submit(self, request: ExternalSubmitRequest) -> dict[str, str]:
        code_uri = self._artifact_uri(request.artifact)
        input_uri = self._input_uri(request.input_snapshot_id)
        if not code_uri.startswith("/Volumes/") or not input_uri.startswith("/Volumes/"):
            raise WorkbenchError(
                WorkbenchErrorCode.POLICY_DENIED,
                "Databricks code and input must use governed Unity Catalog Volume paths",
            )
        result = self._retry(lambda: self._client.jobs.run_now(
            job_id=self._job_id,
            idempotency_token=request.idempotency_key,
            job_parameters={
                "candidate_uri": code_uri,
                "candidate_digest": request.artifact.source_hash,
                "input_uri": input_uri,
                "input_snapshot_id": request.input_snapshot_id,
                "execution_id": request.execution_id,
            },
        ))
        run_id = str(getattr(result, "run_id", "") or "")
        if not run_id:
            raise WorkbenchError(
                WorkbenchErrorCode.EXECUTION_UNKNOWN,
                "Databricks accepted submission without a usable run ID",
            )
        return {"runId": run_id}

    def status(self, run_id: str) -> ExternalRunStatus:
        try:
            response = self._retry(lambda: self._client.jobs.get_run(run_id=int(run_id)))
        except (TypeError, ValueError):
            response = self._retry(lambda: self._client.jobs.get_run(run_id=run_id))
        status = getattr(response, "status", None)
        lifecycle = _enum_text(getattr(status, "life_cycle_state", ""))
        result_state = _enum_text(getattr(status, "result_state", ""))
        message = getattr(status, "state_message", None)

        if lifecycle in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            normalized = _RESULT_STATES.get(result_state, "UNKNOWN")
        else:
            normalized = _LIFECYCLE_STATES.get(lifecycle, "UNKNOWN")
        return ExternalRunStatus(
            state=normalized,
            run_id=run_id,
            provider_message=str(message) if message else None,
            result_state=result_state or lifecycle or "UNKNOWN",
        )

    def wait_for_terminal(self, run_id: str) -> ExternalRunStatus:
        deadline = self._monotonic() + self._max_wait_seconds
        last_status = self.status(run_id)
        while last_status.state in ("QUEUED", "RUNNING", "TERMINATING"):
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                return ExternalRunStatus(
                    state="UNKNOWN",
                    run_id=run_id,
                    provider_message="Databricks run did not reach a terminal state before the polling deadline",
                    result_state=last_status.result_state,
                )
            self._sleep(min(self._poll_interval_seconds, remaining))
            last_status = self.status(run_id)
        return last_status

    def output(self, run_id: str, scope: ScopeRef) -> list[ArtifactRecord]:
        try:
            response = self._retry(lambda: self._client.jobs.get_run(run_id=int(run_id)))
        except (TypeError, ValueError):
            response = self._retry(lambda: self._client.jobs.get_run(run_id=run_id))
        status = getattr(response, "status", None)
        result_state = _enum_text(getattr(status, "result_state", ""))
        if result_state != "SUCCESS":
            raise WorkbenchError(
                WorkbenchErrorCode.EXECUTION_UNKNOWN,
                "Databricks output is unavailable because the run did not succeed cleanly",
                {"runId": run_id, "resultState": result_state or "UNKNOWN"},
            )
        return self._output_materializer(response, scope)

    def cancel(self, run_id: str) -> dict[str, bool]:
        try:
            self._retry(lambda: self._client.jobs.cancel_run(run_id=int(run_id)))
        except (TypeError, ValueError):
            self._retry(lambda: self._client.jobs.cancel_run(run_id=run_id))
        return {"accepted": True}

    def get_driver_logs(self, run_id: str, *, max_bytes: int = 65536, max_lines: int = 500) -> str:
        try:
            try:
                response = self._retry(lambda: self._client.jobs.get_run_output(run_id=int(run_id)))
            except (TypeError, ValueError):
                response = self._retry(lambda: self._client.jobs.get_run_output(run_id=run_id))
            logs = getattr(response, "logs", "") or ""
        except Exception:
            return ""
        if isinstance(logs, str):
            text = logs
        else:
            text = "\n".join(str(line) for line in logs)
        lines = text.splitlines()
        if len(lines) > max_lines:
            lines = lines[-max_lines:]
        result = "\n".join(lines)
        if len(result.encode("utf-8")) > max_bytes:
            result = result[-max_bytes:]
        return result

    def _retry(self, operation: Callable[[], Any]) -> Any:
        import random

        for attempt in range(self._max_attempts):
            try:
                return operation()
            except Exception as exc:
                status_code = getattr(exc, "status_code", None)
                retryable = (
                    status_code == 429
                    or (isinstance(status_code, int) and 500 <= status_code < 600)
                    or isinstance(exc, (TimeoutError, ConnectionError, OSError))
                )
                if not retryable or attempt + 1 >= self._max_attempts:
                    raise
                backoff = self._retry_base_seconds * (2 ** attempt)
                retry_after = self._parse_retry_after(exc)
                wait_seconds = max(backoff, retry_after)
                jitter = wait_seconds * 0.25 * (random.random() * 2 - 1)
                self._sleep(max(0.0, wait_seconds + jitter))
        raise RuntimeError("Databricks retry loop exhausted without a response")

    @staticmethod
    def _parse_retry_after(exc: Exception) -> float:
        headers = getattr(exc, "headers", None)
        if not headers:
            return 0.0
        for key in ("Retry-After", "retry-after", "RetryAfter"):
            value = headers.get(key) if isinstance(headers, dict) else getattr(headers, key, None)
            if value is not None:
                try:
                    return max(0.0, float(value))
                except (TypeError, ValueError):
                    pass
        return 0.0