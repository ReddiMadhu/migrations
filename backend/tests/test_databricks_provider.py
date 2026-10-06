"""Unit tests for Databricks status handling without a live workspace."""

from __future__ import annotations

from types import SimpleNamespace

from sas2pyspark.providers.databricks import DatabricksSparkExecutor


class FakeJobs:
    def __init__(self, statuses):
        self.statuses = list(statuses)

    def get_run(self, *, run_id):
        assert int(run_id) == 42
        return self.statuses.pop(0)


class FakeClient:
    def __init__(self, statuses):
        self.jobs = FakeJobs(statuses)


def _response(lifecycle: str, result: str = ""):
    return SimpleNamespace(status=SimpleNamespace(
        life_cycle_state=lifecycle,
        result_state=result,
        state_message="",
    ))


def _provider(statuses, *, monotonic=lambda: 0.0, sleep=lambda _: None):
    return DatabricksSparkExecutor(
        workspace_client=FakeClient(statuses),
        job_id=7,
        artifact_uri=lambda _: "/Volumes/team/migration/code.py",
        input_uri=lambda _: "/Volumes/team/migration/input",
        output_materializer=lambda _response, _scope: [],
        poll_interval_seconds=1,
        max_wait_seconds=10,
        monotonic=monotonic,
        sleep=sleep,
    )


def test_wait_for_terminal_polls_running_job_to_success():
    now = [0.0]

    def monotonic():
        return now[0]

    def sleep(seconds):
        now[0] += seconds

    provider = _provider([_response("RUNNING"), _response("TERMINATED", "SUCCESS")], monotonic=monotonic, sleep=sleep)

    assert provider.wait_for_terminal("42").state == "SUCCEEDED"


def test_unknown_terminal_result_is_not_reported_as_failure_or_success():
    provider = _provider([_response("TERMINATED", "NEW_PROVIDER_RESULT")])

    assert provider.status("42").state == "UNKNOWN"


def test_success_with_failures_is_partial_success():
    provider = _provider([_response("TERMINATED", "SUCCESS_WITH_FAILURES")])

    assert provider.status("42").state == "FAILED"


def test_transient_poll_error_retries_with_same_run_id():
    class FlakyJobs(FakeJobs):
        def __init__(self):
            super().__init__([_response("TERMINATED", "SUCCESS")])
            self.calls = 0

        def get_run(self, *, run_id):
            self.calls += 1
            if self.calls == 1:
                raise ConnectionError("temporary network failure")
            return super().get_run(run_id=run_id)

    class FlakyClient:
        def __init__(self):
            self.jobs = FlakyJobs()

    provider = DatabricksSparkExecutor(
        workspace_client=FlakyClient(),
        job_id=7,
        artifact_uri=lambda _: "/Volumes/team/code.py",
        input_uri=lambda _: "/Volumes/team/input",
        output_materializer=lambda _response, _scope: [],
        max_attempts=2,
        sleep=lambda _: None,
    )

    assert provider.status("42").state == "SUCCEEDED"