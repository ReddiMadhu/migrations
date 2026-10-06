"""P1-3: Bounded Driver Logs + Failure Taxonomy."""

from __future__ import annotations

import pytest

from sas2pyspark.providers.failure_taxonomy import FailureTaxonomy, classify_failure
from sas2pyspark.services.executors import DummySparkExecutor, SparkExecutor


def test_failure_taxonomy_driver_oom():
    class MockStatus:
        provider_message = "java.lang.OutOfMemoryError: Java heap space"
        result_state = "FAILED"
        state = "FAILED"
    assert classify_failure(status=MockStatus()) == FailureTaxonomy.DRIVER_OOM


def test_failure_taxonomy_executor_oom():
    class MockStatus:
        provider_message = "Executor lost: container killed by YARN for exceeding memory limits"
        result_state = "FAILED"
        state = "FAILED"
    assert classify_failure(status=MockStatus()) == FailureTaxonomy.EXECUTOR_OOM


def test_failure_taxonomy_timeout():
    class MockStatus:
        provider_message = "Job timed out after 900 seconds"
        result_state = "TIMEDOUT"
        state = "TIMED_OUT"
    assert classify_failure(status=MockStatus()) == FailureTaxonomy.TIMEOUT


def test_failure_taxonomy_preemption():
    class MockStatus:
        provider_message = "Task preempted by scheduler"
        result_state = "FAILED"
        state = "FAILED"
    assert classify_failure(status=MockStatus()) == FailureTaxonomy.PREEMPTION


def test_failure_taxonomy_auth_error():
    class MockStatus:
        provider_message = "Unauthorized: invalid token"
        result_state = "FAILED"
        state = "FAILED"
    assert classify_failure(status=MockStatus()) == FailureTaxonomy.AUTHENTICATION_ERROR


def test_failure_taxonomy_transient():
    class MockStatus:
        provider_message = "Service unavailable, please retry"
        result_state = "INTERNAL_ERROR"
        state = "FAILED"
    assert classify_failure(status=MockStatus()) == FailureTaxonomy.TRANSIENT_INFRASTRUCTURE_ERROR


def test_failure_taxonomy_dependency():
    class MockStatus:
        provider_message = "ModuleNotFoundError: No module named 'foo'"
        result_state = "FAILED"
        state = "FAILED"
    assert classify_failure(status=MockStatus()) == FailureTaxonomy.DEPENDENCY


def test_failure_taxonomy_user_code():
    class MockStatus:
        provider_message = "KeyError: 'missing_column'"
        result_state = "FAILED"
        state = "FAILED"
    assert classify_failure(status=MockStatus()) == FailureTaxonomy.USER_CODE


def test_failure_taxonomy_unrecognized_unknown():
    class MockStatus:
        provider_message = "something completely weird happened"
        result_state = "WEIRD"
        state = "WEIRD"
    assert classify_failure(status=MockStatus()) == FailureTaxonomy.UNKNOWN


def test_failure_taxonomy_empty_status():
    assert classify_failure(status=None, error=None) == FailureTaxonomy.UNKNOWN


def test_failure_taxonomy_all_categories_reachable():
    expected = {
        FailureTaxonomy.AUTHENTICATION_ERROR,
        FailureTaxonomy.TRANSIENT_INFRASTRUCTURE_ERROR,
        FailureTaxonomy.DRIVER_OOM,
        FailureTaxonomy.EXECUTOR_OOM,
        FailureTaxonomy.PREEMPTION,
        FailureTaxonomy.TIMEOUT,
        FailureTaxonomy.USER_CODE,
        FailureTaxonomy.DEPENDENCY,
        FailureTaxonomy.UNKNOWN,
    }
    assert set(FailureTaxonomy) == expected


def test_failure_taxonomy_from_exception():
    assert classify_failure(error=TimeoutError("operation timed out")) == FailureTaxonomy.TIMEOUT
    assert classify_failure(error=PermissionError("access denied")) == FailureTaxonomy.AUTHENTICATION_ERROR


def test_dummy_executor_has_get_driver_logs():
    from unittest.mock import MagicMock
    dummy = DummySparkExecutor(MagicMock())
    assert hasattr(dummy, "get_driver_logs")
    result = dummy.get_driver_logs("run-1")
    assert result == ""


def test_dummy_executor_get_driver_logs_respects_bounds():
    from unittest.mock import MagicMock
    dummy = DummySparkExecutor(MagicMock())
    result = dummy.get_driver_logs("run-1", max_bytes=100, max_lines=10)
    assert len(result.encode("utf-8")) <= 100


def test_databricks_executor_has_get_driver_logs():
    from sas2pyspark.providers.databricks import DatabricksSparkExecutor
    assert hasattr(DatabricksSparkExecutor, "get_driver_logs")


def test_spark_executor_protocol_has_get_driver_logs():
    assert hasattr(SparkExecutor, "get_driver_logs")


def test_policy_has_log_bounds():
    from sas2pyspark.services.policy import DEFAULT_EXECUTION_POLICY
    assert DEFAULT_EXECUTION_POLICY.max_log_bytes == 65536
    assert DEFAULT_EXECUTION_POLICY.max_log_lines == 500


def test_databricks_get_driver_logs_bounded():
    from unittest.mock import MagicMock
    from sas2pyspark.providers.databricks import DatabricksSparkExecutor

    mock_client = MagicMock()
    mock_output = MagicMock()
    mock_output.logs = "\n".join(f"line {i}" for i in range(1000))
    mock_client.jobs.get_run_output.return_value = mock_output

    executor = DatabricksSparkExecutor(
        workspace_client=mock_client,
        job_id=1,
        artifact_uri=lambda a: "/Volumes/test/vol/a",
        input_uri=lambda s: "/Volumes/test/vol/i",
        output_materializer=lambda r, s: [],
    )
    result = executor.get_driver_logs("123", max_bytes=1000, max_lines=10)
    lines = result.splitlines()
    assert len(lines) <= 10
    assert len(result.encode("utf-8")) <= 1000


def test_databricks_get_driver_logs_empty_on_error():
    from unittest.mock import MagicMock
    from sas2pyspark.providers.databricks import DatabricksSparkExecutor

    mock_client = MagicMock()
    mock_client.jobs.get_run_output.side_effect = Exception("not found")

    executor = DatabricksSparkExecutor(
        workspace_client=mock_client,
        job_id=1,
        artifact_uri=lambda a: "/Volumes/test/vol/a",
        input_uri=lambda s: "/Volumes/test/vol/i",
        output_materializer=lambda r, s: [],
    )
    result = executor.get_driver_logs("123")
    assert result == ""
