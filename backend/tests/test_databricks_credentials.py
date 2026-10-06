"""P1-1: Databricks Credential Chain — non-static auth in production."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.providers.databricks import (
    DatabricksCredentialProvider,
    DefaultCredentialChainProvider,
    StaticTokenProvider,
)
from sas2pyspark.providers.secret_resolver import EnvSecretResolver


def test_default_chain_provider_satisfies_protocol():
    assert isinstance(DefaultCredentialChainProvider(), DatabricksCredentialProvider)


def test_static_token_provider_satisfies_protocol():
    resolver = EnvSecretResolver()
    assert isinstance(StaticTokenProvider(resolver, "DATABRICKS_TOKEN"), DatabricksCredentialProvider)


def test_default_chain_provider_no_static_token():
    with patch("databricks.sdk.WorkspaceClient") as mock_wc:
        provider = DefaultCredentialChainProvider()
        provider.workspace_client(host="https://test.databricks.com")
        call_kwargs = mock_wc.call_args.kwargs
        assert "token" not in call_kwargs
        assert call_kwargs["host"] == "https://test.databricks.com"


def test_static_token_provider_dev():
    with patch.dict("os.environ", {"DATABRICKS_TOKEN": "test-token-value"}):
        with patch("databricks.sdk.WorkspaceClient") as mock_wc:
            resolver = EnvSecretResolver()
            provider = StaticTokenProvider(resolver, "DATABRICKS_TOKEN")
            provider.workspace_client(host="https://test.databricks.com")
            call_kwargs = mock_wc.call_args.kwargs
            assert call_kwargs["token"] == "test-token-value"
            assert call_kwargs["host"] == "https://test.databricks.com"


def test_auth_failure_raises_authentication_error():
    from sas2pyspark.providers.databricks import DatabricksSparkExecutor

    class FailingProvider:
        def workspace_client(self, *, host: str):
            raise PermissionError("invalid credentials")

    with pytest.raises(WorkbenchError) as exc_info:
        DatabricksSparkExecutor.from_secret_resolver(
            secret_resolver=EnvSecretResolver(),
            credential_ref="DATABRICKS_TOKEN",
            host="https://test.databricks.com",
            job_id=1,
            artifact_uri=lambda a: "/Volumes/test/vol/artifact",
            input_uri=lambda s: "/Volumes/test/vol/input",
            output_materializer=lambda r, s: [],
            credential_provider=FailingProvider(),
        )
    assert exc_info.value.code == WorkbenchErrorCode.AUTHENTICATION_ERROR


def test_no_credential_in_error_details():
    from sas2pyspark.providers.databricks import DatabricksSparkExecutor

    class FailingProvider:
        def workspace_client(self, *, host: str):
            raise PermissionError("token=secret-value is invalid")

    with pytest.raises(WorkbenchError) as exc_info:
        DatabricksSparkExecutor.from_secret_resolver(
            secret_resolver=EnvSecretResolver(),
            credential_ref="DATABRICKS_TOKEN",
            host="https://test.databricks.com",
            job_id=1,
            artifact_uri=lambda a: "/Volumes/test/vol/artifact",
            input_uri=lambda s: "/Volumes/test/vol/input",
            output_materializer=lambda r, s: [],
            credential_provider=FailingProvider(),
        )
    assert "token" not in str(exc_info.value.details)
    assert "secret-value" not in str(exc_info.value.details)


def test_from_secret_resolver_uses_credential_provider():
    from sas2pyspark.providers.databricks import DatabricksSparkExecutor

    mock_client = MagicMock()
    mock_provider = MagicMock()
    mock_provider.workspace_client.return_value = mock_client

    executor = DatabricksSparkExecutor.from_secret_resolver(
        secret_resolver=EnvSecretResolver(),
        credential_ref="DATABRICKS_TOKEN",
        host="https://test.databricks.com",
        job_id=1,
        artifact_uri=lambda a: "/Volumes/test/vol/artifact",
        input_uri=lambda s: "/Volumes/test/vol/input",
        output_materializer=lambda r, s: [],
        credential_provider=mock_provider,
    )
    mock_provider.workspace_client.assert_called_once_with(host="https://test.databricks.com")
    assert executor._client is mock_client
