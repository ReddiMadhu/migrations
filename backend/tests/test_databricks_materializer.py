"""Tests for Databricks output materializer — UC Volume download into artifact store."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
import pytest_asyncio

from sas2pyspark.providers.databricks_materializer import make_output_materializer
from sas2pyspark.services.clock import FixedClock
from sas2pyspark.workbench import WorkbenchConfig, create_workbench


class MockDirectoryEntry:
    def __init__(self, path: str, is_directory: bool = False) -> None:
        self.path = path
        self.is_directory = is_directory


class MockDownloadResponse:
    def __init__(self, contents: bytes) -> None:
        self.contents = io.BytesIO(contents)


import io


@pytest.fixture
async def runtime(test_dir: Path, fixed_clock: FixedClock):
    rt = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
    yield rt
    await rt.dispose()


class TestOutputMaterializer:
    @pytest.mark.asyncio
    async def test_materialize_single_csv(self, runtime: Any, test_dir: Path):
        from .conftest import make_scopes
        tenant, migration, attempt, candidate = make_scopes(runtime)

        client = MagicMock()
        client.files.list_directory_contents.return_value = [
            MockDirectoryEntry("/Volumes/main/migration/artifacts/outputs/run-1/result.csv"),
        ]
        client.files.download.return_value = MockDownloadResponse(b"col1,col2\n1,2\n3,4\n")

        materializer = make_output_materializer(
            workspace_client=client,
            artifacts=runtime.artifacts,
            output_volume_path="/Volumes/main/migration/artifacts/outputs",
        )

        run_response = MagicMock()
        run_response.run_id = "run-1"

        artifacts = materializer(run_response, migration)
        assert len(artifacts) == 1
        assert artifacts[0].media_type == "text/csv"
        assert artifacts[0].size > 0

    @pytest.mark.asyncio
    async def test_materialize_multiple_files(self, runtime: Any, test_dir: Path):
        from .conftest import make_scopes
        tenant, migration, attempt, candidate = make_scopes(runtime)

        client = MagicMock()
        client.files.list_directory_contents.return_value = [
            MockDirectoryEntry("/Volumes/main/migration/artifacts/outputs/run-1/part1.csv"),
            MockDirectoryEntry("/Volumes/main/migration/artifacts/outputs/run-1/part2.json"),
        ]
        client.files.download.side_effect = [
            MockDownloadResponse(b"a,b\n1,2\n"),
            MockDownloadResponse(b'{"x": 1}'),
        ]

        materializer = make_output_materializer(
            workspace_client=client,
            artifacts=runtime.artifacts,
            output_volume_path="/Volumes/main/migration/artifacts/outputs",
        )

        run_response = MagicMock()
        run_response.run_id = "run-1"

        artifacts = materializer(run_response, migration)
        assert len(artifacts) == 2
        assert artifacts[0].media_type == "text/csv"
        assert artifacts[1].media_type == "application/json"

    @pytest.mark.asyncio
    async def test_materialize_skips_directories(self, runtime: Any, test_dir: Path):
        from .conftest import make_scopes
        tenant, migration, attempt, candidate = make_scopes(runtime)

        client = MagicMock()
        client.files.list_directory_contents.return_value = [
            MockDirectoryEntry("/Volumes/main/migration/artifacts/outputs/run-1/_SUCCESS", is_directory=True),
            MockDirectoryEntry("/Volumes/main/migration/artifacts/outputs/run-1/data.csv"),
        ]
        client.files.download.return_value = MockDownloadResponse(b"x\n1\n")

        materializer = make_output_materializer(
            workspace_client=client,
            artifacts=runtime.artifacts,
            output_volume_path="/Volumes/main/migration/artifacts/outputs",
        )

        run_response = MagicMock()
        run_response.run_id = "run-1"

        artifacts = materializer(run_response, migration)
        assert len(artifacts) == 1

    @pytest.mark.asyncio
    async def test_materialize_no_files_raises(self, runtime: Any, test_dir: Path):
        from .conftest import make_scopes
        tenant, migration, attempt, candidate = make_scopes(runtime)

        client = MagicMock()
        client.files.list_directory_contents.return_value = []

        materializer = make_output_materializer(
            workspace_client=client,
            artifacts=runtime.artifacts,
            output_volume_path="/Volumes/main/migration/artifacts/outputs",
        )

        run_response = MagicMock()
        run_response.run_id = "run-1"

        from sas2pyspark.core.errors import WorkbenchError
        with pytest.raises(WorkbenchError):
            materializer(run_response, migration)

    @pytest.mark.asyncio
    async def test_materialize_content_addressed(self, runtime: Any, test_dir: Path):
        from .conftest import make_scopes
        tenant, migration, attempt, candidate = make_scopes(runtime)

        client = MagicMock()
        client.files.list_directory_contents.return_value = [
            MockDirectoryEntry("/vol/run-1/out.csv"),
        ]
        client.files.download.return_value = MockDownloadResponse(b"same,content\n")

        materializer = make_output_materializer(
            workspace_client=client,
            artifacts=runtime.artifacts,
            output_volume_path="/vol",
        )

        run_response = MagicMock()
        run_response.run_id = "run-1"

        artifacts1 = materializer(run_response, migration)
        client.files.list_directory_contents.return_value = [
            MockDirectoryEntry("/vol/run-1/out.csv"),
        ]
        client.files.download.return_value = MockDownloadResponse(b"same,content\n")
        artifacts2 = materializer(run_response, migration)

        assert artifacts1[0].source_hash == artifacts2[0].source_hash
