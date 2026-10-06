"""Databricks output materializer — downloads run outputs from UC Volumes into the artifact store."""

from __future__ import annotations

import io
import json
import os
from collections.abc import Callable
from typing import Any

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.services.artifacts import ArtifactRecord, FileSystemArtifactService
from sas2pyspark.services.scopes import ScopeRef


def make_output_materializer(
    *,
    workspace_client: Any,
    artifacts: FileSystemArtifactService,
    output_volume_path: str,
    media_type: str = "text/csv",
) -> Callable[[Any, ScopeRef], list[ArtifactRecord]]:
    """Create an output_materializer callable for DatabricksSparkExecutor.

    The materializer:
    1. Lists files in the UC Volume output directory (output_volume_path/{execution_id}/)
    2. Downloads each file via the Databricks SDK files API
    3. Creates content-addressed ArtifactRecords in the artifact store
    4. Returns the list of artifacts for reconciliation
    """
    client = workspace_client

    def materialize(response: Any, scope: ScopeRef) -> list[ArtifactRecord]:
        run_id = str(getattr(response, "run_id", "") or "")
        output_dir = f"{output_volume_path.rstrip('/')}/{run_id}"

        try:
            entries = list(client.files.list_directory_contents(output_dir))
        except Exception:
            try:
                entries = list(client.files.list_directory_contents(output_volume_path.rstrip("/")))
            except Exception as exc:
                raise WorkbenchError(
                    WorkbenchErrorCode.EXECUTION_UNKNOWN,
                    "Failed to list Databricks output directory",
                    {"outputDir": output_dir, "error": str(exc)},
                ) from exc

        if not entries:
            raise WorkbenchError(
                WorkbenchErrorCode.EXECUTION_UNKNOWN,
                "Databricks run produced no output files",
                {"outputDir": output_dir, "runId": run_id},
            )

        created: list[ArtifactRecord] = []
        for entry in entries:
            file_path = getattr(entry, "path", None) or str(entry)
            if getattr(entry, "is_directory", False):
                continue

            try:
                download_response = client.files.download(file_path)
                contents = download_response.contents
                if hasattr(contents, "read"):
                    data_bytes = contents.read()
                elif isinstance(contents, bytes):
                    data_bytes = contents
                else:
                    data_bytes = bytes(contents)
            except Exception as exc:
                raise WorkbenchError(
                    WorkbenchErrorCode.EXECUTION_UNKNOWN,
                    "Failed to download Databricks output file",
                    {"filePath": file_path, "error": str(exc)},
                ) from exc

            try:
                data_str = data_bytes.decode("utf-8")
            except UnicodeDecodeError:
                data_str = data_bytes.decode("latin-1")

            file_media_type = media_type
            if file_path.endswith(".json"):
                file_media_type = "application/json"
            elif file_path.endswith(".csv"):
                file_media_type = "text/csv"
            elif file_path.endswith(".parquet"):
                file_media_type = "application/x-parquet"

            label = os.path.basename(file_path)
            artifact = artifacts.create(
                scope=scope,
                data=data_str,
                media_type=file_media_type,
                created_by="databricks-output-materializer",
                label=label,
            )
            created.append(artifact)

        return created

    return materialize
