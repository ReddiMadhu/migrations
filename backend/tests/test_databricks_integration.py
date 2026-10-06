"""Gated Databricks integration test placeholder for configured workspace runs."""

from __future__ import annotations

import os

import pytest


pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    not os.environ.get("DATABRICKS_HOST") or not os.environ.get("DATABRICKS_TOKEN"),
    reason="requires a configured Databricks workspace and reviewed integration job",
)
def test_databricks_end_to_end_job_is_configured():
    """The actual workbook/run assertion requires workspace-specific governed paths."""
    pytest.skip("provide a workspace-specific job ID and test workbook before enabling E2E")