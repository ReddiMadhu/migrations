"""Credential reference resolution; secret values must not enter evidence or prompts."""

from __future__ import annotations

import os
from typing import Protocol

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode


class SecretResolver(Protocol):
    def resolve(self, reference: str) -> str: ...


class EnvSecretResolver:
    def resolve(self, reference: str) -> str:
        value = os.environ.get(reference)
        if not value:
            raise WorkbenchError(
                WorkbenchErrorCode.PROVIDER_UNAVAILABLE,
                "Databricks credential reference is not configured",
                {"credentialRef": reference},
            )
        return value