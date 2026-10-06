"""Artifact tests — content addressing, dedup, path escape, metadata verify."""

from __future__ import annotations

import pytest

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.services.artifacts import artifact_reference

from .conftest import make_scopes


async def test_same_bytes_produce_same_digest(workbench):
    _, _, _, candidate = make_scopes(workbench)
    a1 = workbench.artifacts.create(scope=candidate, data=b"hello world", media_type="text/plain", created_by="test")
    a2 = workbench.artifacts.create(scope=candidate, data=b"hello world", media_type="text/plain", created_by="test")
    assert a1.source_hash == a2.source_hash
    assert a1.artifact_id == a2.artifact_id


async def test_different_bytes_produce_different_digest(workbench):
    _, _, _, candidate = make_scopes(workbench)
    a1 = workbench.artifacts.create(scope=candidate, data=b"hello", media_type="text/plain", created_by="test")
    a2 = workbench.artifacts.create(scope=candidate, data=b"world", media_type="text/plain", created_by="test")
    assert a1.source_hash != a2.source_hash


async def test_dedup_returns_existing_on_same_content(workbench):
    _, _, _, candidate = make_scopes(workbench)
    a1 = workbench.artifacts.create(scope=candidate, data="same content", media_type="text/plain", created_by="test")
    a2 = workbench.artifacts.create(scope=candidate, data="same content", media_type="text/plain", created_by="test")
    assert a1.artifact_id == a2.artifact_id
    assert a1.size == a2.size


async def test_artifact_read_returns_original_bytes(workbench):
    _, _, _, candidate = make_scopes(workbench)
    record = workbench.artifacts.create(scope=candidate, data=b"original bytes", media_type="text/plain", created_by="test")
    data = workbench.artifacts.read(candidate, record)
    assert data == b"original bytes"


async def test_artifact_read_text_returns_original_string(workbench):
    _, _, _, candidate = make_scopes(workbench)
    record = workbench.artifacts.create(scope=candidate, data="text content", media_type="text/plain", created_by="test")
    text = workbench.artifacts.read_text(candidate, record)
    assert text == "text content"


async def test_artifact_reference_preserves_identity(workbench):
    _, _, _, candidate = make_scopes(workbench)
    record = workbench.artifacts.create(scope=candidate, data=b"ref test", media_type="application/json", created_by="test")
    ref = artifact_reference(record)
    assert ref.artifact_id == record.artifact_id
    assert ref.digest == record.source_hash
    assert ref.media_type == record.media_type
    assert ref.size == record.size


async def test_artifact_get_verifies_scope(workbench):
    _, _, _, candidate = make_scopes(workbench)
    record = workbench.artifacts.create(scope=candidate, data=b"scope test", media_type="text/plain", created_by="test")
    verified = workbench.artifacts.get(candidate, record)
    assert verified.source_hash == record.source_hash


async def test_artifact_media_type_required(workbench):
    _, _, _, candidate = make_scopes(workbench)
    with pytest.raises(WorkbenchError) as exc_info:
        workbench.artifacts.create(scope=candidate, data=b"x", media_type="  ", created_by="test")
    assert exc_info.value.code == WorkbenchErrorCode.INVALID_INPUT
