"""Immutable blob store protocol and content-addressed reference types.

Used for checkpoint payload overflow when serialized LangGraph channel states
exceed Cosmos DB document size limits (default threshold: 1.5 MB).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode


@dataclass(frozen=True)
class BlobRef:
    """Content-addressed reference to an immutable blob stored in blob storage."""

    content_hash: str
    size: int
    namespace: str = "checkpoints"

    def to_dict(self) -> dict[str, Any]:
        """Serialize reference as canonical metadata dictionary."""
        return {
            "type": "checkpoint_payload",
            "storage": "artifact_service",
            "namespace": self.namespace,
            "content_hash": self.content_hash,
            "size": self.size,
            "serializer": "langgraph-jsonplus",
            "encoding": "ormsgpack",
            "schema_version": 1,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BlobRef:
        """Construct reference from stored metadata dictionary."""
        if not isinstance(data, dict):
            raise WorkbenchError(
                WorkbenchErrorCode.INVALID_INPUT,
                "BlobRef data must be a dictionary",
            )
        content_hash = data.get("content_hash")
        size = data.get("size")
        if not content_hash or not isinstance(size, int):
            raise WorkbenchError(
                WorkbenchErrorCode.INVALID_INPUT,
                "Invalid BlobRef dictionary: missing content_hash or size",
            )
        return cls(
            content_hash=content_hash,
            size=size,
            namespace=data.get("namespace", "checkpoints"),
        )


@runtime_checkable
class ImmutableBlobStore(Protocol):
    """Protocol for content-addressed immutable blob storage."""

    def put(self, data: bytes, *, namespace: str = "checkpoints") -> BlobRef:
        """Store immutable bytes content-addressed by SHA-256 digest."""
        ...

    def get(self, ref: BlobRef) -> bytes:
        """Retrieve bytes and verify SHA-256 digest integrity."""
        ...

    def delete(self, ref: BlobRef) -> None:
        """Delete blob referenced by content-addressed key."""
        ...


class InMemoryBlobStore:
    """In-memory implementation of ImmutableBlobStore for testing."""

    def __init__(self) -> None:
        self._blobs: dict[tuple[str, str], bytes] = {}

    def put(self, data: bytes, *, namespace: str = "checkpoints") -> BlobRef:
        digest = "sha256:" + hashlib.sha256(data).hexdigest()
        self._blobs[(namespace, digest)] = bytes(data)
        return BlobRef(content_hash=digest, size=len(data), namespace=namespace)

    def get(self, ref: BlobRef) -> bytes:
        key = (ref.namespace, ref.content_hash)
        if key not in self._blobs:
            raise WorkbenchError(
                WorkbenchErrorCode.EVIDENCE_TAMPERED,
                f"Blob not found in namespace '{ref.namespace}': {ref.content_hash}",
            )
        data = self._blobs[key]
        expected_digest = "sha256:" + hashlib.sha256(data).hexdigest()
        if expected_digest != ref.content_hash or len(data) != ref.size:
            raise WorkbenchError(
                WorkbenchErrorCode.EVIDENCE_TAMPERED,
                "Retrieved blob does not match expected digest/size",
            )
        return data

    def delete(self, ref: BlobRef) -> None:
        self._blobs.pop((ref.namespace, ref.content_hash), None)

    def has(self, ref: BlobRef) -> bool:
        return (ref.namespace, ref.content_hash) in self._blobs

    def count(self, namespace: str | None = None) -> int:
        if namespace is None:
            return len(self._blobs)
        return sum(1 for (ns, _) in self._blobs if ns == namespace)
