"""FileSystemArtifactService — content-addressed artifacts with SHA-256 dedup."""

from __future__ import annotations

import hashlib
import json
import os
from typing import Any

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.core.immutable_blob_store import BlobRef, ImmutableBlobStore
from sas2pyspark.core.json import assert_safe_segment, snapshot_json
from sas2pyspark.core.events import ArtifactReferenceData
from sas2pyspark.services.clock import Clock
from sas2pyspark.services.scopes import ScopeRef, ScopeService


class ArtifactRecord:
    __slots__ = (
        "artifact_id", "source_hash", "semantic_hash", "semantic_hash_version",
        "size", "media_type", "parent_artifact", "created_by", "created_at",
        "storage_location", "scope_id",
    )

    def __init__(
        self,
        *,
        artifact_id: str,
        source_hash: str,
        semantic_hash: str,
        semantic_hash_version: str,
        size: int,
        media_type: str,
        created_by: str,
        created_at: str,
        storage_location: str,
        scope_id: str,
        parent_artifact: ArtifactReferenceData | None = None,
    ) -> None:
        self.artifact_id = artifact_id
        self.source_hash = source_hash
        self.semantic_hash = semantic_hash
        self.semantic_hash_version = semantic_hash_version
        self.size = size
        self.media_type = media_type
        self.parent_artifact = parent_artifact
        self.created_by = created_by
        self.created_at = created_at
        self.storage_location = storage_location
        self.scope_id = scope_id

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "artifactId": self.artifact_id,
            "sourceHash": self.source_hash,
            "semanticHash": self.semantic_hash,
            "semanticHashVersion": self.semantic_hash_version,
            "size": self.size,
            "mediaType": self.media_type,
            "createdBy": self.created_by,
            "createdAt": self.created_at,
            "storageLocation": self.storage_location,
            "scopeId": self.scope_id,
        }
        if self.parent_artifact is not None:
            d["parentArtifact"] = self.parent_artifact.model_dump(by_alias=True)
        return d


def _digest_for(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _to_bytes(data: bytes | str) -> bytes:
    return data.encode("utf-8") if isinstance(data, str) else data


def _assert_inside(root: str, candidate: str) -> str:
    abs_root = os.path.abspath(root)
    abs_candidate = os.path.abspath(candidate)
    try:
        rel = os.path.relpath(abs_candidate, abs_root)
    except ValueError:
        raise WorkbenchError(
            WorkbenchErrorCode.ARTIFACT_SCOPE_VIOLATION,
            "artifact path escapes its scope workspace",
            {"root": abs_root, "candidate": abs_candidate},
        )
    if rel == "." or not rel.startswith(".."):
        return abs_candidate
    raise WorkbenchError(
        WorkbenchErrorCode.ARTIFACT_SCOPE_VIOLATION,
        "artifact path escapes its scope workspace",
        {"root": abs_root, "candidate": abs_candidate},
    )


def artifact_reference(record: ArtifactRecord | ArtifactReferenceData) -> ArtifactReferenceData:
    if isinstance(record, ArtifactReferenceData):
        return record
    kwargs: dict[str, Any] = {
        "artifactId": record.artifact_id,
        "digest": record.source_hash,
        "mediaType": record.media_type,
        "size": record.size,
    }
    if record.semantic_hash != record.source_hash:
        kwargs["semanticHash"] = record.semantic_hash
    return ArtifactReferenceData(**kwargs)


def _parse_metadata(value: Any) -> ArtifactRecord:
    if not isinstance(value, dict):
        raise WorkbenchError(WorkbenchErrorCode.SESSION_CORRUPTION, "artifact metadata is not an object")
    raw = value
    for key in ("artifactId", "sourceHash", "semanticHash", "semanticHashVersion", "mediaType", "createdBy", "createdAt", "storageLocation", "scopeId"):
        if not isinstance(raw.get(key), str):
            raise WorkbenchError(WorkbenchErrorCode.SESSION_CORRUPTION, "artifact metadata is incomplete")
    if not isinstance(raw.get("size"), int):
        raise WorkbenchError(WorkbenchErrorCode.SESSION_CORRUPTION, "artifact metadata is incomplete")
    parent = None
    if raw.get("parentArtifact") is not None:
        parent = ArtifactReferenceData(**raw["parentArtifact"])
    return ArtifactRecord(
        artifact_id=raw["artifactId"],
        source_hash=raw["sourceHash"],
        semantic_hash=raw["semanticHash"],
        semantic_hash_version=raw["semanticHashVersion"],
        size=raw["size"],
        media_type=raw["mediaType"],
        created_by=raw["createdBy"],
        created_at=raw["createdAt"],
        storage_location=raw["storageLocation"],
        scope_id=raw["scopeId"],
        parent_artifact=parent,
    )


class FileSystemArtifactService:
    def __init__(self, root_dir: str, clock: Clock, scopes: ScopeService) -> None:
        self._root_dir = os.path.abspath(root_dir)
        self._clock = clock
        self._scopes = scopes

    def create(
        self,
        *,
        scope: ScopeRef,
        data: bytes | str,
        media_type: str,
        created_by: str,
        label: str | None = None,
        semantic_hash: str | None = None,
        semantic_hash_version: str | None = None,
        parent_artifact: ArtifactReferenceData | None = None,
    ) -> ArtifactRecord:
        scope = self._scopes.require_by_id(scope.id)
        assert_safe_segment(created_by, "createdBy")
        if not media_type.strip():
            raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, "artifact mediaType is required")
        bytes_data = _to_bytes(data)
        source_hash = _digest_for(bytes_data)
        suffix = source_hash[7:]
        object_dir = os.path.join(scope.workspace, "objects", suffix[:2])
        object_path = os.path.join(object_dir, suffix)
        metadata_path = os.path.join(scope.workspace, "metadata", f"{suffix}.json")
        _assert_inside(scope.workspace, object_path)
        _assert_inside(scope.workspace, metadata_path)
        os.makedirs(object_dir, exist_ok=True)
        os.makedirs(os.path.dirname(metadata_path), exist_ok=True)

        try:
            with open(object_path, "xb") as f:
                f.write(bytes_data)
        except FileExistsError:
            with open(object_path, "rb") as f:
                existing = f.read()
            if _digest_for(existing) != source_hash:
                raise WorkbenchError(
                    WorkbenchErrorCode.DUPLICATE_ARTIFACT,
                    "artifact path exists with a different digest",
                    {"objectPath": object_path},
                )

        record = ArtifactRecord(
            artifact_id=f"artifact-{suffix[:16]}",
            source_hash=source_hash,
            semantic_hash=semantic_hash or source_hash,
            semantic_hash_version=semantic_hash_version or "exact-v1",
            size=len(bytes_data),
            media_type=media_type,
            created_by=created_by,
            created_at=self._clock.now(),
            storage_location=object_path,
            scope_id=scope.id,
            parent_artifact=parent_artifact,
        )

        try:
            with open(metadata_path, "x") as f:
                f.write(json.dumps(record.to_dict(), indent=2) + "\n")
        except FileExistsError:
            with open(metadata_path) as f:
                existing = _parse_metadata(json.load(f))
            if existing.size != record.size or existing.media_type != record.media_type or existing.source_hash != source_hash:
                raise WorkbenchError(
                    WorkbenchErrorCode.DUPLICATE_ARTIFACT,
                    "artifact metadata conflicts with existing content",
                    {"metadataPath": metadata_path},
                )
            return existing
        return record

    def get(self, scope: ScopeRef, record: ArtifactRecord) -> ArtifactRecord:
        self._scopes.require_by_id(scope.id)
        if record.scope_id != scope.id:
            raise WorkbenchError(
                WorkbenchErrorCode.ARTIFACT_SCOPE_VIOLATION,
                "artifact belongs to another scope",
                {"requested": scope.id, "artifact": record.scope_id},
            )
        suffix = record.source_hash[7:]
        metadata_path = os.path.join(scope.workspace, "metadata", f"{suffix}.json")
        object_path = _assert_inside(scope.workspace, os.path.join(scope.workspace, "objects", suffix[:2], suffix))
        if os.path.abspath(record.storage_location) != object_path:
            raise WorkbenchError(
                WorkbenchErrorCode.ARTIFACT_SCOPE_VIOLATION,
                "artifact storage location is not canonical for its scope",
                {"expected": object_path, "actual": os.path.abspath(record.storage_location)},
            )
        _assert_inside(scope.workspace, metadata_path)
        with open(metadata_path) as f:
            stored = _parse_metadata(json.load(f))
        if stored.source_hash != record.source_hash or stored.scope_id != scope.id:
            raise WorkbenchError(WorkbenchErrorCode.EVIDENCE_TAMPERED, "artifact metadata identity changed", {"record": record.artifact_id})
        with open(object_path, "rb") as f:
            bytes_data = f.read()
        if _digest_for(bytes_data) != record.source_hash or len(bytes_data) != record.size:
            raise WorkbenchError(WorkbenchErrorCode.EVIDENCE_TAMPERED, "artifact bytes do not match metadata", {"record": record.artifact_id})
        return stored

    def read(self, scope: ScopeRef, record: ArtifactRecord) -> bytes:
        verified = self.get(scope, record)
        suffix = verified.source_hash[7:]
        object_path = _assert_inside(scope.workspace, os.path.join(scope.workspace, "objects", suffix[:2], suffix))
        with open(object_path, "rb") as f:
            return f.read()

    def read_text(self, scope: ScopeRef, record: ArtifactRecord | ArtifactReferenceData) -> str:
        if isinstance(record, ArtifactReferenceData):
            return self.read_reference(scope, record).decode("utf-8")
        return self.read(scope, record).decode("utf-8")

    def read_reference(self, scope: ScopeRef, reference: ArtifactReferenceData) -> bytes:
        self._scopes.require_by_id(scope.id)
        import re
        if not re.match(r"^sha256:[0-9a-f]{64}$", reference.digest):
            raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, "artifact reference digest is invalid", {"digest": reference.digest})
        suffix = reference.digest[7:]
        metadata_path = _assert_inside(scope.workspace, os.path.join(scope.workspace, "metadata", f"{suffix}.json"))
        object_path = _assert_inside(scope.workspace, os.path.join(scope.workspace, "objects", suffix[:2], suffix))
        try:
            with open(metadata_path) as f:
                stored = _parse_metadata(json.load(f))
        except FileNotFoundError:
            raise WorkbenchError(WorkbenchErrorCode.EVIDENCE_TAMPERED, "referenced artifact metadata is unavailable or invalid", {"artifactId": reference.artifact_id})
        if (stored.artifact_id != reference.artifact_id or stored.source_hash != reference.digest
                or stored.scope_id != scope.id or stored.size != reference.size
                or stored.media_type != reference.media_type):
            raise WorkbenchError(WorkbenchErrorCode.EVIDENCE_TAMPERED, "referenced artifact identity does not match stored evidence", {"artifactId": reference.artifact_id})
        with open(object_path, "rb") as f:
            bytes_data = f.read()
        if _digest_for(bytes_data) != reference.digest or len(bytes_data) != reference.size:
            raise WorkbenchError(WorkbenchErrorCode.EVIDENCE_TAMPERED, "referenced artifact bytes do not match their digest", {"artifactId": reference.artifact_id})
        return bytes_data

    @property
    def blob_store(self) -> FileSystemBlobStore:
        """Expose an ImmutableBlobStore rooted at the artifact service root dir."""
        return FileSystemBlobStore(os.path.join(self._root_dir, "blobs"))


class FileSystemBlobStore(ImmutableBlobStore):
    """File-system backed ImmutableBlobStore implementing content-addressed storage.

    Stores arbitrary immutable binary payloads partition-addressed by SHA-256 digest
    under `<base_dir>/<namespace>/<xx>/<hash>`.
    """

    def __init__(self, base_dir: str) -> None:
        self._base_dir = os.path.abspath(base_dir)

    def _resolve_path(self, namespace: str, digest: str) -> str:
        assert_safe_segment(namespace, "namespace")
        suffix = digest[7:] if digest.startswith("sha256:") else digest
        return os.path.join(self._base_dir, namespace, suffix[:2], suffix)

    def put(self, data: bytes, *, namespace: str = "checkpoints") -> BlobRef:
        bytes_data = _to_bytes(data)
        digest = _digest_for(bytes_data)
        target_path = self._resolve_path(namespace, digest)
        os.makedirs(os.path.dirname(target_path), exist_ok=True)

        if not os.path.exists(target_path):
            temp_path = f"{target_path}.tmp.{os.getpid()}"
            with open(temp_path, "wb") as f:
                f.write(bytes_data)
            try:
                os.replace(temp_path, target_path)
            except Exception:
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
                if not os.path.exists(target_path):
                    raise
        else:
            with open(target_path, "rb") as f:
                existing = f.read()
            if _digest_for(existing) != digest:
                raise WorkbenchError(
                    WorkbenchErrorCode.DUPLICATE_ARTIFACT,
                    "existing blob has differing digest",
                    {"path": target_path},
                )

        return BlobRef(content_hash=digest, size=len(bytes_data), namespace=namespace)

    def get(self, ref: BlobRef) -> bytes:
        target_path = self._resolve_path(ref.namespace, ref.content_hash)
        if not os.path.exists(target_path):
            raise WorkbenchError(
                WorkbenchErrorCode.EVIDENCE_TAMPERED,
                f"Blob not found: {ref.content_hash}",
                {"path": target_path},
            )
        with open(target_path, "rb") as f:
            data = f.read()
        if _digest_for(data) != ref.content_hash or len(data) != ref.size:
            raise WorkbenchError(
                WorkbenchErrorCode.EVIDENCE_TAMPERED,
                "Blob bytes do not match expected digest or size",
                {"path": target_path},
            )
        return data

    def delete(self, ref: BlobRef) -> None:
        target_path = self._resolve_path(ref.namespace, ref.content_hash)
        try:
            if os.path.exists(target_path):
                os.unlink(target_path)
        except OSError:
            pass
