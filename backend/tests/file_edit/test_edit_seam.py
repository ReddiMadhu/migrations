"""Tests for EditSeam — end-to-end orchestration with artifact routing and event logging."""

from __future__ import annotations

import pytest

from sas2pyspark.services.file_edit.cas import CasVersioner
from sas2pyspark.services.file_edit.diff import DiffPresenter
from sas2pyspark.services.file_edit.literal import LiteralEditEngine
from sas2pyspark.services.file_edit.observation import ObservationGate
from sas2pyspark.services.file_edit.sandbox import SandboxFence, SandboxPolicy
from sas2pyspark.services.file_edit.seam import EditRequest, EditSeam
from sas2pyspark.services.file_edit.text_validator import TextValidator
from sas2pyspark.services.file_edit.publisher import AtomicPublisher


class _FakeSessions:
    def __init__(self):
        self.events = []

    def append_event(self, session, event_type, data):
        self.events.append((event_type, data))


class TestEditSeam:
    def _make_seam(self, tmp_path):
        from sas2pyspark.services.clock import FixedClock
        from sas2pyspark.services.artifacts import FileSystemArtifactService
        from sas2pyspark.services.scopes import ScopeService, ScopeRef

        clock = FixedClock()
        scopes = ScopeService(root_dir=str(tmp_path))
        tenant = scopes.create_tenant("tenant-test")
        scope = scopes.require_by_id(tenant.id)
        artifacts = FileSystemArtifactService(str(tmp_path), clock, scopes)
        sessions = _FakeSessions()

        seam = EditSeam(
            artifacts=artifacts,
            engine=LiteralEditEngine(),
            publisher=AtomicPublisher(staging_root=str(tmp_path)),
            cas=CasVersioner(),
            observation=ObservationGate(),
            sandbox=SandboxFence(workspace_root=str(tmp_path)),
            validator=TextValidator(),
            diff=DiffPresenter(),
            sessions=sessions,
            clock=clock,
        )
        return seam, scope, sessions, artifacts

    def test_edit_produces_new_artifact(self, tmp_path):
        seam, scope, sessions, artifacts = self._make_seam(tmp_path)
        target = str(tmp_path / "candidate.py")
        with open(target, "w") as f:
            f.write("df = spark.read.csv('data.csv')\n")
        from sas2pyspark.services.file_edit.cas import FsVersion
        with open(target, "rb") as f:
            version = FsVersion(value="sha256:" + __import__("hashlib").sha256(f.read()).hexdigest())
        seam._observation.record_read(target=target, version=version)
        result = seam.edit_text(
            scope=scope,
            request=EditRequest(target=target, oldString="data.csv", newString="data.parquet", replaceAll=False),
            sandbox_policy=SandboxPolicy.WORKSPACE_WRITE,
            session=None,
        )
        assert result.advisory is True
        assert result.artifact_digest.startswith("sha256:")
        with open(target, "r") as f:
            assert "data.parquet" in f.read()

    def test_edit_without_read_raises(self, tmp_path):
        from sas2pyspark.core.errors import FsError, FsErrorCode
        seam, scope, sessions, artifacts = self._make_seam(tmp_path)
        target = str(tmp_path / "candidate.py")
        with open(target, "w") as f:
            f.write("hello world")
        with pytest.raises(FsError) as exc_info:
            seam.edit_text(
                scope=scope,
                request=EditRequest(target=target, oldString="hello", newString="hi", replaceAll=False),
                sandbox_policy=SandboxPolicy.WORKSPACE_WRITE,
                session=None,
            )
        assert exc_info.value.code == FsErrorCode.FS_NOT_OBSERVED

    def test_edit_appends_event(self, tmp_path):
        from sas2pyspark.core.events import MigrationEventType
        seam, scope, sessions, artifacts = self._make_seam(tmp_path)
        target = str(tmp_path / "candidate.py")
        with open(target, "w") as f:
            f.write("x = 1")
        from sas2pyspark.services.file_edit.cas import FsVersion
        import hashlib
        with open(target, "rb") as f:
            version = FsVersion(value="sha256:" + hashlib.sha256(f.read()).hexdigest())
        seam._observation.record_read(target=target, version=version)
        seam.edit_text(
            scope=scope,
            request=EditRequest(target=target, oldString="x = 1", newString="x = 2", replaceAll=False),
            sandbox_policy=SandboxPolicy.WORKSPACE_WRITE,
            session=None,
        )
        assert len(sessions.events) == 1
        assert sessions.events[0][0] == MigrationEventType.FILE_EDIT_APPLIED
