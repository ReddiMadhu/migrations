"""EditSeam — the single orchestrator through which all file-edit tool surfaces delegate."""

from __future__ import annotations

from typing import Any

from pydantic import Field

from sas2pyspark.core.events import MigrationEventType
from sas2pyspark.core.models import WorkbenchModel
from sas2pyspark.services.artifacts import FileSystemArtifactService
from sas2pyspark.services.clock import Clock
from sas2pyspark.services.file_edit.cas import CasVersioner, FsVersion
from sas2pyspark.services.file_edit.diff import DiffHunks, DiffPresenter
from sas2pyspark.services.file_edit.fs_port import AbortSignal, NullAbortSignal
from sas2pyspark.services.file_edit.literal import LiteralEditEngine
from sas2pyspark.services.file_edit.observation import ObservationGate, WriteIntent
from sas2pyspark.services.file_edit.sandbox import SandboxFence, SandboxPolicy
from sas2pyspark.services.file_edit.text_validator import TextValidator
from sas2pyspark.services.file_edit.publisher import AtomicPublisher
from sas2pyspark.services.scopes import ScopeRef


class EditRequest(WorkbenchModel):
    target: str
    old_string: str = Field(alias="oldString")
    new_string: str = Field(alias="newString")
    replace_all: bool = Field(default=False, alias="replaceAll")
    expected_version: FsVersion | None = Field(default=None, alias="expectedVersion")

    model_config = {**WorkbenchModel.model_config, "populate_by_name": True}


class EditResult(WorkbenchModel):
    artifact_digest: str = Field(alias="artifactDigest")
    new_version: FsVersion = Field(alias="newVersion")
    diff: DiffHunks
    advisory: bool = True

    model_config = {**WorkbenchModel.model_config, "populate_by_name": True}


class EditSeam:
    """Orchestrates: observation → sandbox → validate → CAS → match → publish → artifact → event."""

    __slots__ = (
        "_artifacts", "_engine", "_publisher", "_cas",
        "_observation", "_sandbox", "_validator", "_diff",
        "_sessions", "_clock",
    )

    def __init__(
        self,
        *,
        artifacts: FileSystemArtifactService,
        engine: LiteralEditEngine,
        publisher: AtomicPublisher,
        cas: CasVersioner,
        observation: ObservationGate,
        sandbox: SandboxFence,
        validator: TextValidator,
        diff: DiffPresenter,
        sessions: Any,
        clock: Clock,
    ) -> None:
        self._artifacts = artifacts
        self._engine = engine
        self._publisher = publisher
        self._cas = cas
        self._observation = observation
        self._sandbox = sandbox
        self._validator = validator
        self._diff = diff
        self._sessions = sessions
        self._clock = clock

    def edit_text(
        self,
        *,
        scope: ScopeRef,
        request: EditRequest,
        sandbox_policy: SandboxPolicy,
        session: Any,
        abort: AbortSignal | None = None,
    ) -> "EditResult":
        if abort is None:
            abort = NullAbortSignal()

        expected_version = self._observation.edit_intent(request.target)
        effective_expected = request.expected_version or expected_version

        canonical_target = self._sandbox.check(
            target=request.target, policy=sandbox_policy, is_write=True,
        )

        with open(canonical_target, "rb") as f:
            raw = f.read()
        decoded = self._validator.validate(content=raw)

        current_version = self._cas.version_for(content=raw)
        self._cas.verify(expected=effective_expected, current=current_version)

        edited = self._engine.apply_literal_edit(
            content=decoded,
            old_string=request.old_string,
            new_string=request.new_string,
            replace_all=request.replace_all,
        )
        edited_bytes = edited.encode("utf-8")

        self._sandbox.recheck_at_commit(target=canonical_target, policy=sandbox_policy)

        self._publisher.publish(
            target=canonical_target, content=edited_bytes, abort=abort,
        )

        new_version = self._cas.version_for(content=edited_bytes)
        self._observation.record_read(target=request.target, version=new_version)

        record = self._artifacts.create(
            scope=scope,
            data=edited_bytes,
            media_type="text/x-python",
            created_by="file-edit-seam",
            parent_artifact=None,
        )

        diff = self._diff.diff(old_content=decoded, new_content=edited)

        self._sessions.append_event(
            session,
            MigrationEventType.FILE_EDIT_APPLIED,
            {
                "target": request.target,
                "artifactDigest": record.source_hash,
                "artifactId": record.artifact_id,
                "oldVersion": current_version.value,
                "newVersion": new_version.value,
                "replaceAll": request.replace_all,
            },
        )

        return EditResult(
            artifactDigest=record.source_hash,
            newVersion=new_version,
            diff=diff,
            advisory=True,
        )
