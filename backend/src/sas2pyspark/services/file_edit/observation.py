"""ObservationGate — read-before-edit policy."""

from __future__ import annotations

from enum import Enum

from sas2pyspark.core.errors import FsError, FsErrorCode
from sas2pyspark.services.file_edit.cas import FsVersion


class WriteIntent(str, Enum):
    CREATE_IF_ABSENT = "createIfAbsent"
    REPLACE_IF_VERSION = "replaceIfVersion"


class _Observation:
    __slots__ = ("version", "kind")

    def __init__(self, *, version: FsVersion, kind: str) -> None:
        self.version = version
        self.kind = kind


class ObservationGate:
    """Records reads and enforces read-before-edit / write-intent decisions."""

    __slots__ = ("_observations",)

    def __init__(self) -> None:
        self._observations: dict[str, _Observation] = {}

    def record_read(self, *, target: str, version: FsVersion) -> None:
        self._observations[target] = _Observation(version=version, kind="present")

    def record_absent(self, *, target: str) -> None:
        self._observations[target] = _Observation(version=FsVersion(value=""), kind="absent")

    def edit_intent(self, target: str) -> FsVersion:
        prior = self._observations.get(target)
        if prior is None:
            raise FsError(
                FsErrorCode.FS_NOT_OBSERVED,
                f'edit requires reading "{target}" first',
            )
        if prior.kind == "absent":
            raise FsError(
                FsErrorCode.FS_NOT_FOUND,
                f'cannot edit "{target}": not found',
            )
        return prior.version

    def write_intent(self, target: str) -> WriteIntent:
        prior = self._observations.get(target)
        if prior is None or prior.kind == "absent":
            return WriteIntent.CREATE_IF_ABSENT
        return WriteIntent.REPLACE_IF_VERSION
