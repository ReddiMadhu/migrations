"""CasVersioner — compare-and-swap versioning for concurrent edit safety."""

from __future__ import annotations

import hashlib

from sas2pyspark.core.errors import FsError, FsErrorCode
from sas2pyspark.core.models import WorkbenchModel


class FsVersion(WorkbenchModel):
    """Opaque content-identity token derived from SHA-256 of content."""

    value: str


class CasVersioner:
    """Compute and verify content versions for stale-edit detection."""

    __slots__ = ()

    @staticmethod
    def version_for(*, content: bytes) -> FsVersion:
        return FsVersion(value="sha256:" + hashlib.sha256(content).hexdigest())

    @staticmethod
    def verify(*, expected: FsVersion | None, current: FsVersion) -> None:
        if expected is not None and expected.value != current.value:
            raise FsError(
                FsErrorCode.FS_STALE_VERSION,
                "file changed since it was read",
            )
