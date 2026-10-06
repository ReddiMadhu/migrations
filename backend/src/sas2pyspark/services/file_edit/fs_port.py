"""Provider-abstract filesystem port — the seam all edit components depend on."""

from __future__ import annotations

import os
from typing import Protocol, runtime_checkable


@runtime_checkable
class AbortSignal(Protocol):
    @property
    def cancelled(self) -> bool: ...


class NullAbortSignal:
    __slots__ = ()

    @property
    def cancelled(self) -> bool:
        return False


@runtime_checkable
class FileSystemPort(Protocol):
    def read_bytes(self, path: str) -> bytes: ...
    def write_bytes(self, path: str, data: bytes) -> None: ...
    def exists(self, path: str) -> bool: ...
    def stat(self, path: str) -> os.stat_result: ...


class LocalFileSystem:
    """Concrete filesystem adapter using stdlib os/pathlib."""

    __slots__ = ()

    def read_bytes(self, path: str) -> bytes:
        with open(path, "rb") as f:
            return f.read()

    def write_bytes(self, path: str, data: bytes) -> None:
        with open(path, "wb") as f:
            f.write(data)

    def exists(self, path: str) -> bool:
        return os.path.exists(path)

    def stat(self, path: str) -> os.stat_result:
        return os.stat(path)
