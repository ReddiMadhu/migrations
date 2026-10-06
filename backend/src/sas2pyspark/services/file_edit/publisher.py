"""AtomicPublisher — staging-file-then-rename with fsync and per-target FIFO lock."""

from __future__ import annotations

import os
import threading
import uuid

from sas2pyspark.core.errors import FsError, FsErrorCode
from sas2pyspark.services.file_edit.fs_port import AbortSignal, NullAbortSignal


class AtomicPublisher:
    """Publish content atomically via staging file + fsync + rename."""

    __slots__ = ("_staging_root", "_locks")

    def __init__(self, *, staging_root: str) -> None:
        self._staging_root = os.path.realpath(staging_root)
        self._locks: dict[str, threading.Lock] = {}

    def _lock_for(self, target: str) -> threading.Lock:
        key = os.path.realpath(target)
        lock = self._locks.get(key)
        if lock is None:
            lock = threading.Lock()
            self._locks[key] = lock
        return lock

    def publish(
        self,
        *,
        target: str,
        content: bytes,
        abort: AbortSignal | None = None,
    ) -> None:
        if abort is None:
            abort = NullAbortSignal()

        lock = self._lock_for(target)
        with lock:
            target_dir = os.path.dirname(target)
            os.makedirs(target_dir, exist_ok=True)
            staging_dir = os.path.join(target_dir, f".staging-{uuid.uuid4().hex[:8]}")
            os.makedirs(staging_dir, mode=0o700, exist_ok=True)
            temp_path = os.path.join(staging_dir, "tmp")
            try:
                with open(temp_path, "wb") as f:
                    f.write(content)
                    f.flush()
                    os.fsync(f.fileno())

                if abort.cancelled:
                    raise FsError(
                        FsErrorCode.FS_SANDBOX_DENIED,
                        "aborted before commit point",
                        remedy="re-read the target and retry",
                    )

                os.replace(temp_path, target)
            except FsError:
                raise
            except OSError as exc:
                raise FsError(
                    FsErrorCode.FS_NOT_TEXT,
                    f"publish failed: {exc}",
                    remedy="re-read the target and retry",
                ) from exc
            finally:
                try:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                    if os.path.exists(staging_dir):
                        os.rmdir(staging_dir)
                except OSError:
                    pass
