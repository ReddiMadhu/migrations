"""ProjectionCache — write-behind session projection cache with durability barrier."""

from __future__ import annotations

import json
import os
import time
from typing import Any

from pydantic import Field

from sas2pyspark.core.models import WorkbenchModel
from sas2pyspark.services.clock import Clock


class CheckpointIdentity(WorkbenchModel):
    format_version: int = Field(alias="formatVersion")
    created_at: str = Field(alias="createdAt")
    cwd: str
    is_seeded: bool = Field(default=False, alias="isSeeded")
    inherited_event_count: int = Field(default=0, alias="inheritedEventCount")

    model_config = {**WorkbenchModel.model_config, "populate_by_name": True}


class ProjectionCache:
    """Write-behind cache of projection-unit state per session."""

    __slots__ = ("_root", "_clock", "_format_version", "_throttle_count", "_throttle_interval_ms", "_event_counts", "_last_flush")

    def __init__(
        self,
        *,
        root_dir: str,
        clock: Clock,
        format_version: int = 1,
        throttle_count: int = 50,
        throttle_interval_ms: float = 5000.0,
    ) -> None:
        self._root = os.path.realpath(root_dir)
        self._clock = clock
        self._format_version = format_version
        self._throttle_count = throttle_count
        self._throttle_interval_ms = throttle_interval_ms
        self._event_counts: dict[str, int] = {}
        self._last_flush: dict[str, float] = {}
        os.makedirs(self._root, exist_ok=True)

    def _record_path(self, session_id: str) -> str:
        return os.path.join(self._root, f"{session_id}.json")

    def checkpoint(
        self,
        *,
        session_id: str,
        identity: CheckpointIdentity,
        projection: dict[str, Any],
        mandatory: bool = False,
    ) -> None:
        if not mandatory:
            count = self._event_counts.get(session_id, 0) + 1
            self._event_counts[session_id] = count
            now = time.time()
            last = self._last_flush.get(session_id, 0.0)
            if count < self._throttle_count and (now - last) * 1000 < self._throttle_interval_ms:
                return

        path = self._record_path(session_id)
        record = {
            "formatVersion": self._format_version,
            "identity": identity.model_dump(by_alias=True),
            "projection": projection,
        }
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(record, f, sort_keys=True, separators=(",", ":"))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        self._event_counts[session_id] = 0
        self._last_flush[session_id] = time.time()

    def flush(self, session_id: str) -> None:
        self._event_counts.pop(session_id, None)
        self._last_flush.pop(session_id, None)

    def load(self, session_id: str, current_format_version: int) -> dict[str, Any] | None:
        path = self._record_path(session_id)
        if not os.path.exists(path):
            return None
        try:
            with open(path) as f:
                record = json.load(f)
        except (json.JSONDecodeError, OSError):
            backup = path + ".bak"
            try:
                os.rename(path, backup)
            except OSError:
                pass
            return None
        if record.get("formatVersion") != current_format_version:
            return None
        return record.get("projection")

    def invalidate(self, *, dependency: str) -> None:
        pass
