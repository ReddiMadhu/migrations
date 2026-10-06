"""ObservationCache — in-memory LRU with lease pinning."""

from __future__ import annotations

from collections import OrderedDict
from typing import Any

from pydantic import Field

from sas2pyspark.core.models import WorkbenchModel

DEFAULT_CAPACITY = 5


class ObservationKey(WorkbenchModel):
    persistence_identity: str = Field(alias="persistenceIdentity")
    session_id: str = Field(alias="sessionId")
    revision: int

    model_config = {**WorkbenchModel.model_config, "populate_by_name": True}

    def cache_key(self) -> str:
        return f"{self.persistence_identity}:{self.session_id}:{self.revision}"


class ObservationEntry:
    __slots__ = ("projection", "refs")

    def __init__(self, *, projection: dict[str, Any], refs: int = 0) -> None:
        self.projection = projection
        self.refs = refs


class Lease:
    __slots__ = ("_cache", "_key")

    def __init__(self, cache: "ObservationCache", key: str) -> None:
        self._cache = cache
        self._key = key

    def release(self) -> None:
        self._cache._release_lease(self._key)


class ObservationCache:
    """Bounded LRU cache with lease pinning for session observations."""

    __slots__ = ("_capacity", "_cache")

    def __init__(self, capacity: int = DEFAULT_CAPACITY) -> None:
        self._capacity = capacity
        self._cache: OrderedDict[str, ObservationEntry] = OrderedDict()

    def get(self, key: ObservationKey) -> dict[str, Any] | None:
        k = key.cache_key()
        entry = self._cache.get(k)
        if entry is None:
            return None
        self._cache.move_to_end(k)
        return entry.projection

    def put(self, key: ObservationKey, projection: dict[str, Any]) -> None:
        k = key.cache_key()
        if k in self._cache:
            self._cache.move_to_end(k)
        self._cache[k] = ObservationEntry(projection=projection)
        self._evict_past_capacity()

    def acquire_lease(self, key: ObservationKey) -> Lease | None:
        k = key.cache_key()
        entry = self._cache.get(k)
        if entry is None:
            return None
        entry.refs += 1
        return Lease(self, k)

    def _release_lease(self, k: str) -> None:
        entry = self._cache.get(k)
        if entry is not None:
            entry.refs -= 1
            if entry.refs <= 0:
                self._evict_past_capacity()

    def _evict_past_capacity(self) -> None:
        if len(self._cache) <= self._capacity:
            return
        for k in list(self._cache.keys()):
            if len(self._cache) <= self._capacity:
                return
            entry = self._cache[k]
            if entry.refs > 0:
                continue
            self._cache.pop(k, None)

    def __len__(self) -> int:
        return len(self._cache)
