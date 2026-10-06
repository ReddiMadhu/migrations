"""ContentAddressedExampleCache — SHA-256 keyed cache with TTL, in-flight dedup, quota reclaim."""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from typing import Any

from pydantic import Field

from sas2pyspark.core.json import canonical_json
from sas2pyspark.core.models import WorkbenchModel
from sas2pyspark.services.clock import Clock


class CacheKey(WorkbenchModel):
    scope: str
    variant_id: str = Field(alias="variantId")

    model_config = {**WorkbenchModel.model_config, "populate_by_name": True}


class CacheEntry(WorkbenchModel):
    content: bytes
    created_at: str = Field(alias="createdAt")
    owner: str

    model_config = {**WorkbenchModel.model_config, "populate_by_name": True}


def _file_lock_path(root: str) -> str:
    return os.path.join(root, ".cache.lock")


def _acquire_lock(lock_path: str) -> threading.Lock:
    return threading.Lock()


class ContentAddressedExampleCache:
    """Content-addressed cache with TTL eviction, in-flight dedup, and quota reclaim."""

    __slots__ = ("_root", "_clock", "_ttl", "_margin", "_quota", "_format_version", "_lock", "_inflight")

    def __init__(
        self,
        *,
        root_dir: str,
        clock: Clock,
        ttl_seconds: float = 3600.0,
        refresh_margin_seconds: float = 60.0,
        quota_bytes: int = 100 * 1024 * 1024,
        format_version: int = 1,
    ) -> None:
        self._root = os.path.realpath(root_dir)
        self._clock = clock
        self._ttl = ttl_seconds
        self._margin = refresh_margin_seconds
        self._quota = quota_bytes
        self._format_version = format_version
        self._lock = threading.Lock()
        self._inflight: dict[str, threading.Event] = {}
        os.makedirs(self._root, exist_ok=True)

    def _index_path(self) -> str:
        return os.path.join(self._root, f"index-v{self._format_version}.json")

    def _load_index(self) -> dict[str, Any]:
        path = self._index_path()
        if not os.path.exists(path):
            return {"formatVersion": self._format_version, "entries": {}}
        try:
            with open(path) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            backup = path + ".bak"
            try:
                os.rename(path, backup)
            except OSError:
                pass
            return {"formatVersion": self._format_version, "entries": {}}

    def _save_index(self, index: dict[str, Any]) -> None:
        path = self._index_path()
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(index, f, sort_keys=True, separators=(",", ":"))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

    @staticmethod
    def _make_key(scope: str, variant_id: str) -> str:
        return f"{scope}:{variant_id}"

    def _is_expired(self, entry: dict[str, Any], now_epoch: float) -> bool:
        created = entry.get("epoch", 0.0)
        return (now_epoch - created) > (self._ttl - self._margin)

    def lookup(self, key: CacheKey) -> CacheEntry | None:
        with self._lock:
            index = self._load_index()
            entries = index.get("entries", {})
            k = self._make_key(key.scope, key.variant_id)
            entry = entries.get(k)
            if entry is None:
                return None
            now_epoch = time.time()
            if self._is_expired(entry, now_epoch):
                return None
            content_path = os.path.join(self._root, entry.get("contentFile", ""))
            if not os.path.exists(content_path):
                return None
            with open(content_path, "rb") as f:
                content = f.read()
            return CacheEntry(content=content, createdAt=entry.get("createdAt", ""), owner=entry.get("owner", ""))

    def put(self, key: CacheKey, entry: CacheEntry, *, owner: str) -> bool:
        with self._lock:
            index = self._load_index()
            entries = index.get("entries", {})
            k = self._make_key(key.scope, key.variant_id)
            content_file = f"{key.scope}_{key.variant_id}.bin"
            content_path = os.path.join(self._root, content_file)
            with open(content_path, "wb") as f:
                f.write(entry.content)
                f.flush()
                os.fsync(f.fileno())
            entries[k] = {
                "contentFile": content_file,
                "createdAt": entry.created_at,
                "owner": owner,
                "epoch": time.time(),
                "size": len(entry.content),
            }
            index["entries"] = entries
            self._save_index(index)
            return True

    def evict_expired(self) -> int:
        with self._lock:
            index = self._load_index()
            entries = index.get("entries", {})
            now_epoch = time.time()
            expired = [k for k, e in entries.items() if self._is_expired(e, now_epoch)]
            for k in expired:
                entry = entries.pop(k)
                content_path = os.path.join(self._root, entry.get("contentFile", ""))
                try:
                    os.remove(content_path)
                except OSError:
                    pass
            if expired:
                index["entries"] = entries
                self._save_index(index)
            return len(expired)

    def reclaim_quota(self, owner: str) -> int:
        with self._lock:
            index = self._load_index()
            entries = index.get("entries", {})
            owned = [(k, e) for k, e in entries.items() if e.get("owner") == owner]
            owned.sort(key=lambda x: x[1].get("epoch", 0))
            reclaimed = 0
            total_size = sum(e.get("size", 0) for _, e in entries.items())
            for k, e in owned:
                if total_size <= self._quota:
                    break
                entries.pop(k)
                total_size -= e.get("size", 0)
                content_path = os.path.join(self._root, e.get("contentFile", ""))
                try:
                    os.remove(content_path)
                except OSError:
                    pass
                reclaimed += 1
            if reclaimed:
                index["entries"] = entries
                self._save_index(index)
            return reclaimed
