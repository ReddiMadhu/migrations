"""Tests for ObservationCache — LRU with lease pinning."""

from __future__ import annotations

import pytest

from sas2pyspark.llm.cache.observation_cache import (
    DEFAULT_CAPACITY,
    Lease,
    ObservationCache,
    ObservationEntry,
    ObservationKey,
)


def _key(rev: int = 0, session: str = "s1") -> ObservationKey:
    return ObservationKey(persistenceIdentity="p1", sessionId=session, revision=rev)


class TestObservationCache:
    def test_default_capacity_is_5(self):
        assert DEFAULT_CAPACITY == 5
        cache = ObservationCache()
        assert cache._capacity == 5

    def test_put_then_get(self):
        cache = ObservationCache(capacity=10)
        key = _key()
        cache.put(key, {"data": "value"})
        result = cache.get(key)
        assert result == {"data": "value"}

    def test_get_missing_returns_none(self):
        cache = ObservationCache()
        assert cache.get(_key()) is None

    def test_lru_eviction(self):
        cache = ObservationCache(capacity=2)
        k1, k2, k3 = _key(1), _key(2), _key(3)
        cache.put(k1, {"v": 1})
        cache.put(k2, {"v": 2})
        cache.put(k3, {"v": 3})
        assert cache.get(k1) is None
        assert cache.get(k2) is not None
        assert cache.get(k3) is not None

    def test_lru_access_reorders(self):
        cache = ObservationCache(capacity=2)
        k1, k2, k3 = _key(1), _key(2), _key(3)
        cache.put(k1, {"v": 1})
        cache.put(k2, {"v": 2})
        cache.get(k1)
        cache.put(k3, {"v": 3})
        assert cache.get(k1) is not None
        assert cache.get(k2) is None

    def test_lease_prevents_eviction(self):
        cache = ObservationCache(capacity=2)
        k1, k2, k3 = _key(1), _key(2), _key(3)
        cache.put(k1, {"v": 1})
        cache.put(k2, {"v": 2})
        lease = cache.acquire_lease(k1)
        assert lease is not None
        cache.put(k3, {"v": 3})
        assert cache.get(k1) is not None
        assert cache.get(k2) is None
        assert cache.get(k3) is not None

    def test_release_lease_allows_eviction(self):
        cache = ObservationCache(capacity=2)
        k1, k2, k3 = _key(1), _key(2), _key(3)
        cache.put(k1, {"v": 1})
        cache.put(k2, {"v": 2})
        lease = cache.acquire_lease(k1)
        assert lease is not None
        lease.release()
        cache.put(k3, {"v": 3})
        assert cache.get(k2) is None or cache.get(k1) is None

    def test_all_pinned_defers_eviction(self):
        cache = ObservationCache(capacity=2)
        k1, k2, k3 = _key(1), _key(2), _key(3)
        cache.put(k1, {"v": 1})
        cache.put(k2, {"v": 2})
        lease1 = cache.acquire_lease(k1)
        lease2 = cache.acquire_lease(k2)
        cache.put(k3, {"v": 3})
        assert cache.get(k1) is not None
        assert cache.get(k2) is not None

    def test_acquire_lease_missing_returns_none(self):
        cache = ObservationCache()
        assert cache.acquire_lease(_key()) is None

    def test_three_part_key(self):
        k1 = ObservationKey(persistenceIdentity="p1", sessionId="s1", revision=1)
        k2 = ObservationKey(persistenceIdentity="p1", sessionId="s1", revision=2)
        k3 = ObservationKey(persistenceIdentity="p2", sessionId="s1", revision=1)
        cache = ObservationCache(capacity=10)
        cache.put(k1, {"v": 1})
        cache.put(k2, {"v": 2})
        cache.put(k3, {"v": 3})
        assert cache.get(k1) == {"v": 1}
        assert cache.get(k2) == {"v": 2}
        assert cache.get(k3) == {"v": 3}

    def test_key_isolation(self):
        k1 = ObservationKey(persistenceIdentity="p1", sessionId="s1", revision=1)
        k2 = ObservationKey(persistenceIdentity="p1", sessionId="s2", revision=1)
        cache = ObservationCache(capacity=10)
        cache.put(k1, {"session": 1})
        cache.put(k2, {"session": 2})
        assert cache.get(k1) == {"session": 1}
        assert cache.get(k2) == {"session": 2}

    def test_observation_key_model(self):
        key = ObservationKey(persistenceIdentity="p", sessionId="s", revision=3)
        assert key.persistence_identity == "p"
        assert key.session_id == "s"
        assert key.revision == 3
        assert key.cache_key() == "p:s:3"
        dumped = key.model_dump(by_alias=True)
        assert "persistenceIdentity" in dumped
        assert "sessionId" in dumped

    def test_put_overwrite_existing(self):
        cache = ObservationCache(capacity=5)
        key = _key()
        cache.put(key, {"v": 1})
        cache.put(key, {"v": 2})
        assert cache.get(key) == {"v": 2}

    def test_capacity_one(self):
        cache = ObservationCache(capacity=1)
        k1, k2 = _key(1), _key(2)
        cache.put(k1, {"v": 1})
        cache.put(k2, {"v": 2})
        assert cache.get(k1) is None
        assert cache.get(k2) is not None

    def test_lease_release_idempotent_safe(self):
        cache = ObservationCache(capacity=5)
        key = _key()
        cache.put(key, {"v": 1})
        lease = cache.acquire_lease(key)
        assert lease is not None
        lease.release()
        lease.release()
        assert cache.get(key) is not None
