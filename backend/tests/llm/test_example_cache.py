"""Tests for ContentAddressedExampleCache — SHA-256 keyed cache with TTL, quota reclaim."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from sas2pyspark.llm.cache.example_cache import CacheEntry, CacheKey, ContentAddressedExampleCache
from sas2pyspark.services.clock import FixedClock


@pytest.fixture
def cache_dir(tmp_path: Path) -> Path:
    d = tmp_path / "cache"
    d.mkdir()
    return d


@pytest.fixture
def cache(cache_dir: Path, fixed_clock: FixedClock) -> ContentAddressedExampleCache:
    return ContentAddressedExampleCache(
        root_dir=str(cache_dir),
        clock=fixed_clock,
        ttl_seconds=3600.0,
        refresh_margin_seconds=60.0,
        quota_bytes=100 * 1024 * 1024,
        format_version=1,
    )


def _make_key(scope: str = "scope-a", variant: str = "variant-1") -> CacheKey:
    return CacheKey(scope=scope, variantId=variant)


class TestContentAddressedExampleCache:
    def test_put_then_lookup_round_trip(self, cache: ContentAddressedExampleCache):
        key = _make_key()
        entry = CacheEntry(content=b"hello world", createdAt="2026-01-01T00:00:00.000Z", owner="agent-1")
        cache.put(key, entry, owner="agent-1")
        result = cache.lookup(key)
        assert result is not None
        assert result.content == b"hello world"
        assert result.owner == "agent-1"

    def test_lookup_missing_key_returns_none(self, cache: ContentAddressedExampleCache):
        key = _make_key(scope="nonexistent")
        assert cache.lookup(key) is None

    def test_put_overwrites_existing(self, cache: ContentAddressedExampleCache):
        key = _make_key()
        entry1 = CacheEntry(content=b"v1", createdAt="2026-01-01T00:00:00.000Z", owner="agent-1")
        entry2 = CacheEntry(content=b"v2", createdAt="2026-01-01T00:00:01.000Z", owner="agent-1")
        cache.put(key, entry1, owner="agent-1")
        cache.put(key, entry2, owner="agent-1")
        result = cache.lookup(key)
        assert result is not None
        assert result.content == b"v2"

    def test_different_keys_isolated(self, cache: ContentAddressedExampleCache):
        key_a = _make_key(scope="scope-a")
        key_b = _make_key(scope="scope-b")
        entry = CacheEntry(content=b"data", createdAt="2026-01-01T00:00:00.000Z", owner="agent-1")
        cache.put(key_a, entry, owner="agent-1")
        assert cache.lookup(key_a) is not None
        assert cache.lookup(key_b) is None

    def test_evict_expired_removes_old_entries(self, cache_dir: Path, fixed_clock: FixedClock):
        cache = ContentAddressedExampleCache(
            root_dir=str(cache_dir),
            clock=fixed_clock,
            ttl_seconds=0.1,
            refresh_margin_seconds=0.0,
            format_version=1,
        )
        key = _make_key()
        entry = CacheEntry(content=b"temp", createdAt="2026-01-01T00:00:00.000Z", owner="agent-1")
        cache.put(key, entry, owner="agent-1")
        time.sleep(0.2)
        evicted = cache.evict_expired()
        assert evicted == 1
        assert cache.lookup(key) is None

    def test_evict_expired_keeps_fresh(self, cache: ContentAddressedExampleCache):
        key = _make_key()
        entry = CacheEntry(content=b"fresh", createdAt="2026-01-01T00:00:00.000Z", owner="agent-1")
        cache.put(key, entry, owner="agent-1")
        evicted = cache.evict_expired()
        assert evicted == 0
        assert cache.lookup(key) is not None

    def test_reclaim_quota_oldest_first(self, cache_dir: Path, fixed_clock: FixedClock):
        cache = ContentAddressedExampleCache(
            root_dir=str(cache_dir),
            clock=fixed_clock,
            ttl_seconds=999999,
            refresh_margin_seconds=0,
            quota_bytes=100,
            format_version=1,
        )
        big_content = b"x" * 80
        key_old = _make_key(scope="old")
        key_new = _make_key(scope="new")
        entry_old = CacheEntry(content=big_content, createdAt="2026-01-01T00:00:00.000Z", owner="agent-1")
        entry_new = CacheEntry(content=big_content, createdAt="2026-01-01T00:00:01.000Z", owner="agent-1")
        cache.put(key_old, entry_old, owner="agent-1")
        time.sleep(0.01)
        cache.put(key_new, entry_new, owner="agent-1")
        reclaimed = cache.reclaim_quota("agent-1")
        assert reclaimed >= 1
        assert cache.lookup(key_old) is None

    def test_reclaim_quota_no_reclaim_when_under_budget(self, cache: ContentAddressedExampleCache):
        key = _make_key()
        entry = CacheEntry(content=b"small", createdAt="2026-01-01T00:00:00.000Z", owner="agent-1")
        cache.put(key, entry, owner="agent-1")
        reclaimed = cache.reclaim_quota("agent-1")
        assert reclaimed == 0

    def test_corrupt_index_fail_soft(self, cache_dir: Path, fixed_clock: FixedClock):
        cache = ContentAddressedExampleCache(
            root_dir=str(cache_dir), clock=fixed_clock, format_version=1,
        )
        key = _make_key()
        entry = CacheEntry(content=b"data", createdAt="2026-01-01T00:00:00.000Z", owner="agent-1")
        cache.put(key, entry, owner="agent-1")
        index_path = os.path.join(str(cache_dir), "index-v1.json")
        with open(index_path, "w") as f:
            f.write("{ broken json")
        result = cache.lookup(key)
        assert result is None
        assert os.path.exists(index_path + ".bak")

    def test_format_version_isolation(self, cache_dir: Path, fixed_clock: FixedClock):
        cache_v1 = ContentAddressedExampleCache(
            root_dir=str(cache_dir), clock=fixed_clock, format_version=1,
        )
        cache_v2 = ContentAddressedExampleCache(
            root_dir=str(cache_dir), clock=fixed_clock, format_version=2,
        )
        key = _make_key()
        entry = CacheEntry(content=b"v1-data", createdAt="2026-01-01T00:00:00.000Z", owner="agent-1")
        cache_v1.put(key, entry, owner="agent-1")
        assert cache_v2.lookup(key) is None

    def test_cache_key_model(self):
        key = CacheKey(scope="s", variantId="v")
        assert key.scope == "s"
        assert key.variant_id == "v"
        dumped = key.model_dump(by_alias=True)
        assert "variantId" in dumped

    def test_cache_entry_model(self):
        entry = CacheEntry(content=b"data", createdAt="2026-01-01T00:00:00.000Z", owner="agent")
        assert entry.content == b"data"
        assert entry.owner == "agent"
