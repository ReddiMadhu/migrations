"""Tests for ProjectionCache — write-behind cache with durability barrier."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from sas2pyspark.llm.cache.projection_cache import CheckpointIdentity, ProjectionCache
from sas2pyspark.services.clock import FixedClock


@pytest.fixture
def cache_dir(tmp_path: Path) -> Path:
    d = tmp_path / "proj_cache"
    d.mkdir()
    return d


@pytest.fixture
def cache(cache_dir: Path, fixed_clock: FixedClock) -> ProjectionCache:
    return ProjectionCache(
        root_dir=str(cache_dir),
        clock=fixed_clock,
        format_version=1,
        throttle_count=50,
        throttle_interval_ms=5000.0,
    )


def _identity(fixed_clock: FixedClock) -> CheckpointIdentity:
    return CheckpointIdentity(
        formatVersion=1,
        createdAt=fixed_clock.now(),
        cwd="/workspace",
        isSeeded=True,
        inheritedEventCount=0,
    )


class TestProjectionCache:
    def test_mandatory_checkpoint_then_load(self, cache: ProjectionCache, fixed_clock: FixedClock):
        identity = _identity(fixed_clock)
        projection = {"key": "value", "nested": {"a": 1}}
        cache.checkpoint(
            session_id="s1", identity=identity, projection=projection, mandatory=True,
        )
        loaded = cache.load("s1", current_format_version=1)
        assert loaded == projection

    def test_load_missing_session_returns_none(self, cache: ProjectionCache):
        assert cache.load("nonexistent", current_format_version=1) is None

    def test_format_version_mismatch_discards(self, cache: ProjectionCache, fixed_clock: FixedClock):
        identity = _identity(fixed_clock)
        projection = {"key": "value"}
        cache.checkpoint(
            session_id="s1", identity=identity, projection=projection, mandatory=True,
        )
        loaded = cache.load("s1", current_format_version=2)
        assert loaded is None

    def test_mandatory_flush_bypasses_throttle(self, cache: ProjectionCache, fixed_clock: FixedClock):
        identity = _identity(fixed_clock)
        for i in range(5):
            cache.checkpoint(
                session_id="s1",
                identity=identity,
                projection={"i": i},
                mandatory=True,
            )
        loaded = cache.load("s1", current_format_version=1)
        assert loaded == {"i": 4}

    def test_throttle_between_checkpoints(self, cache_dir: Path, fixed_clock: FixedClock):
        cache = ProjectionCache(
            root_dir=str(cache_dir),
            clock=fixed_clock,
            format_version=1,
            throttle_count=100,
            throttle_interval_ms=100000.0,
        )
        identity = _identity(fixed_clock)
        cache.checkpoint(
            session_id="s1", identity=identity, projection={"first": True}, mandatory=True,
        )
        cache.checkpoint(
            session_id="s1", identity=identity, projection={"second": True}, mandatory=False,
        )
        loaded = cache.load("s1", current_format_version=1)
        assert loaded == {"first": True}

    def test_corrupt_record_fail_soft(self, cache: ProjectionCache, fixed_clock: FixedClock):
        identity = _identity(fixed_clock)
        cache.checkpoint(
            session_id="s1", identity=identity, projection={"key": "value"}, mandatory=True,
        )
        record_path = os.path.join(cache._root, "s1.json")
        with open(record_path, "w") as f:
            f.write("{ broken json")
        loaded = cache.load("s1", current_format_version=1)
        assert loaded is None
        assert os.path.exists(record_path + ".bak")

    def test_flush_clears_state(self, cache: ProjectionCache, fixed_clock: FixedClock):
        identity = _identity(fixed_clock)
        cache.checkpoint(
            session_id="s1", identity=identity, projection={"key": "value"}, mandatory=True,
        )
        cache.flush("s1")
        assert "s1" not in cache._event_counts
        assert "s1" not in cache._last_flush

    def test_one_record_per_session(self, cache: ProjectionCache, fixed_clock: FixedClock):
        identity = _identity(fixed_clock)
        for i in range(3):
            cache.checkpoint(
                session_id="s1",
                identity=identity,
                projection={"i": i},
                mandatory=True,
            )
        record_path = os.path.join(cache._root, "s1.json")
        assert os.path.exists(record_path)
        with open(record_path) as f:
            record = json.load(f)
        assert record["projection"] == {"i": 2}

    def test_multiple_sessions_isolated(self, cache: ProjectionCache, fixed_clock: FixedClock):
        identity = _identity(fixed_clock)
        cache.checkpoint(session_id="s1", identity=identity, projection={"s": 1}, mandatory=True)
        cache.checkpoint(session_id="s2", identity=identity, projection={"s": 2}, mandatory=True)
        assert cache.load("s1", current_format_version=1) == {"s": 1}
        assert cache.load("s2", current_format_version=1) == {"s": 2}

    def test_checkpoint_identity_model(self, fixed_clock: FixedClock):
        identity = CheckpointIdentity(
            formatVersion=2,
            createdAt="2026-01-01T00:00:00.000Z",
            cwd="/workspace",
            isSeeded=True,
            inheritedEventCount=5,
        )
        assert identity.format_version == 2
        assert identity.is_seeded is True
        assert identity.inherited_event_count == 5
        dumped = identity.model_dump(by_alias=True)
        assert "formatVersion" in dumped
        assert "createdAt" in dumped
        assert "isSeeded" in dumped
        assert "inheritedEventCount" in dumped

    def test_checkpoint_json_uses_canonical_form(self, cache: ProjectionCache, fixed_clock: FixedClock):
        identity = _identity(fixed_clock)
        projection = {"b": 2, "a": 1}
        cache.checkpoint(
            session_id="s1", identity=identity, projection=projection, mandatory=True,
        )
        record_path = os.path.join(cache._root, "s1.json")
        with open(record_path) as f:
            content = f.read()
        assert '"a":1' in content
        assert '"b":2' in content
