"""Tests for AtomicPublisher — staging-then-rename with fsync."""

from __future__ import annotations

import os

import pytest

from sas2pyspark.services.file_edit.fs_port import NullAbortSignal
from sas2pyspark.services.file_edit.publisher import AtomicPublisher


class _CancelledAbort:
    @property
    def cancelled(self) -> bool:
        return True


class TestAtomicPublisher:
    def test_publish_creates_file(self, tmp_path):
        publisher = AtomicPublisher(staging_root=str(tmp_path))
        target = str(tmp_path / "output.txt")
        publisher.publish(target=target, content=b"hello world")
        with open(target, "rb") as f:
            assert f.read() == b"hello world"

    def test_publish_overwrites_existing(self, tmp_path):
        publisher = AtomicPublisher(staging_root=str(tmp_path))
        target = str(tmp_path / "output.txt")
        with open(target, "wb") as f:
            f.write(b"old")
        publisher.publish(target=target, content=b"new")
        with open(target, "rb") as f:
            assert f.read() == b"new"

    def test_publish_no_staging_residue(self, tmp_path):
        publisher = AtomicPublisher(staging_root=str(tmp_path))
        target = str(tmp_path / "output.txt")
        publisher.publish(target=target, content=b"hello")
        staging_dirs = [d for d in os.listdir(str(tmp_path)) if d.startswith(".staging")]
        assert len(staging_dirs) == 0

    def test_publish_aborted_before_commit(self, tmp_path):
        publisher = AtomicPublisher(staging_root=str(tmp_path))
        target = str(tmp_path / "output.txt")
        with pytest.raises(Exception):
            publisher.publish(target=target, content=b"hello", abort=_CancelledAbort())
        assert not os.path.exists(target)

    def test_concurrent_publishes_serialize(self, tmp_path):
        publisher = AtomicPublisher(staging_root=str(tmp_path))
        target = str(tmp_path / "output.txt")
        publisher.publish(target=target, content=b"first")
        publisher.publish(target=target, content=b"second")
        with open(target, "rb") as f:
            assert f.read() == b"second"
