"""Tests for FileSystemPort — protocol and LocalFileSystem adapter."""

from __future__ import annotations

import os

from sas2pyspark.services.file_edit.fs_port import LocalFileSystem, NullAbortSignal


class TestLocalFileSystem:
    def test_write_and_read_bytes(self, tmp_path):
        fs = LocalFileSystem()
        path = str(tmp_path / "test.txt")
        fs.write_bytes(path, b"hello")
        assert fs.read_bytes(path) == b"hello"

    def test_exists(self, tmp_path):
        fs = LocalFileSystem()
        path = str(tmp_path / "test.txt")
        assert not fs.exists(path)
        fs.write_bytes(path, b"hello")
        assert fs.exists(path)

    def test_stat(self, tmp_path):
        fs = LocalFileSystem()
        path = str(tmp_path / "test.txt")
        fs.write_bytes(path, b"hello")
        st = fs.stat(path)
        assert st.st_size == 5


class TestNullAbortSignal:
    def test_never_cancelled(self):
        signal = NullAbortSignal()
        assert signal.cancelled is False
