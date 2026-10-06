"""Tests for CasVersioner — compare-and-swap versioning."""

from __future__ import annotations

import pytest

from sas2pyspark.core.errors import FsError, FsErrorCode
from sas2pyspark.services.file_edit.cas import CasVersioner, FsVersion


class TestCasVersioner:
    def test_version_for_same_content_same_version(self):
        v1 = CasVersioner.version_for(content=b"hello")
        v2 = CasVersioner.version_for(content=b"hello")
        assert v1.value == v2.value
        assert v1.value.startswith("sha256:")

    def test_version_for_different_content_different_version(self):
        v1 = CasVersioner.version_for(content=b"hello")
        v2 = CasVersioner.version_for(content=b"world")
        assert v1.value != v2.value

    def test_verify_matching_version_passes(self):
        v = CasVersioner.version_for(content=b"hello")
        CasVersioner.verify(expected=v, current=v)

    def test_verify_stale_version_raises(self):
        expected = CasVersioner.version_for(content=b"old")
        current = CasVersioner.version_for(content=b"new")
        with pytest.raises(FsError) as exc_info:
            CasVersioner.verify(expected=expected, current=current)
        assert exc_info.value.code == FsErrorCode.FS_STALE_VERSION

    def test_verify_none_expected_skips_check(self):
        current = CasVersioner.version_for(content=b"hello")
        CasVersioner.verify(expected=None, current=current)

    def test_fs_version_is_frozen(self):
        v = FsVersion(value="sha256:abc")
        with pytest.raises(Exception):
            v.value = "sha256:xyz"
