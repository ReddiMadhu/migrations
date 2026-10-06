"""Tests for ObservationGate — read-before-edit policy."""

from __future__ import annotations

import pytest

from sas2pyspark.core.errors import FsError, FsErrorCode
from sas2pyspark.services.file_edit.cas import FsVersion
from sas2pyspark.services.file_edit.observation import ObservationGate, WriteIntent


class TestObservationGate:
    def test_edit_without_read_raises(self):
        gate = ObservationGate()
        with pytest.raises(FsError) as exc_info:
            gate.edit_intent("file.py")
        assert exc_info.value.code == FsErrorCode.FS_NOT_OBSERVED

    def test_edit_after_read_succeeds(self):
        gate = ObservationGate()
        version = FsVersion(value="sha256:abc")
        gate.record_read(target="file.py", version=version)
        result = gate.edit_intent("file.py")
        assert result.value == "sha256:abc"

    def test_edit_after_absent_raises_not_found(self):
        gate = ObservationGate()
        gate.record_absent(target="file.py")
        with pytest.raises(FsError) as exc_info:
            gate.edit_intent("file.py")
        assert exc_info.value.code == FsErrorCode.FS_NOT_FOUND

    def test_write_intent_for_unseen_returns_create_if_absent(self):
        gate = ObservationGate()
        assert gate.write_intent("file.py") == WriteIntent.CREATE_IF_ABSENT

    def test_write_intent_for_absent_returns_create_if_absent(self):
        gate = ObservationGate()
        gate.record_absent(target="file.py")
        assert gate.write_intent("file.py") == WriteIntent.CREATE_IF_ABSENT

    def test_write_intent_for_present_returns_replace_if_version(self):
        gate = ObservationGate()
        gate.record_read(target="file.py", version=FsVersion(value="sha256:abc"))
        assert gate.write_intent("file.py") == WriteIntent.REPLACE_IF_VERSION
