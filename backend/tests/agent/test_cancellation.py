"""Tests for CancellationFuser — fused abort signals."""

from __future__ import annotations

import pytest

from sas2pyspark.agent.cancellation import (
    CancellationFuser,
    ManualAbortSignal,
    NullAbortSignal,
)


class TestCancellationFuser:
    def test_null_signal_never_cancels(self):
        signal = NullAbortSignal()
        assert signal.cancelled is False

    def test_manual_signal_cancels(self):
        signal = ManualAbortSignal()
        assert signal.cancelled is False
        signal.cancel()
        assert signal.cancelled is True

    def test_fuse_no_signals(self):
        fused = CancellationFuser.fuse()
        assert fused.cancelled is False

    def test_fuse_null_signals(self):
        fused = CancellationFuser.fuse(NullAbortSignal(), NullAbortSignal())
        assert fused.cancelled is False

    def test_fuse_one_cancelled(self):
        s1 = ManualAbortSignal()
        s2 = NullAbortSignal()
        s1.cancel()
        fused = CancellationFuser.fuse(s1, s2)
        assert fused.cancelled is True

    def test_fuse_cancel_after_fuse(self):
        s1 = ManualAbortSignal()
        s2 = ManualAbortSignal()
        fused = CancellationFuser.fuse(s1, s2)
        assert fused.cancelled is False
        s1.cancel()
        assert fused.cancelled is True

    def test_fuse_any_source_triggers(self):
        s1 = ManualAbortSignal()
        s2 = ManualAbortSignal()
        s3 = ManualAbortSignal()
        fused = CancellationFuser.fuse(s1, s2, s3)
        s2.cancel()
        assert fused.cancelled is True

    def test_fuse_none_signals_ignored(self):
        s1 = ManualAbortSignal()
        fused = CancellationFuser.fuse(None, s1, None)
        assert fused.cancelled is False
        s1.cancel()
        assert fused.cancelled is True

    def test_on_cancel_callback_fired(self):
        s1 = ManualAbortSignal()
        fused = CancellationFuser.fuse(s1)
        called = []
        fused.on_cancel(lambda: called.append(True))
        assert called == []
        s1.cancel()
        assert called == [True]

    def test_on_cancel_fired_if_already_cancelled(self):
        s1 = ManualAbortSignal()
        s1.cancel()
        fused = CancellationFuser.fuse(s1)
        called = []
        fused.on_cancel(lambda: called.append(True))
        assert called == [True]

    def test_manual_signal_on_cancel(self):
        s1 = ManualAbortSignal()
        called = []
        s1.on_cancel(lambda: called.append(True))
        s1.cancel()
        assert called == [True]
