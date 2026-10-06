"""CancellationFuser — fuses abort signals from caller, wrapper, and factory."""

from __future__ import annotations

import asyncio
import threading
from typing import Callable, Protocol, runtime_checkable


@runtime_checkable
class AbortSignal(Protocol):
    @property
    def cancelled(self) -> bool: ...

    def on_cancel(self, callback: Callable[[], None]) -> None: ...


class _FusedSignal:
    __slots__ = ("_cancelled", "_lock", "_callbacks")

    def __init__(self) -> None:
        self._cancelled = False
        self._lock = threading.Lock()
        self._callbacks: list[Callable[[], None]] = []

    @property
    def cancelled(self) -> bool:
        with self._lock:
            return self._cancelled

    def cancel(self) -> None:
        with self._lock:
            if self._cancelled:
                return
            self._cancelled = True
            callbacks = list(self._callbacks)
        for cb in callbacks:
            try:
                cb()
            except Exception:
                pass

    def on_cancel(self, callback: Callable[[], None]) -> None:
        with self._lock:
            if self._cancelled:
                callback()
                return
            self._callbacks.append(callback)


class CancellationFuser:
    """Fuse multiple abort signals — any source triggers cancellation."""

    @staticmethod
    def fuse(*signals: AbortSignal | None) -> AbortSignal:
        fused = _FusedSignal()

        for signal in signals:
            if signal is None:
                continue
            if signal.cancelled:
                fused.cancel()
                continue
            signal.on_cancel(lambda f=fused: f.cancel())

        return fused


class NullAbortSignal:
    """Abort signal that never cancels."""

    __slots__ = ()

    @property
    def cancelled(self) -> bool:
        return False

    def on_cancel(self, callback: Callable[[], None]) -> None:
        pass


class ManualAbortSignal:
    """Abort signal that can be manually cancelled for testing."""

    __slots__ = ("_signal",)

    def __init__(self) -> None:
        self._signal = _FusedSignal()

    @property
    def cancelled(self) -> bool:
        return self._signal.cancelled

    def cancel(self) -> None:
        self._signal.cancel()

    def on_cancel(self, callback: Callable[[], None]) -> None:
        self._signal.on_cancel(callback)
