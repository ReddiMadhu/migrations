"""Deterministic clock — domain code never calls time.time() / uuid4() directly."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4
from typing import Protocol

_BASE_EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)


class Clock(Protocol):
    def now(self) -> str: ...
    def uuid(self) -> str: ...


class SystemClock:
    def now(self) -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def uuid(self) -> str:
        return str(uuid4())


class FixedClock:
    """Deterministic clock replicating the TS FixedClock tick sequence for tests.

    Seconds wrap modulo 60 with carry into minutes/hours/days so timestamps
    are always valid ISO-8601 regardless of tick count.
    """

    def __init__(self) -> None:
        self._tick = 0

    def now(self) -> str:
        self._tick += 1
        t = self._tick
        seconds = t % 60
        minutes = (t // 60) % 60
        hours = (t // 3600) % 24
        day_offset = t // 86400
        date = (_BASE_EPOCH + timedelta(days=day_offset)).strftime("%Y-%m-%d")
        return f"{date}T{hours:02d}:{minutes:02d}:{seconds:02d}.000Z"

    def uuid(self) -> str:
        self._tick += 1
        return f"00000000-0000-4000-8000-{self._tick % 10**12:012d}"
