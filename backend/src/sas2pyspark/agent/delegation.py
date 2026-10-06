"""DelegationController — multi-agent delegation with depth budget."""

from __future__ import annotations

from typing import Any, Callable

from sas2pyspark.agent.cancellation import AbortSignal, NullAbortSignal
from sas2pyspark.agent.driver import AgentDriver, AgentResult
from sas2pyspark.core.events import MigrationEventType


class DepthExceededError(Exception):
    def __init__(self, depth: int, cap: int) -> None:
        self.depth = depth
        self.cap = cap
        super().__init__(f"delegation depth {depth} exceeds cap {cap}")


class DelegationController:
    """Delegate to a child agent with depth tracking and pinned approval."""

    __slots__ = ("_driver_factory", "_depth_cap")

    def __init__(
        self,
        *,
        driver_factory: Callable[..., AgentDriver],
        depth_cap: int = 3,
    ) -> None:
        self._driver_factory = driver_factory
        self._depth_cap = depth_cap

    async def delegate(
        self,
        *,
        parent: AgentDriver,
        scope: Any,
        session: Any,
        inbox: list[Any],
        sandbox_override: Any | None = None,
        abort: AbortSignal | None = None,
    ) -> AgentResult:
        if abort is None:
            abort = NullAbortSignal()

        child_depth = parent.depth + 1
        if child_depth > self._depth_cap:
            raise DepthExceededError(child_depth, self._depth_cap)

        await session.append(
            MigrationEventType.DELEGATION_STARTED,
            {"childDepth": child_depth, "parentDepth": parent.depth},
        )

        child_driver = self._driver_factory(depth=child_depth, depth_cap=self._depth_cap)

        try:
            result = await child_driver.kick(
                session=session, scope=scope, inbox=inbox, abort=abort,
            )
        except Exception:
            from sas2pyspark.agent.crash_repair import CrashRepairSynthesizer
            from sas2pyspark.services.clock import SystemClock

            repairer = CrashRepairSynthesizer(clock=SystemClock())
            await repairer.repair(session)
            result = AgentResult(interrupted=True)

        await session.append(
            MigrationEventType.DELEGATION_COMPLETED,
            {"childDepth": child_depth, "interrupted": result.interrupted},
        )

        return result
