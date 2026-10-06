"""AgentDriver — phase machine, turn runner, step runner for the agent loop."""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import Field

from sas2pyspark.agent.cancellation import AbortSignal, CancellationFuser, NullAbortSignal
from sas2pyspark.agent.dispatch import ToolCall, ToolDispatchPipeline, ToolOutcome
from sas2pyspark.agent.prompt_assembler import PromptAssembler
from sas2pyspark.core.events import MigrationEventType
from sas2pyspark.core.logging import TokenBudgetPolicy, TokenMeter
from sas2pyspark.core.models import WorkbenchModel
from sas2pyspark.services.clock import Clock


class DriverPhase(str, Enum):
    IDLE = "idle"
    MAINTENANCE = "maintenance"
    RUNNING = "running"


class AgentResult(WorkbenchModel):
    content: Any = None
    turn_count: int = Field(default=0, alias="turnCount")
    step_count: int = Field(default=0, alias="stepCount")
    interrupted: bool = False
    budget_exceeded: bool = Field(default=False, alias="budgetExceeded")
    advisory: bool = True

    model_config = {**WorkbenchModel.model_config, "populate_by_name": True, "arbitrary_types_allowed": True}


class StepRunner:
    """One LLM call → tool execution cycle."""

    __slots__ = ("_dispatch", "_prompt_ordering", "_assembler")

    def __init__(
        self,
        *,
        dispatch: ToolDispatchPipeline,
        prompt_ordering: Any | None = None,
        assembler: PromptAssembler | None = None,
    ) -> None:
        self._dispatch = dispatch
        self._prompt_ordering = prompt_ordering
        self._assembler = assembler

    async def step(
        self,
        *,
        session: Any,
        scope: Any,
        inbox: list[ToolCall],
        abort: AbortSignal | None = None,
    ) -> list[ToolOutcome]:
        if abort is not None and abort.cancelled:
            return []
        outcomes: list[ToolOutcome] = []
        for call in inbox:
            if abort is not None and abort.cancelled:
                break
            outcome = await self._dispatch.execute(
                call=call, scope=scope, session=session, abort=abort,
            )
            outcomes.append(outcome)
        return outcomes


class TurnRunner:
    """Loop step() until inbox empties or budget/cancellation halts."""

    __slots__ = ("_step_runner",)

    def __init__(self, *, step_runner: StepRunner) -> None:
        self._step_runner = step_runner

    async def turn(
        self,
        *,
        session: Any,
        scope: Any,
        inbox: list[ToolCall],
        abort: AbortSignal | None = None,
        budget_policy: TokenBudgetPolicy | None = None,
        token_meter: TokenMeter | None = None,
    ) -> tuple[list[ToolOutcome], bool]:
        all_outcomes: list[ToolOutcome] = []
        budget_exceeded = False
        while inbox:
            if abort is not None and abort.cancelled:
                break
            if budget_policy is not None and token_meter is not None:
                try:
                    token_meter.check_budget(budget_policy)
                except Exception:
                    budget_exceeded = True
                    break
            batch = inbox[:]
            inbox.clear()
            outcomes = await self._step_runner.step(
                session=session, scope=scope, inbox=batch, abort=abort,
            )
            all_outcomes.extend(outcomes)
        return all_outcomes, budget_exceeded


class AgentDriver:
    """Phase machine: idle → maintenance → running → idle."""

    __slots__ = (
        "_turn_runner",
        "_sessions",
        "_clock",
        "_token_meter",
        "_budget_policy",
        "_depth",
        "_depth_cap",
    )

    def __init__(
        self,
        *,
        turn_runner: TurnRunner,
        sessions: Any | None = None,
        clock: Clock,
        token_meter: TokenMeter | None = None,
        budget_policy: TokenBudgetPolicy | None = None,
        depth: int = 0,
        depth_cap: int = 3,
    ) -> None:
        self._turn_runner = turn_runner
        self._sessions = sessions
        self._clock = clock
        self._token_meter = token_meter or TokenMeter()
        self._budget_policy = budget_policy
        self._depth = depth
        self._depth_cap = depth_cap

    @property
    def depth(self) -> int:
        return self._depth

    @property
    def depth_cap(self) -> int:
        return self._depth_cap

    async def kick(
        self,
        *,
        session: Any,
        scope: Any,
        inbox: list[ToolCall],
        abort: AbortSignal | None = None,
    ) -> AgentResult:
        if abort is None:
            abort = NullAbortSignal()

        await session.append(MigrationEventType.AGENT_STARTED, {"depth": self._depth})
        await session.append(MigrationEventType.AGENT_TURN_STARTED, {"turn": 1})

        outcomes, budget_exceeded = await self._turn_runner.turn(
            session=session,
            scope=scope,
            inbox=inbox,
            abort=abort,
            budget_policy=self._budget_policy,
            token_meter=self._token_meter,
        )

        interrupted = abort.cancelled
        turn_end_reason = "interrupted" if interrupted else ("budget-exceeded" if budget_exceeded else "completed")

        await session.append(
            MigrationEventType.AGENT_TURN_ENDED,
            {"reason": turn_end_reason, "outcomeCount": len(outcomes)},
        )
        await session.append(MigrationEventType.AGENT_IDLE, {"depth": self._depth})

        return AgentResult(
            content=outcomes,
            turnCount=1,
            stepCount=len(outcomes),
            interrupted=interrupted,
            budgetExceeded=budget_exceeded,
        )
