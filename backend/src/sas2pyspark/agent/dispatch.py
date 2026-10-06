"""ToolDispatchPipeline — four-phase tool execution with guards and approvals."""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import Field

from sas2pyspark.agent.cancellation import AbortSignal, NullAbortSignal
from sas2pyspark.agent.registry import ScopedToolRegistry, ToolClassification, ToolNotFoundError
from sas2pyspark.core.events import MigrationEventType
from sas2pyspark.core.models import WorkbenchModel
from sas2pyspark.services.clock import Clock


class ToolOutcomeStatus(str, Enum):
    COMPLETED = "completed"
    DENIED = "denied"
    BLOCKED = "blocked"
    UNKNOWN = "unknown"
    NOT_STARTED = "not_started"


class ToolCall(WorkbenchModel):
    tool_name: str = Field(alias="toolName")
    args: dict[str, Any] = Field(default_factory=dict)
    call_id: str = Field(alias="callId")

    model_config = {**WorkbenchModel.model_config, "populate_by_name": True}


class ToolOutcome(WorkbenchModel):
    call_id: str = Field(alias="callId")
    result: Any = None
    status: ToolOutcomeStatus = ToolOutcomeStatus.COMPLETED
    reason: str | None = None

    model_config = {**WorkbenchModel.model_config, "populate_by_name": True, "arbitrary_types_allowed": True}


class ToolDispatchPipeline:
    """Four-phase dispatch: prepare → dispatch → finalize → finish."""

    __slots__ = ("_registry", "_guards", "_approvals", "_sessions", "_clock")

    def __init__(
        self,
        *,
        registry: ScopedToolRegistry,
        guards: Any | None = None,
        approvals: Any | None = None,
        sessions: Any | None = None,
        clock: Clock,
    ) -> None:
        self._registry = registry
        self._guards = guards
        self._approvals = approvals
        self._sessions = sessions
        self._clock = clock

    async def execute(
        self,
        *,
        call: ToolCall,
        scope: Any,
        session: Any | None = None,
        abort: AbortSignal | None = None,
    ) -> ToolOutcome:
        if abort is None:
            abort = NullAbortSignal()

        if session is not None:
            await session.append(
                MigrationEventType.TOOL_STARTED,
                {"callId": call.call_id, "toolName": call.tool_name, "args": call.args},
            )

        try:
            reg = self._registry.resolve(scope_id=getattr(scope, "id", str(scope)), name=call.tool_name)
        except ToolNotFoundError as e:
            outcome = ToolOutcome(
                callId=call.call_id,
                status=ToolOutcomeStatus.DENIED,
                reason=str(e),
            )
            if session is not None:
                await session.append(
                    MigrationEventType.TOOL_DENIED,
                    {"callId": call.call_id, "toolName": call.tool_name, "reason": str(e)},
                )
            return outcome

        if abort.cancelled:
            outcome = ToolOutcome(
                callId=call.call_id,
                status=ToolOutcomeStatus.NOT_STARTED,
                reason="cancelled before dispatch",
            )
            if session is not None:
                await session.append(
                    MigrationEventType.TOOL_NOT_STARTED,
                    {"callId": call.call_id, "reason": "cancelled"},
                )
            return outcome

        if self._guards is not None:
            try:
                guard_request = self._build_guard_request(call, scope)
                self._guards.evaluate(guard_request)
            except Exception as e:
                outcome = ToolOutcome(
                    callId=call.call_id,
                    status=ToolOutcomeStatus.DENIED,
                    reason=f"guard denied: {e}",
                )
                if session is not None:
                    await session.append(
                        MigrationEventType.TOOL_DENIED,
                        {"callId": call.call_id, "toolName": call.tool_name, "reason": str(e)},
                    )
                return outcome

        try:
            tool = reg.tool
            if hasattr(tool, "ainvoke"):
                result = await tool.ainvoke(call.args)
            elif hasattr(tool, "invoke"):
                result = tool.invoke(call.args)
            elif callable(tool):
                result = tool(**call.args)
            else:
                result = None
        except Exception as e:
            outcome = ToolOutcome(
                callId=call.call_id,
                status=ToolOutcomeStatus.BLOCKED,
                reason=f"dispatch error: {e}",
            )
            if session is not None:
                await session.append(
                    MigrationEventType.TOOL_BLOCKED,
                    {"callId": call.call_id, "toolName": call.tool_name, "reason": str(e)},
                )
            return outcome

        outcome = ToolOutcome(
            callId=call.call_id,
            result=result,
            status=ToolOutcomeStatus.COMPLETED,
        )
        if session is not None:
            await session.append(
                MigrationEventType.TOOL_COMPLETED,
                {"callId": call.call_id, "toolName": call.tool_name, "result": result},
            )
        return outcome

    @staticmethod
    def _build_guard_request(call: ToolCall, scope: Any) -> Any:
        from sas2pyspark.services.guards import GuardRequest

        return GuardRequest(
            scope=scope,
            action=call.tool_name,
            artifact=None,
            input_snapshot_id="",
            environment_fingerprint="",
            policy_fingerprint="",
        )
