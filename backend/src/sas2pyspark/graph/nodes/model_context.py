"""assemble_model_context node — deterministic bounded prompt context."""

from __future__ import annotations

from typing import Any

from sas2pyspark.graph.state import MigrationGraphState


def make_model_context_node(runtime: Any):
    async def assemble_model_context(state: MigrationGraphState) -> dict[str, Any]:
        work_unit = state.get("work_unit") or {}
        if not work_unit.get("workUnitId"):
            return {"state": "HUMAN_REQUIRED", "error": "no durable work unit is available"}
        session = await runtime.sessions.open(state.get("session_id", ""))
        context = runtime.model_context.assemble(
            session=session,
            work_unit_id=work_unit["workUnitId"],
        )
        return {
            "state": "CONTEXT_ASSEMBLED",
            "model_context": context.to_dict(),
        }
    return assemble_model_context
