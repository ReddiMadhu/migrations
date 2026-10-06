"""Guarded model proposal node for SAS-to-PySpark translation."""

from __future__ import annotations

from typing import Any

from sas2pyspark.core.events import MigrationEventType
from sas2pyspark.core.logging import BudgetExceeded, log_model_usage
from sas2pyspark.graph.state import MigrationGraphState
from sas2pyspark.services.artifacts import ArtifactReferenceData, artifact_reference


def _usage_cost_units(usage: Any) -> float:
    if not isinstance(usage, dict):
        return 0.0
    for key in ("cost_units", "costUnits", "cost"):
        value = usage.get(key)
        if value is not None:
            try:
                return max(0.0, float(value))
            except (TypeError, ValueError):
                return 0.0
    return 0.0


def make_propose_node(runtime: Any):
    async def propose_translation(state: MigrationGraphState) -> dict[str, Any]:
        translator = getattr(runtime, "translator", None)
        source_ref = state.get("sas_source_artifact")
        if translator is None or source_ref is None:
            return {
                "state": "HUMAN_REQUIRED",
                "error": "translation provider or SAS source artifact is unavailable",
            }

        scope = runtime.scopes.require_by_id(state.get("scope_id", ""))
        source = runtime.artifacts.read_reference(scope, ArtifactReferenceData(**source_ref)).decode("utf-8")
        translate = getattr(translator, "translate", None)
        if not callable(translate):
            return {"state": "HUMAN_REQUIRED", "error": "translation provider has no translate method"}

        try:
            candidate = translate(
                source=source,
                ir_snapshot={
                    **(state.get("ir_snapshot") or {}),
                    "parsedFiles": state.get("parser_results") or {},
                },
                macro_resolution=state.get("macro_resolution") or {},
                model_context=state.get("model_context") or {},
            )
            usage = getattr(candidate, "usage_metadata", None)
            if isinstance(usage, dict):
                runtime.token_meter.record(usage, cost_units=_usage_cost_units(usage))
                budget_policy = getattr(runtime, "token_budget_policy", None)
                if budget_policy is not None:
                    runtime.token_meter.check_budget(budget_policy)
                log_model_usage(
                    session_id=state.get("session_id", ""),
                    usage=runtime.token_meter.snapshot(),
                )
            if hasattr(candidate, "content"):
                candidate = candidate.content
            if not isinstance(candidate, str) or not candidate.strip():
                return {"state": "HUMAN_REQUIRED", "error": "translation provider returned no PySpark source"}
        except BudgetExceeded as exc:
            session = await runtime.sessions.open(state.get("session_id", ""))
            await session.append(MigrationEventType.MIGRATION_ESCALATED, {
                "reasonCode": "BUDGET_EXCEEDED",
                "detail": exc.message,
                "usage": exc.details.get("usage", runtime.token_meter.snapshot()),
                "policy": exc.details.get("policy"),
                "workUnitId": (state.get("work_unit") or {}).get("workUnitId"),
            })
            await session.flush()
            return {"state": "HUMAN_REQUIRED", "error": exc.message}

        record = runtime.artifacts.create(
            scope=scope,
            data=candidate,
            media_type="text/x-python",
            created_by="llm-translation-proposal",
            parent_artifact=ArtifactReferenceData(**source_ref),
        )
        return {
            "state": "TRANSLATING",
            "candidate_artifact": artifact_reference(record).model_dump(by_alias=True),
            "candidate_artifact_record": record.to_dict(),
        }

    return propose_translation