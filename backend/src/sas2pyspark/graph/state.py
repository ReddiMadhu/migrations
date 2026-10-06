"""MigrationGraphState — typed state channels for the LangGraph migration graph."""

from __future__ import annotations

from typing import Annotated, Any, TypedDict

from langgraph.graph.message import add_messages


def _merge_dict(left: dict[str, Any] | None, right: dict[str, Any] | None) -> dict[str, Any]:
    result = dict(left or {})
    result.update(right or {})
    return result


def _append_list(left: list[Any] | None, right: list[Any] | None) -> list[Any]:
    return list(left or []) + list(right or [])


def _replace(left: Any, right: Any) -> Any:
    return right if right is not None else left


class MigrationGraphState(TypedDict, total=False):
    session_id: str
    migration_id: str
    scope_id: str
    state: str
    sas_source_artifact: Annotated[dict[str, Any] | None, _replace]
    sas_source_path: str
    sas_dependency_artifacts: list[dict[str, Any]]
    dependency_graph: Annotated[dict[str, Any] | None, _replace]
    discovery: Annotated[dict[str, Any] | None, _replace]
    candidate_artifact: Annotated[dict[str, Any] | None, _replace]
    candidate_artifact_record: Annotated[dict[str, Any] | None, _replace]
    input_snapshot_id: str
    environment_fingerprint: str
    reference_input_path: Annotated[str | None, _replace]
    reference_output_path: Annotated[str | None, _replace]
    ir_snapshot: Annotated[dict[str, Any] | None, _replace]
    parser_results: Annotated[dict[str, Any] | None, _replace]
    macro_resolution: Annotated[dict[str, Any] | None, _replace]
    work_unit: Annotated[dict[str, Any] | None, _replace]
    model_context: Annotated[dict[str, Any] | None, _replace]
    last_reconciliation_result: Annotated[dict[str, Any] | None, _replace]
    repair_history: Annotated[list[dict[str, Any]], _append_list]
    repair_iteration: Annotated[int, _replace]
    open_execution_ids: Annotated[list[str], _append_list]
    execution_outcomes: Annotated[dict[str, Any], _merge_dict]
    execution_outcome: Annotated[dict[str, Any] | None, _replace]
    messages: Annotated[list, add_messages]
    error: Annotated[str | None, _replace]
    pending_approval: Annotated[bool, _replace]
    approval_request: Annotated[dict[str, Any] | None, _replace]
    approval_receipt: Annotated[dict[str, Any] | None, _replace]
    pending_operator_decision: Annotated[bool, _replace]
    operator_decision_request: Annotated[dict[str, Any] | None, _replace]
    operator_decision: Annotated[dict[str, Any] | None, _replace]
    diagnosis: Annotated[dict[str, Any] | None, _replace]
    repair_proposal: Annotated[dict[str, Any] | None, _replace]
    driver_phase: Annotated[str | None, _replace]
    turn_count: Annotated[int | None, _replace]
    step_count: Annotated[int | None, _replace]
    agent_depth: Annotated[int | None, _replace]
    prompt_assembly: Annotated[dict[str, Any] | None, _replace]
    cache_stats: Annotated[dict[str, Any], _merge_dict]
    tool_results: Annotated[list[dict[str, Any]], _append_list]
    abort_signal: Annotated[Any | None, _replace]
    compaction_history: Annotated[list[dict[str, Any]], _append_list]
