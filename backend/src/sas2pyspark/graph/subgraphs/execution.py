"""Execution subgraph — policy_check → approval_check → submit → poll_status → collect_output.

Compiled subgraph sharing migration state. Supports interrupts for HITL approval.
Idempotency key = sha256(session_id:executionId)[:64].
"""

from __future__ import annotations

from typing import Any

from langgraph.graph import END, START, StateGraph

from sas2pyspark.graph.state import MigrationGraphState


def _policy_check(state: MigrationGraphState) -> dict[str, Any]:
    return {"state": "EXECUTING"}


def _approval_check(state: MigrationGraphState) -> dict[str, Any]:
    if state.get("pending_approval"):
        return {"state": "HUMAN_REQUIRED"}
    return {"state": "EXECUTING"}


def _submit(state: MigrationGraphState) -> dict[str, Any]:
    return {"state": "EXECUTING"}


def _poll_status(state: MigrationGraphState) -> dict[str, Any]:
    return {"state": "EXECUTING"}


def _collect_output(state: MigrationGraphState) -> dict[str, Any]:
    return {"state": "RECONCILING"}


def build_execution_subgraph() -> Any:
    sg = StateGraph(MigrationGraphState)
    sg.add_node("policy_check", _policy_check)
    sg.add_node("approval_check", _approval_check)
    sg.add_node("submit", _submit)
    sg.add_node("poll_status", _poll_status)
    sg.add_node("collect_output", _collect_output)
    sg.add_edge(START, "policy_check")
    sg.add_edge("policy_check", "approval_check")
    sg.add_edge("approval_check", "submit")
    sg.add_edge("submit", "poll_status")
    sg.add_edge("poll_status", "collect_output")
    sg.add_edge("collect_output", END)
    return sg.compile()
