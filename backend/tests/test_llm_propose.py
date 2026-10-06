"""Translation contract tests that do not require an external model provider."""

from __future__ import annotations

from sas2pyspark.llm.prompts import SYSTEM_PREFIX, prompt_payload
from sas2pyspark.graph.nodes.guard import make_guard_node
from sas2pyspark.services.artifacts import artifact_reference
from .conftest import make_scopes


def test_prompt_states_proposal_authority_and_uses_separate_context_suffix():
    assert "you do not certify" in SYSTEM_PREFIX.lower()
    payload = prompt_payload(
        source="data out; set in; run;",
        model_context={"nodes": [{"nodeId": "ir-n1"}]},
        reconciliation={"differenceCount": 2},
    )
    assert "data out" in payload["source"]
    assert '"nodeId":"ir-n1"' in payload["model_context"]
    assert '"differenceCount":2' in payload["reconciliation"]


async def test_guard_accepts_static_pyspark_candidate(workbench):
    _, _, _, scope = make_scopes(workbench)
    artifact = workbench.artifacts.create(
        scope=scope,
        data="from pyspark.sql import functions as F\nresult = spark.range(2)\n",
        media_type="text/x-python",
        created_by="guard-test",
    )
    result = await make_guard_node(workbench)({
        "scope_id": scope.id,
        "candidate_artifact": artifact_reference(artifact).model_dump(by_alias=True),
        "repair_history": [],
        "repair_iteration": 0,
    })

    assert result["state"] == "CANDIDATE_READY"


async def test_guard_rejects_network_import_and_secret_url(workbench):
    _, _, _, scope = make_scopes(workbench)
    artifact = workbench.artifacts.create(
        scope=scope,
        data="import requests\nTOKEN = 'https://user:password@example.invalid/api'\n",
        media_type="text/x-python",
        created_by="guard-test",
    )
    result = await make_guard_node(workbench)({
        "scope_id": scope.id,
        "candidate_artifact": artifact_reference(artifact).model_dump(by_alias=True),
        "repair_history": [],
        "repair_iteration": 0,
    })

    assert result["state"] == "HUMAN_REQUIRED"
    assert "forbidden" in result["error"]