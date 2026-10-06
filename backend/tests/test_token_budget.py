"""P1-6: Token Budget Enforcement."""

from __future__ import annotations

import pytest

from sas2pyspark.core.logging import TokenMeter, TokenBudgetPolicy, BudgetExceeded


def test_check_budget_raises_on_cost_exceed():
    meter = TokenMeter()
    meter.record({"input_tokens": 100}, cost_units=5.0)
    policy = TokenBudgetPolicy(maximumCostUnits=3.0)
    with pytest.raises(BudgetExceeded) as exc_info:
        meter.check_budget(policy)
    assert "cost" in exc_info.value.message


def test_check_budget_raises_on_token_exceed():
    meter = TokenMeter()
    meter.record({"input_tokens": 500, "completion_tokens": 200})
    policy = TokenBudgetPolicy(maximumPromptTokens=400)
    with pytest.raises(BudgetExceeded) as exc_info:
        meter.check_budget(policy)
    assert "prompt" in exc_info.value.message


def test_check_budget_raises_on_completion_exceed():
    meter = TokenMeter()
    meter.record({"input_tokens": 100, "completion_tokens": 300})
    policy = TokenBudgetPolicy(maximumCompletionTokens=200)
    with pytest.raises(BudgetExceeded) as exc_info:
        meter.check_budget(policy)
    assert "completion" in exc_info.value.message


def test_check_budget_raises_on_total_exceed():
    meter = TokenMeter()
    meter.record({"input_tokens": 300, "completion_tokens": 300})
    policy = TokenBudgetPolicy(maximumTotalTokens=500)
    with pytest.raises(BudgetExceeded) as exc_info:
        meter.check_budget(policy)
    assert "total" in exc_info.value.message


def test_cost_units_populated():
    meter = TokenMeter()
    meter.record({"input_tokens": 100}, cost_units=0.002)
    snapshot = meter.snapshot()
    assert snapshot["costUnits"] == 0.002


def test_budget_exceeded_routes_human_required(test_dir, fixed_clock):
    from sas2pyspark.workbench import WorkbenchConfig, create_workbench

    async def _run():
        runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
        try:
            from .conftest import make_scopes, make_session
            _, _, _, candidate = make_scopes(runtime)
            session = await make_session(runtime, scope=candidate)

            runtime.token_budget_policy = TokenBudgetPolicy(maximumCostUnits=1.0)

            class MockCandidate:
                content = "result = spark.range(10)"
                usage_metadata = {"input_tokens": 100, "cost_units": 5.0}

            class MockTranslator:
                def translate(self, **kwargs):
                    return MockCandidate()

            runtime.translator = MockTranslator()
            runtime.token_meter.record({"input_tokens": 999}, cost_units=0.5)

            from sas2pyspark.graph.nodes.propose import make_propose_node
            propose = make_propose_node(runtime)

            scope = runtime.scopes.require_by_id(candidate.id)
            artifact = runtime.artifacts.create(
                scope=scope, data="data a; set b; run;",
                media_type="text/x-sas", created_by="test",
            )
            from sas2pyspark.services.artifacts import artifact_reference
            state = {
                "session_id": session.id,
                "scope_id": candidate.id,
                "sas_source_artifact": artifact_reference(artifact).model_dump(by_alias=True),
            }
            result = await propose(state)
            assert result.get("state") == "HUMAN_REQUIRED"
            escalated = next(
                (e for e in session.events if e.type.value == "migration/escalated"), None,
            )
            assert escalated is not None
            assert escalated.data.get("reasonCode") == "BUDGET_EXCEEDED"
        finally:
            await runtime.dispose()

    import asyncio
    asyncio.new_event_loop().run_until_complete(_run())


def test_no_credential_in_snapshot():
    meter = TokenMeter()
    meter.record({"input_tokens": 100, "completion_tokens": 50}, cost_units=0.5)
    snapshot = meter.snapshot()
    snapshot_str = str(snapshot)
    assert "token" not in snapshot_str.lower() or "tokens" in snapshot_str.lower()
    assert "password" not in snapshot_str.lower()
    assert "secret" not in snapshot_str.lower()
    assert "dapi" not in snapshot_str.lower()


def test_budget_not_set_no_op():
    meter = TokenMeter()
    meter.record({"input_tokens": 999999, "completion_tokens": 999999}, cost_units=999999.0)
    policy = TokenBudgetPolicy()
    meter.check_budget(policy)
    meter.check_budget(None)
