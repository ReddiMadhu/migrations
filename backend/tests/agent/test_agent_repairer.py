"""Tests for AgentLoopRepairer — default repairer replacing None-repairer escalation."""

from __future__ import annotations

import pytest

from sas2pyspark.agent.dispatch import ToolDispatchPipeline
from sas2pyspark.agent.driver import AgentDriver, StepRunner, TurnRunner
from sas2pyspark.agent.prompt_assembler import PromptAssembler
from sas2pyspark.agent.registry import ScopedToolRegistry, ToolClassification
from sas2pyspark.agent.repairer import AgentLoopRepairer
from sas2pyspark.services.clock import FixedClock


@pytest.fixture
def repairer(fixed_clock: FixedClock) -> AgentLoopRepairer:
    registry = ScopedToolRegistry()
    registry.register_global(
        name="echo", tool=lambda **kw: kw, classification=ToolClassification.PARALLEL,
    )
    pipeline = ToolDispatchPipeline(registry=registry, clock=fixed_clock)
    step_runner = StepRunner(dispatch=pipeline)
    turn_runner = TurnRunner(step_runner=step_runner)
    driver = AgentDriver(turn_runner=turn_runner, clock=fixed_clock)
    assembler = PromptAssembler()
    return AgentLoopRepairer(driver=driver, prompt_assembler=assembler)


class TestAgentLoopRepairer:
    @pytest.mark.asyncio
    async def test_diagnose_returns_dict(self, repairer: AgentLoopRepairer):
        result = await repairer.diagnose(
            reconciliation={"summary": {"differenceCount": 5}},
            repair_history=[],
            model_context={},
            iteration=1,
        )
        assert isinstance(result, dict)
        assert result["iteration"] == 1
        assert result["differenceCount"] == 5
        assert result["advisory"] is True

    @pytest.mark.asyncio
    async def test_diagnose_empty_reconciliation(self, repairer: AgentLoopRepairer):
        result = await repairer.diagnose(
            reconciliation={},
            repair_history=[],
            model_context={},
            iteration=2,
        )
        assert result["differenceCount"] == 0
        assert result["iteration"] == 2

    @pytest.mark.asyncio
    async def test_propose_patch_returns_valid_contract(self, repairer: AgentLoopRepairer):
        result = await repairer.propose_patch(
            current_candidate="print('hello')",
            diagnosis={"differenceCount": 3},
            model_context={},
            reconciliation={},
        )
        assert "candidateSource" in result
        assert "hypothesis" in result
        assert "expectedEffect" in result
        assert result["candidateSource"] == "print('hello')"
        assert isinstance(result["hypothesis"], str)
        assert len(result["hypothesis"]) > 0

    @pytest.mark.asyncio
    async def test_propose_patch_expected_effect_contract(self, repairer: AgentLoopRepairer):
        result = await repairer.propose_patch(
            current_candidate="x = 1",
            diagnosis={},
            model_context={},
            reconciliation={},
        )
        expected = result["expectedEffect"]
        assert expected["metric"] == "differenceCount"
        assert isinstance(expected["minimumReduction"], int)
        assert expected["minimumReduction"] >= 1

    @pytest.mark.asyncio
    async def test_propose_patch_advisory(self, repairer: AgentLoopRepairer):
        result = await repairer.propose_patch(
            current_candidate="x = 1",
            diagnosis={},
            model_context={},
            reconciliation={},
        )
        assert result.get("advisory") is True

    @pytest.mark.asyncio
    async def test_diagnose_with_repair_history(self, repairer: AgentLoopRepairer):
        result = await repairer.diagnose(
            reconciliation={"summary": {"differenceCount": 2}},
            repair_history=[{"iteration": 1}, {"iteration": 2}],
            model_context={},
            iteration=3,
        )
        assert result["repairHistoryLength"] == 2
