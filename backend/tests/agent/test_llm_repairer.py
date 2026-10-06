"""Tests for LLM-backed AgentLoopRepairer — code correction from reconciliation errors."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

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


@pytest.fixture
def llm_repairer(fixed_clock: FixedClock) -> AgentLoopRepairer:
    registry = ScopedToolRegistry()
    registry.register_global(
        name="echo", tool=lambda **kw: kw, classification=ToolClassification.PARALLEL,
    )
    pipeline = ToolDispatchPipeline(registry=registry, clock=fixed_clock)
    step_runner = StepRunner(dispatch=pipeline)
    turn_runner = TurnRunner(step_runner=step_runner)
    driver = AgentDriver(turn_runner=turn_runner, clock=fixed_clock)
    assembler = PromptAssembler()

    mock_translator = MagicMock()
    mock_response = MagicMock()
    mock_response.content = "```python\ndf = df.filter(df['col'] > 0)\n```"
    mock_translator.translate.return_value = mock_response

    return AgentLoopRepairer(driver=driver, prompt_assembler=assembler, translator=mock_translator)


class TestLLMRepairer:
    @pytest.mark.asyncio
    async def test_diagnose_includes_verdicts(self, repairer: AgentLoopRepairer):
        result = await repairer.diagnose(
            reconciliation={
                "schemaVerdict": "PASS",
                "aggregateVerdict": "FAIL",
                "rowVerdict": "FAIL",
                "statisticalVerdict": "FAIL",
                "differenceCount": 5,
                "differences": [{"kind": "row", "detail": "row 3 mismatch"}],
                "referenceRows": 100,
                "candidateRows": 95,
            },
            repair_history=[],
            model_context={},
            iteration=1,
        )
        assert result["schemaVerdict"] == "PASS"
        assert result["aggregateVerdict"] == "FAIL"
        assert result["differenceCount"] == 5
        assert result["referenceRows"] == 100
        assert result["candidateRows"] == 95
        assert len(result["differences"]) == 1

    @pytest.mark.asyncio
    async def test_propose_patch_without_llm_returns_current(self, repairer: AgentLoopRepairer):
        result = await repairer.propose_patch(
            current_candidate="df = df.collect()",
            diagnosis={"differenceCount": 3, "schemaVerdict": "PASS", "aggregateVerdict": "FAIL"},
            model_context={},
            reconciliation={},
        )
        assert result["candidateSource"] == "df = df.collect()"
        assert result["expectedEffect"]["metric"] == "differenceCount"
        assert result["expectedEffect"]["minimumReduction"] == 1

    @pytest.mark.asyncio
    async def test_propose_patch_with_llm_generates_correction(self, llm_repairer: AgentLoopRepairer):
        result = await llm_repairer.propose_patch(
            current_candidate="df = df.collect()",
            diagnosis={
                "differenceCount": 3,
                "schemaVerdict": "PASS",
                "aggregateVerdict": "FAIL",
                "rowVerdict": "FAIL",
                "differences": [{"kind": "aggregate", "detail": "row count mismatch"}],
            },
            model_context={},
            reconciliation={},
        )
        assert "filter" in result["candidateSource"]
        assert result["candidateSource"] != "df = df.collect()"

    @pytest.mark.asyncio
    async def test_propose_patch_hypothesis_includes_failed_verdicts(self, llm_repairer: AgentLoopRepairer):
        result = await llm_repairer.propose_patch(
            current_candidate="x = 1",
            diagnosis={
                "differenceCount": 2,
                "schemaVerdict": "PASS",
                "aggregateVerdict": "FAIL",
                "rowVerdict": "FAIL",
                "statisticalVerdict": "PASS",
            },
            model_context={},
            reconciliation={},
        )
        assert "aggregateVerdict" in result["hypothesis"]
        assert "rowVerdict" in result["hypothesis"]

    @pytest.mark.asyncio
    async def test_propose_patch_falls_back_on_llm_error(self, fixed_clock: FixedClock):
        registry = ScopedToolRegistry()
        pipeline = ToolDispatchPipeline(registry=registry, clock=fixed_clock)
        step_runner = StepRunner(dispatch=pipeline)
        turn_runner = TurnRunner(step_runner=step_runner)
        driver = AgentDriver(turn_runner=turn_runner, clock=fixed_clock)

        mock_translator = MagicMock()
        mock_translator.translate.side_effect = RuntimeError("LLM unavailable")

        repairer = AgentLoopRepairer(driver=driver, translator=mock_translator)

        result = await repairer.propose_patch(
            current_candidate="original_code = True",
            diagnosis={"differenceCount": 1},
            model_context={},
            reconciliation={},
        )
        assert result["candidateSource"] == "original_code = True"

    @pytest.mark.asyncio
    async def test_extract_code_from_markdown_block(self):
        code = AgentLoopRepairer._extract_code("Here is the fix:\n```python\ndf = df.distinct()\n```\nDone.")
        assert code == "df = df.distinct()"

    @pytest.mark.asyncio
    async def test_extract_code_from_plain_text(self):
        code = AgentLoopRepairer._extract_code("df = df.distinct()")
        assert code == "df = df.distinct()"

    @pytest.mark.asyncio
    async def test_extract_code_from_generic_block(self):
        code = AgentLoopRepairer._extract_code("```\ndf = df.distinct()\n```")
        assert code == "df = df.distinct()"

    @pytest.mark.asyncio
    async def test_build_repair_prompt_includes_verdicts(self):
        prompt = AgentLoopRepairer._build_repair_prompt({
            "differenceCount": 5,
            "schemaVerdict": "PASS",
            "aggregateVerdict": "FAIL",
            "rowVerdict": "FAIL",
            "statisticalVerdict": "PASS",
            "referenceRows": 100,
            "candidateRows": 95,
            "differences": [{"kind": "row", "detail": "mismatch at row 3"}],
        })
        assert "Difference count: 5" in prompt
        assert "Schema verdict: PASS" in prompt
        assert "Aggregate verdict: FAIL" in prompt
        assert "mismatch at row 3" in prompt
        assert "Fix the PySpark code" in prompt
