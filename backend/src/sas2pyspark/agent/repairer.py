"""AgentLoopRepairer — LLM-backed repair that fixes PySpark code from reconciliation errors."""

from __future__ import annotations

import json
from typing import Any

from sas2pyspark.agent.driver import AgentDriver
from sas2pyspark.agent.prompt_assembler import PromptAssembler


class AgentLoopRepairer:
    """Default repairer that uses the LLM to generate corrected PySpark code."""

    __slots__ = ("_driver", "_assembler", "_translator")

    def __init__(
        self,
        *,
        driver: AgentDriver,
        prompt_assembler: PromptAssembler | None = None,
        translator: Any = None,
    ) -> None:
        self._driver = driver
        self._assembler = prompt_assembler
        self._translator = translator

    async def diagnose(
        self,
        *,
        reconciliation: dict[str, Any],
        repair_history: list[dict[str, Any]],
        model_context: dict[str, Any],
        iteration: int,
    ) -> dict[str, Any]:
        diff_summary = reconciliation.get("summary", reconciliation)
        difference_count = diff_summary.get("differenceCount", reconciliation.get("differenceCount", 0))
        differences = reconciliation.get("differences", diff_summary.get("differences", []))
        return {
            "iteration": iteration,
            "differenceCount": difference_count,
            "diff": diff_summary,
            "differences": differences[:20],
            "schemaVerdict": reconciliation.get("schemaVerdict"),
            "aggregateVerdict": reconciliation.get("aggregateVerdict"),
            "rowVerdict": reconciliation.get("rowVerdict"),
            "statisticalVerdict": reconciliation.get("statisticalVerdict"),
            "referenceRows": reconciliation.get("referenceRows"),
            "candidateRows": reconciliation.get("candidateRows"),
            "repairHistoryLength": len(repair_history),
            "advisory": True,
        }

    async def propose_patch(
        self,
        *,
        current_candidate: str,
        diagnosis: dict[str, Any],
        model_context: dict[str, Any],
        reconciliation: dict[str, Any],
    ) -> dict[str, Any]:
        corrected_source = await self._generate_correction(
            current_candidate=current_candidate,
            diagnosis=diagnosis,
            model_context=model_context,
        )

        if not corrected_source or not corrected_source.strip():
            corrected_source = current_candidate

        difference_count = diagnosis.get("differenceCount", 1)
        minimum_reduction = 1 if difference_count > 0 else 1

        return {
            "candidateSource": corrected_source,
            "hypothesis": self._build_hypothesis(diagnosis),
            "expectedEffect": {
                "metric": "differenceCount",
                "minimumReduction": minimum_reduction,
            },
            "advisory": True,
        }

    async def _generate_correction(
        self,
        *,
        current_candidate: str,
        diagnosis: dict[str, Any],
        model_context: dict[str, Any],
    ) -> str:
        if self._translator is None:
            return current_candidate

        try:
            response = self._translator.translate(
                source=current_candidate,
                ir_snapshot=model_context.get("ir_snapshot", {}),
                macro_resolution=model_context.get("macro_resolution", {}),
                model_context={
                    **model_context,
                    "repair": True,
                    "diagnosis": diagnosis,
                    "instruction": self._build_repair_prompt(diagnosis),
                },
            )
            content = getattr(response, "content", None)
            if content is None:
                content = str(response)
            return self._extract_code(content)
        except Exception:
            return current_candidate

    @staticmethod
    def _build_repair_prompt(diagnosis: dict[str, Any]) -> str:
        parts = [
            "The PySpark code produced output that does not match the SAS reference output.",
            f"Difference count: {diagnosis.get('differenceCount', 'unknown')}",
            f"Schema verdict: {diagnosis.get('schemaVerdict', 'unknown')}",
            f"Aggregate verdict: {diagnosis.get('aggregateVerdict', 'unknown')}",
            f"Row verdict: {diagnosis.get('rowVerdict', 'unknown')}",
            f"Statistical verdict: {diagnosis.get('statisticalVerdict', 'unknown')}",
            f"Reference rows: {diagnosis.get('referenceRows', 'unknown')}",
            f"Candidate rows: {diagnosis.get('candidateRows', 'unknown')}",
        ]
        differences = diagnosis.get("differences", [])
        if differences:
            parts.append("Differences (first 20):")
            for d in differences[:20]:
                parts.append(f"  - {d.get('kind', 'unknown')}: {d.get('detail', d)}")
        parts.append("Fix the PySpark code so the output matches the reference exactly.")
        return "\n".join(parts)

    @staticmethod
    def _build_hypothesis(diagnosis: dict[str, Any]) -> str:
        verdicts = []
        for key in ("schemaVerdict", "aggregateVerdict", "rowVerdict", "statisticalVerdict"):
            v = diagnosis.get(key)
            if v and v != "PASS":
                verdicts.append(f"{key}={v}")
        if verdicts:
            return f"LLM- proposed correction for: {', '.join(verdicts)}"
        return "LLM-proposed correction based on reconciliation diff"

    @staticmethod
    def _extract_code(content: Any) -> str:
        if isinstance(content, list):
            for block in content:
                if isinstance(block, dict) and block.get("type") == "text":
                    text = block.get("text", "")
                    return AgentLoopRepairer._extract_from_text(text)
            return AgentLoopRepairer._extract_from_text(str(content))
        return AgentLoopRepairer._extract_from_text(str(content))

    @staticmethod
    def _extract_from_text(text: str) -> str:
        if "```python" in text:
            start = text.index("```python") + 9
            end = text.index("```", start) if "```" in text[start:] else len(text)
            return text[start:end].strip()
        if "```" in text:
            start = text.index("```") + 3
            end = text.index("```", start) if "```" in text[start:] else len(text)
            return text[start:end].strip()
        return text.strip()
