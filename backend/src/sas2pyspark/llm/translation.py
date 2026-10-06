"""LangChain-backed SAS-to-PySpark proposal adapter."""

from __future__ import annotations

import time
from typing import Any

from sas2pyspark.llm.prompts import TRANSLATION_PROMPT, prompt_payload
from sas2pyspark.llm.tools import make_translation_tools


class LangChainSasTranslator:
    """Small adapter around a configured LangChain chat model."""

    def __init__(
        self,
        model: Any,
        *,
        runtime: Any = None,
        max_attempts: int = 3,
        max_tool_rounds: int = 4,
        base_delay_seconds: float = 0.25,
        sleep: Any = time.sleep,
    ) -> None:
        self._model = model
        self._runtime = runtime
        self._max_attempts = max(1, max_attempts)
        self._max_tool_rounds = max(0, max_tool_rounds)
        self._base_delay_seconds = max(0.0, base_delay_seconds)
        self._sleep = sleep

    def translate(
        self,
        *,
        source: str,
        ir_snapshot: dict[str, Any],
        macro_resolution: dict[str, Any],
        model_context: dict[str, Any],
    ) -> Any:
        payload = prompt_payload(source=source, model_context={
            "ir": ir_snapshot,
            "macros": macro_resolution,
            "context": model_context,
        })
        state = {
            "ir_snapshot": ir_snapshot,
            "parser_results": ir_snapshot.get("parsedFiles", {}),
            "macro_resolution": macro_resolution,
            "model_context": model_context,
        }
        model = self._model
        tools: list[Any] = []
        if self._runtime is not None:
            tools = make_translation_tools(state=state, runtime=self._runtime)
            model = model.bind_tools(tools)
        prompt = TRANSLATION_PROMPT.invoke(payload)
        tool_map = {tool.name: tool for tool in tools}
        messages = list(prompt.messages)
        for round_number in range(self._max_tool_rounds + 1):
            response = self._invoke_with_retry(model, messages)
            self._record_usage(response)
            calls = getattr(response, "tool_calls", []) or []
            if not calls or round_number >= self._max_tool_rounds:
                return response
            from langchain_core.messages import ToolMessage

            messages.append(response)
            for call in calls:
                tool = tool_map.get(call.get("name", ""))
                if tool is None:
                    result = {"error": "tool is not available in this bounded translation context"}
                else:
                    result = tool.invoke(call.get("args", {}))
                messages.append(ToolMessage(
                    content=str(result),
                    tool_call_id=call.get("id", "missing-tool-call-id"),
                ))
        raise RuntimeError("translation tool loop exhausted without a final response")

    def _record_usage(self, response: Any) -> None:
        if self._runtime is None:
            return
        token_meter = getattr(self._runtime, "token_meter", None)
        if token_meter is None:
            return
        usage = getattr(response, "usage_metadata", None) or getattr(response, "response_metadata", {}).get("token_usage", {})
        if not isinstance(usage, dict) or not usage:
            return
        from sas2pyspark.core.logging import TokenMeter
        disjoint = TokenMeter.map_wire_fields(usage)
        token_meter.record_disjoint(disjoint)

    def _invoke_with_retry(self, model: Any, messages: list[Any]) -> Any:
        for attempt in range(self._max_attempts):
            try:
                return model.invoke(messages)
            except Exception as exc:
                status_code = getattr(exc, "status_code", None)
                retryable = (
                    status_code == 429
                    or (isinstance(status_code, int) and 500 <= status_code < 600)
                    or isinstance(exc, (TimeoutError, ConnectionError, OSError))
                )
                if not retryable or attempt + 1 >= self._max_attempts:
                    raise
                self._sleep(self._base_delay_seconds * (2 ** attempt))
        raise RuntimeError("translation retry loop exhausted without a model response")


def create_sas_translator(
    *, model_provider: str, model_name: str, runtime: Any, model_kwargs: dict[str, Any] | None = None,
) -> LangChainSasTranslator:
    from langchain.chat_models import init_chat_model

    model = init_chat_model(model_name, model_provider=model_provider, **(model_kwargs or {}))
    return LangChainSasTranslator(model, runtime=runtime)