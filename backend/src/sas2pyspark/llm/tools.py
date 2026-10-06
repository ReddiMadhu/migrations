"""Bounded read-only tools available to translation and repair models."""

from __future__ import annotations

from typing import Any


def make_translation_tools(*, state: dict[str, Any], runtime: Any) -> list[Any]:
    from langchain_core.tools import StructuredTool

    def inspect_ir_nodes(node_ids: list[str]) -> dict[str, Any]:
        snapshot = state.get("ir_snapshot") or {}
        by_id = {node.get("nodeId"): node for node in snapshot.get("nodes", [])}
        selected = {node_id: by_id[node_id] for node_id in node_ids if node_id in by_id}
        for path, parsed_file in (state.get("parser_results") or {}).items():
            for node in parsed_file.get("nodes", []):
                if node.get("nodeId") in node_ids:
                    selected[node["nodeId"]] = {"path": path, **node}
        return selected

    def inspect_macro_definition(macro_name: str) -> dict[str, Any]:
        macros = state.get("macro_resolution") or {}
        target = macro_name.casefold()
        definitions = [item for item in macros.get("definitions", []) if item.get("normalizedName", "").casefold() == target]
        calls = [item for item in macros.get("calls", []) if item.get("normalizedName", "").casefold() == target]
        return {"definitions": definitions, "calls": calls}

    def inspect_dependency(path: str) -> str:
        graph = state.get("dependency_graph") or {}
        item = next((file for file in graph.get("files", []) if file.get("path") == path), None)
        if item is None:
            return "dependency not in discovered graph"
        from sas2pyspark.services.artifacts import ArtifactReferenceData
        scope = runtime.scopes.require_by_id(state.get("scope_id", ""))
        reference = ArtifactReferenceData(**item["artifact"])
        return runtime.artifacts.read_reference(scope, reference).decode("utf-8")[:16_384]

    def diagnostics(candidate: str) -> dict[str, Any]:
        import ast
        import importlib.util

        try:
            tree = ast.parse(candidate)
        except SyntaxError as exc:
            return {"valid": False, "line": exc.lineno, "message": exc.msg}
        forbidden_calls = [
            node.func.id for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in {"eval", "exec", "compile", "__import__"}
        ]
        available = {
            name: importlib.util.find_spec(name) is not None
            for name in ("pylint", "mypy")
        }
        return {
            "valid": not forbidden_calls,
            "message": "syntax valid" if not forbidden_calls else "dynamic code execution is not allowed",
            "forbiddenCalls": sorted(set(forbidden_calls)),
            "externalDiagnosticsAvailable": available,
        }

    def search_translation_examples(construct: str, query: str) -> dict[str, Any]:
        store = getattr(runtime, "translation_store", None)
        if store is None:
            return {"examples": [], "available": False}
        return {"examples": store.search(namespace=(construct,), query=query, limit=3), "available": True}

    return [
        StructuredTool.from_function(inspect_ir_nodes, name="inspect_ir_nodes", description="Read selected nodes from the current bounded semantic IR."),
        StructuredTool.from_function(inspect_macro_definition, name="inspect_macro_definition", description="Inspect static macro definitions and resolution evidence."),
        StructuredTool.from_function(inspect_dependency, name="inspect_dependency", description="Read one dependency already present in the discovered graph."),
        StructuredTool.from_function(diagnostics, name="diagnostics", description="Check candidate Python syntax without executing it."),
        StructuredTool.from_function(search_translation_examples, name="search_translation_examples", description="Search approved examples from the configured translation store."),
    ]


def make_tool_registry(*, state: dict[str, Any], runtime: Any) -> Any:
    from sas2pyspark.agent.registry import ScopedToolRegistry, ToolClassification

    tools = make_translation_tools(state=state, runtime=runtime)
    registry = ScopedToolRegistry()
    for tool in tools:
        registry.register_global(
            name=tool.name,
            tool=tool,
            classification=ToolClassification.PARALLEL,
        )
    return registry
