"""Deterministic static and secret checks for a proposed PySpark artifact."""

from __future__ import annotations

import ast
import re
from typing import Any

from pydantic import ValidationError

from sas2pyspark.core.events import ArtifactReferenceData
from sas2pyspark.graph.state import MigrationGraphState

_FORBIDDEN_IMPORTS = {
    "os", "subprocess", "socket", "requests", "urllib", "httpx",
    "importlib", "sys", "pickle", "ctypes", "shutil", "pathlib",
    "marshal", "multiprocessing", "pty", "tempfile", "builtins",
}
_DYNAMIC_EXEC_NAMES = {"eval", "exec", "compile", "__import__"}
_UDF_DECORATORS = {"udf", "pandas_udf"}
_DYNAMIC_IMPORT_ATTRS = {"import_module"}
_BUILTINS_INDIRECTION = {"__builtins__", "builtins"}
_SECRET_PATTERNS = (
    re.compile(r"""(?i)(?:token|password|client_secret|api_key)\s*[:=]\s*['"][^'"]{8,}['"]"""),
    re.compile(r"\b(?:dapi|gh[pousr])_[A-Za-z0-9]{20,}\b"),
    re.compile(r"(?i)\b(?:jdbc|odbc):[^\s'\"]+"),
    re.compile(r"(?i)\bhttps?://[^/@\s]+:[^/@\s]+@[^\s'\"]+"),
    re.compile(r"(?i)\bhttps?://[^/\s?#]+:(?!(?:80|443)(?:/|$))\d+[^\s'\"]*"),
    re.compile(r"(?i)[?&](?:token|api[_-]?key|password)=[^&\s'\"]+"),
    re.compile(r"(?i)\blibname\s+\w+\s+(?:odbc|oracle|db2|mysql)\b[^;]*(?:dsn|password|user\s*=)[^;]*;"),
)

_DATAFRAME_METHODS = {
    "agg", "alias", "approxQuantile", "cache", "count", "createOrReplaceTempView", "crossJoin", "drop", "dropDuplicates",
    "dropna", "exceptAll", "fillna", "filter", "first", "foreach", "groupBy", "head", "intersect", "intersectAll", "join",
    "limit", "load", "na", "orderBy", "persist", "printSchema", "randomSplit", "select", "show", "sort", "take", "toDF",
    "union", "unionByName", "where", "withColumn", "withColumnRenamed", "withWatermark", "write", "selectExpr", "distinct",
    "schema", "checkpoint",
}
_COLUMN_METHODS = {
    "alias", "astype", "cast", "contains", "endswith", "isin", "isNull", "like", "lower", "rlike", "startswith", "substr",
    "upper", "when", "otherwise", "between", "desc", "asc", "concat", "lit",
}


def _attribute_chain(node: ast.AST | None) -> str | None:
    if node is None:
        return None
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        base = _attribute_chain(node.value)
        return f"{base}.{node.attr}" if base else node.attr
    return None


def _infer_runtime_type(node: ast.AST | None, env: dict[str, str]) -> str | None:
    if node is None:
        return None
    if isinstance(node, ast.Name):
        return env.get(node.id)
    if isinstance(node, ast.Call):
        target = _attribute_chain(node.func)
        if target in {"spark.range", "spark.table", "spark.read.parquet", "spark.read.csv", "spark.sql", "spark.createDataFrame"}:
            return "dataframe"
        if target in {"col", "lit", "F.col", "functions.col", "functions.lit"}:
            return "column"
        if isinstance(node.func, ast.Attribute) and node.func.attr in {"select", "filter", "where", "withColumn", "agg", "join", "groupBy", "drop", "union", "intersect", "limit", "selectExpr", "dropDuplicates", "dropna", "fillna", "orderBy", "sort", "distinct"}:
            return _infer_runtime_type(node.func.value, env) or "dataframe"
        if isinstance(node.func, ast.Name) and node.func.id in {"col", "lit"}:
            return "column"
    if isinstance(node, ast.Attribute):
        if node.attr in {"filter", "select", "where", "join", "groupBy", "withColumn", "drop", "union", "intersect", "limit", "selectExpr", "dropDuplicates", "dropna", "fillna", "orderBy", "sort", "distinct"}:
            return _infer_runtime_type(node.value, env) or "dataframe"
    return None


def _hallucinated_api_violations(source: str) -> tuple[bool, str | None]:
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return False, None

    env: dict[str, str] = {}
    ordered_nodes = sorted(ast.walk(tree), key=lambda node: (getattr(node, "lineno", 0), getattr(node, "col_offset", 0), type(node).__name__))
    for node in ordered_nodes:
        if isinstance(node, ast.Assign):
            names = [target.id for target in node.targets if isinstance(target, ast.Name)]
            inferred = _infer_runtime_type(node.value, env)
            if inferred:
                for name in names:
                    env[name] = inferred
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            inferred = _infer_runtime_type(node.value, env)
            if inferred:
                env[node.target.id] = inferred

    for node in ordered_nodes:
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        method_name = node.func.attr
        target = _attribute_chain(node.func)
        receiver_type = _infer_runtime_type(node.func.value, env)
        if receiver_type == "dataframe" and method_name not in _DATAFRAME_METHODS:
            return True, f"nonexistent PySpark method '{method_name}' on DataFrame"
        if receiver_type == "column" and method_name not in _COLUMN_METHODS:
            return True, f"nonexistent PySpark method '{method_name}' on Column"
        if receiver_type is None and target and "." in target and not target.startswith("spark."):
            return False, "validation inconclusive"
    return False, None


def make_guard_node(runtime: Any):
    async def guard_proposal(state: MigrationGraphState) -> dict[str, Any]:
        candidate_ref = state.get("candidate_artifact")
        if candidate_ref is None:
            return {"state": "HUMAN_REQUIRED", "error": "no candidate artifact was proposed"}
        if state.get("repair_iteration", 0) > 3:
            return {"state": "HUMAN_REQUIRED", "error": "repair iteration budget exceeded"}

        digest = candidate_ref.get("digest")
        history = state.get("repair_history", [])
        if not isinstance(digest, str) or any(item.get("artifactDigest") == digest for item in history):
            return {"state": "HUMAN_REQUIRED", "error": "candidate repeats a prior artifact; repair stopped"}
        proposal = state.get("repair_proposal") or {}
        hypothesis = proposal.get("hypothesis")
        if hypothesis and any(item.get("hypothesis") == hypothesis for item in history):
            return {"state": "HUMAN_REQUIRED", "error": "repair repeats a previously falsified hypothesis"}
        scope = runtime.scopes.require_by_id(state.get("scope_id", ""))
        try:
            reference = ArtifactReferenceData(**candidate_ref)
        except (ValidationError, TypeError, ValueError):
            return {"state": "HUMAN_REQUIRED", "error": "candidate artifact reference is invalid"}
        source = runtime.artifacts.read_reference(scope, reference).decode("utf-8")
        try:
            tree = ast.parse(source)
        except SyntaxError as exc:
            return {"state": "HUMAN_REQUIRED", "error": f"candidate has invalid Python syntax: {exc.msg}"}

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name.split(".", 1)[0] for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module.split(".", 1)[0]]
            else:
                names = []
            if _FORBIDDEN_IMPORTS.intersection(names):
                return {"state": "HUMAN_REQUIRED", "error": "candidate imports a forbidden network, process, or dynamic-import module"}
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in _DYNAMIC_EXEC_NAMES:
                return {"state": "HUMAN_REQUIRED", "error": "candidate uses dynamic code execution"}
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                attr_name = node.func.attr
                if attr_name in _DYNAMIC_EXEC_NAMES and isinstance(node.func.value, ast.Name) and node.func.value.id in _BUILTINS_INDIRECTION:
                    return {"state": "HUMAN_REQUIRED", "error": "candidate uses builtins indirection to call dynamic code execution"}
                if attr_name in _DYNAMIC_IMPORT_ATTRS:
                    return {"state": "HUMAN_REQUIRED", "error": "candidate uses dynamic import via importlib.import_module"}
                if attr_name == "sql" and isinstance(node.func.value, ast.Name):
                    for arg in node.args:
                        if isinstance(arg, ast.JoinedStr):
                            return {"state": "HUMAN_REQUIRED", "error": "candidate uses dynamic SQL with f-string interpolation (injection risk)"}
                        if isinstance(arg, ast.BinOp) and isinstance(arg.op, ast.Add):
                            return {"state": "HUMAN_REQUIRED", "error": "candidate uses dynamic SQL with string concatenation (injection risk)"}
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for dec in node.decorator_list:
                    dec_name: str | None = None
                    if isinstance(dec, ast.Name):
                        dec_name = dec.id
                    elif isinstance(dec, ast.Attribute) and isinstance(dec.value, ast.Name):
                        dec_name = dec.attr
                    if dec_name in _UDF_DECORATORS:
                        return {"state": "HUMAN_REQUIRED", "error": f"candidate defines a non-vectorized UDF (@{dec_name}); route for review"}
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Subscript):
                sub = node.func.value
                if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name) and sub.func.id == "globals":
                    return {"state": "HUMAN_REQUIRED", "error": "candidate uses globals() indirection for dynamic lookup"}
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "getattr":
                if node.args and isinstance(node.args[0], ast.Name) and node.args[0].id in _BUILTINS_INDIRECTION:
                    return {"state": "HUMAN_REQUIRED", "error": "candidate uses getattr(__builtins__, ...) indirection"}

        hallucinated, warning = _hallucinated_api_violations(source)
        if hallucinated:
            return {"state": "HUMAN_REQUIRED", "error": warning}
        if warning == "validation inconclusive":
            return {"state": "HUMAN_REQUIRED", "error": "PySpark API validation inconclusive"}
        if any(pattern.search(source) for pattern in _SECRET_PATTERNS):
            return {"state": "HUMAN_REQUIRED", "error": "candidate appears to contain a credential"}
        return {
            "state": "CANDIDATE_READY",
            "repair_history": [{
                "artifactDigest": digest,
                **({"hypothesis": hypothesis} if hypothesis else {}),
            }],
        }

    return guard_proposal