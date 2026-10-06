"""Baseline guard set — registered by default in create_workbench for fail-closed safety."""

from __future__ import annotations

import ast
import re
from typing import Any

from sas2pyspark.services.artifacts import ArtifactRecord
from sas2pyspark.services.guards import Guard, GuardDecision, GuardRequest, GuardService
from sas2pyspark.services.scopes import ScopeRef

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


def _read_source(artifacts: Any, request: GuardRequest) -> str | None:
    try:
        return artifacts.read_text(request.scope, request.artifact)
    except Exception:
        return None


def _parse_tree(source: str) -> ast.AST | None:
    try:
        return ast.parse(source)
    except SyntaxError:
        return None


def make_forbidden_import_guard(artifacts: Any) -> Guard:
    def guard(request: GuardRequest) -> GuardDecision:
        source = _read_source(artifacts, request)
        if source is None:
            return GuardDecision(allow=True)
        tree = _parse_tree(source)
        if tree is None:
            return GuardDecision(allow=True)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name.split(".", 1)[0] for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module.split(".", 1)[0]]
            else:
                continue
            if _FORBIDDEN_IMPORTS.intersection(names):
                return GuardDecision(allow=False, reason_code="FORBIDDEN_IMPORT", detail="candidate imports a forbidden module")
        return GuardDecision(allow=True)
    return guard


def make_dynamic_execution_guard(artifacts: Any) -> Guard:
    def guard(request: GuardRequest) -> GuardDecision:
        source = _read_source(artifacts, request)
        if source is None:
            return GuardDecision(allow=True)
        tree = _parse_tree(source)
        if tree is None:
            return GuardDecision(allow=True)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in _DYNAMIC_EXEC_NAMES:
                return GuardDecision(allow=False, reason_code="DYNAMIC_EXEC", detail=f"candidate uses {node.func.id}()")
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in _DYNAMIC_EXEC_NAMES:
                if isinstance(node.func.value, ast.Name) and node.func.value.id in _BUILTINS_INDIRECTION:
                    return GuardDecision(allow=False, reason_code="DYNAMIC_EXEC", detail="builtins indirection for dynamic exec")
        return GuardDecision(allow=True)
    return guard


def make_secret_scan_guard(artifacts: Any) -> Guard:
    def guard(request: GuardRequest) -> GuardDecision:
        source = _read_source(artifacts, request)
        if source is None:
            return GuardDecision(allow=True)
        for pattern in _SECRET_PATTERNS:
            if pattern.search(source):
                return GuardDecision(allow=False, reason_code="SECRET_DETECTED", detail="candidate appears to contain a credential")
        return GuardDecision(allow=True)
    return guard


def make_udf_guard(artifacts: Any) -> Guard:
    def guard(request: GuardRequest) -> GuardDecision:
        source = _read_source(artifacts, request)
        if source is None:
            return GuardDecision(allow=True)
        tree = _parse_tree(source)
        if tree is None:
            return GuardDecision(allow=True)
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for dec in node.decorator_list:
                    dec_name: str | None = None
                    if isinstance(dec, ast.Name):
                        dec_name = dec.id
                    elif isinstance(dec, ast.Attribute) and isinstance(dec.value, ast.Name):
                        dec_name = dec.attr
                    if dec_name in _UDF_DECORATORS:
                        return GuardDecision(allow=False, reason_code="UDF_DETECTED", detail=f"non-vectorized UDF (@{dec_name})")
        return GuardDecision(allow=True)
    return guard


def make_dynamic_sql_guard(artifacts: Any) -> Guard:
    def guard(request: GuardRequest) -> GuardDecision:
        source = _read_source(artifacts, request)
        if source is None:
            return GuardDecision(allow=True)
        tree = _parse_tree(source)
        if tree is None:
            return GuardDecision(allow=True)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "sql":
                for arg in node.args:
                    if isinstance(arg, ast.JoinedStr):
                        return GuardDecision(allow=False, reason_code="DYNAMIC_SQL", detail="f-string SQL interpolation")
                    if isinstance(arg, ast.BinOp) and isinstance(arg.op, ast.Add):
                        return GuardDecision(allow=False, reason_code="DYNAMIC_SQL", detail="string concatenation in SQL")
        return GuardDecision(allow=True)
    return guard


def make_dangerous_fs_process_guard(artifacts: Any) -> Guard:
    _dangerous_calls = {"rmtree", "system", "popen", "spawn", "fork", "execv", "execvp", "execl"}
    def guard(request: GuardRequest) -> GuardDecision:
        source = _read_source(artifacts, request)
        if source is None:
            return GuardDecision(allow=True)
        tree = _parse_tree(source)
        if tree is None:
            return GuardDecision(allow=True)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in _dangerous_calls:
                return GuardDecision(allow=False, reason_code="DANGEROUS_FS_PROCESS", detail=f"dangerous call: {node.func.attr}()")
        return GuardDecision(allow=True)
    return guard


def make_dynamic_import_guard(artifacts: Any) -> Guard:
    def guard(request: GuardRequest) -> GuardDecision:
        source = _read_source(artifacts, request)
        if source is None:
            return GuardDecision(allow=True)
        tree = _parse_tree(source)
        if tree is None:
            return GuardDecision(allow=True)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in _DYNAMIC_IMPORT_ATTRS:
                return GuardDecision(allow=False, reason_code="DYNAMIC_IMPORT", detail="importlib.import_module indirection")
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Subscript):
                sub = node.func.value
                if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name) and sub.func.id == "globals":
                    return GuardDecision(allow=False, reason_code="DYNAMIC_IMPORT", detail="globals() indirection")
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "getattr":
                if node.args and isinstance(node.args[0], ast.Name) and node.args[0].id in _BUILTINS_INDIRECTION:
                    return GuardDecision(allow=False, reason_code="DYNAMIC_IMPORT", detail="getattr(__builtins__, ...) indirection")
        return GuardDecision(allow=True)
    return guard


def register_baseline_guards(guard_service: GuardService, artifacts: Any) -> None:
    """Register the 7 baseline guards in deterministic order."""
    guard_service.register("forbidden-import", make_forbidden_import_guard(artifacts))
    guard_service.register("dynamic-exec", make_dynamic_execution_guard(artifacts))
    guard_service.register("secret-scan", make_secret_scan_guard(artifacts))
    guard_service.register("udf", make_udf_guard(artifacts))
    guard_service.register("dynamic-sql", make_dynamic_sql_guard(artifacts))
    guard_service.register("dangerous-fs-process", make_dangerous_fs_process_guard(artifacts))
    guard_service.register("dynamic-import", make_dynamic_import_guard(artifacts))
