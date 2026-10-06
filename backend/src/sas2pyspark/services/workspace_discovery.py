"""WorkspaceDiscoveryService — SAS %include graph with lazy closure and cycle detection."""

from __future__ import annotations

import re
from typing import Any, Sequence

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.core.json import assert_safe_segment, snapshot_json
from sas2pyspark.core.events import ArtifactReferenceData
from sas2pyspark.services.artifacts import ArtifactRecord, artifact_reference
from sas2pyspark.services.scopes import ScopeRef

MAX_WORKSPACE_FILES = 512
MAX_DEPENDENCY_EDGES = 4096
MAX_UNRESOLVED_DEPENDENCIES = 1024
_MAX_INCLUDE_EXPR_LENGTH = 2048

_INCLUDE_RE = re.compile(r'^\s*%include\s+(?:"([^"]+)"|\'([^\']+)\'|([^\s;]+))', re.IGNORECASE)


def _normalize_workspace_path(value: Any, code: WorkbenchErrorCode = WorkbenchErrorCode.INVALID_INPUT, field: str = "path") -> str:
    if not isinstance(value, str) or len(value) == 0 or len(value) > 1024:
        raise WorkbenchError(code, f"{field} must be a bounded non-empty string", {"field": field})
    text = value.replace("\\", "/")
    if text.startswith("/") or re.match(r"^[A-Za-z]:/", text):
        raise WorkbenchError(code, f"{field} must be relative to the workspace", {"field": field, "path": text})
    segments: list[str] = []
    for seg in text.split("/"):
        if seg == "" or seg == ".":
            continue
        if seg == "..":
            raise WorkbenchError(code, f"{field} cannot escape the workspace", {"field": field, "path": text})
        if len(seg) > 256:
            raise WorkbenchError(code, f"{field} contains an oversized segment", {"field": field, "path": text})
        segments.append(seg)
    if not segments:
        raise WorkbenchError(code, f"{field} cannot be empty", {"field": field, "path": text})
    return "/".join(segments)


def _strip_sas_comments(text: str) -> list[str]:
    result: list[str] = []
    in_block = False
    for original_line in re.split(r"\r\n|\n|\r", text):
        if original_line.lstrip().startswith("%*"):
            result.append("")
            continue
        visible = ""
        quote: str | None = None
        i = 0
        while i < len(original_line):
            ch = original_line[i]
            nxt = original_line[i + 1] if i + 1 < len(original_line) else None
            if in_block:
                if ch == "*" and nxt == "/":
                    in_block = False
                    i += 2
                    continue
                i += 1
                continue
            if quote is not None:
                visible += ch
                if ch == quote:
                    quote = None
                i += 1
                continue
            if ch == "/" and nxt == "*":
                in_block = True
                i += 2
                continue
            if ch == "/" and nxt == "/":
                break
            if ch in ("'", '"'):
                quote = ch
                visible += ch
                i += 1
                continue
            visible += ch
            i += 1
        result.append(visible)
    return result


def _include_directives(text: str) -> list[tuple[str, int, str]]:
    directives: list[tuple[str, int, str]] = []
    lines = _strip_sas_comments(text)
    for idx, line in enumerate(lines):
        match = _INCLUDE_RE.match(line)
        if match:
            target = match.group(1) or match.group(2) or match.group(3)
            if target is not None:
                expr = line.strip()
                if len(expr) > _MAX_INCLUDE_EXPR_LENGTH:
                    raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, "include expression exceeds its bound", {"line": idx + 1, "max": _MAX_INCLUDE_EXPR_LENGTH})
                directives.append((target, idx + 1, expr))
    return directives


def _include_candidates(from_path: str, target: str) -> tuple[list[str], bool]:
    normalized = target.strip().replace("\\", "/")
    if normalized.startswith("/") or re.match(r"^[A-Za-z]:/", normalized):
        return [], True
    base = from_path[:from_path.rfind("/")] if "/" in from_path else ""
    combined = f"{base}/{normalized}" if base else normalized
    try:
        normalized_path = _normalize_workspace_path(combined)
        paths = [normalized_path]
        if not normalized_path.lower().endswith(".sas"):
            paths.append(f"{normalized_path}.sas")
        return paths, False
    except WorkbenchError:
        return [], True


class WorkspaceFileData:
    __slots__ = ("path", "artifact", "is_sas")

    def __init__(self, *, path: str, artifact: ArtifactReferenceData, is_sas: bool) -> None:
        self.path = path
        self.artifact = artifact
        self.is_sas = is_sas

    def to_dict(self) -> dict[str, Any]:
        return {"path": self.path, "artifact": self.artifact.model_dump(by_alias=True), "isSas": self.is_sas}


class DependencyEdgeData:
    __slots__ = ("from_path", "to_path", "kind", "line", "expression")

    def __init__(self, *, from_path: str, to_path: str, kind: str, line: int, expression: str) -> None:
        self.from_path = from_path
        self.to_path = to_path
        self.kind = kind
        self.line = line
        self.expression = expression

    def to_dict(self) -> dict[str, Any]:
        return {"from": self.from_path, "to": self.to_path, "kind": self.kind, "line": self.line, "expression": self.expression}


class UnresolvedDependencyData:
    __slots__ = ("from_path", "target", "line", "reason")

    def __init__(self, *, from_path: str, target: str, line: int, reason: str) -> None:
        self.from_path = from_path
        self.target = target
        self.line = line
        self.reason = reason

    def to_dict(self) -> dict[str, Any]:
        return {"from": self.from_path, "target": self.target, "line": self.line, "reason": self.reason}


class DependencyCycleData:
    __slots__ = ("paths",)

    def __init__(self, *, paths: list[str]) -> None:
        self.paths = paths

    def to_dict(self) -> dict[str, Any]:
        return {"paths": self.paths}


class DependencyGraph:
    __slots__ = ("graph_id", "discovery_id", "migration_id", "scope_id", "root_paths", "files", "edges", "unresolved", "cycles")

    def __init__(self, **kwargs: Any) -> None:
        self.graph_id = kwargs["graph_id"]
        self.discovery_id = kwargs["discovery_id"]
        self.migration_id = kwargs["migration_id"]
        self.scope_id = kwargs["scope_id"]
        self.root_paths = kwargs["root_paths"]
        self.files = kwargs["files"]
        self.edges = kwargs["edges"]
        self.unresolved = kwargs["unresolved"]
        self.cycles = kwargs["cycles"]

    def to_dict(self) -> dict[str, Any]:
        return {
            "graphId": self.graph_id,
            "discoveryId": self.discovery_id,
            "migrationId": self.migration_id,
            "scopeId": self.scope_id,
            "rootPaths": list(self.root_paths),
            "files": [f.to_dict() for f in self.files],
            "edges": [e.to_dict() for e in self.edges],
            "unresolved": [u.to_dict() for u in self.unresolved],
            "cycles": [c.to_dict() for c in self.cycles],
        }


class WorkspaceDiscoveryData:
    __slots__ = ("discovery_id", "scope_id", "file_count", "root_paths")

    def __init__(self, **kwargs: Any) -> None:
        self.discovery_id = kwargs["discovery_id"]
        self.scope_id = kwargs["scope_id"]
        self.file_count = kwargs["file_count"]
        self.root_paths = kwargs["root_paths"]

    def to_dict(self) -> dict[str, Any]:
        return {"discoveryId": self.discovery_id, "scopeId": self.scope_id, "fileCount": self.file_count, "rootPaths": list(self.root_paths)}


def _find_cycles(files: list[WorkspaceFileData], edges: list[DependencyEdgeData]) -> list[DependencyCycleData]:
    adjacency: dict[str, list[str]] = {f.path: [] for f in files}
    for edge in edges:
        adjacency.setdefault(edge.from_path, []).append(edge.to_path)
    for vals in adjacency.values():
        vals.sort()
    state: dict[str, str] = {}
    stack: list[str] = []
    found: dict[str, DependencyCycleData] = {}

    def dfs(node: str) -> None:
        state[node] = "VISITING"
        stack.append(node)
        for neighbor in adjacency.get(node, []):
            if state.get(neighbor) == "VISITING":
                cycle_start = stack.index(neighbor)
                cycle = stack[cycle_start:] + [neighbor]
                key = "->".join(cycle)
                if key not in found:
                    found[key] = DependencyCycleData(paths=cycle)
            elif state.get(neighbor) is None:
                dfs(neighbor)
        stack.pop()
        state[node] = "DONE"

    for f in files:
        if state.get(f.path) is None:
            dfs(f.path)
    return list(found.values())


def dependency_closure(graph: DependencyGraph, root_path: str) -> dict[str, Any]:
    file_by_path = {f.path: f for f in graph.files}
    if root_path not in file_by_path:
        raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, "root path is not in the dependency graph", {"rootPath": root_path})
    adjacency: dict[str, list[str]] = {f.path: [] for f in graph.files}
    for edge in graph.edges:
        adjacency.setdefault(edge.from_path, []).append(edge.to_path)
    visited: set[str] = set()
    order: list[str] = []
    unresolved: list[UnresolvedDependencyData] = []

    def dfs(node: str) -> None:
        if node in visited:
            return
        visited.add(node)
        for neighbor in adjacency.get(node, []):
            if neighbor in file_by_path:
                dfs(neighbor)
            else:
                pass
        order.append(node)

    dfs(root_path)
    return {"paths": order, "unresolved": [u.to_dict() for u in unresolved], "hasCycle": bool(graph.cycles)}


class WorkspaceDiscoveryService:
    def __init__(self) -> None:
        pass

    def discover(
        self, *,
        session: Any, scope: ScopeRef, files: Sequence[tuple[str, ArtifactRecord]],
        discovery_id: str | None = None, root_paths: Sequence[str] | None = None,
    ) -> tuple[WorkspaceDiscoveryData, DependencyGraph]:
        from sas2pyspark.services.clock import Clock
        scope = session._scopes.require_by_id(scope.id) if hasattr(session, "_scopes") else scope
        d_id = assert_safe_segment(discovery_id or f"discovery-{session._clock.uuid()}", "discoveryId") if hasattr(session, "_clock") else (discovery_id or "discovery")
        file_map: dict[str, WorkspaceFileData] = {}
        for path, artifact in files:
            norm = _normalize_workspace_path(path)
            file_map[norm] = WorkspaceFileData(path=norm, artifact=artifact_reference(artifact), is_sas=norm.lower().endswith(".sas"))
        roots = [_normalize_workspace_path(p) for p in (root_paths or [files[0][0]])]
        edges: list[DependencyEdgeData] = []
        unresolved: list[UnresolvedDependencyData] = []
        for f in file_map.values():
            if not f.is_sas:
                continue
            text = session._artifacts.read_text(scope, next(a for p, a in files if _normalize_workspace_path(p) == f.path)) if hasattr(session, "_artifacts") else ""
            for target, line, expr in _include_directives(text):
                candidates, invalid = _include_candidates(f.path, target)
                if invalid:
                    unresolved.append(UnresolvedDependencyData(from_path=f.path, target=target, line=line, reason="INVALID_PATH"))
                    continue
                resolved = next((c for c in candidates if c in file_map), None)
                if resolved:
                    edges.append(DependencyEdgeData(from_path=f.path, to_path=resolved, kind="include", line=line, expression=expr))
                else:
                    unresolved.append(UnresolvedDependencyData(from_path=f.path, target=target, line=line, reason="NOT_FOUND"))
        cycles = _find_cycles(list(file_map.values()), edges)
        graph = DependencyGraph(
            graph_id=f"graph-{d_id}",
            discovery_id=d_id,
            migration_id=session.migration_id,
            scope_id=scope.id,
            root_paths=roots,
            files=list(file_map.values()),
            edges=edges,
            unresolved=unresolved,
            cycles=cycles,
        )
        discovery = WorkspaceDiscoveryData(discovery_id=d_id, scope_id=scope.id, file_count=len(file_map), root_paths=roots)
        return discovery, graph


def dependency_graph_from_event(value: Any) -> DependencyGraph:
    if not isinstance(value, dict):
        raise WorkbenchError(WorkbenchErrorCode.SESSION_CORRUPTION, "dependency graph event data is invalid")
    raw = value.get("graph", value)
    if not isinstance(raw, dict):
        raise WorkbenchError(WorkbenchErrorCode.SESSION_CORRUPTION, "dependency graph must be an object")
    files = []
    for f in raw.get("files", []):
        files.append(WorkspaceFileData(
            path=_normalize_workspace_path(f["path"], WorkbenchErrorCode.SESSION_CORRUPTION),
            artifact=ArtifactReferenceData(**f["artifact"]),
            is_sas=f["isSas"],
        ))
    edges = []
    for e in raw.get("edges", []):
        edges.append(DependencyEdgeData(
            from_path=_normalize_workspace_path(e["from"], WorkbenchErrorCode.SESSION_CORRUPTION),
            to_path=_normalize_workspace_path(e["to"], WorkbenchErrorCode.SESSION_CORRUPTION),
            kind=e["kind"], line=e["line"], expression=e["expression"],
        ))
    unresolved = []
    for u in raw.get("unresolved", []):
        unresolved.append(UnresolvedDependencyData(
            from_path=_normalize_workspace_path(u["from"], WorkbenchErrorCode.SESSION_CORRUPTION),
            target=u["target"], line=u["line"], reason=u["reason"],
        ))
    cycles = [DependencyCycleData(paths=c["paths"]) for c in raw.get("cycles", [])]
    return DependencyGraph(
        graph_id=raw["graphId"], discovery_id=raw["discoveryId"],
        migration_id=raw["migrationId"], scope_id=raw["scopeId"],
        root_paths=[_normalize_workspace_path(p, WorkbenchErrorCode.SESSION_CORRUPTION) for p in raw.get("rootPaths", [])],
        files=files, edges=edges, unresolved=unresolved, cycles=cycles,
    )


def workspace_discovery_from_event(value: Any) -> WorkspaceDiscoveryData:
    if not isinstance(value, dict):
        raise WorkbenchError(WorkbenchErrorCode.SESSION_CORRUPTION, "workspace discovery event data is invalid")
    return WorkspaceDiscoveryData(
        discovery_id=value["discoveryId"],
        scope_id=value["scopeId"],
        file_count=value["fileCount"],
        root_paths=[_normalize_workspace_path(p, WorkbenchErrorCode.SESSION_CORRUPTION) for p in value.get("rootPaths", [])],
    )
