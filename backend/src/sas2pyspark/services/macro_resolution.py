"""MacroResolutionService — static macro definitions, call sites, unresolved evidence."""

from __future__ import annotations

import re
from typing import Any, Sequence

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.core.json import assert_safe_segment, snapshot_json
from sas2pyspark.core.events import ArtifactReferenceData
from sas2pyspark.services.scopes import ScopeRef
from sas2pyspark.services.semantic_ir import SemanticIrFile, SourceSpan

MAX_MACRO_DEFINITIONS = 256
MAX_MACRO_CALLS = 2048
MAX_MACRO_UNRESOLVED = 512
MAX_MACRO_DIAGNOSTICS = 256
MAX_MACRO_TEXT_LENGTH = 1024
MAX_MACRO_PARAMETERS = 64
RESOLVER_VERSION = "sas-static-macro-v1"

MacroCallStatus = str  # "RESOLVED" | "UNRESOLVED" | "AMBIGUOUS"
MacroResolutionReason = str  # "NO_DEFINITION" | "MULTIPLE_DEFINITIONS" | ...


class MacroDefinition:
    __slots__ = ("definition_id", "name", "normalized_name", "path", "parameters", "span", "body_span")

    def __init__(self, **kwargs: Any) -> None:
        self.definition_id = kwargs["definition_id"]
        self.name = kwargs["name"]
        self.normalized_name = kwargs["normalized_name"]
        self.path = kwargs["path"]
        self.parameters = kwargs["parameters"]
        self.span = kwargs["span"]
        self.body_span = kwargs["body_span"]

    def to_dict(self) -> dict[str, Any]:
        return {"definitionId": self.definition_id, "name": self.name, "normalizedName": self.normalized_name, "path": self.path, "parameters": list(self.parameters), "span": self.span.to_dict(), "bodySpan": self.body_span.to_dict()}


class MacroCall:
    __slots__ = ("call_id", "name", "normalized_name", "path", "span", "argument_text", "status", "definition_id", "reason_code")

    def __init__(self, **kwargs: Any) -> None:
        self.call_id = kwargs["call_id"]
        self.name = kwargs["name"]
        self.normalized_name = kwargs["normalized_name"]
        self.path = kwargs["path"]
        self.span = kwargs["span"]
        self.argument_text = kwargs.get("argument_text")
        self.status = kwargs["status"]
        self.definition_id = kwargs.get("definition_id")
        self.reason_code = kwargs.get("reason_code")

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"callId": self.call_id, "name": self.name, "normalizedName": self.normalized_name, "path": self.path, "span": self.span.to_dict(), "status": self.status}
        if self.argument_text is not None:
            d["argumentText"] = self.argument_text
        if self.definition_id is not None:
            d["definitionId"] = self.definition_id
        if self.reason_code is not None:
            d["reasonCode"] = self.reason_code
        return d


class UnresolvedMacroEvidence:
    __slots__ = ("call_id", "name", "normalized_name", "path", "span", "reason_code")

    def __init__(self, **kwargs: Any) -> None:
        self.call_id = kwargs["call_id"]
        self.name = kwargs["name"]
        self.normalized_name = kwargs["normalized_name"]
        self.path = kwargs["path"]
        self.span = kwargs["span"]
        self.reason_code = kwargs["reason_code"]

    def to_dict(self) -> dict[str, Any]:
        return {"callId": self.call_id, "name": self.name, "normalizedName": self.normalized_name, "path": self.path, "span": self.span.to_dict(), "reasonCode": self.reason_code}


class MacroDiagnostic:
    __slots__ = ("code", "message", "path", "span")

    def __init__(self, **kwargs: Any) -> None:
        self.code = kwargs["code"]
        self.message = kwargs["message"]
        self.path = kwargs["path"]
        self.span = kwargs["span"]

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "path": self.path, "span": self.span.to_dict()}


class MacroResolutionSnapshot:
    __slots__ = ("resolution_id", "graph_id", "migration_id", "scope_id", "root_path", "resolver_version", "files", "definitions", "calls", "unresolved", "diagnostics")

    def __init__(self, **kwargs: Any) -> None:
        self.resolution_id = kwargs["resolution_id"]
        self.graph_id = kwargs["graph_id"]
        self.migration_id = kwargs["migration_id"]
        self.scope_id = kwargs["scope_id"]
        self.root_path = kwargs["root_path"]
        self.resolver_version = kwargs["resolver_version"]
        self.files = kwargs["files"]
        self.definitions = kwargs["definitions"]
        self.calls = kwargs["calls"]
        self.unresolved = kwargs["unresolved"]
        self.diagnostics = kwargs["diagnostics"]

    def to_dict(self) -> dict[str, Any]:
        return {"resolutionId": self.resolution_id, "graphId": self.graph_id, "migrationId": self.migration_id, "scopeId": self.scope_id, "rootPath": self.root_path, "resolverVersion": self.resolver_version, "files": [f.to_dict() for f in self.files], "definitions": [d.to_dict() for d in self.definitions], "calls": [c.to_dict() for c in self.calls], "unresolved": [u.to_dict() for u in self.unresolved], "diagnostics": [d.to_dict() for d in self.diagnostics]}


def _span_from_dict(value: Any, code: WorkbenchErrorCode) -> SourceSpan:
    if not isinstance(value, dict):
        raise WorkbenchError(code, "macro source span must be an object")
    return SourceSpan(
        path=value["path"], start_line=value["startLine"], end_line=value["endLine"],
        start_column=value["startColumn"], end_column=value["endColumn"],
        start_offset=value["startOffset"], end_offset=value["endOffset"],
    )


class MacroResolutionService:
    def __init__(self) -> None:
        pass

    def create(
        self, *,
        session: Any, scope: ScopeRef, graph: Any, root_path: str, resolution_id: str | None = None,
    ) -> MacroResolutionSnapshot:
        scope = session._scopes.require_by_id(scope.id) if hasattr(session, "_scopes") else scope
        rid = assert_safe_segment(resolution_id or f"macro-{session._clock.uuid()}", "resolutionId") if hasattr(session, "_clock") else (resolution_id or "macro")
        files: list[SemanticIrFile] = []
        definitions: list[MacroDefinition] = []
        calls: list[MacroCall] = []
        unresolved: list[UnresolvedMacroEvidence] = []
        diagnostics: list[MacroDiagnostic] = []
        def_by_name: dict[str, MacroDefinition] = {}
        for f in graph.files:
            if not f.is_sas:
                continue
            text = session._artifacts.read_text(scope, f.artifact) if hasattr(session, "_artifacts") else ""
            lines = re.split(r"\r\n|\n|\r", text)
            files.append(SemanticIrFile(path=f.path, artifact=f.artifact, line_count=len(lines)))
            offset = 0
            for line_idx, line in enumerate(lines):
                line_offset = offset
                macro_def = re.match(r"^%macro\s+([A-Za-z_][\w]*)(?:\s*\(([^)]*)\))?", line.strip(), re.I)
                if macro_def:
                    name = macro_def.group(1)
                    params = [p.strip() for p in macro_def.group(2).split(",")] if macro_def.group(2) else []
                    span = SourceSpan(path=f.path, start_line=line_idx + 1, end_line=line_idx + 1, start_column=1, end_column=len(line) + 1, start_offset=line_offset, end_offset=line_offset + len(line))
                    defn = MacroDefinition(definition_id=f"def-{name}-{f.path}", name=name, normalized_name=name.lower(), path=f.path, parameters=params, span=span, body_span=span)
                    definitions.append(defn)
                    def_by_name[name.lower()] = defn
                macro_call = re.match(r"^%([A-Za-z_][\w]*)(?:\s*\(([^)]*)\))?", line.strip())
                if macro_call and not macro_def:
                    name = macro_call.group(1)
                    if name.lower() in ("macro", "mend", "let", "include", "if", "then", "else", "do", "end", "global", "local", "put", "abort"):
                        continue
                    norm = name.lower()
                    span = SourceSpan(path=f.path, start_line=line_idx + 1, end_line=line_idx + 1, start_column=1, end_column=len(line) + 1, start_offset=line_offset, end_offset=line_offset + len(line))
                    call_id = f"call-{name}-{f.path}-{line_idx + 1}"
                    if norm in def_by_name:
                        calls.append(MacroCall(call_id=call_id, name=name, normalized_name=norm, path=f.path, span=span, argument_text=macro_call.group(2), status="RESOLVED", definition_id=def_by_name[norm].definition_id))
                    else:
                        calls.append(MacroCall(call_id=call_id, name=name, normalized_name=norm, path=f.path, span=span, argument_text=macro_call.group(2), status="UNRESOLVED", reason_code="NO_DEFINITION"))
                        unresolved.append(UnresolvedMacroEvidence(call_id=call_id, name=name, normalized_name=norm, path=f.path, span=span, reason_code="NO_DEFINITION"))
                offset += len(line) + 1
        return MacroResolutionSnapshot(
            resolution_id=rid, graph_id=graph.graph_id, migration_id=session.migration_id,
            scope_id=scope.id, root_path=root_path, resolver_version=RESOLVER_VERSION,
            files=files, definitions=definitions[:MAX_MACRO_DEFINITIONS],
            calls=calls[:MAX_MACRO_CALLS], unresolved=unresolved[:MAX_MACRO_UNRESOLVED],
            diagnostics=diagnostics[:MAX_MACRO_DIAGNOSTICS],
        )


def macro_resolution_from_event(value: Any) -> MacroResolutionSnapshot:
    if not isinstance(value, dict):
        raise WorkbenchError(WorkbenchErrorCode.SESSION_CORRUPTION, "macro resolution event data is invalid")
    raw = value.get("resolution", value)
    files = [SemanticIrFile(path=f["path"], artifact=ArtifactReferenceData(**f["artifact"]), line_count=f["lineCount"]) for f in raw.get("files", [])]
    definitions = []
    for d in raw.get("definitions", []):
        definitions.append(MacroDefinition(definition_id=d["definitionId"], name=d["name"], normalized_name=d["normalizedName"], path=d["path"], parameters=d.get("parameters", []), span=_span_from_dict(d["span"], WorkbenchErrorCode.SESSION_CORRUPTION), body_span=_span_from_dict(d["bodySpan"], WorkbenchErrorCode.SESSION_CORRUPTION)))
    calls = []
    for c in raw.get("calls", []):
        calls.append(MacroCall(call_id=c["callId"], name=c["name"], normalized_name=c["normalizedName"], path=c["path"], span=_span_from_dict(c["span"], WorkbenchErrorCode.SESSION_CORRUPTION), argument_text=c.get("argumentText"), status=c["status"], definition_id=c.get("definitionId"), reason_code=c.get("reasonCode")))
    unresolved = []
    for u in raw.get("unresolved", []):
        unresolved.append(UnresolvedMacroEvidence(call_id=u["callId"], name=u["name"], normalized_name=u["normalizedName"], path=u["path"], span=_span_from_dict(u["span"], WorkbenchErrorCode.SESSION_CORRUPTION), reason_code=u["reasonCode"]))
    diagnostics = []
    for d in raw.get("diagnostics", []):
        diagnostics.append(MacroDiagnostic(code=d["code"], message=d["message"], path=d["path"], span=_span_from_dict(d["span"], WorkbenchErrorCode.SESSION_CORRUPTION)))
    return MacroResolutionSnapshot(
        resolution_id=raw["resolutionId"], graph_id=raw["graphId"], migration_id=raw["migrationId"],
        scope_id=raw["scopeId"], root_path=raw["rootPath"], resolver_version=raw["resolverVersion"],
        files=files, definitions=definitions, calls=calls, unresolved=unresolved, diagnostics=diagnostics,
    )
