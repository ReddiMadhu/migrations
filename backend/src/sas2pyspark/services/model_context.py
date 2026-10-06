"""ModelContextService — bounded prompt context as a projection of durable data."""

from __future__ import annotations

from typing import Any

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.core.json import snapshot_json
from sas2pyspark.core.hash import sha256_json
from sas2pyspark.services.scopes import is_descendant

MAX_MODEL_CONTEXT_NODES = 128
MAX_MODEL_CONTEXT_EXCERPT_BYTES = 64 * 1024
MAX_MODEL_CONTEXT_EVIDENCE = 32
MAX_MODEL_CONTEXT_SOURCE_ARTIFACT_BYTES = 2 * 1024 * 1024

CONTEXT_VERSION = "bounded-model-context-v1"


class BoundedModelContext:
    __slots__ = (
        "context_version", "context_digest", "session_id", "migration_id", "scope_id",
        "work_unit_id", "sas_module", "dependency_closure", "status",
        "ir_snapshot_id", "macro_resolution_id", "macro", "reference_output",
        "candidate_id", "failure_signature", "nodes", "evidence",
        "omitted_node_ids", "omitted_evidence_count", "total_excerpt_bytes", "limits",
    )

    def __init__(self, **kwargs: Any) -> None:
        for slot in self.__slots__:
            setattr(self, slot, kwargs.get(slot))

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "contextVersion": self.context_version,
            "contextDigest": self.context_digest,
            "sessionId": self.session_id,
            "migrationId": self.migration_id,
            "scopeId": self.scope_id,
            "workUnitId": self.work_unit_id,
            "sasModule": self.sas_module,
            "dependencyClosure": list(self.dependency_closure),
            "status": self.status,
            "nodes": self.nodes,
            "evidence": self.evidence,
            "omittedNodeIds": list(self.omitted_node_ids),
            "omittedEvidenceCount": self.omitted_evidence_count,
            "totalExcerptBytes": self.total_excerpt_bytes,
            "limits": self.limits,
        }
        for attr, alias in [
            ("ir_snapshot_id", "irSnapshotId"),
            ("macro_resolution_id", "macroResolutionId"),
            ("candidate_id", "candidateId"),
            ("failure_signature", "failureSignature"),
        ]:
            val = getattr(self, attr)
            if val is not None:
                d[alias] = val
        if self.macro is not None:
            d["macro"] = self.macro
        if self.reference_output is not None:
            d["referenceOutput"] = self.reference_output
        return d


class ModelContextService:
    def __init__(self, artifacts: Any, scopes: Any, clock: Any, work_units: Any) -> None:
        self._artifacts = artifacts
        self._scopes = scopes
        self._clock = clock
        self._work_units = work_units

    def assemble(
        self, *,
        session: Any, work_unit_id: str,
        max_nodes: int | None = None,
        max_excerpt_bytes: int | None = None,
        max_evidence: int | None = None,
    ) -> BoundedModelContext:
        session_scope = self._scopes.require_by_id(session.scope.id)
        work_unit = self._work_units.get(session, work_unit_id)
        if work_unit is None:
            raise WorkbenchError(
                WorkbenchErrorCode.INVALID_INPUT,
                "work unit does not exist in the session projection",
                {"workUnitId": work_unit_id, "sessionId": session.id},
            )
        if work_unit.migration_id != session.migration_id:
            raise WorkbenchError(
                WorkbenchErrorCode.INVALID_INPUT,
                "work unit migration identity does not match the session",
                {"workUnitId": work_unit.work_unit_id},
            )
        work_unit_scope = self._scopes.get(work_unit.scope_id)
        if work_unit_scope is None or not is_descendant(session_scope, work_unit_scope):
            raise WorkbenchError(
                WorkbenchErrorCode.POLICY_DENIED,
                "work unit belongs to another candidate branch",
                {"workUnitId": work_unit.work_unit_id, "scopeId": work_unit.scope_id},
            )
        mn = max_nodes or MAX_MODEL_CONTEXT_NODES
        meb = max_excerpt_bytes or MAX_MODEL_CONTEXT_EXCERPT_BYTES
        me = max_evidence or MAX_MODEL_CONTEXT_EVIDENCE
        ir = session.projection["semanticIrs"].get(work_unit.ir_snapshot_id) if work_unit.ir_snapshot_id else None
        if work_unit.ir_snapshot_id and not ir:
            raise WorkbenchError(
                WorkbenchErrorCode.INVALID_INPUT,
                "work unit IR snapshot is not present in the session projection",
                {"workUnitId": work_unit.work_unit_id, "irSnapshotId": work_unit.ir_snapshot_id},
            )
        nodes: list[dict[str, Any]] = []
        omitted_node_ids: list[str] = []
        total_excerpt_bytes = 0
        if ir and work_unit.ir_node_ids:
            by_id = {n.node_id: n for n in ir.nodes}
            allowed_paths = {work_unit.sas_module, *work_unit.dependency_closure}
            selected = list(work_unit.ir_node_ids[:mn])
            omitted_node_ids = list(work_unit.ir_node_ids[mn:])
            file_by_path = {f.path: f for f in ir.files}
            remaining = meb
            for node_id in selected:
                if remaining == 0:
                    omitted_node_ids.append(node_id)
                    continue
                node = by_id.get(node_id)
                if node is None:
                    raise WorkbenchError(
                        WorkbenchErrorCode.INVALID_INPUT,
                        "work unit references an IR node that is absent from its immutable snapshot",
                        {"workUnitId": work_unit.work_unit_id, "nodeId": node_id},
                    )
                if node.span.path not in allowed_paths:
                    raise WorkbenchError(
                        WorkbenchErrorCode.INVALID_INPUT,
                        "work unit IR node is outside its bounded dependency closure",
                        {"workUnitId": work_unit.work_unit_id, "nodeId": node_id, "path": node.span.path},
                    )
                file = file_by_path.get(node.span.path)
                if file is None:
                    raise WorkbenchError(
                        WorkbenchErrorCode.INVALID_INPUT,
                        "work unit IR node references a file absent from its snapshot",
                        {"workUnitId": work_unit.work_unit_id, "nodeId": node_id, "path": node.span.path},
                    )
                try:
                    text = self._artifacts.read_reference(work_unit_scope, file.artifact).decode("utf-8")
                except Exception:
                    text = ""
                if node.span.start_offset < 0 or node.span.end_offset < node.span.start_offset or node.span.end_offset > len(text):
                    raise WorkbenchError(
                        WorkbenchErrorCode.INVALID_INPUT,
                        "work unit IR node has an invalid source span",
                        {"workUnitId": work_unit.work_unit_id, "nodeId": node_id, "path": node.span.path},
                    )
                raw_excerpt = text[node.span.start_offset:node.span.end_offset]
                excerpt = raw_excerpt[:remaining]
                excerpt_bytes = len(excerpt.encode("utf-8"))
                remaining -= excerpt_bytes
                total_excerpt_bytes += excerpt_bytes
                nodes.append({
                    "nodeId": node.node_id, "kind": node.kind, "name": node.name,
                    "span": node.span.to_dict(),
                    "attributes": [a.to_dict() for a in node.attributes],
                    "artifact": file.artifact.model_dump(by_alias=True),
                    "excerpt": excerpt, "excerptBytes": excerpt_bytes,
                    "excerptTruncated": excerpt != raw_excerpt,
                })
        evidence: list[dict[str, Any]] = []
        for event in session.events:
            data = event.data
            if event.type.value == "reconciliation/completed" and data.get("workUnitId") == work_unit_id:
                evidence.append({
                    "type": "reconciliation-completed", "eventId": event.event_id,
                    "sequence": event.sequence, "workUnitId": work_unit_id,
                    "reconciliationId": data.get("reconciliationId"),
                    "certifiable": data.get("certifiable"),
                    "schemaVerdict": data.get("schemaVerdict"),
                    "aggregateVerdict": data.get("aggregateVerdict"),
                    "rowVerdict": data.get("rowVerdict"),
                    "statisticalVerdict": data.get("statisticalVerdict"),
                    "differenceCount": data.get("differenceCount"),
                    "maxDifferences": data.get("maxDifferences"),
                    "referenceRows": data.get("referenceRows"),
                    "candidateRows": data.get("candidateRows"),
                    "reportArtifact": data.get("reportArtifact"),
                })
            elif event.type.value == "reconciliation/failed" and data.get("workUnitId") == work_unit_id:
                evidence.append({
                    "type": "reconciliation-failed", "eventId": event.event_id,
                    "sequence": event.sequence, "workUnitId": work_unit_id,
                    "reconciliationId": data.get("reconciliationId"),
                    "reasonCode": data.get("reasonCode"),
                    "detail": data.get("detail"),
                })
            elif event.type.value == "execution/completed" and data.get("workUnitId") == work_unit_id:
                evidence.append({
                    "type": "execution-completed", "eventId": event.event_id,
                    "sequence": event.sequence, "workUnitId": work_unit_id,
                    "executionId": data.get("executionId"),
                    "outcome": data.get("outcome"),
                })
        evidence.sort(key=lambda e: (e["sequence"], e["eventId"]))
        omitted_evidence = max(0, len(evidence) - me)
        evidence = evidence[max(0, len(evidence) - me):]
        draft: dict[str, Any] = {
            "context_version": CONTEXT_VERSION,
            "session_id": session.id,
            "migration_id": work_unit.migration_id,
            "scope_id": work_unit.scope_id,
            "work_unit_id": work_unit.work_unit_id,
            "sas_module": work_unit.sas_module,
            "dependency_closure": list(work_unit.dependency_closure),
            "status": work_unit.status,
            "nodes": nodes,
            "evidence": evidence,
            "omitted_node_ids": omitted_node_ids,
            "omitted_evidence_count": omitted_evidence,
            "total_excerpt_bytes": total_excerpt_bytes,
            "limits": {"maxNodes": mn, "maxExcerptBytes": meb, "maxEvidence": me},
        }
        if work_unit.ir_snapshot_id is not None:
            draft["ir_snapshot_id"] = work_unit.ir_snapshot_id
        if work_unit.macro_resolution_id is not None:
            draft["macro_resolution_id"] = work_unit.macro_resolution_id
        if work_unit.candidate_id is not None:
            draft["candidate_id"] = work_unit.candidate_id
        if work_unit.failure_signature is not None:
            draft["failure_signature"] = work_unit.failure_signature
        context = BoundedModelContext(**draft)
        context_digest = sha256_json(snapshot_json(context.to_dict()))
        context.context_digest = context_digest
        return context
