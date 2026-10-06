"""Reconciler Protocol + InMemoryReconciler adapter — bridges the graph's reconcile() calling convention to ReconciliationService.compare()."""

from __future__ import annotations

import json
import os
from typing import Any, Protocol, runtime_checkable

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.services.artifacts import ArtifactReferenceData
from sas2pyspark.services.reconciliation import (
    Dataset,
    ReconciliationDifference,
    ReconciliationPolicy,
    ReconciliationService,
    ReconciliationSummary,
    _csv_rows,
    _json_rows,
    _parse_csv,
    _resolve_policy,
    _summarize_dataset,
)
from sas2pyspark.services.scopes import ScopeRef, ScopeService


@runtime_checkable
class Reconciler(Protocol):
    """Reconciler protocol — the graph calls reconcile() with keyword arguments."""

    def reconcile(
        self,
        *,
        reference_output_path: str,
        candidate_artifacts: list[dict[str, Any]],
        input_snapshot_id: str,
        environment_fingerprint: str,
        policy: dict[str, Any],
        scope_id: str,
    ) -> dict[str, Any]: ...


def _summary_to_dict(summary: ReconciliationSummary) -> dict[str, Any]:
    return {
        "schemaVerdict": summary.schema_verdict,
        "aggregateVerdict": summary.aggregate_verdict,
        "rowVerdict": summary.row_verdict,
        "statisticalVerdict": summary.statistical_verdict,
        "certifiable": summary.certifiable,
        "differenceCount": summary.difference_count,
        "differences": [d.to_dict() for d in summary.differences],
        "referenceRows": summary.reference_rows,
        "candidateRows": summary.candidate_rows,
    }


def _load_reference_dataset(path: str, policy: ReconciliationPolicy) -> Dataset:
    if not os.path.isfile(path):
        raise WorkbenchError(
            WorkbenchErrorCode.INVALID_INPUT,
            "reference output path does not exist",
            {"path": path},
        )
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    if path.endswith(".json"):
        return _summarize_dataset(_json_rows(text), policy)
    if path.endswith(".csv"):
        raw_rows = _parse_csv(text)
        if not raw_rows:
            return _summarize_dataset([], policy)
        headers = [str(v) for v in raw_rows[0]]
        nl = text.index("\n") + 1 if "\n" in text else len(text)
        return _summarize_dataset(_csv_rows(text[nl:], headers), policy)
    if text.lstrip().startswith("[") or text.lstrip().startswith("{"):
        return _summarize_dataset(_json_rows(text), policy)
    raw_rows = _parse_csv(text)
    if not raw_rows:
        return _summarize_dataset([], policy)
    headers = [str(v) for v in raw_rows[0]]
    nl = text.index("\n") + 1 if "\n" in text else len(text)
    return _summarize_dataset(_csv_rows(text[nl:], headers), policy)


class InMemoryReconciler:
    """Adapter that wraps ReconciliationService.compare() as reconcile()."""

    def __init__(self, service: ReconciliationService, scopes: ScopeService) -> None:
        self._service = service
        self._scopes = scopes

    def _load_candidate_dataset(self, scope: ScopeRef, artifact_ref: dict[str, Any], policy: ReconciliationPolicy) -> Dataset:
        ref = ArtifactReferenceData(**artifact_ref)
        text = self._service._artifacts.read_text(scope, ref)
        media_type = ref.media_type
        if "json" in media_type:
            return _summarize_dataset(_json_rows(text), policy)
        if "csv" in media_type:
            raw_rows = _parse_csv(text)
            if not raw_rows:
                return _summarize_dataset([], policy)
            headers = [str(v) for v in raw_rows[0]]
            nl = text.index("\n") + 1 if "\n" in text else len(text)
            return _summarize_dataset(_csv_rows(text[nl:], headers), policy)
        raise WorkbenchError(WorkbenchErrorCode.UNKNOWN_FORMAT, "reconciliation supports JSON or CSV artifacts", {"mediaType": media_type})

    def reconcile(
        self,
        *,
        reference_output_path: str,
        candidate_artifacts: list[dict[str, Any]],
        input_snapshot_id: str,
        environment_fingerprint: str,
        policy: dict[str, Any],
        scope_id: str,
    ) -> dict[str, Any]:
        resolved_policy = _resolve_policy(policy)
        scope = self._scopes.require_by_id(scope_id)
        reference = _load_reference_dataset(reference_output_path, resolved_policy)
        if not candidate_artifacts:
            raise WorkbenchError(
                WorkbenchErrorCode.INVALID_INPUT,
                "reconciliation requires at least one candidate artifact",
            )
        candidate = self._load_candidate_dataset(scope, candidate_artifacts[0], resolved_policy)
        summary = self._service.compare(reference, candidate, resolved_policy)
        return _summary_to_dict(summary)


class SparkReconciler:
    """Reconciler that delegates to spark_profile_job.profile_pair for Spark-native profiling."""

    def __init__(self, spark: Any, scopes: ScopeService) -> None:
        self._spark = spark
        self._scopes = scopes

    def reconcile(
        self,
        *,
        reference_output_path: str,
        candidate_artifacts: list[dict[str, Any]],
        input_snapshot_id: str,
        environment_fingerprint: str,
        policy: dict[str, Any],
        scope_id: str,
    ) -> dict[str, Any]:
        from sas2pyspark.providers.spark_profile_job import profile_pair

        resolved_policy = _resolve_policy(policy)
        if not candidate_artifacts:
            raise WorkbenchError(
                WorkbenchErrorCode.INVALID_INPUT,
                "reconciliation requires at least one candidate artifact",
            )
        candidate_ref = ArtifactReferenceData(**candidate_artifacts[0])
        scope = self._scopes.require_by_id(scope_id)
        candidate_path = self._service._artifacts._path_for(scope, candidate_ref) if hasattr(self, "_service") else None
        if candidate_path is None:
            raise WorkbenchError(
                WorkbenchErrorCode.INVALID_INPUT,
                "SparkReconciler requires a candidate artifact path",
            )
        result = profile_pair(
            self._spark,
            reference_path=reference_output_path,
            candidate_path=candidate_path,
            row_order=resolved_policy.row_order,
        )
        result["referenceRows"] = result.get("referenceProfile", {}).get("rowCount", 0)
        result["candidateRows"] = result.get("candidateProfile", {}).get("rowCount", 0)
        return result


class ThresholdReconciler:
    """Delegates to InMemoryReconciler below threshold, SparkReconciler above."""

    def __init__(self, in_memory: InMemoryReconciler, spark: SparkReconciler | None = None) -> None:
        self._in_memory = in_memory
        self._spark = spark

    def _estimate_size(self, reference_output_path: str, candidate_artifacts: list[dict[str, Any]]) -> int:
        total = 0
        if os.path.isfile(reference_output_path):
            total += os.path.getsize(reference_output_path)
        for art in candidate_artifacts:
            if isinstance(art.get("size"), int):
                total += art["size"]
        return total

    def reconcile(
        self,
        *,
        reference_output_path: str,
        candidate_artifacts: list[dict[str, Any]],
        input_snapshot_id: str,
        environment_fingerprint: str,
        policy: dict[str, Any],
        scope_id: str,
    ) -> dict[str, Any]:
        resolved_policy = _resolve_policy(policy)
        threshold = resolved_policy.spark_threshold_bytes
        estimated_size = self._estimate_size(reference_output_path, candidate_artifacts)
        if estimated_size > threshold and self._spark is not None:
            return self._spark.reconcile(
                reference_output_path=reference_output_path,
                candidate_artifacts=candidate_artifacts,
                input_snapshot_id=input_snapshot_id,
                environment_fingerprint=environment_fingerprint,
                policy=policy,
                scope_id=scope_id,
            )
        return self._in_memory.reconcile(
            reference_output_path=reference_output_path,
            candidate_artifacts=candidate_artifacts,
            input_snapshot_id=input_snapshot_id,
            environment_fingerprint=environment_fingerprint,
            policy=policy,
            scope_id=scope_id,
        )
