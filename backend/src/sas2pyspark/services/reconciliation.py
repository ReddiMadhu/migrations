"""ReconciliationService — deterministic dataset parity with four verdicts."""

from __future__ import annotations

import json
import re
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any, Sequence

from pydantic import Field

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode, error_message
from sas2pyspark.core.json import canonical_json, snapshot_json
from sas2pyspark.core.events import ArtifactReferenceData, Verdict
from sas2pyspark.services.artifacts import ArtifactRecord, FileSystemArtifactService, artifact_reference
from sas2pyspark.services.clock import Clock
from sas2pyspark.services.policy import canonical_fingerprint
from sas2pyspark.services.scopes import ScopeRef
from sas2pyspark.core.models import WorkbenchModel


class ReconciliationPolicy(WorkbenchModel):
    version: str
    schema: str  # "exact" | "relaxed"
    row_order: str = Field(alias="rowOrder")  # "ordered" | "unordered"
    null_equality: str = Field(alias="nullEquality")  # "strict" | "sql-like"
    decimal_normalization: str = Field(alias="decimalNormalization")  # "none" | "trim-zeros"
    max_differences: int = Field(alias="maxDifferences")
    statistical: str  # "deterministic-profile" | "disabled"
    spark_threshold_bytes: int = Field(default=52428800, alias="sparkThresholdBytes")
    float_tolerance: float = Field(default=0.0, alias="floatTolerance")
    nan_equality: str = Field(default="equal", alias="nanEquality")
    timezone: str = Field(default="UTC", alias="timezone")

    model_config = {**WorkbenchModel.model_config, "populate_by_name": True, "protected_namespaces": ()}


DEFAULT_RECONCILIATION_POLICY = ReconciliationPolicy(
    version="reconciliation-v1",
    schema="exact",
    rowOrder="ordered",
    nullEquality="sql-like",
    decimalNormalization="trim-zeros",
    maxDifferences=25,
    statistical="deterministic-profile",
)


def _resolve_policy(policy: dict[str, Any] | None = None) -> ReconciliationPolicy:
    p = policy or {}
    merged: dict[str, Any] = {
        "version": DEFAULT_RECONCILIATION_POLICY.version,
        "schema": DEFAULT_RECONCILIATION_POLICY.schema,
        "rowOrder": DEFAULT_RECONCILIATION_POLICY.row_order,
        "nullEquality": DEFAULT_RECONCILIATION_POLICY.null_equality,
        "decimalNormalization": DEFAULT_RECONCILIATION_POLICY.decimal_normalization,
        "maxDifferences": DEFAULT_RECONCILIATION_POLICY.max_differences,
        "statistical": DEFAULT_RECONCILIATION_POLICY.statistical,
        "sparkThresholdBytes": DEFAULT_RECONCILIATION_POLICY.spark_threshold_bytes,
        "floatTolerance": DEFAULT_RECONCILIATION_POLICY.float_tolerance,
        "nanEquality": DEFAULT_RECONCILIATION_POLICY.nan_equality,
        "timezone": DEFAULT_RECONCILIATION_POLICY.timezone,
    }
    merged.update(p)
    if not isinstance(merged["maxDifferences"], int) or not (1 <= merged["maxDifferences"] <= 1000):
        raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, "reconciliation maxDifferences must be an integer between 1 and 1000")
    return ReconciliationPolicy(**merged)


def _non_null(value: Any, null_equality: str) -> bool:
    if value is None:
        return False
    if null_equality == "sql-like" and isinstance(value, str) and value.strip().upper() == "NULL":
        return False
    return True


def _normalize_decimal(value: float | int) -> str:
    if isinstance(value, int):
        return str(value)
    if not isinstance(value, (int, float)):
        raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, "reconciliation encountered a non-numeric value")
    text = repr(value)
    if "e" not in text.lower():
        return text
    d = Decimal(str(value))
    return format(d, "f")


def _trim_zeros(value: str) -> str:
    negative = value.startswith("-")
    unsigned = value[1:] if negative else value
    if "." in unsigned:
        int_part, frac_part = unsigned.split(".", 1)
    else:
        int_part, frac_part = unsigned, ""
    trimmed_int = int_part.lstrip("0") or "0"
    trimmed_frac = frac_part.rstrip("0")
    if trimmed_frac:
        result = f"{trimmed_int}.{trimmed_frac}"
    else:
        result = trimmed_int
    if result == "0":
        return "0"
    return f"-{result}" if negative else result


def _value_key(value: Any, policy: ReconciliationPolicy) -> str:
    if not _non_null(value, policy.null_equality):
        return "null"
    if isinstance(value, str):
        return f"string:{json.dumps(value)}"
    if isinstance(value, bool):
        return f"boolean:{value}"
    if isinstance(value, (int, float)):
        decimal = _normalize_decimal(value)
        if policy.decimal_normalization == "trim-zeros":
            decimal = _trim_zeros(decimal)
        return f"number:{decimal}"
    return f"json:{canonical_json(snapshot_json(value))}"


def _row_key(row: dict[str, Any], policy: ReconciliationPolicy) -> str:
    entries = [[key, _value_key(row[key], policy)] for key in sorted(row.keys())]
    return canonical_json(entries)


def _type_name(value: Any) -> str:
    if not _non_null(value, "strict"):
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, (list, tuple)):
        return "array"
    return "object"


def _add_decimal(left: str, right: str) -> str:
    try:
        with localcontext() as ctx:
            ctx.prec = 50
            d = Decimal(left) + Decimal(right)
        result = format(d, "f")
    except (InvalidOperation, ValueError) as exc:
        raise WorkbenchError(
            WorkbenchErrorCode.INVALID_INPUT,
            "reconciliation encountered a non-numeric value during decimal aggregation",
            {"left": left, "right": right, "error": str(exc)},
        ) from exc
    return _trim_zeros(result) if result else "0"


class ValueProfile:
    __slots__ = ("null_count", "non_null_count", "type_counts", "numeric_sum")

    def __init__(self) -> None:
        self.null_count = 0
        self.non_null_count = 0
        self.type_counts: dict[str, int] = {}
        self.numeric_sum = "0"


class Dataset:
    __slots__ = ("rows", "headers", "profiles", "row_hashes", "profile_hash")

    def __init__(self, rows: list[dict[str, Any]], headers: list[str], profiles: dict[str, ValueProfile], row_hashes: list[str], profile_hash: str) -> None:
        self.rows = rows
        self.headers = headers
        self.profiles = profiles
        self.row_hashes = row_hashes
        self.profile_hash = profile_hash


def _profile_column(rows: list[dict[str, Any]], column: str, policy: ReconciliationPolicy) -> ValueProfile:
    profile = ValueProfile()
    for row in rows:
        value = row.get(column)
        if not _non_null(value, policy.null_equality):
            profile.null_count += 1
            profile.type_counts["null"] = profile.type_counts.get("null", 0) + 1
            continue
        profile.non_null_count += 1
        tn = _type_name(value)
        profile.type_counts[tn] = profile.type_counts.get(tn, 0) + 1
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            profile.numeric_sum = _add_decimal(profile.numeric_sum, _normalize_decimal(value))
    return profile


def _dataset_hash(row_hashes: list[str], headers: list[str], policy: ReconciliationPolicy) -> str:
    ordered = sorted(row_hashes) if policy.row_order == "unordered" else row_hashes
    return canonical_json({"headers": headers, "rows": ordered})


def _empty_profile() -> ValueProfile:
    return ValueProfile()


def _summarize_dataset(rows: list[dict[str, Any]], policy: ReconciliationPolicy) -> Dataset:
    header_set: set[str] = set()
    for row in rows:
        header_set.update(row.keys())
    headers = sorted(header_set)
    profiles = {col: _profile_column(rows, col, policy) for col in headers}
    row_hashes = [_row_key(row, policy) for row in rows]
    return Dataset(rows, headers, profiles, row_hashes, _dataset_hash(row_hashes, headers, policy))


def _extract_rows(value: Any, artifact: ArtifactRecord) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [_object_row(entry, artifact) for entry in value]
    if isinstance(value, dict):
        if isinstance(value.get("rows"), list):
            return [_object_row(entry, artifact) for entry in value["rows"]]
        if len(value) == 0:
            return []
    raise WorkbenchError(
        WorkbenchErrorCode.INVALID_INPUT,
        "reconciliation artifact must contain a JSON array or an object with a rows array",
        {"artifact": artifact.artifact_id},
    )


def _object_row(value: Any, artifact: ArtifactRecord) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, "reconciliation rows must be objects", {"artifact": artifact.artifact_id})
    return value


def _parse_csv(text: str) -> list[list[str]]:
    rows: list[list[str]] = []
    row: list[str] = []
    field = ""
    in_quotes = False
    col_index = 0
    i = 0
    while i < len(text):
        char = text[i]
        if in_quotes:
            if char == '"':
                if i + 1 < len(text) and text[i + 1] == '"':
                    field += '"'
                    i += 2
                    continue
                in_quotes = False
            else:
                field += char
            i += 1
            continue
        if char == '"':
            in_quotes = True
            i += 1
            continue
        if char == ",":
            while len(row) <= col_index:
                row.append("")
            row[col_index] = field
            field = ""
            col_index += 1
            i += 1
            continue
        if char in ("\n", "\r"):
            if char == "\r" and i + 1 < len(text) and text[i + 1] == "\n":
                i += 1
            while len(row) <= col_index:
                row.append("")
            row[col_index] = field
            rows.append(row)
            row = []
            field = ""
            col_index = 0
            i += 1
            continue
        field += char
        i += 1
    if field or row:
        while len(row) <= col_index:
            row.append("")
        row[col_index] = field
        rows.append(row)
    return rows


def _typed_value(value: str) -> Any:
    if value == "":
        return None
    if value.strip().upper() == "NULL":
        return None
    if value == "true":
        return True
    if value == "false":
        return False
    if re.match(r"^-?\d+$", value):
        return int(value)
    if re.match(r"^-?\d*\.\d+$", value):
        return float(value)
    return value


def _json_rows(text: str) -> list[dict[str, Any]]:
    return _extract_rows(json.loads(text), ArtifactRecord(
        artifact_id="json-payload", source_hash="", semantic_hash="",
        semantic_hash_version="", size=0, media_type="", created_by="",
        created_at="", storage_location="", scope_id="",
    ))


def _csv_rows(text: str, headers: list[str]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for values in _parse_csv(text):
        row: dict[str, Any] = {}
        for idx, header in enumerate(headers):
            row[header] = _typed_value(values[idx] if idx < len(values) else "")
        result.append(row)
    return result


class ReconciliationDifference:
    __slots__ = ("kind", "path", "reference", "candidate", "row", "detail")

    def __init__(self, *, kind: str, path: str, reference: Any, candidate: Any, detail: str, row: int | None = None) -> None:
        self.kind = kind
        self.path = path
        self.reference = reference
        self.candidate = candidate
        self.row = row
        self.detail = detail

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"kind": self.kind, "path": self.path, "reference": self.reference, "candidate": self.candidate, "detail": self.detail}
        if self.row is not None:
            d["row"] = self.row
        return d


class ReconciliationSummary:
    __slots__ = ("schema_verdict", "aggregate_verdict", "row_verdict", "statistical_verdict", "certifiable", "difference_count", "differences", "reference_rows", "candidate_rows")

    def __init__(self, *, schema_verdict: Verdict, aggregate_verdict: Verdict, row_verdict: Verdict, statistical_verdict: Verdict, certifiable: bool, difference_count: int, differences: list[ReconciliationDifference], reference_rows: int, candidate_rows: int) -> None:
        self.schema_verdict = schema_verdict
        self.aggregate_verdict = aggregate_verdict
        self.row_verdict = row_verdict
        self.statistical_verdict = statistical_verdict
        self.certifiable = certifiable
        self.difference_count = difference_count
        self.differences = differences
        self.reference_rows = reference_rows
        self.candidate_rows = candidate_rows


class ReconciliationService:
    def __init__(self, artifacts: FileSystemArtifactService, clock: Clock) -> None:
        self._artifacts = artifacts
        self._clock = clock

    def _load_dataset(self, scope: ScopeRef, artifact: ArtifactRecord, policy: ReconciliationPolicy) -> Dataset:
        text = self._artifacts.read_text(scope, artifact)
        if "json" in artifact.media_type:
            return _summarize_dataset(_json_rows(text), policy)
        if "csv" in artifact.media_type:
            raw_rows = _parse_csv(text)
            if not raw_rows:
                return _summarize_dataset([], policy)
            headers = [str(v) for v in raw_rows[0]]
            nl = text.index("\n") + 1 if "\n" in text else len(text)
            return _summarize_dataset(_csv_rows(text[nl:], headers), policy)
        raise WorkbenchError(WorkbenchErrorCode.UNKNOWN_FORMAT, "reconciliation supports JSON or CSV artifacts", {"artifact": artifact.artifact_id, "mediaType": artifact.media_type})

    def compare(self, reference: Dataset, candidate: Dataset, policy: ReconciliationPolicy) -> ReconciliationSummary:
        differences: list[ReconciliationDifference] = []
        schema_verdict = self._compare_schema(reference, candidate, policy, differences)
        aggregate_verdict = self._compare_aggregates(reference, candidate, policy, differences)
        row_verdict = self._compare_rows(reference, candidate, policy, differences)
        statistical_verdict = self._compare_statistics(reference, candidate, policy, differences)
        certifiable = schema_verdict == "PASS" and aggregate_verdict == "PASS" and row_verdict == "PASS" and statistical_verdict != "FAIL"
        return ReconciliationSummary(
            schema_verdict=schema_verdict, aggregate_verdict=aggregate_verdict,
            row_verdict=row_verdict, statistical_verdict=statistical_verdict,
            certifiable=certifiable, difference_count=len(differences),
            differences=differences, reference_rows=len(reference.rows), candidate_rows=len(candidate.rows),
        )

    def _compare_schema(self, reference: Dataset, candidate: Dataset, policy: ReconciliationPolicy, differences: list[ReconciliationDifference]) -> Verdict:
        if policy.schema == "relaxed":
            return "NOT_APPLICABLE"
        if ",".join(reference.headers) != ",".join(candidate.headers):
            differences.append(ReconciliationDifference(kind="schema", path="headers", reference=reference.headers, candidate=candidate.headers, detail="column names or ordering differ"))
            return "FAIL"
        for column in reference.headers:
            ref_profile = reference.profiles.get(column, _empty_profile())
            cand_profile = candidate.profiles.get(column, _empty_profile())
            ref_types = ",".join(sorted(ref_profile.type_counts.keys()))
            cand_types = ",".join(sorted(cand_profile.type_counts.keys()))
            if ref_types != cand_types:
                differences.append(ReconciliationDifference(kind="schema", path=f"columns.{column}.types", reference=ref_types, candidate=cand_types, detail="column value types differ"))
        return "FAIL" if any(d.kind == "schema" for d in differences) else "PASS"

    def _compare_aggregates(self, reference: Dataset, candidate: Dataset, policy: ReconciliationPolicy, differences: list[ReconciliationDifference]) -> Verdict:
        if len(reference.rows) != len(candidate.rows):
            differences.append(ReconciliationDifference(kind="aggregate", path="rows.count", reference=len(reference.rows), candidate=len(candidate.rows), detail="row counts differ"))
        for column in set(reference.headers) | set(candidate.headers):
            ref_profile = reference.profiles.get(column, _empty_profile())
            cand_profile = candidate.profiles.get(column, _empty_profile())
            if ref_profile.null_count != cand_profile.null_count:
                differences.append(ReconciliationDifference(kind="aggregate", path=f"columns.{column}.nullCount", reference=ref_profile.null_count, candidate=cand_profile.null_count, detail="null counts differ"))
            if ref_profile.non_null_count != cand_profile.non_null_count:
                differences.append(ReconciliationDifference(kind="aggregate", path=f"columns.{column}.nonNullCount", reference=ref_profile.non_null_count, candidate=cand_profile.non_null_count, detail="non-null counts differ"))
            if ref_profile.numeric_sum != cand_profile.numeric_sum:
                differences.append(ReconciliationDifference(kind="aggregate", path=f"columns.{column}.numericSum", reference=ref_profile.numeric_sum, candidate=cand_profile.numeric_sum, detail="numeric sums differ"))
            ref_types = sorted(ref_profile.type_counts.items())
            cand_types = sorted(cand_profile.type_counts.items())
            if str(ref_types) != str(cand_types):
                differences.append(ReconciliationDifference(kind="aggregate", path=f"columns.{column}.typeCounts", reference=ref_types, candidate=cand_types, detail="value type distributions differ"))
        return "FAIL" if any(d.kind == "aggregate" for d in differences) else "PASS"

    def _compare_rows(self, reference: Dataset, candidate: Dataset, policy: ReconciliationPolicy, differences: list[ReconciliationDifference]) -> Verdict:
        max_diff = min(policy.max_differences, 1000)
        if policy.row_order == "ordered":
            length = max(len(reference.row_hashes), len(candidate.row_hashes))
            for i in range(length):
                ref_hash = reference.row_hashes[i] if i < len(reference.row_hashes) else None
                cand_hash = candidate.row_hashes[i] if i < len(candidate.row_hashes) else None
                if ref_hash != cand_hash:
                    if len(differences) < max_diff:
                        differences.append(ReconciliationDifference(kind="row", path=f"rows[{i}]", row=i, reference=reference.rows[i] if i < len(reference.rows) else None, candidate=candidate.rows[i] if i < len(candidate.rows) else None, detail="ordered row values differ"))
                    else:
                        differences.append(ReconciliationDifference(kind="row", path=f"rows[{i}]", row=i, reference=None, candidate=None, detail="ordered row values differ"))
            return "FAIL" if any(d.kind == "row" for d in differences) else "PASS"
        ref_counts: dict[str, int] = {}
        ref_first_index: dict[str, int] = {}
        cand_first_index: dict[str, int] = {}
        for idx, h in enumerate(reference.row_hashes):
            ref_counts[h] = ref_counts.get(h, 0) + 1
            if h not in ref_first_index:
                ref_first_index[h] = idx
        for idx, h in enumerate(candidate.row_hashes):
            ref_counts[h] = ref_counts.get(h, 0) - 1
            if h not in cand_first_index:
                cand_first_index[h] = idx
        for h, count in ref_counts.items():
            if count == 0:
                continue
            actual_row = ref_first_index.get(h, cand_first_index.get(h, 0))
            if len(differences) < max_diff:
                differences.append(ReconciliationDifference(kind="row", path=f"rows#{h}", row=actual_row, reference=h if count > 0 else None, candidate=h if count < 0 else None, detail="row multiset membership differs"))
            else:
                differences.append(ReconciliationDifference(kind="row", path=f"rows#{h}", row=actual_row, reference=None, candidate=None, detail="row multiset membership differs"))
        return "FAIL" if any(d.kind == "row" for d in differences) else "PASS"

    def _compare_statistics(self, reference: Dataset, candidate: Dataset, policy: ReconciliationPolicy, differences: list[ReconciliationDifference]) -> Verdict:
        if policy.statistical == "disabled":
            return "NOT_APPLICABLE"
        if reference.profile_hash != candidate.profile_hash:
            differences.append(ReconciliationDifference(kind="statistical", path="profileHash", reference=reference.profile_hash, candidate=candidate.profile_hash, detail="deterministic row profiles differ"))
            return "FAIL"
        return "PASS"
