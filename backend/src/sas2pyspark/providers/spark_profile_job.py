"""Databricks-side Excel profiling job; only bounded profile JSON leaves Spark."""

from __future__ import annotations

import hashlib
import json
from typing import Any


def profile_pair(
    spark: Any,
    *,
    reference_path: str,
    candidate_path: str,
    row_order: str = "ordered",
    order_columns: list[str] | None = None,
) -> dict[str, Any]:
    """Profile two workbooks in Spark without collecting full datasets to the driver."""
    reference = spark.read.excel(reference_path, headerRows=1)
    candidate = spark.read.excel(candidate_path, headerRows=1)
    reference_schema = [(field.name, field.dataType.simpleString(), field.nullable) for field in reference.schema.fields]
    candidate_schema = [(field.name, field.dataType.simpleString(), field.nullable) for field in candidate.schema.fields]

    def aggregate(frame: Any) -> dict[str, Any]:
        from pyspark.sql import functions as functions

        expressions = [functions.count(functions.lit(1)).alias("__row_count")]
        for field in frame.schema.fields:
            name = field.name
            expressions.extend([
                functions.sum(functions.when(functions.col(name).isNull(), 1).otherwise(0)).alias(f"{name}__nulls"),
                functions.sum(functions.when(functions.col(name).isNotNull(), 1).otherwise(0)).alias(f"{name}__nonnulls"),
            ])
            if field.dataType.simpleString().startswith(("int", "bigint", "smallint", "decimal", "double", "float")):
                expressions.append(functions.sum(functions.col(name).cast("decimal(38, 12)")).cast("string").alias(f"{name}__sum"))
        return frame.agg(*expressions).first().asDict(recursive=True)

    def row_fingerprint(frame: Any) -> str:
        from pyspark.sql import functions as functions

        columns = sorted(frame.columns)
        canonical_row = functions.to_json(functions.struct(*[functions.col(name).alias(name) for name in columns]), options={"ignoreNullFields": "false"})
        row_hash = functions.sha2(canonical_row, 256)
        if row_order == "unordered":
            hashes = frame.select(row_hash.alias("row_hash"))
            collected = sorted(h for h in hashes.select("row_hash").collect() if h is not None)
            return hashlib.sha256(json.dumps(collected, separators=(",", ":")).encode()).hexdigest()
        else:
            if not order_columns or any(name not in frame.columns for name in order_columns):
                raise ValueError("ordered row profiling requires valid, policy-approved order_columns")
            from pyspark.sql import Window

            ordered_window = Window.orderBy(*[functions.col(name).asc_nulls_first() for name in order_columns])
            indexed = frame.select(
                row_hash.alias("row_hash"),
            ).withColumn("row_position", functions.row_number().over(ordered_window))
            collected = [(r.row_position, r.row_hash) for r in indexed.select("row_position", "row_hash").collect() if r.row_hash is not None]
            collected.sort(key=lambda x: x[0])
            return hashlib.sha256(json.dumps([h for _, h in collected], separators=(",", ":")).encode()).hexdigest()

    ref_aggregate = aggregate(reference)
    cand_aggregate = aggregate(candidate)

    def make_profile(schema: list[tuple[str, str, bool]], aggregates: dict[str, Any], frame: Any) -> dict[str, Any]:
        core = {
            "schema": schema,
            "aggregate": aggregates,
            "rowCount": aggregates.get("__row_count"),
            "rowFingerprint": row_fingerprint(frame),
        }
        core["profileDigest"] = hashlib.sha256(json.dumps(
            core, sort_keys=True, default=str, separators=(",", ":"),
        ).encode()).hexdigest()
        return core

    reference_profile = make_profile(reference_schema, ref_aggregate, reference)
    candidate_profile = make_profile(candidate_schema, cand_aggregate, candidate)
    schema_verdict = "PASS" if reference_profile["schema"] == candidate_profile["schema"] else "FAIL"
    aggregate_verdict = "PASS" if reference_profile["aggregate"] == candidate_profile["aggregate"] else "FAIL"
    row_verdict = "PASS" if reference_profile["rowFingerprint"] == candidate_profile["rowFingerprint"] else "FAIL"
    statistical_verdict = "PASS" if reference_profile["profileDigest"] == candidate_profile["profileDigest"] else "FAIL"
    certifiable = all(verdict == "PASS" for verdict in (
        schema_verdict, aggregate_verdict, row_verdict, statistical_verdict,
    ))
    differences = []
    for dimension, passed in (
        ("schema", schema_verdict == "PASS"),
        ("aggregate", aggregate_verdict == "PASS"),
        ("row", row_verdict == "PASS"),
        ("statistical", statistical_verdict == "PASS"),
    ):
        if not passed:
            differences.append({"kind": dimension, "path": f"profiles.{dimension}", "detail": f"{dimension} profiles differ"})
    return {
        "referenceProfile": reference_profile,
        "candidateProfile": candidate_profile,
        "rowOrder": row_order,
        "schemaVerdict": schema_verdict,
        "aggregateVerdict": aggregate_verdict,
        "rowVerdict": row_verdict,
        "statisticalVerdict": statistical_verdict,
        "differenceCount": len(differences),
        "differences": differences,
        "certifiable": certifiable,
    }