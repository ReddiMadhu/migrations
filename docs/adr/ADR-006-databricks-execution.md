# ADR-006: Databricks Lakeflow Job execution

**Status:** Accepted

## Problem
The App/control process must not execute generated PySpark or stream large datasets through its memory.

## Options
Local `exec`; App-side Spark; saved Lakeflow Job; one-time Jobs API submission.

## Decision
Use a pinned saved Lakeflow Job in production and one-time submission for controlled development. Store artifacts in governed storage and monitor via the current `status` representation.

## Why
It provides actual Spark isolation, compute, logs, UC governance, and asynchronous control-plane operation.

## Tradeoffs
Provider latency, quota/configuration, remote job identity, and crash recovery complexity. One-time runs lack UI and automatic retries.

## Failure modes
Partial success, unknown state, duplicate submission, missing output, permission drift.

## Reversal cost
Moderate: the provider contract can target local/container Spark, but the deployment path changes.
