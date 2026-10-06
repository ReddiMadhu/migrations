# ADR-005: Independent reconciliation

**Status:** Accepted

## Problem
If translation or repair can influence reconciliation semantics, it can certify itself.

## Options
Shared comparison implementation; model-generated checks; independent service/strategy providers under a versioned policy.

## Decision
Reconciliation depends only on materialized reference/candidate artifacts, policy, and execution evidence. It never imports translation, repair, or model code.

## Implementation status
Implemented for JSON/CSV dataset evidence by `ReconciliationService`. It reads artifacts through `ArtifactService`, verifies scope ownership, applies an immutable `ReconciliationPolicy`, writes a report artifact, and appends `reconciliation/completed` or `reconciliation/failed`.

## Why
Independent deterministic verdicts are the core product value.

## Tradeoffs
Canonicalization policy must be shared as a versioned data contract, not shared mutable code that creates a back channel.

## Failure modes
Policy bug, hidden translator-owned metadata, incomplete dataset sampling presented as exact parity.

## Reversal cost
Very high; certification trust depends on this boundary.
