# ADR-009: Isolated, deterministic repair branching

**Status:** Accepted

## Problem
Sequential speculative edits can contaminate prior evidence, repeat failures, and let the model choose its own winner.

## Options
Single mutable workspace; sequential immutable candidates; parallel candidate scopes from a safe prefix.

## Decision
Start with one candidate per iteration. Fork from a safe committed prefix into an isolated candidate scope, execute/reconcile independently, and choose by deterministic policy. Preserve the API for three concurrent candidates.

## Why
It makes hypotheses reproducible, prevents oscillation/cross-branch writes, and keeps repair subordinate to verification.

## Tradeoffs
Storage and compute multiply with branches; lineage and expected-effect comparison are more complex.

## Failure modes
Duplicate semantic patch, oscillation, regression masked by aggregate improvement, branch budget exhaustion.

## Reversal cost
High; sessions, artifacts, approvals, certificates, and UI all encode branch lineage.
