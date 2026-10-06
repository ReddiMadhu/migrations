# ADR-012: Hash-chained evidence

**Status:** Accepted

## Problem
A valid JSON event log can still be modified without detection. Compaction and retention increase the risk.

## Options
Sequence checks only; database permissions; per-event hash chain; external transparency seal/signature.

## Decision
Canonicalize each event, include the previous digest, and compute SHA-256 per event. A certificate references the final root; production adds KMS/HSM-backed sealing or transparency evidence. This is a deliberate workbench extension: the audited DSH session log validates contiguous sequence but does not natively provide this hash chain.

## Why
It detects event mutation and binds model-visible evidence to an exact trajectory.

## Tradeoffs
A hash chain is not itself non-repudiation. Key compromise, truncation, and archive verification need additional controls.

## Failure modes
Canonicalization mismatch, chain reset, fork root ambiguity, unsealed root, compromised signer.

## Reversal cost
High; historical records need a migration/sealing strategy.
