# ADR-016: Deterministic reconciliation policy and canonical evidence

**Status:** Accepted

## Problem
Floating-point formatting, null spellings, column order, row order, and differing policy versions can make semantically equivalent outputs appear different—or worse, make different outputs appear equivalent.

## Options
Locale-aware fuzzy matching; SQL-side approximate joins; exact byte comparison only; an immutable reconciliation policy plus deterministic canonicalization and decimal arithmetic.

## Decision
Bind every reconciliation to an immutable `ReconciliationPolicy` and its canonical fingerprint. Compare JSON/CSV evidence using:

- exact schema names/order/type profiles,
- deterministic null, non-null, type, and decimal-sum aggregates,
- canonical row keys under ordered or unordered policy,
- a deterministic statistical profile that is advisory unless policy disables it,
- a bounded difference report stored as an immutable artifact.

Numeric sums use decimal-string arithmetic with optional trailing-zero normalization. `1.10` and `1.1` compare equal only under the configured normalization policy.

## Why
The verdict must be reproducible by replay and auditable against the exact policy version. Approximate floating-point equality and implicit locale normalization are false-parity risks.

## Tradeoffs
The policy is deliberately conservative: unsupported formats fail closed, statistical profiles are deterministic summaries rather than probabilistic guarantees, and large differences are bounded by policy.

## Failure modes
Over-normalization, under-normalization, unsupported encoding, unbounded report growth, policy-version drift between components.

## Reversal cost
Moderate; the report and event schema can carry a new policy version, but historical verdicts remain bound to their original fingerprint.
