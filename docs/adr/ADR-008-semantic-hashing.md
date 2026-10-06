# ADR-008: Exact and semantic hashes are distinct

**Status:** Accepted

## Problem
Whitespace/comments/formatter changes may preserve semantics, while source, input, policy, and environment changes may alter certification.

## Options
One hash; normalized text; language-aware semantic canonicalization; explicit exact and semantic identities.

## Decision
Store `source_hash = sha256(bytes)` and `semantic_hash = sha256(versioned canonical form)`. Never equate them. Approval normally binds both plus policy/input/environment.

## Why
Audit needs exact bytes while approval and repair guards need semantic comparison.

## Tradeoffs
Canonicalization can be wrong. It must be versioned, domain-specific, conservative, and mutation-tested; unsupported constructs fail closed.

## Failure modes
Over-normalization, under-normalization, stale canonicalizer version, parser ambiguity.

## Reversal cost
Moderate; stored exact hashes preserve migration options, but existing approval bindings need reevaluation.
