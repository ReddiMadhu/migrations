# ADR-014: Code mode is diagnostic-only

**Status:** Accepted

## Problem
Programmatic tool use improves analysis but obscures individual side effects if used as the primary repair mechanism.

## Options
No code mode; unrestricted generated programs; bounded read-only/idempotent diagnostic programs.

## Decision
Allow programmatic tool use for read-only investigation, query composition, and counterexample exploration. Every visible side effect still crosses an individually guarded capability call. Repairs are artifact/patch proposals, not arbitrary program side effects. Program-level PTC escalation widens the program's direct-effect world only; nested tools retain their own policy and approval checks.

## Why
It separates diagnostic composability from repair authority and preserves auditability.

## Tradeoffs
More explicit tool declarations and less expressive one-shot repair. Diagnostic runtime isolation must still be real. Node PTC's managed-process budgets and Python PTC's bounded process controls are provider facts, not evidence of a universal filesystem/network boundary.

## Failure modes
False claim that resource limits equal sandboxing, hidden network/filesystem access, oversized result, non-idempotent diagnostic.

## Reversal cost Low; the PTC service is replaceable and isolated to diagnostics.
