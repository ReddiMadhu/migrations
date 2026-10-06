# ADR-004: Guarded canonical tool middleware

**Status:** Accepted

## Problem
Providers, models, and plugins can otherwise disagree about whether an action ran or what result is authoritative.

## Options
Direct provider calls; model-authored tool protocol; a runtime with immutable arguments, policy, approval, deny-only guards, around middleware, schema validation, and one canonical result.

## Decision
All side effects cross a tool/service seam. The runtime emits one frozen canonical result and a durable execution event. The workbench's public seam is the guarded runtime contract; any scheduler implementation is an internal composition concern, not a model-selected provider or a general plugin authority.

## Why
The model proposes an invocation but cannot invent success, alter frozen evidence, bypass policy, or select a provider directly.

## Tradeoffs
Middleware ordering and ownership must be explicit; cancellation and timeout semantics require careful tests.

## Failure modes
Missing guard, unsafe waterfall, provider result normalization gap, event-before-side-effect ordering bug.

## Reversal cost
High; this is the main safety boundary.
