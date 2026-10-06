# ADR-003: Hierarchical, non-widenable scopes

**Status:** Accepted

## Problem
Repair candidates can leak state, permissions, output paths, and authority into siblings or parent sessions.

## Options
Flat labels; filesystem permissions alone; distributed locks; Cordis child contexts plus opaque domain scope chains.

## Decision
Use `Tenant → Migration → Attempt → Candidate → Execution`. A child context inherits registrations and evidence, may narrow policy, and emits to ancestors but never siblings. Candidate/execution boundaries use isolated executor labels by default; other boundaries can request explicit isolation. Domain listeners use `ScopeService.on(...)` so admission remains enforceable even when a Cordis trace proxy does not retain the original listener context.

## Why
The invariant directly maps to candidate isolation and branch observability.

## Tradeoffs
Scope lifecycle must be explicit. Distributed enforcement still requires storage/lease controls.

## Failure modes
Incorrect ancestry, accidental provider isolation leakage, mutable policy after branch creation.

## Reversal cost
High; fork lineage, approvals, artifact paths, and replay use scope IDs.
